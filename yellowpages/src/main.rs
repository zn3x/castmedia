mod config;
mod state;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use castmedia::{broadcast::metadata_decode, http::{ChunkedResponseReader, HttpClient}, relay::MountUpdate, source::IcyProperties, utils::basic_auth};
use config::{Config, Directory};
use hashbrown::HashMap;
use qanat::{broadcast, mpsc};
use reqwest::Client;
use state::{DirProperties, State, StreamState, Update};
use tracing::{error, info};
use url::Url;


async fn server_mountupdates_listener(config: Arc<Config>) -> Result<()> {
    let state;
    let auth;
    {
        state = match std::fs::read_to_string(&config.state) {
            Ok(v) => serde_json::from_str::<HashMap<String, StreamState>>(&v)?,
            Err(_) => {
                info!("State file couldn't be read, starting a new state");
                HashMap::new()
            }
        };

        let user = match config.server.username() {
            "" => return Err(anyhow::anyhow!("No authentication username supplied for server")),
            u => u
        };
        let pass = match config.server.password() {
            Some(v) => v,
            None => return Err(anyhow::anyhow!("No authentication password supplied for server")),
        };
        auth = format!("Authorization: Basic {}", basic_auth(user, pass));
    }

    let (tx, rx) = mpsc::unbounded_channel();
    let mut state = State {
        state,
        channel: HashMap::new(),
        update_ch: rx,
        update_ch_tx: tx
    };

    let client = reqwest::Client::new();

    loop {
        if let Err(e) = server_mountupdates_listener_inner(&config, &mut state, &client, &auth).await {
            error!("Mountupdates listener failed: {e}");
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    };
}

async fn server_mountupdates_listener_inner(config: &Arc<Config>, state: &mut State, cl: &Client, auth: &str) -> Result<()> {
    let mut stream = tokio::time::timeout(Duration::from_millis(config.timeout), async move {
        let mut client = HttpClient::connect(&config.server, "/admin/mountupdates", 8192).await?;
        client.add_header(auth);
        let mut reader = client.get().await?;

        let headers_buf = reader.read_headers().await?;
        // Parsing response headers
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut resp    = httparse::Response::new(&mut headers);

        match resp.parse(&headers_buf) {
            Ok(httparse::Status::Complete(_)) => {},
            Ok(httparse::Status::Partial) => return Err(anyhow::Error::msg("Incomplete response")),
            Err(e) => return Err(e.into())
        };

        if !resp.code.is_some_and(|c| c == 200) {
            return Err(anyhow::Error::msg("Unexpected response status code"));
        }

        Ok(reader.get_inner_stream())
    }).await??;

    let mut chunked = ChunkedResponseReader::new();
    let mut len_enc = [0u8; 4];
    let mut buf     = Vec::new();

    loop {
        tokio::select! {
            _ = chunked.read_exact(&mut stream, &mut len_enc) => {
                let len = u32::from_be_bytes(len_enc) as usize;
                if len > buf.len() {
                    buf.resize(len, 0);
                }
                chunked.read_exact(&mut stream, &mut buf[..len]).await?;

                let event: MountUpdate = serde_json::from_slice(&buf[..len])?;

                mountupdates_event_reader(config, state, cl, event).await;
            },
            r = state.update_ch.recv() => match r {
                Ok(Update::Sid { mount, dir, sid, touch_freq }) => {
                    if let Some(v) = state.state.get_mut(&mount) {
                        match v.dirs.get_mut(&dir) {
                            Some(v) => {
                                v.touch_freq = touch_freq;
                                v.sid        = sid;
                            },
                            None => {
                                v.dirs.insert(
                                    dir,
                                    DirProperties { sid, touch_freq }
                                );
                            }
                        }
                    }
                    if state.update_ch.len() > 0 {
                        continue;
                    }

                    persist_state(config, state).await;
                },
                Err(_) => unreachable!()
            }
        }
    };
}

async fn persist_state(config: &Config, state: &State) {
    match serde_json::to_vec(&state.state) {
        Ok(v) => {
            if let Err(e) = tokio::fs::write(&config.state, &v).await {
                error!("Failed saving state: {e}");
            }
        },
        Err(e) => {
            error!("Failed serializing state: {e}");
        }
    }
}

async fn mountupdates_event_reader(config: &Arc<Config>, state: &mut State, client: &Client, event: MountUpdate) {
    match event {
        MountUpdate::New { mount, properties } => {
            match state.state.get_mut(&mount) {
                Some(v) => if !v.properties.eq(&properties) {
                    // We must have skipped when stream was unmounted
                    v.properties = properties.clone();
                } else if state.channel.contains_key(&mount) {
                    // No need to retransmit when channel is identic
                    return;
                },
                None => {
                    _ = state.state.insert(mount.clone(), StreamState { properties: properties.clone(), dirs: HashMap::new() });
                }
            }

            let mut tx = match state.channel.get(&mount) {
                None => {
                    let (tx1, rx) = broadcast::channel(10.try_into().expect("Should work"));

                    config.directories.iter()
                        .for_each(|dir| {
                            let conf = config.clone();
                            let st   = state.state.get(&mount).cloned();
                            let url  = dir.yp_url.clone();
                            let tx   = state.update_ch_tx.clone();
                            let rxc  = rx.clone();
                            let cl   = client.clone();
                            let m    = mount.clone();
                            tokio::spawn(async move {
                                stream_update_task(conf, cl, url, m, st, rxc, tx).await;
                            });
                            state.channel.insert(mount.clone(), state::Channel { tx: tx1.clone() });
                        });

                    tx1
                },
                Some(v) => v.tx.clone()
            };

            tx.send(Arc::new(MountUpdate::New { mount, properties }));
        },
        MountUpdate::Metadata { mount, metadata } => if let Some(v) = state.channel.get_mut(&mount) {
            v.tx.send(Arc::new(MountUpdate::Metadata { mount, metadata }));
        },
        MountUpdate::Unmounted { mount } => if let Some(mut v) = state.channel.remove(&mount) {
            _ = state.state.remove(&mount);
            v.tx.send(Arc::new(MountUpdate::Unmounted { mount }));
            persist_state(config, state).await;
        },
        MountUpdate::Heartbeat => ()
    }
}

async fn stream_update_task(config: Arc<Config>, client: Client, dir: Url, mount: String, state: Option<StreamState>,
                            mut rx: broadcast::Receiver<Arc<MountUpdate>>, mut tx: mpsc::UnboundedSender<Update>) {
    let conf = config.directories.iter()
        .find(|x| x.yp_url.eq(&dir))
        .expect("Should find YP url in config");
    let (mut sid, mut tick) = match state.as_ref().and_then(|s| s.dirs.get(&dir).cloned()) {
        Some(v) => (Some(v.sid), tokio::time::interval(Duration::from_secs((v.touch_freq * 3) / 4))),
        None => {
            let mut tick = tokio::time::interval(Duration::from_secs(10000));
            tick.reset();
            (None, tick)
        }
    };

    loop {
        tokio::select! {
            r = rx.recv() => match r {
                Ok(v) => match v.as_ref() {
                    MountUpdate::New { properties, .. } => if sid.is_none() { add_action(&config, conf, &client, &dir, &mount, properties, &mut sid, &mut tick, &mut tx).await },
                    MountUpdate::Metadata { metadata, .. } => touch_action(&config, conf, &client, &dir, &mount, &state, Some(metadata), &mut sid, &mut tick, &mut tx).await,
                    MountUpdate::Unmounted { .. } => {
                        remove_action(conf, &client, &dir, &mount, &sid, &mut tick).await;
                        return;
                    },
                    _ => unreachable!()
                },
                Err(_) => break
            },
            _ = tick.tick() => if sid.is_some() {
                touch_action(&config, conf, &client, &dir, &mount, &state, None, &mut sid, &mut tick, &mut tx).await;
            }
        }
    };
}

#[inline(always)]
async fn remove_action(conf: &Directory, client: &Client, dir: &Url, mount: &str,
                       sid: &Option<String>, tick: &mut tokio::time::Interval) {
    let sid = match sid {
        Some(v) => v.to_owned(),
        None => return
    };

    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "remove".to_owned());
    form.insert("sid", sid);

    for _ in 0..10 {
        match post_req(conf, client, dir, &form).await {
            PostReqStatus::Ok | PostReqStatus::WrongResp => {
                tick.reset();
                info!("Stream {mount} on {dir} was removed");
                break
            },
            PostReqStatus::Unreachable => {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
        }
    }
}

#[inline(always)]
async fn touch_action(config: &Config, conf: &Directory, client: &Client, dir: &Url,
                      mount: &str, state: &Option<StreamState>, metadata: Option<&Vec<u8>>,
                      sid: &mut Option<String>, tick: &mut tokio::time::Interval,
                      tx: &mut mpsc::UnboundedSender<Update>) {
    let mut sidu = match sid {
        Some(v) => v.to_owned(),
        None => return
    };

    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "touch".to_owned());
    form.insert("sid", sidu);
    if let Some((Some(title), Some(_))) = metadata.and_then(|m| std::str::from_utf8(&m[1..]).ok().and_then(|m| metadata_decode(m).ok())) {
        form.insert("st", title);
    }

    for _ in 0..3 {
        match post_req(conf, client, dir, &form).await {
            PostReqStatus::Ok => {
                tick.reset();
                info!("Stream {mount} on {dir} was touched");
                break
            },
            PostReqStatus::WrongResp => {
                // Do we need to register again?
                if let Some(properties) = state.as_ref().map(|s| s.properties.clone()) {
                    add_action(config, conf, client, dir, mount, &properties, sid, tick, tx).await;
                    if let Some(v) = sid.as_ref() {
                        sidu = v.clone();
                        form.insert("sid", sidu);
                    }
                }
            },
            PostReqStatus::Unreachable => tokio::time::sleep(Duration::from_secs(20)).await
        }
    }
}


enum PostReqStatus {
    Ok,
    Unreachable,
    WrongResp
}

async fn post_req(conf: &Directory, client: &Client, dir: &Url, form: &HashMap<&str, String>) -> PostReqStatus {
    let resp = client.post(dir.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&form)
        .timeout(Duration::from_secs(conf.timeout))
        .send()
        .await;
    match resp {
        Ok(v) => {
            let headers = v.headers();
            match (
                headers.get("ypresponse").and_then(|x| x.to_str().ok()),
                headers.get("ypmessage").and_then(|x| x.to_str().ok())
            ) {
                (Some(v), Some(e)) => match v.eq("1") {
                    true => PostReqStatus::Ok,
                    false => {
                        error!("{dir} not accepting request due to: {e}");
                        PostReqStatus::WrongResp
                    }
                },
                _ => {
                    error!("{dir} did not return valid reply headers");
                    PostReqStatus::WrongResp
                }
            }
        },
        Err(e) => {
            error!("Can't contact {dir}: {e}");
            PostReqStatus::Unreachable
        }
    }
}

#[inline(always)]
async fn add_action(config: &Config, conf: &Directory, client: &Client, dir: &Url,
                    mount: &str, properties: &IcyProperties,
                    sid: &mut Option<String>, tick: &mut tokio::time::Interval,
                    tx: &mut mpsc::UnboundedSender<Update>) {
    let listenurl_path = match config.public_server.path_segments() {
        Some(v) => {
            let mut v = v.collect::<Vec<&str>>();
            if v.last().is_some_and(|x| (*x).eq("")) {
                v.pop();
            }
            v.push(&mount[1..]);
            v
        },
        None => vec![ &mount[1..] ]
    };
    let mut listenurl = config.public_server.clone();
    listenurl.set_path(&listenurl_path.join("/"));
    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "add".to_owned());
    form.insert("sn", properties.name.clone().unwrap_or_default());
    form.insert("type", properties.content_type.clone());
    form.insert("genre", properties.genre.clone().unwrap_or_default());
    form.insert("b", properties.bitrate.clone().unwrap_or_default());
    if let Some(desc) = &properties.description {
        form.insert("desc", desc.clone());
    }
    form.insert("url", config.url.to_string());
    form.insert("listenurl", listenurl.to_string());
    for _ in 0..10 {
        let resp = client.post(dir.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .form(&form)
            .timeout(Duration::from_secs(conf.timeout))
            .send()
            .await;
        match resp {
            Ok(v) => {
                let headers = v.headers();
                match (
                    headers.get("ypresponse").and_then(|x| x.to_str().ok()),
                    headers.get("ypmessage").and_then(|x| x.to_str().ok()),
                    headers.get("sid").and_then(|x| x.to_str().ok()),
                    headers.get("touchfreq").and_then(|x| x.to_str().ok().and_then(|x| x.parse::<u64>().ok()))
                ) {
                    (Some(_), Some(e), None, None) => {
                        error!("{dir} not accepting Add request due to: {e}");
                    },
                    (Some(v), Some(e), Some(new_sid), Some(touch_freq)) => match v.eq("1") {
                        true => {
                            *sid  = Some(new_sid.to_string());
                            *tick = tokio::time::interval(Duration::from_secs((touch_freq * 3) / 4));
                            _     = tx.send(Update::Sid {
                                mount: mount.to_string(),
                                dir: dir.clone(),
                                sid: new_sid.to_string(),
                                touch_freq
                            });
                            info!("Stream {mount} added to {dir}");
                            break;
                        },
                        false => {
                            error!("{dir} not accepting Add request due to: {e}");
                        }
                    },
                    _ => {
                        error!("{dir} did not return valid reply headers for Add");
                    }
                }
            },
            Err(e) => {
                error!("Can't contact {dir}: {e}");
            }
        }
        tokio::time::sleep(Duration::from_secs(20)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::INFO).init();

    let config_path;
    {
        let mut args = std::env::args().collect::<Vec<String>>();
        match args.pop() {
            Some(v) => config_path = v,
            None => {
                error!("Configuration file argument is missing");
                std::process::exit(1);
            }
        }
    }

    let config = Config::load(&config_path);

    server_mountupdates_listener(Arc::new(config)).await
}
