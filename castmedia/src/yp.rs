use std::{path::PathBuf, sync::Arc};
use hashbrown::HashMap;
use qanat::broadcast::{Receiver, RecvError};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant};
use tracing::{error, info};
use url::Url;

use crate::{
    config::{YP, YPDirectory},
    internal_api::v1::IcyProperties,
    server::Server,
    source::MetadataMsg
};

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamState {
    pub properties: IcyProperties,
    pub dirs: HashMap<Url, DirState>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DirState {
    pub sid: Option<String>,
    pub touch_freq: u64,
    /// Recalculated from touch_freq when a persisted state is loaded
    #[serde(default = "Instant::now", skip_serializing, skip_deserializing)]
    pub next_touch: Instant
}

fn touch_interval(touch_freq: u64) -> Duration {
    Duration::from_secs((touch_freq * 3) / 4).max(Duration::from_secs(1))
}

fn state_file(yp: &YP, mount: &str) -> PathBuf {
    yp.state.join(format!("{}.json", mount.trim_start_matches('/').replace('/', "_")))
}

/// Event loop handling a single mounted stream against every configured YP directory.
///
/// It subscribes to the mount metadata broadcast channel: every metadata update is
/// pushed to the directories with a touch, and a closed channel means the mount was
/// unmounted and its listing should be removed. Persisted stream state is kept in the
/// yellow pages state directory and removed once the mount is unmounted.
pub async fn mount_events(
    server: Arc<Server>,
    mount: String,
    properties: IcyProperties,
    mut metadata_rx: Receiver<Arc<MetadataMsg>>,
) {
    let yp = match &server.config.yellow_pages {
        Some(v) if v.enabled => v,
        _ => return
    };
    let state_path = state_file(yp, &mount);
    let mut state: Option<StreamState> = match tokio::fs::read_to_string(&state_path).await {
        Ok(v) => match serde_json::from_str(&v) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Failed parsing state file {}: {e}", state_path.display());
                None
            }
        },
        Err(_) => None
    };
    let client = reqwest::Client::new();

    let mut dirs: HashMap<Url, DirState> = HashMap::new();
    for directory in &yp.directories {
        // Persisted state means the stream was already registered (e.g. after migration),
        // we schedule the next touch from the persisted touch_freq
        let mut ds = match state.as_mut().and_then(|s| s.dirs.get_mut(&directory.yp_url)) {
            Some(v) => {
                v.next_touch = Instant::now() + touch_interval(v.touch_freq);
                v.clone()
            },
            None => DirState {
                sid: None,
                touch_freq: 0,
                next_touch: Instant::now() + Duration::from_secs(10000)
            }
        };

        if ds.sid.is_none() {
            add_action(&server, directory, &client, &directory.yp_url, &mount, &properties, &mut ds, &mut state, &state_path).await;
        }

        dirs.insert(directory.yp_url.clone(), ds);
    }

    let mut metadata_ref = None;
    loop {
        let next_touch = dirs.values()
            .filter(|d| d.sid.is_some())
            .map(|d| d.next_touch)
            .min();

        tokio::select! {
            r = metadata_rx.recv() => match r {
                Ok(v) => {
                    metadata_ref = Some(v.obj.title.clone());
                    for (dir, ds) in &mut dirs {
                        if ds.sid.is_some() {
                            let directory = yp.directories.iter()
                                .find(|x| x.yp_url.eq(dir))
                                .expect("Should find YP url in config");
                            touch_action(&server, directory, &client, dir, &mount, &properties, metadata_ref.as_ref(), ds, &mut state, &state_path).await;
                            ds.next_touch = Instant::now() + touch_interval(ds.touch_freq);
                        }
                    }
                },
                Err(RecvError::Lagged) => continue,
                Err(RecvError::Closed) => {
                    for (dir, ds) in &mut dirs {
                        if ds.sid.is_some() {
                            let directory = yp.directories.iter()
                                .find(|x| x.yp_url.eq(dir))
                                .expect("Should find YP url in config");
                            remove_action(directory, &client, dir, &mount, &ds.sid).await;
                        }
                    }
                    _ = tokio::fs::remove_file(&state_path).await;
                    break;
                }
            },
            _ = async {
                match next_touch {
                    Some(v) => tokio::time::sleep_until(v).await,
                    None => std::future::pending().await
                }
            } => {
                let now = Instant::now();
                for (dir, ds) in &mut dirs {
                    if ds.sid.is_some() && ds.next_touch <= now {
                        let directory = yp.directories.iter()
                            .find(|x| x.yp_url.eq(dir))
                            .expect("Should find YP url in config");
                        touch_action(&server, directory, &client, dir, &mount, &properties, metadata_ref.as_ref(), ds, &mut state, &state_path).await;
                        ds.next_touch = Instant::now() + touch_interval(ds.touch_freq);
                    }
                }
            }
        }
    }
}

async fn add_action(server: &Server, directory: &YPDirectory, client: &Client,
                    dir: &Url, mount: &str, properties: &IcyProperties,
                    ds: &mut DirState, state: &mut Option<StreamState>,
                    state_path: &PathBuf) {
    let yp = server.config.yellow_pages.as_ref()
        .expect("Should have yellow pages config");

    let listenurl_path = match yp.public_server.path_segments() {
        Some(v) => {
            let mut v = v.collect::<Vec<&str>>();
            if v.last().is_some_and(|x| x.is_empty()) {
                v.pop();
            }
            v.push(&mount[1..]);
            v
        },
        None => vec![ &mount[1..] ]
    };
    let mut listenurl = yp.public_server.clone();
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
    form.insert("url", yp.url.to_string());
    form.insert("listenurl", listenurl.to_string());

    for _ in 0..10 {
        let resp = client.post(dir.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .form(&form)
            .timeout(Duration::from_secs(directory.timeout))
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
                            ds.sid        = Some(new_sid.to_string());
                            ds.touch_freq = touch_freq;
                            ds.next_touch = Instant::now() + touch_interval(touch_freq);
                            let st = state.get_or_insert_with(|| StreamState {
                                properties: properties.clone(),
                                dirs: HashMap::new()
                            });
                            st.dirs.insert(dir.clone(), ds.clone());
                            persist_state(state_path, st).await;
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

async fn touch_action(server: &Server, directory: &YPDirectory, client: &Client,
                      dir: &Url, mount: &str, properties: &IcyProperties,
                      metadata: Option<&String>, ds: &mut DirState,
                      state: &mut Option<StreamState>, state_path: &PathBuf) {
    let mut sid = match &ds.sid {
        Some(v) => v.to_owned(),
        None => return
    };

    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "touch".to_owned());
    form.insert("sid", sid.clone());
    if let Some(title) = metadata {
        form.insert("st", title.to_string());
    }

    for _ in 0..3 {
        match post_req(directory, client, dir, &form).await {
            PostReqStatus::Ok => {
                info!("Stream {mount} on {dir} was touched");
                break;
            },
            PostReqStatus::WrongResp => {
                add_action(server, directory, client, dir, mount, properties, ds, state, state_path).await;
                if let Some(v) = &ds.sid {
                    sid = v.clone();
                    form.insert("sid", sid.clone());
                }
            },
            PostReqStatus::Unreachable => tokio::time::sleep(Duration::from_secs(20)).await
        }
    }
}

async fn persist_state(state_path: &PathBuf, state: &StreamState) {
    match serde_json::to_vec(state) {
        Ok(v) => {
            if let Err(e) = tokio::fs::write(state_path, &v).await {
                error!("Failed saving state: {e}");
            }
        },
        Err(e) => {
            error!("Failed serializing state: {e}");
        }
    }
}

async fn remove_action(directory: &YPDirectory, client: &Client, dir: &Url,
                       mount: &str, sid: &Option<String>) {
    let sid = match sid {
        Some(v) => v.to_owned(),
        None => return
    };

    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "remove".to_owned());
    form.insert("sid", sid);

    for _ in 0..10 {
        match post_req(directory, client, dir, &form).await {
            PostReqStatus::Ok | PostReqStatus::WrongResp => {
                info!("Stream {mount} on {dir} was removed");
                break;
            },
            PostReqStatus::Unreachable => {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
        }
    }
}

enum PostReqStatus {
    Ok,
    Unreachable,
    WrongResp
}

async fn post_req(directory: &YPDirectory, client: &Client, dir: &Url,
                  form: &HashMap<&str, String>) -> PostReqStatus {
    let resp = client.post(dir.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&form)
        .timeout(Duration::from_secs(directory.timeout))
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
