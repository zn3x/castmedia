use std::{path::PathBuf, sync::Arc};
use hashbrown::HashMap;
use qanat::broadcast::{Receiver, RecvError};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant};
use tracing::{error, info};
use url::Url;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

use crate::{
    config::{YP, YPDirectory},
    internal_api::v1::IcyProperties,
    server::Server,
    source::MetadataMsg, stream::BroadcastInfo
};

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamState {
    pub dirs: HashMap<Url, DirState>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DirState {
    pub sid: Option<String>,
    pub touch_freq: u64,
    /// Unix timestamp of the last successful registration, used to detect a
    /// listing that likely expired while the process was down
    #[serde(default)]
    pub last_updated: u64,
    /// Recalculated from touch_freq when a persisted state is loaded
    #[serde(default = "Instant::now", skip_serializing, skip_deserializing)]
    pub next_touch: Instant
}

fn touch_interval(touch_freq: u64) -> Duration {
    Duration::from_secs((touch_freq * 3) / 4).max(Duration::from_secs(1))
}

/// Period between re-add attempts when a directory couldn't be reached or
/// rejected the registration of a stream
const ADD_RETRY_INTERVAL: Duration = Duration::from_secs(60);

/// A persisted registration is re-registered when the stream was silent for at
/// least two touch intervals, or at least this long
const STALE_FLOOR: Duration = Duration::from_secs(30);

/// Removal attempts before giving up on an unreachable directory
const REMOVE_ATTEMPTS: usize = 3;

/// Sleep between removal retries
const REMOVE_RETRY_SLEEP: Duration = Duration::from_secs(5);

/// Normalize a YP directory url so the persisted state uses a stable key
/// regardless of how the config string is written (e.g. a trailing slash)
fn canonical_yp_url(url: &Url) -> Url {
    let mut url = url.clone();
    if url.path().ends_with('/') {
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.pop_if_empty();
        }
    }
    url
}

fn state_file(yp: &YP, mount: &str) -> PathBuf {
    yp.state.join(format!("{}.json", URL_SAFE_NO_PAD.encode(mount)))
}

pub async fn start_mount_events(s: &BroadcastInfo<'_>) {
    if s.session.server.config.yellow_pages.as_ref().is_some_and(|x| x.enabled) {
        let mut ctx = None;
        {
            let sources = s.session.server.sources.read().await;
            let source  = sources.get(s.mountpoint);
            if let Some(source) = source {
                ctx = Some((source.meta_broadcast.clone(), source.properties.clone()));
            }
        }
        let server = s.session.server.clone();
        let mount  = s.mountpoint.to_string();
        if let Some((metadata_rx, properties)) = ctx {
            tokio::spawn(async move {
                crate::yp::mount_events(server, mount, properties, metadata_rx).await;
            });
        }
    }
}

/// Event loop handling a single mounted stream against every configured YP directory.
///
/// It subscribes to the mount metadata broadcast channel: every metadata update is
/// pushed to the directories with a touch, and a closed channel means the mount was
/// unmounted and its listing should be removed. Persisted stream state is kept in the
/// yellow pages state directory and removed once the mount is unmounted.
async fn mount_events(
    server: Arc<Server>,
    mount: String,
    properties: Arc<IcyProperties>,
    mut metadata_rx: Receiver<Arc<MetadataMsg>>,
) {
    let yp = match &server.config.yellow_pages {
        Some(v) if v.enabled => v,
        _ => return
    };
    let state_path = state_file(yp, &mount);
    let mut state: Option<StreamState> = match tokio::fs::read_to_string(&state_path).await {
        Ok(v) => match serde_json::from_str::<StreamState>(&v) {
            Ok(mut v) => {
                // Normalize persisted keys so a config rewrite (e.g. a trailing
                // slash) still matches the registration
                let dirs = std::mem::take(&mut v.dirs);
                for (k, ds) in dirs {
                    v.dirs.insert(canonical_yp_url(&k), ds);
                }
                Some(v)
            },
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
        // we schedule the next touch from the persisted touch_freq. A registration older
        // than the directory's expiry is dropped and re-registered on the first loop iteration.
        let url = canonical_yp_url(&directory.yp_url);
        let ds = match state.as_mut().and_then(|s| s.dirs.get(&url)) {
            Some(v) => {
                let mut v = v.clone();
                let age = Duration::from_secs((chrono::offset::Utc::now().timestamp() as u64).saturating_sub(v.last_updated));
                let stale_after = touch_interval(v.touch_freq) * 2;
                if v.sid.is_some() && age > stale_after.max(STALE_FLOOR) {
                    v.sid = None;
                }
                v.next_touch = if v.sid.is_some() {
                    Instant::now() + touch_interval(v.touch_freq)
                } else {
                    Instant::now()
                };
                v
            },
            None => DirState {
                sid: None,
                touch_freq: 0,
                last_updated: 0,
                next_touch: Instant::now()
            }
        };
        dirs.insert(url, ds);
    }

    if dirs.values().any(|d| d.sid.is_some()) {
        persist_dirs(&mut state, &dirs, &state_path).await;
    }

    let mut metadata_ref = None;
    loop {
        let next_touch = dirs.values()
            .map(|d| d.next_touch)
            .min();

        tokio::select! {
            r = metadata_rx.recv() => match r {
                Ok(v) => {
                    metadata_ref = Some(v.obj.title.clone());
                    let changed = touch_all(server.clone(), yp, &mut dirs, &client, &mount, &properties, metadata_ref.clone(), None).await;
                    if changed {
                        persist_dirs(&mut state, &dirs, &state_path).await;
                    }
                },
                Err(RecvError::Lagged) => continue,
                Err(RecvError::Closed) => {
                    let mut tasks = Vec::new();
                    for (dir, ds) in std::mem::take(&mut dirs) {
                        if let Some(sid) = ds.sid {
                            let directory = yp.directories.iter()
                                .find(|x| canonical_yp_url(&x.yp_url).eq(&dir))
                                .expect("Should find YP url in config")
                                .clone();
                            let client = client.clone();
                            let mount = mount.clone();
                            tasks.push(tokio::spawn(async move {
                                remove_action(&directory, &client, &dir, &mount, &Some(sid)).await;
                            }));
                        }
                    }
                    for task in tasks {
                        if let Err(e) = task.await {
                            error!("Remove task failed: {e}");
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
                let changed = touch_all(server.clone(), yp, &mut dirs, &client, &mount, &properties, metadata_ref.clone(), Some(now)).await;
                if changed {
                    persist_dirs(&mut state, &dirs, &state_path).await;
                }
            }
        }
    }
}

async fn add_action(server: &Server, directory: &YPDirectory, client: &Client,
                    mount: &str, properties: &IcyProperties, ds: &mut DirState) {
    let yp = server.config.yellow_pages.as_ref()
        .expect("Should have yellow pages config");

    let mount_path = mount.strip_prefix('/').unwrap_or(mount);
    let listenurl_path = match yp.public_server.path_segments() {
        Some(v) => {
            let mut v = v.collect::<Vec<&str>>();
            if v.last().is_some_and(|x| x.is_empty()) {
                v.pop();
            }
            v.push(mount_path);
            v
        },
        None => vec![ mount_path ]
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

    let resp = client.post(directory.yp_url.clone())
        .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .form(&form)
        .timeout(Duration::from_millis(directory.timeout))
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
                    error!("{} not accepting Add request due to: {e}", directory.yp_url);
                },
                (Some(v), Some(e), Some(new_sid), Some(touch_freq)) if touch_freq > 0 => match v.eq("1") {
                    true => {
                        ds.sid          = Some(new_sid.to_string());
                        ds.touch_freq   = touch_freq;
                        ds.last_updated = chrono::offset::Utc::now().timestamp() as u64;
                        ds.next_touch   = Instant::now() + touch_interval(touch_freq);
                        info!("Stream {mount} added to {}", directory.yp_url);
                    },
                    false => {
                        error!("{} not accepting Add request due to: {e}", directory.yp_url);
                    }
                },
                (Some(_), Some(_), Some(_), _) => {
                    error!("{} returned an invalid touchfreq for Add", directory.yp_url);
                },
                _ => {
                    error!("{} did not return valid reply headers for Add", directory.yp_url);
                }
            }
        },
        Err(e) => {
            error!("Can't contact {}: {e}", directory.yp_url);
        }
    }
}

async fn touch_action(server: &Server, directory: &YPDirectory, client: &Client,
                      mount: &str, properties: &IcyProperties,
                      metadata: Option<&String>, ds: &mut DirState) {
    let sid = match &ds.sid {
        Some(v) => v.to_owned(),
        None => return
    };

    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("action", "touch".to_owned());
    form.insert("sid", sid);
    if let Some(title) = metadata {
        form.insert("st", title.to_string());
    }

    match post_req(directory, client, &directory.yp_url, &form).await {
        PostReqStatus::Ok => {
            info!("Stream {mount} on {} was touched", directory.yp_url);
        },
        PostReqStatus::WrongResp => {
            // The registration was dropped by the directory, register again
            add_action(server, directory, client, mount, properties, ds).await;
        },
        PostReqStatus::Unreachable => ()
    }
}

/// Run `touch_action` for every registered directory concurrently, awaiting all
/// spawned tasks before returning. Directories that couldn't be registered get a
/// re-add attempt instead. Returns whether any directory got a new sid
/// (i.e. it was re-registered) and thus the persisted state should be refreshed.
async fn touch_all(server: Arc<Server>, yp: &YP, dirs: &mut HashMap<Url, DirState>,
                   client: &Client, mount: &str, properties: &IcyProperties,
                   metadata: Option<String>, due_only: Option<Instant>) -> bool {
    let mut tasks = Vec::new();
    for (dir, mut ds) in std::mem::take(dirs) {
        let due = match due_only {
            Some(now) => ds.next_touch <= now,
            None => ds.sid.is_some()
        };
        if !due {
            dirs.insert(dir, ds);
            continue;
        }
        let directory = yp.directories.iter()
            .find(|x| canonical_yp_url(&x.yp_url).eq(&dir))
            .expect("Should find YP url in config")
            .clone();
        let client = client.clone();
        let server = server.clone();
        let mount = mount.to_owned();
        let properties = properties.clone();
        let metadata = metadata.clone();
        let prev_sid = ds.sid.clone();
        let fallback = ds.clone();
        tasks.push((dir, prev_sid, fallback, tokio::spawn(async move {
            if ds.sid.is_some() {
                touch_action(&server, &directory, &client, &mount, &properties, metadata.as_ref(), &mut ds).await;
            } else {
                add_action(&server, &directory, &client, &mount, &properties, &mut ds).await;
            }
            ds.next_touch = match &ds.sid {
                Some(_) => Instant::now() + touch_interval(ds.touch_freq),
                None => Instant::now() + ADD_RETRY_INTERVAL
            };
            ds
        })));
    }

    let mut changed = false;
    for (dir, prev_sid, fallback, task) in tasks {
        match task.await {
            Ok(ds) => {
                if ds.sid != prev_sid {
                    changed = true;
                }
                dirs.insert(dir, ds);
            },
            Err(e) => {
                error!("Touch task failed: {e}");
                let mut ds = fallback;
                ds.next_touch = Instant::now() + ADD_RETRY_INTERVAL;
                dirs.insert(dir, ds);
            }
        }
    }
    changed
}

async fn persist_dirs(state: &mut Option<StreamState>,
                      dirs: &HashMap<Url, DirState>, state_path: &PathBuf) {
    let st = state.get_or_insert_with(|| StreamState {
        dirs: HashMap::new()
    });
    st.dirs = dirs.clone();
    persist_state(state_path, st).await;
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

    for _ in 0..REMOVE_ATTEMPTS {
        match post_req(directory, client, dir, &form).await {
            PostReqStatus::Ok | PostReqStatus::WrongResp => {
                info!("Stream {mount} on {dir} was removed");
                break;
            },
            PostReqStatus::Unreachable => {
                tokio::time::sleep(REMOVE_RETRY_SLEEP).await;
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
        .timeout(Duration::from_millis(directory.timeout))
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
