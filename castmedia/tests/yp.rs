use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper::header::HeaderValue;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use test_utils::{get_status_code, spawn_server, spawn_source};
use tokio::time::Instant;

use hashbrown::HashMap;

static TEST_DIR: &str = env!("CARGO_TARGET_TMPDIR");

/// touch_freq returned by the dummy yp dir.
/// touch_interval = (4 * 3) / 4 = 3 seconds
const YP_TOUCHFREQ: u64 = 4;

const CONFIG_YP_ADD_TOUCH_REMOVE: &str = "
address:
  - bind: 127.0.0.1:9200
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '/stream.mp3'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9300
misc:
  unsafe_pass: true
yellow_pages:
  enabled: true
  public_server: http://127.0.0.1:9200
  url: http://127.0.0.1:9200
  directories:
    - yp_url: http://127.0.0.1:9500
      timeout: 5000
  state: /tmp/yp_state_mount_events
";

const CONFIG_YP_RESUMES_PERSISTED_STATE: &str = "
address:
  - bind: 127.0.0.1:9201
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '/stream.mp3'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9301
migrate:
  enabled: true
  bind: /tmp/yp_persisted.sock
misc:
  unsafe_pass: true
yellow_pages:
  enabled: true
  public_server: http://127.0.0.1:9201
  url: http://127.0.0.1:9201
  directories:
    - yp_url: http://127.0.0.1:9501
      timeout: 5000
  state: /tmp/yp_state_persisted
";

// yp_add_touch_remove
const ADMIN: &str         = "127.0.0.1:9300";
const PUBLIC_SERVER: &str = "http://127.0.0.1:9200";
const YP_DIR: u16         = 9500;

// yp_resumes_persisted_state
const ADMIN1: &str = "127.0.0.1:9301";
const YP_DIR1: u16 = 9501;

const AUTH_ADMIN: &str   = "admin:pass";
const AUTH_SOURCE: &str  = "source:pass";
const MOUNT_SOURCE: &str = "/stream.mp3";

#[derive(Debug, Clone)]
enum YpEvent {
    Add { sn: String, ct: String, listenurl: String },
    Touch { sid: String, st: Option<String> },
    Remove { sid: String }
}

/// Wait until `pred` returns true, panicking if it doesn't within 15 seconds.
async fn wait_until(pred: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if pred() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the state file exists/doesn't exist.
async fn wait_until_file(path: &str, exists: bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::fs::try_exists(path).await.unwrap() == exists {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for state file");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn parse_form(body: &[u8]) -> HashMap<String, String> {
    url::form_urlencoded::parse(body).into_owned().collect()
}

async fn handle(
    req: Request<Body>,
    events: Arc<Mutex<Vec<YpEvent>>>,
    sid: Arc<AtomicU64>,
) -> Result<Response<Body>, Infallible> {
    let body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
    let form = parse_form(&body);

    let mut resp = Response::new(Body::empty());
    let headers = resp.headers_mut();
    headers.insert("ypresponse", HeaderValue::from_static("1"));
    headers.insert("ypmessage", HeaderValue::from_static("OK"));

    match form.get("action").map(|s| s.as_str()) {
        Some("add") => {
            let new_sid = sid.fetch_add(1, Ordering::SeqCst);
            events.lock().unwrap().push(YpEvent::Add {
                sn: form.get("sn").cloned().unwrap_or_default(),
                ct: form.get("type").cloned().unwrap_or_default(),
                listenurl: form.get("listenurl").cloned().unwrap_or_default()
            });
            headers.insert("sid", HeaderValue::from_str(&new_sid.to_string()).unwrap());
            headers.insert("touchfreq", HeaderValue::from_str(&YP_TOUCHFREQ.to_string()).unwrap());
        },
        Some("touch") => {
            events.lock().unwrap().push(YpEvent::Touch {
                sid: form.get("sid").cloned().unwrap_or_default(),
                st: form.get("st").cloned()
            });
        },
        Some("remove") => {
            events.lock().unwrap().push(YpEvent::Remove {
                sid: form.get("sid").cloned().unwrap_or_default()
            });
        },
        _ => ()
    }

    Ok(resp)
}

/// Dummy YP directory that accepts add/touch/remove requests, recording them
/// and replying with valid ypresponse headers.
async fn start_yp_dir(port: u16) -> Arc<Mutex<Vec<YpEvent>>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let events = Arc::new(Mutex::new(Vec::new()));
    let sid = Arc::new(AtomicU64::new(1));

    let (make_events, make_sid) = (events.clone(), sid.clone());
    let make_svc = make_service_fn(move |_conn| {
        let events = make_events.clone();
        let sid = make_sid.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let events = events.clone();
                let sid = sid.clone();
                async move { handle(req, events, sid).await }
            }))
        }
    });

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&addr).serve(make_svc).await {
            eprintln!("yp dir server error: {e}");
        }
    });

    events
}

#[tokio::test]
async fn yp_add_touch_remove() {
    let events = start_yp_dir(YP_DIR).await;

    // Clean stale state from previous runs
    let _ = tokio::fs::remove_dir_all("/tmp/yp_state_mount_events").await;

    let server = spawn_server(TEST_DIR, CONFIG_YP_ADD_TOUCH_REMOVE, "yp_mount_events.yaml").await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut source = spawn_source(AUTH_SOURCE, ADMIN, MOUNT_SOURCE).await;

    // Registration: add request must reach the YP dir carrying the stream info
    wait_until(|| events.lock().unwrap().iter().any(|e| matches!(e, YpEvent::Add { .. }))).await;
    let (sn, ct, listenurl) = {
        let evs = events.lock().unwrap();
        let add = evs.iter().find_map(|e| match e {
            YpEvent::Add { sn, ct, listenurl } => Some((sn.clone(), ct.clone(), listenurl.clone())),
            _ => None
        }).unwrap();
        add
    };
    assert_eq!("", sn);
    assert_eq!("audio/mpeg", ct);
    assert_eq!(format!("{PUBLIC_SERVER}{MOUNT_SOURCE}"), listenurl);

    // State file must be persisted after registration
    let state_file = "/tmp/yp_state_mount_events/stream.mp3.json";
    wait_until_file(state_file, true).await;

    // Metadata update triggers a touch carrying the new song title
    let r = get_status_code(&format!(
        "http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here&song=title_here",
        AUTH_ADMIN, ADMIN, MOUNT_SOURCE
    )).await;
    assert_eq!(r, 200);
    wait_until(|| {
        events.lock().unwrap().iter().any(|e| {
            matches!(e, YpEvent::Touch { st: Some(v), .. } if v == "title_here")
        })
    }).await;
    let sid = {
        let evs = events.lock().unwrap();
        evs.iter().find_map(|e| match e {
            YpEvent::Touch { sid, .. } => Some(sid.clone()),
            _ => None
        }).unwrap()
    };

    // Periodic touch (touchfreq 4 -> 3s interval) keeps the listing alive
    wait_until(|| {
        events.lock().unwrap().iter().filter(|e| matches!(e, YpEvent::Touch { .. })).count() >= 2
    }).await;

    // Killing the source unmounts the stream
    source.kill().await.ok();
    wait_until(|| events.lock().unwrap().iter().any(|e| matches!(e, YpEvent::Remove { .. }))).await;
    let remove_sid = {
        let evs = events.lock().unwrap();
        evs.iter().find_map(|e| match e {
            YpEvent::Remove { sid } => Some(sid.clone()),
            _ => None
        }).unwrap()
    };
    assert_eq!(sid, remove_sid);

    // State file must be removed on unmount
    wait_until_file(state_file, false).await;

    drop(server);
}

#[tokio::test]
async fn yp_resumes_persisted_state() {
    let events = start_yp_dir(YP_DIR1).await;

    // Clean stale state from previous runs
    let _ = tokio::fs::remove_dir_all("/tmp/yp_state_persisted").await;
    let _ = tokio::fs::remove_file("/tmp/yp_persisted.sock").await;

    let mut server = spawn_server(TEST_DIR, CONFIG_YP_RESUMES_PERSISTED_STATE, "yp_persisted.yaml").await;

    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut source = spawn_source(AUTH_SOURCE, ADMIN1, MOUNT_SOURCE).await;

    // Registration: add request must reach the YP dir carrying the stream info
    wait_until(|| events.lock().unwrap().iter().any(|e| matches!(e, YpEvent::Add { .. }))).await;

    // State file must be persisted after registration
    let state_file = "/tmp/yp_state_persisted/stream.mp3.json";
    wait_until_file(state_file, true).await;

    // Trigger migration by starting a new instance with the same config
    let server1 = spawn_server(TEST_DIR, CONFIG_YP_RESUMES_PERSISTED_STATE, "yp_persisted.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Old instance should exit after migration
    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))), "Old server should have exited after migration");
    server = server1;

    // The new instance resumes from the persisted state, so no add request
    // should be issued again: the same sid must be reused
    let adds = events.lock().unwrap().iter()
        .filter(|e| matches!(e, YpEvent::Add { .. }))
        .count();
    assert_eq!(adds, 1, "No add request should be issued after migration");

    // Metadata updates are still pushed to the directory with the same sid
    let r = get_status_code(&format!(
        "http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here&song=title_here",
        AUTH_ADMIN, ADMIN1, MOUNT_SOURCE
    )).await;
    assert_eq!(r, 200);
    wait_until(|| {
        events.lock().unwrap().iter().any(|e| {
            matches!(e, YpEvent::Touch { st: Some(v), .. } if v == "title_here")
        })
    }).await;
    let sid = {
        let evs = events.lock().unwrap();
        evs.iter().find_map(|e| match e {
            YpEvent::Touch { sid, .. } => Some(sid.clone()),
            _ => None
        }).unwrap()
    };

    // Unmount removes the listing using the same sid
    source.kill().await.ok();
    wait_until(|| {
        events.lock().unwrap().iter().any(|e| matches!(e, YpEvent::Remove { sid: v } if v == &sid))
    }).await;
    wait_until_file(state_file, false).await;

    drop(server);
}
