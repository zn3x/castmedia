use std::time::Duration;

const CONFIG_ADMIN_API: &str = "
address:
  - bind: 127.0.0.1:9000
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '/stream.mp3'
  source1:
    pass: 0$pass
    role: source
    mount:
      - path: '/stream1.mp3'
  slave:
    pass: 0$pass
    role: slave
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9100
";

const CONFIG_PUBLIC_API: &str = "
address:
  - bind: 127.0.0.1:9001
info:
  id: test1
  admin: test2
  location: test3
  description: test4
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
    bind: 127.0.0.1:9101
";

static TEST_DIR: &str     = env!("CARGO_TARGET_TMPDIR");

// admin_api
const BASE: &str          = "127.0.0.1:9000";
const ADMIN: &str         = "127.0.0.1:9100";

// public_api
const BASE1: &str         = "127.0.0.1:9001";
const ADMIN1: &str        = "127.0.0.1:9101";

const AUTH_ADMIN: &str    = "admin:pass";
const AUTH_SOURCE: &str   = "source:pass";
const AUTH_SOURCE1: &str  = "source1:pass";
const AUTH_SLAVE: &str    = "slave:pass";
const AUTH_INVALID: &str  = "giberish:andmoregiberish";

const MOUNT_SOURCE: &str  = "/stream.mp3";
const MOUNT_SOURCE1: &str = "/stream1.mp3";

struct Server {
    child: tokio::process::Child
}

impl Drop for Server {
    fn drop(&mut self) {
        _ = self.child.start_kill();
    }
}

async fn spawn_server(conf: &str, conf_name: &str) -> Server {
    let conf = conf.to_owned();

    let conf_file = format!("{}/{}", TEST_DIR, conf_name);

    tokio::fs::write(&conf_file, conf).await
        .expect("Failed to write config file");
    
    let server = tokio::process::Command::new("cargo")
        .args([
              "run",
              "--",
              "--unsafe-password",
              &conf_file
        ])
        .spawn()
        .expect("failed to start castmedia");

    Server {
        child: server
    }
}

async fn get_response(url: &str) -> reqwest::Response {
    let resp = reqwest::get(url).await;
    assert!(resp.is_ok());
    resp.unwrap()
}

async fn get_status_code(url: &str) -> u16 {
    let resp = get_response(url).await;
    resp.status().as_u16()
}

async fn assert_medatadata(mount: &str, url: &str, title: &str) {
    let resp = get_response(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let body = body.as_object()
        .unwrap();
    let source = body.get(mount)
        .unwrap()
        .as_object()
        .unwrap();
    let metadata = source.get("metadata")
        .unwrap()
        .as_object()
        .unwrap();
    let purl = metadata.get("url")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(url, purl);
    let ptitle = metadata.get("title")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(title, ptitle);
}

async fn assert_fallback(mount: &str, fallback: Option<&str>) {
    let resp = get_response(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let body = body.as_object()
        .unwrap();
    let source = body.get(mount)
        .unwrap()
        .as_object()
        .unwrap();
    let pfallback = source.get("fallback")
        .unwrap()
        .as_str();
    assert_eq!(fallback, pfallback);
}

async fn spawn_source(auth: &str, addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("ffmpeg")
        .args([
              "-loglevel", "panic",
              "-re", "-f", "lavfi",
              "-i", "sine=frequency=1000",
              "-content_type", "audio/mpeg",
              "-vn", "-f", "mp3",
              &format!("icecast://{}@{}{}", auth, addr, mount)
        ])
        .spawn()
        .expect("ffmpeg missing")
}

async fn spawn_listener(addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("curl")
        .args([
              "-o", "/dev/null",
              "-L",
              "-H \"Icy-Metadata: 1\"",
              &format!("http://{}{}", addr, mount)
        ])
        .spawn()
        .expect("curl missing")
}

#[tokio::test]
async fn admin_api() {
    let server = spawn_server(CONFIG_ADMIN_API, "admin_api.yaml").await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut source = spawn_source(AUTH_SOURCE, ADMIN, MOUNT_SOURCE).await;

    let mut r;
    // Checking admin api services that only admin should be able to reach
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_ADMIN, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SOURCE, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SLAVE, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_INVALID, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_ADMIN, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SOURCE, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_INVALID, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/mountupdates", AUTH_INVALID, BASE)).await;
    assert_eq!(r, 405);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(r, 200);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SOURCE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SLAVE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_INVALID, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(r, 200);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SOURCE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SLAVE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_INVALID, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_SOURCE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_SLAVE, ADMIN)).await;
    assert_eq!(r, 401);
    r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_INVALID, ADMIN)).await;
    assert_eq!(r, 401);

    // Checking admin api that source can access given that it's on own mount
    //
    // Change metadata
    r = get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here&song=title_here", AUTH_ADMIN, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 200);
    assert_medatadata(MOUNT_SOURCE, "url_here", "title_here").await;
    r = get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here1&song=title_here1", AUTH_SOURCE, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 200);
    assert_medatadata(MOUNT_SOURCE, "url_here1", "title_here1").await;
    r = get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here2&song=title_here2", AUTH_SLAVE, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 401);
    assert_medatadata(MOUNT_SOURCE, "url_here1", "title_here1").await;
    r = get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here3&song=title_here3", AUTH_INVALID, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 401);
    assert_medatadata(MOUNT_SOURCE, "url_here1", "title_here1").await;

    // Change fallback
    r = get_status_code(&format!("http://{}@{}/admin/fallbacks?mount={}&fallback={}", AUTH_ADMIN, ADMIN, MOUNT_SOURCE, MOUNT_SOURCE1)).await;
    assert_eq!(r, 200);
    assert_fallback(MOUNT_SOURCE, Some(MOUNT_SOURCE1)).await;
    r = get_status_code(&format!("http://{}@{}/admin/fallbacks?mount={}", AUTH_SOURCE, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 200);
    assert_fallback(MOUNT_SOURCE, None).await;
    r = get_status_code(&format!("http://{}@{}/admin/fallbacks?mount={}", AUTH_SLAVE, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 401);
    assert_fallback(MOUNT_SOURCE, None).await;
    r = get_status_code(&format!("http://{}@{}/admin/fallbacks?mount={}", AUTH_INVALID, ADMIN, MOUNT_SOURCE)).await;
    assert_eq!(r, 401);
    assert_fallback(MOUNT_SOURCE, None).await;

    r = get_status_code(&format!("http://{}@{}/admin/fallbacks?mount={}&fallback={}", AUTH_SOURCE, ADMIN, MOUNT_SOURCE, MOUNT_SOURCE1)).await;
    assert_eq!(r, 200);
    assert_fallback(MOUNT_SOURCE, Some(MOUNT_SOURCE1)).await;

    source.kill().await.ok();
    drop(server);
}

async fn get_stat(path: &str, key: &str) -> serde_json::Value {
    let resp = get_response(&format!("http://{}{}", BASE1, path)).await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let body = body.as_object()
        .unwrap();
    let value = body.get(key)
        .unwrap()
        .clone();

    value
}

#[tokio::test]
async fn public_api() {
    let start = chrono::offset::Utc::now().timestamp();

    let server = spawn_server(CONFIG_PUBLIC_API, "public_api.yaml").await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let stats = get_stat("/api/serverinfo", "stats").await;
    let s     = stats.as_object()
        .unwrap();

    assert_eq!(0, s.get("active_listeners").unwrap().as_u64().unwrap());
    assert_eq!(0, s.get("peak_listeners").unwrap().as_u64().unwrap());
    assert!(s.get("start_time").unwrap().as_i64().unwrap() >= start);
    assert!(s.get("start_time").unwrap().as_i64().unwrap() <= chrono::offset::Utc::now().timestamp());

    let stats = get_stat("/api/serverinfo", "properties").await;
    let s     = stats.as_object()
        .unwrap();
    assert_eq!("test1", s.get("id").unwrap().as_str().unwrap());
    assert_eq!("test2", s.get("admin").unwrap().as_str().unwrap());
    assert_eq!("test3", s.get("location").unwrap().as_str().unwrap());
    assert_eq!("test4", s.get("description").unwrap().as_str().unwrap());

    let stats = get_stat("/api/serverinfo", "mounts").await;
    assert!(
        stats.as_array().unwrap().is_empty()
    );

    let mut source = spawn_source(AUTH_SOURCE, ADMIN1, MOUNT_SOURCE).await;
    
    tokio::time::sleep(Duration::from_secs(2)).await;

    let stats = get_stat("/api/serverinfo", "mounts").await;
    assert_eq!(1, stats.as_array().unwrap().len());
    assert!(
        stats.as_array().unwrap().first().unwrap().as_str().eq(&Some(MOUNT_SOURCE))
    );

    let listener  = spawn_listener(BASE1, MOUNT_SOURCE).await;
    let listener1 = spawn_listener(BASE1, MOUNT_SOURCE).await;

    source.kill().await.ok();
    drop(server);
}
