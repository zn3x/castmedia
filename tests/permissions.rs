use tokio::runtime::Runtime;
use std::time::Duration;

use arg::Args;

const CONFIG: &str = "
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

const BASE: &str          = "127.0.0.1:9000";
const ADMIN: &str         = "127.0.0.1:9100";

const AUTH_ADMIN: &str    = "admin:pass";
const AUTH_SOURCE: &str   = "source:pass";
const AUTH_SOURCE1: &str  = "source1:pass";
const AUTH_SLAVE: &str    = "slave:pass";
const AUTH_INVALID: &str  = "giberish:andmoregiberish";

const MOUNT_SOURCE: &str  = "/stream.mp3";
const MOUNT_SOURCE1: &str = "/stream1.mp3";

fn spawn_server(conf: &str) {
    let conf = conf.to_owned();
    std::thread::spawn(move || {
        let rt   = Runtime::new().unwrap();
        rt.block_on(async {
            let mut config = castmedia::config::ServerSettings::from_str(&conf).unwrap();
            castmedia::config::ServerSettings::hash_passwords(&mut config);

            castmedia::server::listener(
                config,
                Args::from_text("").unwrap(),
                None
            ).await;
        });
    });
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

#[test]
fn admin_api() {
    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();
    spawn_server(CONFIG);

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut source = tokio::process::Command::new("ffmpeg")
            .args([
                  "-loglevel", "panic",
                  "-re", "-f", "lavfi",
                  "-i", "sine=frequency=1000",
                  "-content_type", "audio/mpeg",
                  "-vn", "-f", "mp3",
                  &format!("icecast://{}@{}{}", AUTH_SOURCE, ADMIN, MOUNT_SOURCE)
            ])
            .spawn()
            .expect("ffmpeg missing");

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
    });
}
