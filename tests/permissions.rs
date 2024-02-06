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
  slave:
    pass: 0$pass
    role: slave
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9100
";

const BASE: &str         = "127.0.0.1:9000";
const ADMIN: &str        = "127.0.0.1:9100";

const AUTH_ADMIN: &str   = "admin:pass";
const AUTH_SOURCE: &str  = "source:pass";
const AUTH_SLAVE: &str   = "slave:pass";
const AUTH_INVALID: &str = "giberish:andmoregiberish";

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

#[test]
fn admin_api() {
    spawn_server(CONFIG);

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut r;
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_ADMIN, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SOURCE, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_INVALID, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_ADMIN, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SOURCE, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/mountupdates", AUTH_INVALID, BASE)).await;
        assert_eq!(r, 405);
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_ADMIN, ADMIN)).await;
        assert_eq!(r, 200);
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SOURCE, ADMIN)).await;
        assert_eq!(r, 401);
        r = get_status_code(&format!("http://{}@{}/admin/stats", AUTH_SLAVE, ADMIN)).await;
        assert_eq!(r, 401);
        r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN)).await;
        assert_eq!(r, 200);
        r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SOURCE, ADMIN)).await;
        assert_eq!(r, 401);
        r = get_status_code(&format!("http://{}@{}/admin/listmounts", AUTH_SLAVE, ADMIN)).await;
        assert_eq!(r, 401);
        r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_SOURCE, ADMIN)).await;
        assert_eq!(r, 401);
        r = get_status_code(&format!("http://{}@{}/admin/restart", AUTH_SLAVE, ADMIN)).await;
        assert_eq!(r, 401);
    });
}
