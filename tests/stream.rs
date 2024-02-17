
// TODO:
// Add test for source stream
// Add test for metaint

use std::{time::Duration, io::Read};

use castmedia::broadcast::metadata_decode;
use test_utils::{spawn_server, spawn_source};

const CONFIG: &str = "
address:
  - bind: 127.0.0.1:9002
metadata_interval: 100000
limits:
  queue_size: 400000
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
    bind: 127.0.0.1:9102
";

static TEST_DIR: &str     = env!("CARGO_TARGET_TMPDIR");
const BASE: &str          = "127.0.0.1:9002";
const ADMIN: &str         = "127.0.0.1:9102";
const AUTH_ADMIN: &str    = "admin:pass";
const AUTH_SOURCE: &str   = "source:pass";
const MOUNT_SOURCE: &str  = "/stream.mp3";

#[test]
fn stream_metadata() {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut server = spawn_server(TEST_DIR, CONFIG, "stream.yaml").await;

            tokio::time::sleep(Duration::from_secs(2)).await;

            _ = spawn_source(AUTH_SOURCE, ADMIN, MOUNT_SOURCE).await;

            server.child.wait().await.ok();
        });
    });

    std::thread::sleep(Duration::from_secs(4));

    let mut buf = [0u8; 100000];
    let mut len = [8u8; 1];

    for i in 1..=5 {
        let mut r = test_utils::reqwest::blocking::Client::new()
            .get(&format!("http://{}{}", BASE, MOUNT_SOURCE))
            .header("Icy-Metadata", "1")
            .timeout(Duration::from_secs(60))
            .send()
            .unwrap();

        let mut c = 0;
        loop {
            c += 1;
            r.read_exact(&mut buf).unwrap();
            r.read_exact(&mut len).unwrap();
            let metadata_len = (len[0] as usize) << 4;
            let mut metadata_buf = vec![0u8; metadata_len];
            r.read_exact(&mut metadata_buf).unwrap();
            let metadata = std::str::from_utf8(&metadata_buf).unwrap();
            assert_eq!((Some("".to_owned()), Some("".to_owned())), metadata_decode(metadata).unwrap());
            if c == i {
                break;
            }
        }

        if i != 10 {
            let r = test_utils::reqwest::blocking::get(format!("http://{}@{}/admin/restart", AUTH_ADMIN, ADMIN))
                .unwrap()
                .status()
                .as_u16();
            assert_eq!(r, 200);
        }
    }

    let r = test_utils::reqwest::blocking::get(format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN))
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(r, 200);
}
