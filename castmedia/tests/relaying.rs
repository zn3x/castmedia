use std::{io::Write, time::Duration};

use futures::{AsyncReadExt, StreamExt, TryStreamExt};
use test_utils::{spawn_server, spawn_source_manual};
use castmedia::broadcast::metadata_decode;
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, probe::Hint, formats::FormatOptions, meta::MetadataOptions};


const CONFIG_MASTER: &str = "
address:
  - bind: 127.0.0.1:9004
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '*'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9104
misc:
  unsafe_pass: true
migrate:
  enabled: true
  bind: /tmp/relay_master.sock
limits:
  source_timeout: 30000
metadata_interval: 3000
";

const CONFIG_SLAVE: &str = "
address:
  - bind: 127.0.0.1:9005
account:
  admin:
    pass: 0$pass
    role: admin
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9105
misc:
  unsafe_pass: true
migrate:
  enabled: true
  bind: /tmp/relay_slave.sock
master:
  - url: http://127.0.0.1:9004
    relay_scheme:
      type: transparent
      update_interval: 10000
metadata_interval: 3000
";

const CONFIG_MASTER1: &str = "
address:
  - bind: 127.0.0.1:9004
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '*'
  slave:
    pass: 0$pass
    role: slave
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9104
misc:
  unsafe_pass: true
migrate:
  enabled: true
  bind: /tmp/relay_master.sock
limits:
  source_timeout: 30000
metadata_interval: 3000
";

const CONFIG_SLAVE1: &str = "
address:
  - bind: 127.0.0.1:9005
account:
  admin:
    pass: 0$pass
    role: admin
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9105
misc:
  unsafe_pass: true
migrate:
  enabled: true
  bind: /tmp/relay_slave.sock
master:
  - url: http://127.0.0.1:9004
    relay_scheme:
      type: authenticated
      user: slave
      pass: pass
      reconnect_timeout: 5000
      stream_on_demand: false
metadata_interval: 3000
";

const CONFIG_SLAVE2: &str = "
address:
  - bind: 127.0.0.1:9005
account:
  admin:
    pass: 0$pass
    role: admin
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9105
misc:
  unsafe_pass: true
migrate:
  enabled: true
  bind: /tmp/relay_transparent_slave.sock
master:
  - url: http://127.0.0.1:9004
    relay_scheme:
      type: authenticated
      user: slave
      pass: pass
      reconnect_timeout: 5000
      stream_on_demand: true
metadata_interval: 3000
";

static TEST_DIR: &str    = env!("CARGO_TARGET_TMPDIR");
//const BASE_MASTER: &str  = "127.0.0.1:9004";
const ADMIN_MASTER: &str = "127.0.0.1:9104";
const BASE_SLAVE: &str   = "127.0.0.1:9005";
const ADMIN_SLAVE: &str  = "127.0.0.1:9105";
const AUTH_ADMIN: &str   = "admin:pass";
const AUTH_SOURCE: &str  = "source:pass";
const MOUNT_SOURCE: &str = "/stream.mp3";


#[tokio::test]
#[allow(unused_assignments)]
async fn relaying() {
    let mut master_server = spawn_server(TEST_DIR, CONFIG_MASTER, "relay_master.yaml").await;
    let mut slave_server  = spawn_server(TEST_DIR, CONFIG_SLAVE, "relay_slave.yaml").await;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let (mut source_sock, media) = spawn_source_manual(AUTH_SOURCE, ADMIN_MASTER, MOUNT_SOURCE).unwrap();
    let stdout                   = media.stdout.unwrap();

    let mss  = MediaSourceStream::new(Box::new(ReadOnlySource::new(stdout)), Default::default());
    let hint = Hint::new();

    // Use the default options for metadata and format readers.
    let meta_opts: MetadataOptions  = Default::default();
    let fmt_opts: FormatOptions     = Default::default();

    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &fmt_opts, &meta_opts)
        .unwrap();

    let mut format = probed.format;

    let r = test_utils::get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=1&song=1", AUTH_SOURCE, ADMIN_MASTER, MOUNT_SOURCE)).await;
    assert_eq!(r, 200);

    let packet1 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet1.buf()).is_ok());

    // Mount should not be present in slave
    let mounts = test_utils::get_response(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN_SLAVE)).await
        .json::<serde_json::Value>().await.unwrap();
    let mounts = mounts.as_object()
        .unwrap();
    assert!(!mounts.contains_key(MOUNT_SOURCE));

    // Waiting for next mount poll by slave server
    tokio::time::sleep(Duration::from_secs(5)).await;

    let packet2 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet2.buf()).is_ok());

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mounts = test_utils::get_response(&format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN_SLAVE)).await
        .json::<serde_json::Value>().await.unwrap();
    let mounts = mounts.as_object()
        .unwrap();
    assert!(mounts.contains_key(MOUNT_SOURCE));

    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", BASE_SLAVE, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    
    assert_eq!(resp.status().as_u16(), 200);
    let mut r = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let packet3 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet3.buf()).is_ok());

    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    r.read_exact(&mut buf).await.unwrap();
    r.read_exact(&mut len).await.unwrap();

    assert_eq!(&buf[0..packet1.buf().len()], packet1.buf());
    assert_eq!(&buf[packet1.buf().len()..packet1.buf().len()+packet2.buf().len()], packet2.buf());
    assert_eq!(&buf[packet1.buf().len()+packet2.buf().len()..], &packet3.buf()[..3000-(packet1.buf().len()+packet2.buf().len())]);

    let metadata_len = (len[0] as usize) << 4;
    let mut metadata_buf = vec![0u8; metadata_len];
    r.read_exact(&mut metadata_buf).await.unwrap();
    let metadata = std::str::from_utf8(&metadata_buf).unwrap();
    assert_eq!(("1".to_owned(), "1".to_owned()), metadata_decode(metadata).unwrap());

    // Restarting master, but we also add a slave user
    let master_server1 = spawn_server(TEST_DIR, CONFIG_MASTER1, "relay_master.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    master_server = master_server1;

    let ret = test_utils::get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=2&song=2", AUTH_SOURCE, ADMIN_MASTER, MOUNT_SOURCE)).await;
    assert_eq!(ret, 200);

    let packet4 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet4.buf()).is_ok());

    let packet5 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet5.buf()).is_ok());

    let packet6 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet6.buf()).is_ok());

    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    r.read_exact(&mut buf).await.unwrap();
    r.read_exact(&mut len).await.unwrap();
    let mut metadata_buf = vec![0u8; metadata_len];
    r.read_exact(&mut metadata_buf).await.unwrap();
    let metadata = std::str::from_utf8(&metadata_buf).unwrap();
    assert_eq!(("2".to_owned(), "2".to_owned()), metadata_decode(metadata).unwrap());

    // Now we want slave to run in authenticated mode
    
    let slave_server1 = spawn_server(TEST_DIR, CONFIG_SLAVE1, "relay_slave.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    slave_server = slave_server1;

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }

    let mut buf = [0u8; 3000];
    r.read_exact(&mut buf).await.unwrap();

    drop(r);
    // Now enabling on_demand mode

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }

    let slave_server1 = spawn_server(TEST_DIR, CONFIG_SLAVE2, "relay_slave.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    slave_server = slave_server1;

    let packet = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet.buf()).is_ok());

    // Waiting until source becomes inactive
    tokio::time::sleep(Duration::from_secs(2)).await;

    let packet = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet.buf()).is_ok());

    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", BASE_SLAVE, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let mut r = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    // We should be able to read from start
    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    r.read_exact(&mut buf).await.unwrap();
    r.read_exact(&mut len).await.unwrap();
    assert_eq!(&buf[0..packet1.buf().len()], packet1.buf());

    // Now we want to return back from authenticated to transparent
    let slave_server1 = spawn_server(TEST_DIR, CONFIG_SLAVE, "relay_slave.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    slave_server = slave_server1;

    let mut buf = [0u8; 3000];
    r.read_exact(&mut buf).await.unwrap();

    drop(r);

    // Removing slave user from master, it's safe to do so because we no longer use authenticated
    // mode in slave
    let master_server1 = spawn_server(TEST_DIR, CONFIG_MASTER, "relay_master.yaml").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    master_server = master_server1;

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }
    
    let r = test_utils::get_status_code(&format!("http://{}{}", BASE_SLAVE, MOUNT_SOURCE)).await;
    assert_eq!(r, 200);

    drop(master_server);
    drop(slave_server);
}
