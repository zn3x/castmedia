use std::{io::{Read, Write}, time::Duration};

use test_utils::{spawn_server_blocking, spawn_source_manual};
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
  bind: /tmp/relay_transparent_master.sock
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
  bind: /tmp/relay_transparent_slave.sock
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
  bind: /tmp/relay_transparent_master.sock
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
  bind: /tmp/relay_transparent_slave.sock
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
const BASE_MASTER: &str  = "127.0.0.1:9004";
const ADMIN_MASTER: &str = "127.0.0.1:9104";
const BASE_SLAVE: &str   = "127.0.0.1:9005";
const ADMIN_SLAVE: &str  = "127.0.0.1:9105";
const AUTH_ADMIN: &str   = "admin:pass";
const AUTH_SOURCE: &str  = "source:pass";
const MOUNT_SOURCE: &str = "/stream.mp3";


#[test]
fn transparent() {
    let mut master_server = spawn_server_blocking(TEST_DIR, CONFIG_MASTER, "master_transparent.yaml");
    let mut slave_server  = spawn_server_blocking(TEST_DIR, CONFIG_SLAVE, "slave_transparent.yaml");

    std::thread::sleep(Duration::from_secs(4));

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

    let r = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=1&song=1", AUTH_SOURCE, ADMIN_MASTER, MOUNT_SOURCE))
        .send()
        .unwrap()
        .status();
    assert_eq!(r, 200);

    let packet1 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet1.buf()).is_ok());

    // Mount should not be present in slave
    let mounts = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN_SLAVE))
        .send()
        .unwrap()
        .json::<serde_json::Value>()
        .unwrap();
    let mounts = mounts.as_object()
        .unwrap();
    assert!(!mounts.contains_key(MOUNT_SOURCE));

    // Waiting for next mount poll by slave server
    std::thread::sleep(Duration::from_secs(5));

    let packet2 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet2.buf()).is_ok());

    std::thread::sleep(Duration::from_secs(5));

    let mounts = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}@{}/admin/listmounts", AUTH_ADMIN, ADMIN_SLAVE))
        .send()
        .unwrap()
        .json::<serde_json::Value>()
        .unwrap();
    let mounts = mounts.as_object()
        .unwrap();
    assert!(mounts.contains_key(MOUNT_SOURCE));

    let mut r = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}{}", BASE_SLAVE, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .unwrap();
    
    assert_eq!(r.status().as_u16(), 200);

    let packet3 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet3.buf()).is_ok());

    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    assert!(r.read_exact(&mut buf).is_ok());
    r.read_exact(&mut len).unwrap();

    assert_eq!(&buf[0..packet1.buf().len()], packet1.buf());
    assert_eq!(&buf[packet1.buf().len()..packet1.buf().len()+packet2.buf().len()], packet2.buf());
    assert_eq!(&buf[packet1.buf().len()+packet2.buf().len()..], &packet3.buf()[..3000-(packet1.buf().len()+packet2.buf().len())]);

    let metadata_len = (len[0] as usize) << 4;
    let mut metadata_buf = vec![0u8; metadata_len];
    r.read_exact(&mut metadata_buf).unwrap();
    let metadata = std::str::from_utf8(&metadata_buf).unwrap();
    assert_eq!((Some("1".to_owned()), Some("1".to_owned())), metadata_decode(metadata).unwrap());

    // Restarting master, but we also add a slave user
    let master_server1 = spawn_server_blocking(TEST_DIR, CONFIG_MASTER1, "master_transparent.yaml");
    std::thread::sleep(Duration::from_secs(2));
    master_server = master_server1;

    let ret = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=2&song=2", AUTH_SOURCE, ADMIN_MASTER, MOUNT_SOURCE))
        .send()
        .unwrap()
        .status();
    assert_eq!(ret, 200);

    let packet4 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet4.buf()).is_ok());

    let packet5 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet5.buf()).is_ok());

    let packet6 = format.next_packet().unwrap();
    assert!(source_sock.write_all(packet6.buf()).is_ok());

    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    assert!(r.read_exact(&mut buf).is_ok());
    r.read_exact(&mut len).unwrap();
    let mut metadata_buf = vec![0u8; metadata_len];
    r.read_exact(&mut metadata_buf).unwrap();
    let metadata = std::str::from_utf8(&metadata_buf).unwrap();
    assert_eq!((Some("2".to_owned()), Some("2".to_owned())), metadata_decode(metadata).unwrap());

    // Now we want slave to run in authenticated mode
    
    let slave_server1 = spawn_server_blocking(TEST_DIR, CONFIG_SLAVE1, "slave_transparent.yaml");
    std::thread::sleep(Duration::from_secs(2));
    slave_server = slave_server1;

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }

    let mut buf = [0u8; 3000];
    assert!(r.read_exact(&mut buf).is_ok());

    // Now enabling on_demand mode
    drop(r);

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }

    let slave_server1 = spawn_server_blocking(TEST_DIR, CONFIG_SLAVE2, "slave_transparent.yaml");

    std::thread::sleep(Duration::from_secs(2));
    slave_server = slave_server1;

    for _ in 0..3 {
        let packet = format.next_packet().unwrap();
        assert!(source_sock.write_all(packet.buf()).is_ok());
    }

    // Waiting until source becomes inactive
    std::thread::sleep(Duration::from_secs(2));

    let mut r = test_utils::reqwest::blocking::Client::new()
        .get(format!("http://{}{}", BASE_SLAVE, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .unwrap();

    // We should be able to read from start
    let mut buf = [0u8; 3000];
    let mut len = [0u8; 1];
    assert!(r.read_exact(&mut buf).is_ok());
    r.read_exact(&mut len).unwrap();
    assert_eq!(&buf[0..packet1.buf().len()], packet1.buf());

    drop(r);


    drop(master_server);
    drop(slave_server);
}
