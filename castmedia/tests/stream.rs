
// TODO:
// Add test for source stream
// Add test for metaint

use std::{time::Duration, io::{Read, Write}};

use castmedia::broadcast::metadata_decode;
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, probe::Hint, formats::FormatOptions, meta::MetadataOptions};
use test_utils::{spawn_source_manual, spawn_server_blocking};

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
migrate:
  enabled: true
  bind: /tmp/stream_config.sock
misc:
  unsafe_pass: true
";

static TEST_DIR: &str     = env!("CARGO_TARGET_TMPDIR");
const BASE: &str          = "127.0.0.1:9002";
const ADMIN: &str         = "127.0.0.1:9102";
const AUTH_ADMIN: &str    = "admin:pass";
const AUTH_SOURCE: &str   = "source:pass";
const MOUNT_SOURCE: &str  = "/stream.mp3";

#[test]
fn stream_general() {
    let mut server = spawn_server_blocking(TEST_DIR, CONFIG, "stream.yaml");

    std::thread::sleep(Duration::from_secs(4));

    let mut buf = [0u8; 100000];
    let mut len = [8u8; 1];

    let (mut source_sock, media) = spawn_source_manual(AUTH_SOURCE, ADMIN, MOUNT_SOURCE).unwrap();
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

    let mut total_written = 0;

    for i in 1..=5 {
        let mut r = test_utils::reqwest::blocking::Client::new()
            .get(&format!("http://{}{}", BASE, MOUNT_SOURCE))
            .header("Icy-Metadata", "1")
            .send()
            .unwrap();

        let mut c = 0;
        loop {
            c += 1;

            // Metadata update on when we read third time
            if i == 2 && c == 1 {
                let r = test_utils::reqwest::blocking::Client::new()
                    .get(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here&song=title_here", AUTH_SOURCE, ADMIN, MOUNT_SOURCE))
                    .send()
                    .unwrap()
                    .status();
                assert_eq!(r, 200);
            }

            loop {
                let packet = format.next_packet().expect("Should be no end of stream");
                source_sock.write_all(packet.buf()).expect("Should be able to write to source socket");
                total_written += packet.buf().len();
                if total_written >= 100000 {
                    total_written -= 100000;
                    break;
                }
            }

            r.read_exact(&mut buf).unwrap();
            r.read_exact(&mut len).unwrap();
            let metadata_len = (len[0] as usize) << 4;
            let mut metadata_buf = vec![0u8; metadata_len];
            r.read_exact(&mut metadata_buf).unwrap();
            let metadata = std::str::from_utf8(&metadata_buf).unwrap();
            
            if i <= 2 && c < 2 {
                assert_eq!((Some("".to_owned()), Some("".to_owned())), metadata_decode(metadata).unwrap());
            } else {
                assert_eq!((Some("title_here".to_owned()), Some("url_here".to_owned())), metadata_decode(metadata).unwrap());
            }
            
            if c == i {
                break;
            }
        }

        drop(r);
        if i != 5 {
            let server1 = spawn_server_blocking(TEST_DIR, CONFIG, "stream.yaml");
            std::thread::sleep(Duration::from_secs(2));

            let status = server.child.try_wait();
            assert!(matches!(status, Ok(Some(_))));
            server = server1;
        }
    }

    let r = test_utils::reqwest::blocking::get(format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN))
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(r, 200);
}
