use std::{io::{Read, Write}, net::TcpStream, time::Duration};

use test_utils::{spawn_server_blocking, spawn_source_manual};


const CONFIG: &str = "
address:
  - bind: 127.0.0.1:9003
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
    bind: 127.0.0.1:9103
misc:
  unsafe_pass: true
limits:
  clients: 4
  listeners: 2
  sources: 2
  queue_size: 400000
  header_timeout: 5000
  source_timeout: 10000
  http_max_len: 8192
";

static TEST_DIR: &str     = env!("CARGO_TARGET_TMPDIR");
const BASE: &str          = "127.0.0.1:9003";
const ADMIN: &str         = "127.0.0.1:9103";
const AUTH_SOURCE: &str   = "source:pass";
const MOUNT_SOURCE1: &str = "/stream1.mp3";
const MOUNT_SOURCE2: &str = "/stream2.mp3";
const MOUNT_SOURCE3: &str = "/stream3.mp3";

#[test]
fn limits() {
    let server = spawn_server_blocking(TEST_DIR, CONFIG, "limits.yaml");

    std::thread::sleep(Duration::from_secs(4));

    // Checking if server respects header_timeout
    let mut tasks = Vec::new();
    for _ in 0..4 {
        tasks.push(std::thread::spawn(move || {
            let mut con = TcpStream::connect(BASE).unwrap();
            assert!(con.write_all(b"PUT /stream HTTP/1.1\r\n").is_ok());
            std::thread::sleep(Duration::from_secs(6));
            let mut buf = [0u8; 1];
            assert!(con.read_exact(&mut buf).is_err());
        }));
    }
    for task in tasks {
        _ = task.join();
    }

    // Max clients
    let mut tasks = Vec::new();
    for _ in 0..20 {
        tasks.push(std::thread::spawn(move || {
            let mut con = TcpStream::connect(BASE).unwrap();
            con.set_nonblocking(true).unwrap();
            std::thread::sleep(Duration::from_secs(3));
            _ = con.write_all(format!("GET /api/serverinfo HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", BASE).as_bytes());
            std::thread::sleep(Duration::from_secs(1));
            let mut buf = [0u8; 1];
            con.read(&mut buf)
        }));
    }
    let mut ok = 0;
    for task in tasks {
        if task.join().is_ok_and(|x| x.unwrap() == 1) {
            ok += 1;
        }
    }
    assert_eq!(ok, 4);

    // Sources limit
    {
        let source1 = spawn_source_manual(AUTH_SOURCE, ADMIN, MOUNT_SOURCE1);
        let source2 = spawn_source_manual(AUTH_SOURCE, ADMIN, MOUNT_SOURCE2);
        let source3 = spawn_source_manual(AUTH_SOURCE, ADMIN, MOUNT_SOURCE3);
        assert!(source1.is_ok());
        assert!(source2.is_ok());
        assert!(source3.is_err());
        
        // Testing source timeout
        std::thread::sleep(Duration::from_secs(10));
        let mut s1 = source1.unwrap();
        _ = s1.0.write_all(b"b");
        let mut s2 = source2.unwrap();
        _ = s2.0.write_all(b"b");

        assert!(s1.0.write_all(b"a").is_err());
        assert!(s2.0.write_all(b"a").is_err());
    }

    // Listener limit
    {
        let source1 = spawn_source_manual(AUTH_SOURCE, ADMIN, MOUNT_SOURCE1);
        assert!(source1.is_ok());

        let r1 = test_utils::reqwest::blocking::Client::new()
            .get(format!("http://{}{}", BASE, MOUNT_SOURCE1))
            .header("Icy-Metadata", "1")
            .send()
            .unwrap();
        assert_eq!(r1.status().as_u16(), 200);

        let r2 = test_utils::reqwest::blocking::Client::new()
            .get(format!("http://{}{}", BASE, MOUNT_SOURCE1))
            .header("Icy-Metadata", "1")
            .send()
            .unwrap();
        assert_eq!(r2.status().as_u16(), 200);

        let r3 = test_utils::reqwest::blocking::Client::new()
            .get(format!("http://{}{}", BASE, MOUNT_SOURCE1))
            .header("Icy-Metadata", "1")
            .send()
            .unwrap();
        assert_ne!(r3.status().as_u16(), 200);
    }

    // Http header max len
    let mut con = TcpStream::connect(BASE).unwrap();
    let buf     = [b'H'; 8193];
    _ = con.write_all(&buf);
    let mut buf = [0u8; 1];
    assert!(con.read_exact(&mut buf).is_err());


    drop(server);
}
