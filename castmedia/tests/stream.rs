use std::{time::Duration, net::TcpStream, io::{Read, Write}};
use castmedia::broadcast::metadata_decode;
use futures::{AsyncRead, AsyncReadExt, StreamExt, TryStreamExt};
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, probe::Hint, formats::FormatOptions, meta::MetadataOptions};
use test_utils::{spawn_source, spawn_source_manual, spawn_source_manual_aac, spawn_server};

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
      - path: '/stream.aac'
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
const MOUNT_AAC: &str     = "/stream.aac";

const CONFIG_SOURCE: &str = "
address:
  - bind: 127.0.0.1:9003
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
    bind: 127.0.0.1:9103
migrate:
  enabled: true
  bind: /tmp/source_stream_config.sock
misc:
  unsafe_pass: true
";

const CONFIG_METAINT: &str = "
address:
  - bind: 127.0.0.1:9004
metadata_interval: 10000
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
    bind: 127.0.0.1:9104
migrate:
  enabled: true
  bind: /tmp/metaint_config.sock
misc:
  unsafe_pass: true
";

const BASE2: &str         = "127.0.0.1:9003";
const ADMIN2: &str        = "127.0.0.1:9103";

const BASE3: &str         = "127.0.0.1:9004";
const ADMIN3: &str        = "127.0.0.1:9104";

#[tokio::test]
async fn stream_general() {
    let mut server = spawn_server(TEST_DIR, CONFIG, "stream.yaml").await;

    tokio::time::sleep(Duration::from_secs(4)).await;

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
        let resp = test_utils::reqwest::Client::new()
            .get(&format!("http://{}{}", BASE, MOUNT_SOURCE))
            .header("Icy-Metadata", "1")
            .send()
            .await
            .unwrap();
        let mut r = resp.bytes_stream()
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
            .into_async_read();

        let mut c = 0;
        loop {
            c += 1;

            // Metadata update on when we read third time
            if i == 2 && c == 1 {
                let r = test_utils::get_status_code(&format!("http://{}@{}/admin/metadata?mode=updinfo&mount={}&url=url_here&song=title_here", AUTH_SOURCE, ADMIN, MOUNT_SOURCE)).await;
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

            r.read_exact(&mut buf).await.unwrap();
            r.read_exact(&mut len).await.unwrap();
            let metadata_len = (len[0] as usize) << 4;
            let mut metadata_buf = vec![0u8; metadata_len];
            r.read_exact(&mut metadata_buf).await.unwrap();
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
            let server1 = spawn_server(TEST_DIR, CONFIG, "stream.yaml").await;
            tokio::time::sleep(Duration::from_secs(2)).await;

            let status = server.child.try_wait();
            assert!(matches!(status, Ok(Some(_))));
            server = server1;
        }
    }

    let r = test_utils::get_status_code(&format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(r, 200);

    let server = spawn_server(TEST_DIR, CONFIG, "stream_aac.yaml").await;

    tokio::time::sleep(Duration::from_secs(4)).await;

    let (mut source_sock, media) = spawn_source_manual_aac(AUTH_SOURCE, ADMIN, MOUNT_AAC).unwrap();
    let mut stdout = media.stdout.unwrap();

    let mut ffmpeg_buf = [0u8; 4096];
    let mut total_written = 0;

    // Feed some AAC data first so listeners have something to connect to
    loop {
        let n = stdout.read(&mut ffmpeg_buf).expect("Should read from ffmpeg");
        if n == 0 {
            break;
        }
        source_sock.write_all(&ffmpeg_buf[..n]).expect("Should write to source socket");
        total_written += n;
        if total_written >= 50000 {
            break;
        }
    }

    // Connect a listener and verify we receive valid ADTS data
    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", BASE, MOUNT_AAC))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();

    // Verify the content-type is audio/aac
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "audio/aac");

    let mut r = resp.bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let mut read_buf = [0u8; 8192];
    let mut frames_found = 0;
    let mut data_read = 0;

    loop {
        let n = stdout.read(&mut ffmpeg_buf).unwrap_or(0);
        if n > 0 {
            source_sock.write_all(&ffmpeg_buf[..n]).expect("Should write to source socket");
        }

        match tokio::time::timeout(Duration::from_secs(2), r.read(&mut read_buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                data_read += n;
                for i in 0..read_buf.len().saturating_sub(1) {
                    if read_buf[i] == 0xFF && (read_buf[i + 1] & 0xF0) == 0xF0 {
                        frames_found += 1;
                    }
                }
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }

        if data_read >= 10000 && frames_found >= 2 {
            break;
        }
    }

    assert!(frames_found >= 2, "Should find at least 2 ADTS sync words, found {}", frames_found);
    assert!(data_read >= 1000, "Should have read at least 1000 bytes, got {}", data_read);

    let r = test_utils::get_status_code(&format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN)).await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn source_stream() {
    let mut server = spawn_server(TEST_DIR, CONFIG_SOURCE, "source_stream.yaml").await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    let mut source = spawn_source(AUTH_SOURCE, ADMIN2, MOUNT_SOURCE).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    for i in 0..3 {
        let resp = test_utils::reqwest::Client::new()
            .get(&format!("http://{}{}", BASE2, MOUNT_SOURCE))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status().as_u16(), 200);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert_eq!(ct, "audio/mpeg");

        let mut r = resp.bytes_stream()
            .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
            .into_async_read();

        let mut buf = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), r.read(&mut buf)).await;
        assert!(n.is_ok(), "Should receive data from source stream");
        let n = n.unwrap().unwrap();
        assert!(n > 0, "Should read at least some bytes from stream");

        drop(r);

        if i < 2 {
            let server1 = spawn_server(TEST_DIR, CONFIG_SOURCE, "source_stream.yaml").await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let status = server.child.try_wait();
            assert!(matches!(status, Ok(Some(_))));
            server = server1;
        }
    }

    _ = source.kill().await;

    let r = test_utils::get_status_code(&format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN2)).await;
    assert_eq!(r, 200);

    drop(server);
}

async fn feed_source_data(source_sock: &mut TcpStream, stdout: &mut impl Read, ffmpeg_buf: &mut [u8; 4096]) {
    let mut total_written = 0;
    loop {
        let n = stdout.read(ffmpeg_buf).expect("Should read from ffmpeg");
        if n == 0 {
            break;
        }
        source_sock.write_all(&ffmpeg_buf[..n]).expect("Should write to source socket");
        total_written += n;
        if total_written >= 50000 {
            break;
        }
    }
}

async fn verify_metaint_listener(base: &str, expected_metaint: usize) {
    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", base, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "audio/mpeg");

    let metaint = resp.headers().get("icy-metaint").unwrap().to_str().unwrap().to_string();
    assert_eq!(metaint, format!("{}", expected_metaint));

    let mut r = resp.bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let mut audio_buf = vec![0u8; expected_metaint];
    r.read_exact(&mut audio_buf).await.unwrap();

    let mut len_byte = [0u8; 1];
    r.read_exact(&mut len_byte).await.unwrap();
    let metadata_len = (len_byte[0] as usize) << 4;
    assert!(metadata_len > 0, "Metadata length should be greater than 0");

    let mut metadata_buf = vec![0u8; metadata_len];
    r.read_exact(&mut metadata_buf).await.unwrap();

    let metadata_str = std::str::from_utf8(&metadata_buf).unwrap();
    assert!(metadata_str.contains("StreamTitle="), "Metadata should contain StreamTitle");
}

async fn read_metaint_chunk(r: &mut (impl AsyncRead + Unpin), metaint: usize) -> usize {
    let audio_buf = &mut vec![0u8; metaint];
    _ = tokio::time::timeout(Duration::from_secs(10), r.read_exact(audio_buf)).await.unwrap();

    let mut len_byte = [0u8; 1];
    _ = tokio::time::timeout(Duration::from_secs(10), r.read_exact(&mut len_byte)).await.unwrap();
    let metadata_len = (len_byte[0] as usize) << 4;

    if metadata_len > 0 {
        let mut metadata_buf = vec![0u8; metadata_len];
        _ = tokio::time::timeout(Duration::from_secs(10), r.read_exact(&mut metadata_buf)).await.unwrap();
        let metadata_str = std::str::from_utf8(&metadata_buf).unwrap();
        assert!(metadata_str.contains("StreamTitle="), "Metadata should contain StreamTitle");
    }

    metadata_len
}

#[tokio::test]
async fn metaint_test() {
    let mut server = spawn_server(TEST_DIR, CONFIG_METAINT, "metaint.yaml").await;
    tokio::time::sleep(Duration::from_secs(4)).await;

    let (mut source_sock, media) = spawn_source_manual(AUTH_SOURCE, ADMIN3, MOUNT_SOURCE).unwrap();
    let mut stdout = media.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];

    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    // Persistent listener that must survive all migrations
    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", BASE3, MOUNT_SOURCE))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200);
    let mut r = resp.bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    // Read first metaint chunk before any migration
    assert!(read_metaint_chunk(&mut r, 10000).await > 0);

    for i in 0..3 {
        // Verify metaint and content-type via a fresh listener after each migration
        verify_metaint_listener(BASE3, 10000).await;

        if i < 2 {
            feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

            let server1 = spawn_server(TEST_DIR, CONFIG_METAINT, "metaint.yaml").await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            let status = server.child.try_wait();
            assert!(matches!(status, Ok(Some(_))));
            server = server1;

            feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;
        }

        // Persistent listener must still receive data after each migration (zero-downtime)
        let metadata_len = read_metaint_chunk(&mut r, 10000).await;
        assert!(metadata_len > 0, "Persistent listener must receive metadata after migration");
    }

    drop(r);

    let r = test_utils::get_status_code(&format!("http://{}@{}/admin/shutdown", AUTH_ADMIN, ADMIN3)).await;
    assert_eq!(r, 200);

    drop(server);
}
