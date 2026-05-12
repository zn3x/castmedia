use std::{io::Write, time::Duration};

use futures::{AsyncReadExt, StreamExt, TryStreamExt};
use test_utils::{spawn_server, spawn_source_manual, spawn_source_manual_aac};

const CONFIG_FALLBACK: &str = "
address:
  - bind: 127.0.0.1:9020
metadata_interval: 30000
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
        fallback: '/fallback.mp3'
  sourcefb:
    pass: 0$pass
    role: source
    mount:
      - path: '/fallback.mp3'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9120
migrate:
  enabled: true
  bind: /tmp/migration_fallback.sock
misc:
  unsafe_pass: true
";

/// Config with two mounts for multi-mount migration test
const CONFIG_MULTI_MOUNT: &str = "
address:
  - bind: 127.0.0.1:9021
metadata_interval: 30000
limits:
  queue_size: 400000
  sources: 4
account:
  admin:
    pass: 0$pass
    role: admin
  source:
    pass: 0$pass
    role: source
    mount:
      - path: '/stream1.mp3'
      - path: '/stream2.mp3'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9121
migrate:
  enabled: true
  bind: /tmp/migration_multi.sock
misc:
  unsafe_pass: true
";

#[allow(dead_code)]
const CONFIG_NO_SOURCE_ACCOUNT: &str = "
address:
  - bind: 127.0.0.1:9022
metadata_interval: 30000
limits:
  queue_size: 400000
account:
  admin:
    pass: 0$pass
    role: admin
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9122
migrate:
  enabled: true
  bind: /tmp/migration_no_source.sock
misc:
  unsafe_pass: true
";

const CONFIG_IDLE: &str = "
address:
  - bind: 127.0.0.1:9024
metadata_interval: 30000
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
    bind: 127.0.0.1:9124
migrate:
  enabled: true
  bind: /tmp/migration_idle.sock
misc:
  unsafe_pass: true
";

const CONFIG_METADATA: &str = "
address:
  - bind: 127.0.0.1:9023
metadata_interval: 30000
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
    bind: 127.0.0.1:9123
migrate:
  enabled: true
  bind: /tmp/migration_metadata.sock
misc:
  unsafe_pass: true
";

const CONFIG_RAPID: &str = "
address:
  - bind: 127.0.0.1:9027
metadata_interval: 30000
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
    bind: 127.0.0.1:9127
migrate:
  enabled: true
  bind: /tmp/migration_rapid.sock
misc:
  unsafe_pass: true
";

const CONFIG_MOVECLIENTS: &str = "
address:
  - bind: 127.0.0.1:9025
metadata_interval: 30000
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
      - path: '/stream1.mp3'
      - path: '/stream2.mp3'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9125
migrate:
  enabled: true
  bind: /tmp/migration_moveclients.sock
misc:
  unsafe_pass: true
";

const CONFIG_AAC: &str = "
address:
  - bind: 127.0.0.1:9026
metadata_interval: 30000
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
      - path: '/stream.aac'
admin_access:
  enabled: true
  address:
    bind: 127.0.0.1:9126
migrate:
  enabled: true
  bind: /tmp/migration_aac.sock
misc:
  unsafe_pass: true
";

static TEST_DIR: &str = env!("CARGO_TARGET_TMPDIR");

const AUTH_ADMIN: &str = "admin:pass";
const AUTH_SOURCE: &str = "source:pass";
const AUTH_SOURCEFB: &str = "sourcefb:pass";

async fn feed_source_data(
    source_sock: &mut std::net::TcpStream,
    stdout: &mut impl std::io::Read,
    ffmpeg_buf: &mut [u8; 4096],
) {
    let mut total_written = 0;
    loop {
        let n = stdout.read(ffmpeg_buf).expect("Should read from ffmpeg");
        if n == 0 {
            break;
        }
        source_sock
            .write_all(&ffmpeg_buf[..n])
            .expect("Should write to source socket");
        total_written += n;
        if total_written >= 50000 {
            break;
        }
    }
}

#[tokio::test]
async fn migration_idle_server() {
    // Start server with no sources connected
    let mut server = spawn_server(TEST_DIR, CONFIG_IDLE, "migration_idle.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify no mounts exist
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9124"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(mounts.is_empty(), "Idle server should have no mounts");

    // Trigger migration by starting a new instance with the same socket
    let server1 = spawn_server(TEST_DIR, CONFIG_IDLE, "migration_idle.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Old instance should exit
    let status = server.child.try_wait();
    assert!(
        matches!(status, Ok(Some(_))),
        "Old idle server should have exited after migration"
    );
    server = server1;

    // New instance should still be running and have no mounts
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9124"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(mounts.is_empty(), "Migrated idle server should have no mounts");

    // Verify we can mount a source on the new instance
    let mut source = test_utils::spawn_source(AUTH_SOURCE, "127.0.0.1:9124", "/stream.mp3").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9124"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(
        mounts.contains_key("/stream.mp3"),
        "Should be able to mount after idle migration"
    );

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9124"
    ))
    .await;
    assert_eq!(r, 200);

    source.kill().await.ok();
    drop(server);
}

#[tokio::test]
async fn migration_fallback_mount() {
    let mut server = spawn_server(TEST_DIR, CONFIG_FALLBACK, "migration_fallback.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Mount both main and fallback sources
    let (mut main_sock, main_media) =
        spawn_source_manual(AUTH_SOURCE, "127.0.0.1:9120", "/stream.mp3").unwrap();
    let mut main_stdout = main_media.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut main_sock, &mut main_stdout, &mut ffmpeg_buf).await;

    let (mut fb_sock, fb_media) =
        spawn_source_manual(AUTH_SOURCEFB, "127.0.0.1:9120", "/fallback.mp3").unwrap();
    let mut fb_stdout = fb_media.stdout.unwrap();
    feed_source_data(&mut fb_sock, &mut fb_stdout, &mut ffmpeg_buf).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify fallback is set
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9120"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    let stream_info = mounts.get("/stream.mp3").unwrap().as_object().unwrap();
    assert_eq!(
        stream_info.get("fallback").unwrap().as_str(),
        Some("/fallback.mp3")
    );

    // Connect a listener to the main mount
    let resp = test_utils::reqwest::Client::new()
        .get("http://127.0.0.1:9020/stream.mp3")
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let mut listener_stream = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    // Read some data to confirm the listener is receiving
    let mut buf = [0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), listener_stream.read(&mut buf))
        .await
        .expect("Listener should receive data")
        .expect("Read should succeed");
    assert!(n > 0, "Listener should have received some data");

    // Trigger migration
    let server1 = spawn_server(TEST_DIR, CONFIG_FALLBACK, "migration_fallback.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    // Verify both mounts still exist after migration
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9120"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(
        mounts.contains_key("/stream.mp3"),
        "Main mount should survive migration"
    );
    assert!(
        mounts.contains_key("/fallback.mp3"),
        "Fallback mount should survive migration"
    );

    // Verify fallback is still set on the migrated mount
    let stream_info = mounts.get("/stream.mp3").unwrap().as_object().unwrap();
    assert_eq!(
        stream_info.get("fallback").unwrap().as_str(),
        Some("/fallback.mp3"),
        "Fallback config should survive migration"
    );

    // The persistent listener should still be receiving data after migration
    feed_source_data(&mut main_sock, &mut main_stdout, &mut ffmpeg_buf).await;
    let n = tokio::time::timeout(Duration::from_secs(5), listener_stream.read(&mut buf))
        .await
        .expect("Listener should still receive data after migration");
    assert!(
        n.unwrap_or(0) > 0,
        "Listener should receive data after migration"
    );

    drop(listener_stream);

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9120"
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn migration_multiple_mounts() {
    let mut server = spawn_server(TEST_DIR, CONFIG_MULTI_MOUNT, "migration_multi.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Mount two sources
    let (mut sock1, media1) =
        spawn_source_manual(AUTH_SOURCE, "127.0.0.1:9121", "/stream1.mp3").unwrap();
    let mut stdout1 = media1.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut sock1, &mut stdout1, &mut ffmpeg_buf).await;

    let (mut sock2, media2) =
        spawn_source_manual(AUTH_SOURCE, "127.0.0.1:9121", "/stream2.mp3").unwrap();
    let mut stdout2 = media2.stdout.unwrap();
    feed_source_data(&mut sock2, &mut stdout2, &mut ffmpeg_buf).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Connect listeners to both mounts
    let resp1 = test_utils::reqwest::Client::new()
        .get("http://127.0.0.1:9021/stream1.mp3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status().as_u16(), 200);
    let mut listener1 = resp1
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let resp2 = test_utils::reqwest::Client::new()
        .get("http://127.0.0.1:9021/stream2.mp3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status().as_u16(), 200);
    let mut listener2 = resp2
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    // Read some data to confirm
    let mut buf = [0u8; 1024];
    let n1 = tokio::time::timeout(Duration::from_secs(5), listener1.read(&mut buf))
        .await
        .expect("Listener 1 should receive data")
        .expect("Read should succeed");
    assert!(n1 > 0);

    let n2 = tokio::time::timeout(Duration::from_secs(5), listener2.read(&mut buf))
        .await
        .expect("Listener 2 should receive data")
        .expect("Read should succeed");
    assert!(n2 > 0);

    // Check stats before migration
    let stats = test_utils::get_response(&format!(
        "http://{}@{}/admin/stats",
        AUTH_ADMIN, "127.0.0.1:9121"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    assert_eq!(stats.get("active_sources").unwrap().as_u64().unwrap(), 2);
    assert!(stats.get("active_listeners").unwrap().as_u64().unwrap() >= 2);

    // Trigger migration
    let server1 = spawn_server(TEST_DIR, CONFIG_MULTI_MOUNT, "migration_multi.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    // Both mounts should still exist
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9121"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(mounts.contains_key("/stream1.mp3"));
    assert!(mounts.contains_key("/stream2.mp3"));

    // Both persistent listeners should still receive data
    feed_source_data(&mut sock1, &mut stdout1, &mut ffmpeg_buf).await;
    feed_source_data(&mut sock2, &mut stdout2, &mut ffmpeg_buf).await;

    let n1 = tokio::time::timeout(Duration::from_secs(5), listener1.read(&mut buf))
        .await
        .expect("Listener 1 should still receive data after migration");
    assert!(n1.unwrap_or(0) > 0);

    let n2 = tokio::time::timeout(Duration::from_secs(5), listener2.read(&mut buf))
        .await
        .expect("Listener 2 should still receive data after migration");
    assert!(n2.unwrap_or(0) > 0);

    drop(listener1);
    drop(listener2);

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9121"
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn migration_metadata_persistence() {
    let mut server = spawn_server(TEST_DIR, CONFIG_METADATA, "migration_metadata.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (mut source_sock, media) =
        spawn_source_manual(AUTH_SOURCE, "127.0.0.1:9123", "/stream.mp3").unwrap();
    let mut stdout = media.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Set metadata before migration
    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/metadata?mode=updinfo&mount=/stream.mp3&song=PreMigration&url=http://pre.example.com",
        AUTH_ADMIN, "127.0.0.1:9123"
    ))
    .await;
    assert_eq!(r, 200);

    // Verify metadata is set
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9123"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    let metadata = mounts
        .get("/stream.mp3")
        .unwrap()
        .as_object()
        .unwrap()
        .get("metadata")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(
        metadata.get("title").unwrap().as_str().unwrap(),
        "PreMigration"
    );
    assert_eq!(
        metadata.get("url").unwrap().as_str().unwrap(),
        "http://pre.example.com"
    );

    // Trigger migration
    let server1 = spawn_server(TEST_DIR, CONFIG_METADATA, "migration_metadata.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    // Verify mount still exists
    let mounts = test_utils::get_response(&format!(
        "http://{}@{}/admin/listmounts",
        AUTH_ADMIN, "127.0.0.1:9123"
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let mounts = mounts.as_object().unwrap();
    assert!(mounts.contains_key("/stream.mp3"));

    // Verify metadata is preserved after migration
    let metadata = mounts
        .get("/stream.mp3")
        .unwrap()
        .as_object()
        .unwrap()
        .get("metadata")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(
        metadata.get("title").unwrap().as_str().unwrap(),
        "PreMigration",
        "Metadata title should survive migration"
    );
    assert_eq!(
        metadata.get("url").unwrap().as_str().unwrap(),
        "http://pre.example.com",
        "Metadata URL should survive migration"
    );

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9123"
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn moveclients_then_migration() {
    let mut server =
        spawn_server(TEST_DIR, CONFIG_MOVECLIENTS, "moveclients_migration.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let base = "127.0.0.1:9025";
    let admin = "127.0.0.1:9125";

    // Mount two sources
    let (mut sock1, media1) = spawn_source_manual(AUTH_SOURCE, admin, "/stream1.mp3").unwrap();
    let mut stdout1 = media1.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut sock1, &mut stdout1, &mut ffmpeg_buf).await;

    let (mut sock2, media2) = spawn_source_manual(AUTH_SOURCE, admin, "/stream2.mp3").unwrap();
    let mut stdout2 = media2.stdout.unwrap();
    feed_source_data(&mut sock2, &mut stdout2, &mut ffmpeg_buf).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Connect a listener to stream1
    let resp = test_utils::reqwest::Client::new()
        .get(&format!("http://{}{}", base, "/stream1.mp3"))
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let mut listener = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    // Read some data
    let mut buf = [0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf)).await;

    // Move the listener from stream1 to stream2 using admin command
    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/moveclients?mount=/stream1.mp3&destination=/stream2.mp3",
        AUTH_ADMIN, admin
    ))
    .await;
    assert_eq!(r, 200, "moveclients should succeed");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // The listener should now be on stream2
    let clients = test_utils::get_response(&format!(
        "http://{}@{}/admin/listclients?mount=/stream2.mp3",
        AUTH_ADMIN, admin
    ))
    .await
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let clients = clients.as_object().unwrap();
    assert!(
        !clients.is_empty(),
        "Moved listener should appear on stream2"
    );

    // Now trigger migration while the moved listener is active
    let server1 = spawn_server(TEST_DIR, CONFIG_MOVECLIENTS, "moveclients_migration.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    // The listener should still be receiving data after migration
    feed_source_data(&mut sock2, &mut stdout2, &mut ffmpeg_buf).await;
    let n = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Moved listener should still receive data after migration");
    assert!(n.unwrap_or(0) > 0);

    drop(listener);

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, admin
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn rapid_successive_migrations() {
    let mut server = spawn_server(TEST_DIR, CONFIG_RAPID, "rapid_migration.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (mut source_sock, media) =
        spawn_source_manual(AUTH_SOURCE, "127.0.0.1:9127", "/stream.mp3").unwrap();
    let mut stdout = media.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    // Connect a persistent listener
    let resp = test_utils::reqwest::Client::new()
        .get("http://127.0.0.1:9027/stream.mp3")
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let mut listener = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let mut buf = [0u8; 1024];
    let _ = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Initial read should work");

    // First migration
    let server1 = spawn_server(TEST_DIR, CONFIG_RAPID, "rapid_migration.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    // Verify listener still works after first migration
    let n = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Should receive data after first migration");
    assert!(n.unwrap_or(0) > 0);

    // Second migration immediately
    let server2 = spawn_server(TEST_DIR, CONFIG_RAPID, "rapid_migration.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server2;

    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    // Verify listener still works after second migration
    let n = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Should receive data after second migration");
    assert!(n.unwrap_or(0) > 0);

    drop(listener);

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9127"
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}

#[tokio::test]
async fn migration_aac_stream() {
    let _admin = "127.0.0.1:9126";
    let _base = "127.0.0.1:9026";

    let mut server = spawn_server(TEST_DIR, CONFIG_AAC, "migration_aac.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (mut source_sock, media) =
        spawn_source_manual_aac(AUTH_SOURCE, "127.0.0.1:9126", "/stream.aac").unwrap();
    let mut stdout = media.stdout.unwrap();
    let mut ffmpeg_buf = [0u8; 4096];
    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;

    // Connect a listener
    let resp = test_utils::reqwest::Client::new()
        .get("http://127.0.0.1:9026/stream.aac")
        .header("Icy-Metadata", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "audio/aac");
    let mut listener = resp
        .bytes_stream()
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)))
        .into_async_read();

    let mut buf = [0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Should receive AAC data")
        .expect("Read should succeed");
    assert!(n > 0, "Should receive some AAC data");

    // Trigger migration
    let server1 = spawn_server(TEST_DIR, CONFIG_AAC, "migration_aac.yaml").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = server.child.try_wait();
    assert!(matches!(status, Ok(Some(_))));
    server = server1;

    // Feed more data and verify listener still works
    feed_source_data(&mut source_sock, &mut stdout, &mut ffmpeg_buf).await;
    let n = tokio::time::timeout(Duration::from_secs(5), listener.read(&mut buf))
        .await
        .expect("Should still receive AAC data after migration");
    assert!(n.unwrap_or(0) > 0, "AAC listener should survive migration");

    drop(listener);

    let r = test_utils::get_status_code(&format!(
        "http://{}@{}/admin/shutdown",
        AUTH_ADMIN, "127.0.0.1:9126"
    ))
    .await;
    assert_eq!(r, 200);

    drop(server);
}
