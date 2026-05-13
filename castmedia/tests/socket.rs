use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use castmedia::server::Stream;

async fn connected_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

#[tokio::test]
async fn prefixed_stream_empty() {
    let (mut client, server) = connected_pair().await;
    let mut stream = Stream::new_migrated(server, vec![]);

    client.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
}

#[tokio::test]
async fn prefixed_stream_prefix_before_data() {
    let (mut client, server) = connected_pair().await;
    let mut stream = Stream::new_migrated(server, b"\x01\x02\x03".to_vec());

    client.write_all(b"abc").await.unwrap();

    let mut buf = [0u8; 6];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf[..3], b"\x01\x02\x03");

    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 3);
    assert_eq!(&buf[..3], b"abc");
}

#[tokio::test]
async fn prefixed_stream_partial_reads() {
    let (mut client, server) = connected_pair().await;
    let mut stream = Stream::new_migrated(server, b"\x01\x02\x03\x04\x05".to_vec());

    let mut buf = [0u8; 2];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf, &[0x01, 0x02]);

    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf, &[0x03, 0x04]);

    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 1);
    assert_eq!(buf[0], 0x05);

    client.write_all(b"data").await.unwrap();
    let mut data_buf = [0u8; 4];
    stream.read_exact(&mut data_buf).await.unwrap();
    assert_eq!(&data_buf, b"data");
}

#[tokio::test]
async fn prefixed_stream_write() {
    let (mut client, server) = connected_pair().await;
    let mut stream = Stream::new_migrated(server, b"\x01".to_vec());

    stream.write_all(b"write_test").await.unwrap();
    stream.flush().await.unwrap();

    let mut buf = [0u8; 10];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"write_test");

    let mut small_buf = [0u8; 1];
    stream.read_exact(&mut small_buf).await.unwrap();
    assert_eq!(small_buf[0], 0x01);
}

#[tokio::test]
async fn prefixed_stream_interleaved() {
    let (mut client, server) = connected_pair().await;
    let mut stream = Stream::new_migrated(server, b"\xAA\xBB".to_vec());

    let mut buf = [0u8; 2];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &[0xAA, 0xBB]);

    stream.write_all(b"from_server").await.unwrap();
    stream.flush().await.unwrap();

    let mut read_buf = [0u8; 11];
    client.read_exact(&mut read_buf).await.unwrap();
    assert_eq!(&read_buf, b"from_server");

    client.write_all(b"from_client").await.unwrap();

    let mut read_buf2 = [0u8; 11];
    stream.read_exact(&mut read_buf2).await.unwrap();
    assert_eq!(&read_buf2, b"from_client");
}

#[tokio::test]
async fn prefixed_stream_socket_properties() {
    let (_client, server) = connected_pair().await;
    let stream = Stream::new_migrated(server, b"\x01".to_vec());

    let local = stream.local_addr().unwrap();
    let peer = stream.peer_addr().unwrap();

    assert!(local.ip().is_loopback());
    assert!(peer.ip().is_loopback());
    assert_ne!(local.port(), 0);
    assert_ne!(peer.port(), 0);
}

#[tokio::test]
async fn prefixed_stream_consume_all() {
    let (_client, server) = connected_pair().await;
    let prefix = b"\xDE\xAD\xBE\xEF";
    let stream = Stream::new_migrated(server, prefix.to_vec());

    let (_sock, remaining) = stream.prepare_for_migration();
    assert_eq!(remaining, prefix);
}

#[tokio::test]
async fn prefixed_stream_migration_roundtrip() {
    let (mut client, server_incoming) = connected_pair().await;
    let server = Stream::new_migrated(server_incoming, b"\x10\x20\x30".to_vec());

    client.write_all(b"remaining_data").await.unwrap();

    let mut server = server;
    let mut buf = [0u8; 2];
    server.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &[0x10, 0x20]);

    let (raw_sock, leftover) = server.prepare_for_migration();

    let mut restored = Stream::new_migrated(raw_sock, leftover);
    let mut buf = [0u8; 1];
    restored.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], 0x30);

    let mut big_buf = [0u8; 14];
    restored.read_exact(&mut big_buf).await.unwrap();
    assert_eq!(&big_buf, b"remaining_data");
}
