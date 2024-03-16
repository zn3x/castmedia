use core::panic;
use std::{net::TcpStream, io::{Write, Read}, process::Stdio};

use base64::Engine;
pub use reqwest;

pub struct Server {
    pub child: tokio::process::Child
}

impl Drop for Server {
    fn drop(&mut self) {
        _ = self.child.start_kill();
    }
}

pub async fn spawn_server(test_dir: &str, conf: &str, conf_name: &str) -> Server {
    let conf = conf.to_owned();

    let conf_file = format!("{}/{}", test_dir, conf_name);

    tokio::fs::write(&conf_file, conf).await
        .expect("Failed to write config file");
    
    let server = tokio::process::Command::new("cargo")
        .args([
              "run",
              "--",
              &conf_file
        ])
        .spawn()
        .expect("failed to start castmedia");

    Server {
        child: server
    }
}

pub async fn get_response(url: &str) -> reqwest::Response {
    let resp = reqwest::get(url).await;
    assert!(resp.is_ok());
    resp.unwrap()
}

pub async fn get_status_code(url: &str) -> u16 {
    let resp = get_response(url).await;
    resp.status().as_u16()
}

pub async fn spawn_source(auth: &str, addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("ffmpeg")
        .args([
              "-loglevel", "panic",
              "-re", "-f", "lavfi",
              "-i", "sine=frequency=1000",
              "-content_type", "audio/mpeg",
              "-vn", "-f", "mp3",
              "-b:a", "320k",
              &format!("icecast://{}@{}{}", auth, addr, mount)
        ])
        .spawn()
        .expect("ffmpeg missing")
}

pub fn spawn_source_manual(auth: &str, addr: &str, mount: &str) -> (TcpStream, std::process::Child) {
    let mut sock = TcpStream::connect(addr)
        .expect("Should be able to connect");

    let bs64 = base64::engine::general_purpose::URL_SAFE;

    sock.write_all(format!("SOURCE {} HTTP/1.1\r\nHost: {}\r\nContent-Type: audio/mpeg\r\nAuthorization: Basic {}\r\nConnection: close\r\nIcy-Metadata: 1\r\nIce-Public: 1\r\n\r\n",
                       mount, addr, bs64.encode(auth)).as_bytes())
        .expect("Should be able to write");


    let mut buf     = [0u8; 1];
    let mut headers = Vec::new();
    loop {
        sock.read_exact(&mut buf).expect("Can't read");
        headers.push(buf[0]);

        if headers.windows(4).last() == Some(b"\r\n\r\n") {
            break;
        }
    };

    if !headers.starts_with(b"HTTP/1.1 200 OK") {
        panic!("Source: got bad response");
    }

    let media = std::process::Command::new("ffmpeg")
        .args([
              "-loglevel", "panic",
              "-f", "lavfi",
              "-i", "sine=frequency=1000",
              "-content_type", "audio/mpeg",
              "-vn", "-f", "mp3",
              "-b:a", "320k",
              "-"
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("ffmpeg missing");

    (sock, media)
}

pub async fn spawn_listener(addr: &str, mount: &str) -> tokio::process::Child {
    tokio::process::Command::new("curl")
        .args([
              "-o", "/dev/null",
              "-L",
              "-s",
              &format!("http://{}{}", addr, mount)
        ])
        .spawn()
        .expect("curl missing")
}
