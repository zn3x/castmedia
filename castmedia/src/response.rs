use anyhow::Result;
use tokio::io::AsyncWriteExt;

use crate::{server::Stream, source::IcyProperties};

fn current_time() -> String {
    chrono::offset::Utc::now()
        .format("%a, %e %b %Y %T GMT")
        .to_string()
}

async fn server_info(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("Server: {}\r\n\
Date: {}\r\n\
Cache-Control: no-cache, no-store\r\n\
Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n\
Pragma: no-cache\r\n\
Access-Control-Allow-Origin: *\r\n\r\n",
        server_id,
        current_time()
    ).as_bytes()).await?;

    stream.flush().await?;
    Ok(())
}

async fn write_status(stream: &mut Stream, server_id: &str, status: &[u8]) -> Result<()> {
    stream.write_all(status).await?;
    server_info(stream, server_id).await
}

async fn write_status_body(stream: &mut Stream, server_id: &str, status_header: &str, content_type: &str, body: &[u8]) -> Result<()> {
    stream.write_all(format!("{}\r\n\
Connection: close\r\n\
Content-Length: {}\r\n\
Content-Type: {}\r\n",
        status_header, body.len(), content_type
    ).as_bytes()).await?;
    server_info(stream, server_id).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn method_not_allowed(stream: &mut Stream, server_id: &str) -> Result<()> {
    write_status(stream, server_id, b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n").await
}

pub async fn not_found(stream: &mut Stream, server_id: &str) -> Result<()> {
    write_status(stream, server_id, b"HTTP/1.1 404 File Not Found\r\nConnection: close\r\n").await
}

pub async fn authentication_needed(stream: &mut Stream, server_id: &str) -> Result<()> {
    write_status(stream, server_id, b"HTTP/1.1 401 Authorization Required\r\nWWW-Authenticate: Basic realm=\"Icy Server\"\r\nConnection: close\r\n").await
}

pub async fn internal_error(stream: &mut Stream, server_id: &str) -> Result<()> {
    write_status(stream, server_id, b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n").await
}

pub async fn forbidden(stream: &mut Stream, server_id: &str, message: &str) -> Result<()> {
    write_status_body(stream, server_id, "HTTP/1.1 403 Forbidden", "text/plain; charset=utf-8", message.as_bytes()).await
}

pub async fn bad_request(stream: &mut Stream, server_id: &str, message: &str) -> Result<()> {
    write_status_body(stream, server_id, "HTTP/1.1 400 Bad request", "text/plain; charset=utf-8", message.as_bytes()).await
}

pub async fn ok_200(stream: &mut Stream, server_id: &str) -> Result<()> {
    write_status(stream, server_id, b"HTTP/1.1 200 OK\r\nConnection: close\r\n").await
}

pub async fn ok_200_json_body(stream: &mut Stream, server_id: &str, body: &[u8]) -> Result<()> {
    write_status_body(stream, server_id, "HTTP/1.1 200 OK", "application/json; charset=utf-8", body).await
}

#[derive(Default)]
pub struct ChunkedResponse {}

impl ChunkedResponse {
    pub async fn new(stream: &mut Stream, server_id: &str) -> Result<Self> {
        write_status(stream, server_id, b"HTTP/1.1 200 OK\r\n\
Connection: close\r\n\
Transfer-Encoding: Chunked\r\n\
Content-Type: application/json; charset=utf-8\r\n").await?;
        Ok(Self {})
    }

    /// Reuse stream already sent headers
    pub fn new_ready() -> Self {
        Self::default()
    }

    pub async fn send(&self, stream: &mut Stream, buf: &[u8]) -> Result<()> {
        stream.write_all(format!("{:x}\r\n", buf.len()).as_bytes()).await?;
        stream.write_all(buf).await?;
        stream.write_all(b"\r\n").await?;
        Ok(())
    }

    pub async fn flush(&self, stream: &mut Stream) -> Result<()> {
        stream.write_all(b"0\r\n\r\n").await?;
        stream.flush().await?;
        Ok(())
    }
}

pub async fn ok_200_icy_metadata(stream: &mut Stream, server_id: &str, properties: &IcyProperties, metaint: usize) -> Result<()> {
    let mut status = format!("HTTP/1.1 200 OK\r\n\
Connection: close\r\n\
Content-Type: {}\r\n\
icy-metaint: {}\r\n\
icy-description: {}\r\n\
icy-genre: {}\r\n\
icy-name: {}\r\n\
icy-pub: {}\r\n\
icy-url: {}\r\n",
        properties.content_type, metaint,
        properties.description.as_deref().unwrap_or("Unknown"),
        properties.genre.as_deref().unwrap_or("Unknown"),
        properties.name.as_deref().unwrap_or("Unknown"),
        properties.public as usize,
        properties.url.as_deref().unwrap_or("Unknown"),
    );

    if let Some(bitrate) = properties.bitrate.as_ref() {
        status.push_str(&format!("icy-bitrate: {}\r\n", bitrate));
    }

    write_status(stream, server_id, status.as_bytes()).await
}

pub async fn ok_200_listener(stream: &mut Stream, server_id: &str, properties: &IcyProperties) -> Result<()> {
    write_status(stream, server_id, format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: {}\r\n", properties.content_type).as_bytes()).await
}
