use std::time::SystemTime;
use anyhow::Result;
use httpdate::fmt_http_date;
use tokio::io::AsyncWriteExt;

use crate::{server::Stream, source::IcyProperties};

async fn server_info(stream: &mut Stream) -> Result<()> {
    stream.write_all(format!("Date: {}\r\n\
    Cache-Control: no-cache, no-store\r\n\
    Expires: Mon, 26 Jul 1997 05:00:00 GMT\r\n\
    Pragma: no-cache\r\n\
    Access-Control-Allow-Origin: *\r\n\r\n",
    fmt_http_date(SystemTime::now())
    ).as_bytes()).await?;

    stream.flush().await?;
    Ok(())
}

pub async fn method_not_allowed(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 405 Method Not Allowed\r\n\
Server: {}\r\n\
Connection: close\r\n",
    server_id).as_bytes()).await?;

    server_info(stream).await?;
    Ok(())
}

pub async fn not_found(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 404 File Not Found\r\n\
Server: {}\r\n\
Connection: close\r\n",
    server_id).as_bytes()).await?;

    server_info(stream).await?;
    Ok(())
}

pub async fn authentication_needed(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 401 Authorization Required\r\n\
Server: {}\r\n\
WWW-Authenticate: Basic realm=\"Icy Server\"\r\n\
Connection: close\r\n",
    server_id).as_bytes()).await?;

    server_info(stream).await?;
    Ok(())
}

pub async fn internal_error(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 500 Internal Server Error\r\n\
Server: {}\r\n\
Connection: close\r\n",
    server_id).as_bytes()).await?;

    server_info(stream).await?;
    Ok(())
}

pub async fn forbidden(stream: &mut Stream, server_id: &str, message: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 403 Forbidden\r\n\
Server: {}\r\n\
Content-Type: text/plain; charset=utf-8\r\n\
Content-Length: {}\r\n\
Connection: close\r\n",
        server_id,
        message.len()
    ).as_bytes()).await?;

    server_info(stream).await?;
    stream.write_all(message.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

pub async fn bad_request(stream: &mut Stream, server_id: &str, message: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 400 Bad request\r\n\
Server: {}\r\n\
Content-Type: text/plain; charset=utf-8\r\n\
Content-Length: {}\r\n\
Connection: close\r\n",
        server_id,
        message.len()
    ).as_bytes()).await?;

    server_info(stream).await?;
    stream.write_all(message.as_bytes()).await?;
    stream.flush().await?;

    Ok(())
}

pub async fn ok_200(stream: &mut Stream, server_id: &str) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 200 OK\r\n\
Server: {}\r\n\
Connection: close\r\n",
    server_id).as_bytes()).await?;

    server_info(stream).await?;
    Ok(())
}

pub async fn ok_200_json_body(stream: &mut Stream, server_id: &str, body: &[u8]) -> Result<()> {
    stream.write_all(format!("HTTP/1.0 200 OK\r\n\
Server: {}\r\n\
Connection: close\r\n\
Content-Length: {}\r\n\
Content-Type: application/json; charset=utf-8\r\n",
        server_id,
        body.len()).as_bytes()).await?;

    server_info(stream).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

pub struct ChunkedResponse {}

impl ChunkedResponse {
    pub async fn new(stream: &mut Stream, server_id: &str) -> Result<Self> {
        stream.write_all(format!("HTTP/1.0 200 OK\r\n\
Server: {}\r\n\
Connection: close\r\n\
Transfer-Encoding: Chunked\r\n\
Content-Type: application/json; charset=utf-8\r\n",
            server_id).as_bytes()).await?;

        server_info(stream).await?;
        Ok(Self {})
    }

    pub async fn send(&self, stream: &mut Stream, buf: &[u8]) -> Result<()> {
        stream.write_all(format!("{:x}\r\n", buf.len()).as_bytes()).await?;
        stream.write_all(buf).await?;
        stream.write_all(b"\r\n").await?;
        Ok(())
    }

    pub async fn flush(&self, stream: &mut Stream) -> Result<()> {
        stream.write_all(b"0\r\n").await?;
        stream.flush().await?;
        Ok(())
    }
}

pub async fn ok_200_icy_metadata(stream: &mut Stream, server_id: &str, properties: &IcyProperties, metaint: usize) -> Result<()> {
    stream.write_all(format!("HTTP/1.1 200 OK\r\n\
Server: {}\r\n\
Connection: close\r\n\
icy-metaint: {}\r\n\
icy-description: {}\r\n\
icy-genre: {}\r\n\
icy-name: {}\r\n\
icy-pub: {}\r\n\
icy-url: {}\r\n",
    server_id, metaint,
    properties.description.as_ref().unwrap_or(&"Unknewn".to_string()),
    properties.genre.as_ref().unwrap_or(&"Unknewn".to_string()),
    properties.name.as_ref().unwrap_or(&"Unknewn".to_string()),
    properties.public as usize,
    properties.url.as_ref().unwrap_or(&"Unknewn".to_string()),
    ).as_bytes()).await?;

    if let Some(bitrate) = properties.bitrate.as_ref() {
        stream.write_all(format!("icy-bitrate: {}\r\n", bitrate).as_bytes()).await?;
    }

    server_info(stream).await?;
    Ok(())
}
