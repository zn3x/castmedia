use std::os::{unix::net::UnixStream, fd::FromRawFd};

use anyhow::Result;
use base64::Engine;
use passfd::FdPassingExt;
use tokio::net::TcpStream;
use hashbrown::HashMap;
use url::Url;

use crate::server::Stream;
use tokio::io::AsyncReadExt;

pub fn get_queries(path: &str) -> Result<HashMap<String, String>> {
    let parsed_url = Url::parse(&format!("s://h{}", path))?;
    Ok(parsed_url.query_pairs().into_owned().collect())
}

/// Get header from headers list if it exits
/// Key given to this function *must be lowercase*
pub fn get_header< 'a >(key: &str, headers: &[httparse::Header< 'a >]) -> Option<&'a [ u8 ]> {
    for header in headers {
        if header.name.to_lowercase() == key {
            return Some(header.value)
        }
    }
    None
}

pub fn get_basic_auth( headers: &[httparse::Header] ) -> Result<Option<(String, String)>> {
    if let Some(auth) = get_header("authorization", headers) {
        let basic_auth = std::str::from_utf8(auth)?.replace("Basic ", "");
        let bs64       = base64::engine::general_purpose::STANDARD;
        if let Some((name, pass)) = std::str::from_utf8(&bs64.decode(basic_auth)?)?.split_once(':') {
            return Ok(Some((String::from(name), String::from(pass))))
        }
    }
    Ok(None)
}

pub fn basic_auth(user: &str, pass: &str) -> String {
    let mut s = String::from(user);
    s.push(':');
    s.push_str(pass);
    let bs64 = base64::engine::general_purpose::STANDARD;
    bs64.encode(s)
}

pub fn concat_path(url: &str, path: &str) -> String {
    let mut url = url.to_string();
    if url.as_str().ends_with('/') {
        url.push_str(&path[1..]);
    } else {
        url.push_str(path);
    }

    url
}

pub async fn read_http_headers(stream: &mut Stream, max_len: usize) -> Result<Vec<u8>> {
    let mut buf  = Vec::new();
    let mut byte = [0; 1];
    loop {
        match stream.read(&mut byte).await {
            Ok(0) => break,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.len() >= 4 && buf[buf.len()-4..] == *b"\r\n\r\n" {
                    break;
                }
                if buf.len() > max_len {
                    return Err(anyhow::Error::msg("Header is too big"));
                }
            },
            Err(e) => return Err(e.into())
        }
    }
    Ok(buf)
}

pub async fn hang() -> ! {
    std::future::pending::<()>().await;
    unreachable!()
}

pub fn read_stream_from_unix_socket(unixsock: &mut UnixStream) -> Result<TcpStream> {
    let fd       = unixsock.recv_fd()?;
    // Safety: We just got the fd from older instance, we are sure it's a tcp socket
    let std_sock = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    std_sock.set_nonblocking(true)?;

    Ok(TcpStream::from_std(std_sock)?)
}

#[cfg(test)]
mod tests {
    #[test]
    fn queries() {
        let q = super::get_queries("/a?a=b&b=c").unwrap();
        assert_eq!(q.get("a").map(|s| s.as_str()), Some("b"));
        assert_eq!(q.get("b").map(|s| s.as_str()), Some("c"));
        assert_eq!(q.len(), 2);

        let q = super::get_queries("/a?a=b&b=c&a=d").unwrap();
        assert_eq!(q.get("a").map(|s| s.as_str()), Some("d"));
        assert_eq!(q.get("b").map(|s| s.as_str()), Some("c"));

        let q = super::get_queries("/a?%24O9r0%234%3D=%23T343op&a=b").unwrap();
        assert_eq!(q.get("$O9r0#4=").map(|s| s.as_str()), Some("#T343op"));
        assert_eq!(q.get("a").map(|s| s.as_str()), Some("b"));

        let q = super::get_queries("/a?a=b&b=c&a=v").unwrap();
        assert_eq!(q.get("a").map(|s| s.as_str()), Some("v"));
        assert_eq!(q.get("b").map(|s| s.as_str()), Some("c"));
        assert_eq!(q.get("d"), None);
    }

    #[test]
    fn basic_auth() {
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let (_, heads)  = match httparse::parse_headers(
            b"Host: 127.0.0.1\r\nAuthorization: Basic YWRtaW46cGFzcw==\r\n\r\n",
            &mut headers)
            .unwrap() {
            httparse::Status::Complete(v) => v,
            _ => unreachable!()
        };

        let auth = super::get_basic_auth(heads)
            .unwrap();
        assert_eq!(auth, Some(("admin".to_string(), "pass".to_string())));

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let (_, heads)  = match httparse::parse_headers(
            b"Host: 127.0.0.1\r\nAuthorization: Basic garbage\r\n\r\n",
            &mut headers)
            .unwrap() {
            httparse::Status::Complete(v) => v,
            _ => unreachable!()
        };

        let auth = super::get_basic_auth(heads);
        assert!(auth.is_err());

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let (_, heads)  = match httparse::parse_headers(
            b"Host: 127.0.0.1\r\nAuthorization: Basic YWRtaW5wYXNz\r\n\r\n",
            &mut headers)
            .unwrap() {
            httparse::Status::Complete(v) => v,
            _ => unreachable!()
        };

        let auth = super::get_basic_auth(heads)
            .unwrap();
        assert_eq!(auth, None);
    }
}
