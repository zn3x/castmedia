use std::{os::{unix::net::UnixStream, fd::FromRawFd}, net::SocketAddr};

use anyhow::Result;
use base64::Engine;
use passfd::FdPassingExt;
use tokio::{net::TcpStream, io::BufStream};

use crate::server::Stream;

// Shamelessly taken from clean_path crate https://docs.rs/clean-path/latest/clean_path/
pub fn clean_path(path: &str) -> String {
    match path {
        "" => return ".".to_string(),
        "." => return ".".to_string(),
        ".." => return "..".to_string(),
        "/" => return "/".to_string(),
        _ => {}
    }

    let mut out = vec![];
    let is_root = path.starts_with('/');

    let path = path.trim_end_matches('/');
    let num_segments = path.split('/').count();

    for segment in path.split('/') {
        match segment {
            "" => continue,
            "." => {
                if num_segments == 1 {
                    out.push(segment);
                };
                continue;
            }
            ".." => {
                let previous = out.pop();
                if previous.is_some() && !can_backtrack(previous.unwrap()) {
                    out.push(previous.unwrap());
                    out.push(segment);
                } else if previous.is_none() && !is_root {
                    out.push(segment);
                };
                continue;
            }
            _ => {
                out.push(segment);
            }
        };
    }

    let mut out_str = out.join("/");

    if is_root {
        out_str = format!("/{}", out_str);
    }

    if out_str.is_empty() {
        return ".".to_string();
    }

    out_str
}

fn can_backtrack(segment: &str) -> bool {
    !matches!(segment, "." | "..")
}

#[derive(Debug)]
pub struct Query {
    pub key: String,
    pub val: String
}

pub fn get_queries(path: &str) -> Vec<Query> {
    let mut queries = Vec::new();
    if let Some(i) = path.find('?') {
        for query in path[i+1..].split('&') {
            if let Some((key, val)) = query.replace( '+', " " ).split_once('=') {
                let key = urlencoding::decode(key);
                let val = urlencoding::decode(val);
                if let Ok(key) = key {
                    if let Ok(val) = val {
                        queries.push(Query { key: key.to_string(), val: val.to_string() });
                    }
                }
            }
        }
    }

    queries
}

pub fn get_queries_val_for_keys<'a>(keys: &[&str], queries: &'a [Query]) -> Vec<Option<&'a str>> {
    let mut vals = vec![None; keys.len()];

    for i in 0..keys.len() {
        for query in queries {
            if query.key.eq(keys[i]) {
                vals[i] = Some(query.val.as_str());
                break;
            }
        }
    }

    vals
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

pub fn read_socket_from_unix_socket(unixsock: &mut UnixStream) -> Result<TcpStream> {
    let fd       = unixsock.recv_fd()?;
    // Safety: We just got the fd from older instance, we are sure it's a tcp socket
    let std_sock = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    std_sock.set_nonblocking(true)?;
    Ok(TcpStream::from_std(std_sock)?)
}

pub fn read_stream_from_unix_socket(unixsock: &mut UnixStream) -> Result<(Stream, SocketAddr)> {
    let sock = read_socket_from_unix_socket(unixsock)?;
    let addr = sock.peer_addr()?;
    let stream: Stream = Box::new(BufStream::new(sock));

    Ok((stream, addr))
}

#[cfg(test)]
mod tests {
    #[test]
    fn clean_path() {
        assert_eq!(super::clean_path("././file"), "file");
        assert_eq!(super::clean_path("./../file"), "../file");
        assert_eq!(super::clean_path("./../"), "..");
        assert_eq!(super::clean_path("/file.mp3"), "/file.mp3");
        assert_eq!(super::clean_path("/a/b/"), "/a/b");
        assert_eq!(super::clean_path("/a/../b/"), "/b");
    }

    #[test]
    fn queries() {
        let q = super::get_queries("/a?a=b&b=c");
        assert_eq!(q[0].key, "a".to_string());
        assert_eq!(q[0].val, "b".to_string());
        assert_eq!(q[1].key, "b".to_string());
        assert_eq!(q[1].val, "c".to_string());
        let q = super::get_queries("/a?a=b&b=c&a=d");
        assert_eq!(q[0].key, "a".to_string());
        assert_eq!(q[0].val, "b".to_string());
        assert_eq!(q[1].key, "b".to_string());
        assert_eq!(q[1].val, "c".to_string());
        assert_eq!(q[2].key, "a".to_string());
        assert_eq!(q[2].val, "d".to_string());
        let mut path = String::new();
        path.push('/');
        path.push_str(&urlencoding::encode(r#"'4"4ef$%GH?"#).to_string());
        path.push('?');
        path.push_str(&urlencoding::encode(r#"$O9r0#4="#).to_string());
        path.push('=');
        path.push_str(&urlencoding::encode(r#"#T343op"#).to_string());
        let q = super::get_queries(&path);
        assert_eq!(q[0].key, r#"$O9r0#4="#.to_string());
        assert_eq!(q[0].val, r#"#T343op"#.to_string());

        let q = super::get_queries("/a?a=b&b=c&a=v");
        let r = super::get_queries_val_for_keys(&["a", "b", "d"], &q);
        assert_eq!(r[0], Some("b"));
        assert_eq!(r[1], Some("c"));
        assert_eq!(r[2], None);
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
