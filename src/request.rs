use std::time::Duration;
use anyhow::Result;
use httparse::Status;
use tokio::io::AsyncReadExt;

use crate::{
    server::{ClientSession, Stream},
    utils::{self, get_basic_auth, Query}
};

pub struct Request<'a> {
    pub headers_buf: Vec<u8>,
    pub headers: Vec<httparse::Header<'a>>,
    pub method: &'a str
}

#[derive(Debug)]
pub enum RequestType {
    Source(SourceRequest),
    Listen(ListenRequest),
    Admin(AdminRequest),
    Api(ApiRequest)
}

#[derive(Debug)]
pub struct AdminRequest {
    pub path: String,
    pub queries: Vec<Query>,
    pub auth: Option<(String, String)>
}

#[derive(Debug)]
pub struct SourceRequest {
    pub mountpoint: String,
    pub auth: Option<(String, String)>
}

#[derive(Debug)]
pub struct ListenRequest {
    pub mountpoint: String
}

#[derive(Debug)]
pub struct ApiRequest {
    pub path: String,
    pub queries: Vec<Query>
}

async fn read_request_header(stream: &mut Stream, buf: &mut Vec<u8>, max_len: usize) -> Result<()> {
    let mut byte = [ 0; 1 ];
    while buf.windows(4).last() != Some(b"\r\n\r\n") {
        match stream.read(&mut byte).await {
            Ok(read) => if read > 0 {
                buf.push(byte[0]);
                if buf.len() > max_len {
                    // Stop any potential attack
                    return Err(anyhow::Error::msg("Header is too big"));
                }
            } else {
                // Here we already read whole header
                break;
            }
            Err(e) => return Err(anyhow::Error::from(e))
        }
    }

    Ok(())
}

pub async fn read_request<'a>(session: &mut ClientSession, request: &'a mut Request<'a>) -> Result<RequestType> {
    // We first read header using predefined timeout
    tokio::time::timeout(
        Duration::from_millis(session.server.config.limits.header_timeout),
        read_request_header(&mut session.stream, &mut request.headers_buf, session.server.config.limits.http_max_len)
    ).await??;


    // Now we parse the headers
    // We can guess number of headers by counting \r\n occurences - 2
    // One is for first line of headers then another at the end of headers
    let occurences  = request.headers_buf
        .windows(2)
        .filter(|x| x.eq(b"\r\n"))
        .count();
    if occurences <= 2 {
        // Avoid empty headers attack
        return Err(anyhow::Error::msg("Received empty header"));
    }
    request.headers = vec![ httparse::EMPTY_HEADER; occurences - 2 ];
    let mut req     = httparse::Request::new(&mut request.headers);
    if req.parse(&request.headers_buf)? == Status::Partial {
        return Err(anyhow::Error::msg("Received an incomplete request"));
    }

    if !req.headers.first().is_some_and(|x| !x.name.is_empty()) {
        // Httparse may parse faulty headers (ie. one without value)
        // without sanity checks
        // doing it here
        return Err(anyhow::Error::msg("Parsed invalid headers"));
    }

    request.method = match req.method {
        Some(v) => v,
        None => return Err(anyhow::Error::msg("Request header has no method"))
    };

    let path = match req.path {
        Some(v) => v,
        None => return Err(anyhow::Error::msg("Request header has no path"))
    };

    let queries = utils::get_queries(path);
    let path    = utils::clean_path(path);

    // Now we check request made by user
    match request.method {
        // ICECAST protocol info: https://gist.github.com/ePirat/adc3b8ba00d85b7e3870
        "PUT" | "SOURCE" => {
            let auth = get_basic_auth(&request.headers)?;

            Ok(RequestType::Source(SourceRequest {
                mountpoint: path,
                auth
            }))
        },
        "GET" => {
            let source_id = utils::clean_path(&path);

            if source_id.starts_with("/admin/") {
                if !session.admin_addr {
                    // If this is not an admin interface we properly block this request
                    return Err(anyhow::Error::msg("Attempt to access admin api from public interface"));
                }

                let auth = get_basic_auth(&request.headers)?;
                // Warning!! Don't forget to check user && pass are empty
                if let Some((u, p)) = auth.as_ref() {
                    if u.is_empty() || p.is_empty() {
                        return Err(anyhow::Error::msg("Empty Basic authentication"));
                    }
                }

                let p = path.split('?').collect::<Vec<&str>>();
                return Ok(RequestType::Admin(AdminRequest { path: p[0].to_owned(), queries, auth }));
            } else if source_id.starts_with("/api/") {
                let p = path.split('?').collect::<Vec<&str>>();
                return Ok(RequestType::Api(ApiRequest { path: p[0].to_owned(), queries }));
            }

            if !session.server.sources.read().await.contains_key(&source_id) {
                return Err(anyhow::Error::msg("Unknewn path wanted by client"));
            }

            Ok(RequestType::Listen(ListenRequest { mountpoint: source_id }))
        },
        _ => Err(anyhow::Error::msg("Unknewn method sent by user"))
    }
}
