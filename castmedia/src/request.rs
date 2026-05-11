use std::{time::Duration, net::SocketAddr};
use anyhow::Result;
use hashbrown::HashMap;
use httparse::Status;

use crate::{
    server::ClientSession,
    utils::{self, get_basic_auth, get_header}, response
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
    pub queries: HashMap<String, String>,
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
    pub queries: HashMap<String, String>
}

pub async fn read_request<'a>(session: &mut ClientSession, request: &'a mut Request<'a>) -> Result<RequestType> {
    request.headers_buf = tokio::time::timeout(
        Duration::from_millis(session.server.config.limits.header_timeout),
        utils::read_http_headers(&mut session.stream, session.server.config.limits.http_max_len)
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

    if session.server.config.misc.check_forwardedfor {
        if let Some(addr) = get_header("x-forwarded-for", &request.headers)
            .and_then(|header| std::str::from_utf8(header).ok())
            .and_then(|addr_str| addr_str.parse::<SocketAddr>().ok()) {
            session.addr = addr;
        }
    }

    let queries = utils::get_queries(path)?;
    let path    = clean_path::clean(path).to_str().map(|x| x.to_string()).ok_or(anyhow::anyhow!("Can't parse path"))?;

    // Now we check request made by user
    match request.method {
        // ICECAST protocol info: https://gist.github.com/ePirat/adc3b8ba00d85b7e3870
        "PUT" | "SOURCE" => {
            let auth = match get_basic_auth(&request.headers) {
                Ok(v) => v,
                Err(e) => {
                    _ = response::authentication_needed(&mut session.stream, &session.server.config.info.id).await;
                    return Err(e);
                }
            };

            Ok(RequestType::Source(SourceRequest {
                mountpoint: path,
                auth
            }))
        },
        "GET" => {
            if path.starts_with("/admin/") {
                let auth = match get_basic_auth(&request.headers) {
                    Ok(v) => v,
                    Err(e) => {
                        _ = response::authentication_needed(&mut session.stream, &session.server.config.info.id).await;
                        return Err(e);
                    }
                };
                // Warning!! Don't forget to check user && pass are empty
                if let Some((u, p)) = auth.as_ref() {
                    if u.is_empty() || p.is_empty() {
                        return Err(anyhow::Error::msg("Empty Basic authentication"));
                    }
                }

                let p = path.split('?').collect::<Vec<&str>>();
                return Ok(RequestType::Admin(AdminRequest { path: p[0].to_owned(), queries, auth }));
            } else if path.starts_with("/api/") {
                let p = path.split('?').collect::<Vec<&str>>();
                return Ok(RequestType::Api(ApiRequest { path: p[0].to_owned(), queries }));
            }

            if !session.server.sources.read().await.contains_key(&path) {
                _ = response::not_found(&mut session.stream, &session.server.config.info.id).await;
                return Err(anyhow::Error::msg("Unknown path wanted by client"));
            }

            Ok(RequestType::Listen(ListenRequest { mountpoint: path }))
        },
        _ => {
            _ = response::method_not_allowed(&mut session.stream, &session.server.config.info.id).await;
            Err(anyhow::Error::msg("Unknown method sent by user"))
        }
    }
}
