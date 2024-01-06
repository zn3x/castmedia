use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration, net::SocketAddr
};
use anyhow::Result;
use ::futures::{future::select_all, FutureExt};
use hashbrown::{HashMap, hash_map::OccupiedError};
use qanat::broadcast::RecvError;
use serde_json::json;
use tokio::{
    net::TcpStream,
    io::{BufStream, AsyncWriteExt}
};
use tokio_native_tls::native_tls::TlsConnector;
use tracing::{info, error};
use url::Url;
use serde::{Deserialize, Serialize};

use crate::{
    server::{Server, Stream, Socket, Session, ClientSession},
    utils::get_header, config::MasterServerRelayScheme,
    http::ResponseReader,
    client::{SourceInfo, RelayedInfo, RelayStream},
    source::IcyProperties, response::ChunkedResponse
};

#[derive(Debug, Deserialize)]
struct MasterMounts {
    mounts: Vec<String>
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum MountUpdate {
    New {
        mount: String,
        properties: IcyProperties
    },
    Metadata {
        mount: String,
        metadata: Vec<u8>
    },
    Unmounted {
        mount: String
    }
}

pub async fn master_mount_updates(session: &mut ClientSession, user_id: String) -> Result<()> {
    info!("Mount updates stream initialized for {} ({})", user_id, session.addr);

    let chunked_writer = ChunkedResponse::new_ready();

    let mut new_source_notify = session.server
        .relay_params
        .new_source_event_rx
        .clone();

    // map holding all mounts as tuple (mount, metadata_channel)
    let mut mounts = HashMap::new();
    // vec holding futures of reads we must perform on each source channel
    let mut reads;
    let mut ret;
    let mut remove = None;

    'NO_SOURCE: loop {
        ret = new_source_notify.recv().await;
        'SOURCE: loop {
            if ret.is_err() {
                // This should never happen
                unreachable!("new_source_event_channel should not be closed");
            }

            let mut new_mounts = Vec::new();
            // We here only insert new sources
            for m in session.server.sources.read().await.iter() {
                match mounts.try_insert(m.0.to_owned(), m.1.meta_broadcast.clone()) {
                    Err(OccupiedError { mut entry, value }) => {
                        if !entry.get().same_channel(&value) {
                            entry.insert(value);
                            new_mounts.push((m.0.clone(), m.1.properties.clone()));
                        }
                    },
                    Ok(_) => {
                        new_mounts.push((m.0.clone(), m.1.properties.clone()));
                    },
                }
            }
            
            // Reread everything, we got premature notification
            if mounts.is_empty() {
                continue 'NO_SOURCE;
            }
            
            // Now we write new sources to slave
            for new_mount in new_mounts {
                let ser = serde_json::to_vec(&json!({
                    "mount": new_mount.0,
                    "properties": new_mount.1.as_ref(),
                    "type": "new"
                }))?;

                chunked_writer.send(&mut session.stream, &ser).await?;
            }
            session.stream.flush().await?;

            loop {
                reads = Vec::new();
                if let Some(rem) = remove.take() {
                    mounts.remove(&rem);
                }
                // we must prevent polling empty futures list
                if mounts.is_empty() {
                    continue 'NO_SOURCE;
                }

                for m in &mut mounts {
                    reads.push(async {
                        (m.0.clone(), m.1.recv().await)
                    }.boxed());
                }
                tokio::select! {
                    ((mount, metadata), _, _) = select_all(reads) => {
                        let ser = match metadata {
                            Ok(v) => {
                                // TODO: Can we use futures returned by select_all directly
                                // Holy borrowing hell
                                // We need to re-add recv future for this channel
                                serde_json::to_vec(&json!({
                                    "mount": mount,
                                    "metadata": v.as_ref(),
                                    "type": "metadata"
                                }))?
                            },
                            Err(RecvError::Lagged(_)) => continue,
                            Err(RecvError::Closed) => {
                                let ser = serde_json::to_vec(&json!({
                                    "mount": mount,
                                    "type": "unmounted"
                                }))?;

                                // We remove source because it's no longer active
                                remove = Some(mount);

                                ser
                            }
                        };
                        
                        chunked_writer.send(&mut session.stream, &ser).await?;
                        session.stream.flush().await?;
                    },
                    r = new_source_notify.recv() => {
                        ret = r;
                        continue 'SOURCE;
                    }
                }
            };
        };
    };
}

async fn http_get_request(url: &Url, path: &str, headers: &str) -> Result<(Stream, SocketAddr)> {
    // We did already check before running
    let host = url.host()
        .expect("Should be able to fetch master host")
        .to_string();
    let port = url.port()
        .expect("Should be able to fetch master port");
    let stream     = TcpStream::connect(format!("{}:{}", host, port)).await?;
    let addr       = stream.peer_addr()?;
    let mut stream = if url.scheme().eq("https") {
        let cx     = tokio_native_tls::TlsConnector::from(TlsConnector::builder().build()?);
        Box::new(BufStream::new(cx.connect(&host, stream).await?))
    } else {
        Box::new(BufStream::new(stream)) as Stream
    };

    stream.write_all(format!("GET {} HTTP/1.1\r\n\
Host: {}:{}\r\n\
User-Agent: {}\r\n{}\
Connection: close\r\n\r\n",
        path,
        host, port,
        crate::config::SERVER_ID,
        headers
    ).as_bytes()).await?;
    stream.flush().await?;

    Ok((stream, addr))
}

async fn fetch_available_sources(server: &Server, url: &Url) -> Result<MasterMounts> {
    let timeout = Duration::from_millis(server.config.limits.master_timeout);
    tokio::time::timeout(
        timeout,
        async {
            let (mut stream, _) = http_get_request(url, "/api/serverinfo", "").await?;
            let mut reader      = ResponseReader::new(&mut stream, server.config.limits.master_http_max_len);
            server.stats.source_relay_connections.fetch_add(1, Ordering::Relaxed);
            let body_buf        = reader.read_to_end("application/json").await?;
            // Now we go to parsing response
            let info: MasterMounts = serde_json::from_slice(&body_buf)?;

            Ok(info)
        }
    ).await?
}

async fn transparent_get_mountpoint(serv: &Arc<Server>, url: &Url, mount: &str)
    -> Result<(Box<dyn Socket>, SocketAddr, usize, usize, IcyProperties, bool)> {
    // Fetching media stream from master
    let (mut stream,
         addr)      = http_get_request(url, mount, "Icy-Metadata: 1\r\n").await?;
    let mut reader  = ResponseReader::new(&mut stream, serv.config.limits.master_http_max_len);
    let headers_buf = reader.read_headers().await?;

    serv.stats.source_relay_connections.fetch_add(1, Ordering::Relaxed);

    // Parsing response headers
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut resp    = httparse::Response::new(&mut headers);

    match resp.parse(&headers_buf) {
        Ok(httparse::Status::Complete(_)) => {},
        Ok(httparse::Status::Partial) => return Err(anyhow::Error::msg("Incomplete response")),
        Err(e) => return Err(e.into())
    };

    if !resp.code.is_some_and(|c| c == 200) {
        return Err(anyhow::Error::msg("Unexpected response status code"));
    }

    // We should check if this is an icecast server
    if get_header("icy-name", resp.headers).is_none() {
        return Err(anyhow::Error::msg("not an icecast server"));
    }

    let chunked = match get_header("Transfer-Encoding", resp.headers) {
        // If nothing is set then it's identity
        Some(b"identity") | None => false,
        Some(b"chunked") => true,
        _ => return Err(anyhow::Error::msg("Unsupported transfer encoding"))
    };

    // Getting metaint
    let metaint = match get_header("Icy-Metaint", resp.headers) {
        Some(metaint) => std::str::from_utf8(metaint)?.parse::<usize>()?,
        None => {
            return Err(anyhow::Error::msg("missing icy-metaint header"));
        }
    };

    let properties = IcyProperties::new(match get_header("Content-Type", resp.headers) {
        Some(v) => std::str::from_utf8(v)?.to_owned(),
        None => {
            return Err(anyhow::Error::msg("missing content-type header"));
        }
    });

    Ok((stream, addr, metaint, headers_buf.len(), properties, chunked))
}

async fn transparent_relay_mountpoint(serv: &Arc<Server>, master_ind: usize, mount: String) {
    let url = &serv.config.master[master_ind].url;

    match transparent_get_mountpoint(serv, url, &mount).await {
        Ok((stream, addr, metaint, initial_bytes_read, properties, chunked)) => {
            let url = {
                let mut url = url.to_string();
                if url.as_str().ends_with('/') {
                    url.push_str(&mount[1..]);
                } else {
                    url.push_str(&mount[..]);
                }

                url
            };

            let ret = crate::source::handle_source(
                Session {
                    server: serv.clone(),
                    stream,
                    addr
                },
                SourceInfo {
                    mountpoint: mount,
                    properties,
                    initial_bytes_read,
                    chunked,
                    fallback: None,
                    queue_size: 0,
                    broadcast: None,
                    metadata: None,
                    relayed: Some(RelayStream {
                        info: RelayedInfo {
                            metaint,
                            metaint_position: 0,
                            metadata_reading: false,
                            metadata_remaining: 0,
                            metadata_buffer: Vec::new()
                        },
                        url
                    })
                }
            ).await;
            if let Err(e) = ret {
                error!(
                    "Master server {} relayind ended with: {}",
                    serv.config.master[master_ind].url,
                    e
                );
            }
        },
        Err(e) => {
            error!(
                "Master server {} relaying failed: {}",
                serv.config.master[master_ind].url,
                e
            );
        }
    }
}

pub async fn slave_instance(serv: Arc<Server>, master_ind: usize) {
    // Here we should be fine a we did check bounds before
    let master_server  = &serv.config.master[master_ind];

    match &master_server.relay_scheme {
        MasterServerRelayScheme::Transparent { update_interval } => {
            let mut interval = tokio::time::interval(Duration::from_millis(*update_interval));
            loop {
                interval.tick().await;

                match fetch_available_sources(&serv, &master_server.url).await {
                    Ok(mounts) => {
                        let lock = serv.sources.read().await;
                        for mount in &mounts.mounts {
                            if !lock.contains_key(mount) {
                                let serv_clone  = serv.clone();
                                let mount_clone = mount.clone();
                                tokio::spawn(async move {
                                    transparent_relay_mountpoint(&serv_clone, master_ind, mount_clone).await
                                });
                            }
                        }
                    },
                    Err(e) => {
                        info!("Fetching mounts from master server failed: {}", e);
                    }
                }
            }
        },
        MasterServerRelayScheme::Authenticated { .. } => {}
    }
}
