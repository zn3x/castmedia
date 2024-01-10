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
    io::{BufStream, AsyncWriteExt}, task::JoinHandle
};
use tokio_native_tls::native_tls::TlsConnector;
use tracing::{info, error};
use url::Url;
use serde::{Deserialize, Serialize};

use crate::{
    server::{Server, Stream, Socket, Session, ClientSession},
    utils::{get_header, basic_auth, concat_path}, config::MasterServerRelayScheme,
    http::{ResponseReader, ChunkedResponseReader},
    client::{SourceInfo, RelayedInfo, RelayStream, StreamOnDemand},
    source::{IcyProperties, Source}, response::ChunkedResponse, stream::relay_broadcast_metadata
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

                chunked_writer.send(&mut session.stream, &(ser.len() as u32).to_be_bytes()).await?;
                chunked_writer.send(&mut session.stream, &ser).await?;
                session.stream.flush().await?;
            }

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
                        
                        chunked_writer.send(&mut session.stream, &(ser.len() as u32).to_be_bytes()).await?;
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

async fn get_stream(serv: &Arc<Server>, url: &Url, mount: &str,
                    on_demand: bool, auth: Option<&str>)
    -> Result<(Box<dyn Socket>, SocketAddr, Option<usize>, usize, IcyProperties, bool)> {
    // Fetching media stream from master
    let mut headers = String::new();
    if !on_demand {
        headers.push_str("Icy-Metadata: 1\r\n");
    }
    if let Some(auth) = auth {
        headers.push_str(auth);
    }
    let (mut stream,
         addr)      = http_get_request(url, mount, &headers).await?;
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
    if !on_demand && get_header("icy-name", resp.headers).is_none() {
        return Err(anyhow::Error::msg("not an icecast server"));
    }

    let chunked = match get_header("Transfer-Encoding", resp.headers) {
        // If nothing is set then it's identity
        Some(b"identity") | None => false,
        Some(b"chunked") => true,
        _ => return Err(anyhow::Error::msg("Unsupported transfer encoding"))
    };

    // Getting metaint
    let metaint = if on_demand {
        None
    } else {
        Some(match get_header("Icy-Metaint", resp.headers) {
            Some(metaint) => std::str::from_utf8(metaint)?.parse::<usize>()?,
            None => {
                return Err(anyhow::Error::msg("missing icy-metaint header"));
            }
        })
    };

    let properties = IcyProperties::new(match get_header("Content-Type", resp.headers) {
        Some(v) => std::str::from_utf8(v)?.to_owned(),
        None => {
            return Err(anyhow::Error::msg("missing content-type header"));
        }
    });

    Ok((stream, addr, metaint, headers_buf.len(), properties, chunked))
}

async fn authenticated_poll(serv: &Arc<Server>, master_ind: usize) -> Result<()> {
    let url               = &serv.config.master[master_ind].url;
    let (auth, on_demand) = match &serv.config.master[master_ind].relay_scheme {
        MasterServerRelayScheme::Authenticated { user, pass, stream_on_demand, .. } => (
            format!("Authorization: Basic {}\r\n", basic_auth(user, pass)),
            *stream_on_demand
        ),
        // Should not reach this as we did treat it already
        _ => unreachable!()
    };

    let (mut stream, _) = http_get_request(url, "/admin/mountupdates", &auth).await?;
    let mut reader      = ResponseReader::new(&mut stream, serv.config.limits.master_http_max_len);
    let headers_buf     = reader.read_headers().await?;

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
    
    // TODO: check transfer-encoding??
    // Shouldn't matter anyway
    info!("Starting mount updates from master {}", url);

    serv.stats.active_relay.fetch_add(1, Ordering::Relaxed);

    let mut sources = HashMap::new();

    let mut len_enc = [0u8; 4];
    let mut buf     = vec![0u8; 1024];
    let mut chunked = ChunkedResponseReader::new();
    loop {
        match authenticated_poll_reader(&mut stream, &mut len_enc, &mut buf, &mut chunked).await {
            Err(e)    => {
                error!("Mount updates from master {} stopped: {}", url, e);
                break;
            },
            Ok(event) => handle_mount_update(&mut sources, serv, master_ind, on_demand, event, &auth).await
        }
    }
    
    serv.stats.active_relay.fetch_sub(1, Ordering::Relaxed);

    Ok(())
}

async fn authenticated_poll_reader(stream: &mut Stream, len_enc: &mut [u8; 4],
                                   buf: &mut Vec<u8>, chunked: &mut ChunkedResponseReader) -> Result<MountUpdate> {
    // Reading message first from stream
    chunked.read_exact(stream, len_enc).await?;
    let len = u32::from_be_bytes(*len_enc) as usize;
    if len > buf.len() {
        buf.resize(len, 0);
    }
    chunked.read_exact(stream, &mut buf[..len]).await?;

    // Now we deserialize it
    let event: MountUpdate = serde_json::from_slice(&buf[..len])?;


    Ok(event)
}

async fn handle_mount_update(sources: &mut HashMap<String, JoinHandle<()>>,
                             serv: &Arc<Server>, master_ind: usize,
                             on_demand: bool, event: MountUpdate, auth: &str) {
    let master = &serv.config.master[master_ind].url;

    // Now handling events for every source
    match event {
        MountUpdate::New { mount, properties } => {
            // If we don't have on_demand enabled
            // we don't need to do all the following hassle
            // handle it like normal relay
            if !on_demand {
                let serv_cl  = serv.clone();
                let auth_cl  = auth.to_owned();
                let mount_cl = mount.clone();
                tokio::task::spawn(async move {
                    _ = relay_stream(
                        &serv_cl, master_ind, mount_cl,
                        None,
                        Some(&auth_cl)
                    ).await;
                });
                return;
            }

            if let Some(task_handle) = sources.remove(&mount) {
                // When we have a new source by same name, that means source in master
                // disconnected and reconnected again.
            
                // Cases to handle:
                // 1. Relay task for this source cleaned mount,
                // we normally will remount source.
                // 2. In the period task cleaned mount, some source mounted
                // a stream with same mount as ours, we literally can't do nothing
                // at this point.
                // 3. source relay task still didn't exit
                // we should here ask source relay task to exit politely
                // so we don't found source still existant after
                let mut lock = serv.sources.write().await;
                if let Some(source) = lock.get_mut(&mount) {
                    if let Some(v) = &source.relayed_source {
                        if v.starts_with(serv.config.master[master_ind].url.as_str()) {
                            // This is the same source
                            let kill = source.kill.take();
                            drop(lock);
                            if let Some(kill) = kill {
                                _ = kill.send(());
                                _ = task_handle.await;
                            }
                        } else {
                            // 💀 what can we do...
                            // Some asshat have same mount
                            return;
                        }
                    }
                }
            }

            let mut lock = serv.sources.write().await;
            if lock.len() >= serv.config.limits.sources {
                // We can't have another source
                return;
            }

            if lock.contains_key(&mount) {
                // Source with same mount present?
                return;
            }

            let wake_sink                  = diatomic_waker::WakeSink::new();
            let wake_src                   = wake_sink.source();
            let url                        = concat_path(master.as_str(), &mount);
            let (mut source, broadcast,
                 kill_notifier)            = Source::new(properties, None, Some(url), None);
            source.on_demand_notify_reader = Some(wake_src);

            let stats = source.stats.clone();

            crate::stream::broadcast_metadata(&source, &None, &None).await;

            if lock.try_insert(mount.clone(), source).is_err() {
                panic!("Should be able to add relay source as we have lock");
            }

            let serv_cl  = serv.clone();
            let auth_cl  = auth.to_owned();
            let mount_cl = mount.clone();
            let task     = tokio::task::spawn(async move {
                relay_stream_on_demand(
                    &serv_cl, master_ind, mount_cl,
                    StreamOnDemand {
                        stats,
                        broadcast,
                        kill_notifier,
                        on_demand_notify: wake_sink
                    },
                    auth_cl
                ).await;
            });

            drop(lock);

            sources.insert(mount, task);
        },
        MountUpdate::Metadata { mount, metadata } => {
            if !on_demand || !sources.contains_key(&mount) {
                return;
            }

            if let Some(source) = serv.sources.read().await.get(&mount) {
                let mut lock = source.meta_broadcast_sender.lock().await;
                relay_broadcast_metadata(
                    &source.metadata,
                    &mut lock,
                    metadata
                ).await;
            }
        },
        MountUpdate::Unmounted { mount } => {
            // If source disconnects from master then we should do the same
            // Send a kill notification to task
            if sources.remove(&mount).is_some() {
                let mut lock = serv.sources.write().await;
                if let Some(source) = lock.get_mut(&mount) {
                    if let Some(kill) = source.kill.take() {
                        drop(lock);
                        _ = kill.send(());
                    }
                }
            }
        }
    }
}

async fn relay_stream_on_demand(serv: &Arc<Server>, master_ind: usize, mount: String,
                                mut on_demand: StreamOnDemand, auth: String) {
    // We here need to wait until we get notification that we have
    // more than 0 listener
    // ofc we should also handle also the event of killing ourselves
    // Migration is not handled because there is stream to migrate :)
    loop {
        on_demand
            .on_demand_notify
            .wait_until(|| {
                if on_demand.stats.active_listeners.load(Ordering::Relaxed) != 0 {
                    Some(())
                } else {
                    None
                }
            })
            .await;

        match relay_stream(serv, master_ind, mount.clone(), Some(on_demand), Some(&auth)).await {
            // We don't have any listener to continue pulling stream
            Ok(Some(v)) => on_demand = v,
            // Either:
            // 1. killed by admin command or because stream
            // 2. stream prematurely closed due to something like network problem
            Ok(None) | Err(_) => break,
        }
    }
}

async fn relay_stream(serv: &Arc<Server>, master_ind: usize, mount: String,
                      on_demand: Option<StreamOnDemand>, auth: Option<&str>)
    -> Result<Option<StreamOnDemand>> {
    let url = &serv.config.master[master_ind].url;

    match get_stream(serv, url, &mount, on_demand.is_some(), auth).await {
        Ok((stream, addr, metaint, initial_bytes_read, properties, chunked)) => {
            let url = concat_path(url.as_str(), &mount);
            let ret = crate::source::handle_source(
                Session {
                    server: serv.clone(),
                    stream,
                    addr
                },
                SourceInfo {
                    mountpoint: mount.clone(),
                    properties,
                    initial_bytes_read,
                    chunked,
                    fallback: None,
                    queue_size: 0,
                    broadcast: None,
                    metadata: None,
                    relayed: Some(RelayStream {
                        info: RelayedInfo {
                            metaint: metaint.unwrap_or(0),
                            metaint_position: 0,
                            metadata_reading: false,
                            metadata_remaining: 0,
                            metadata_buffer: Vec::new()
                        },
                        url,
                        on_demand
                    })
                }
            ).await;
            if let Err(e) = ret.as_ref() {
                error!(
                    "Master server {} relayind for {} ended with: {}",
                    serv.config.master[master_ind].url,
                    mount,
                    e
                );
            }
            ret
        },
        Err(e) => {
            error!(
                "Master server {} relaying for {} failed: {}",
                serv.config.master[master_ind].url,
                mount,
                e
            );
            Err(e)
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
                                    relay_stream(
                                        &serv_clone, master_ind,
                                        mount_clone, None, None
                                    ).await
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
        MasterServerRelayScheme::Authenticated { reconnect_timeout, .. } => {
            let timeout = Duration::from_millis(*reconnect_timeout);
            loop {
                if let Err(e) = authenticated_poll(&serv, master_ind).await {
                    error!("Initial mountupdates connection to master {} failed: {}", master_server.url, e);
                }
                tokio::time::sleep(timeout).await;
            }
        }
    }
}
