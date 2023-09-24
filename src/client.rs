use std::{
    sync::{Arc, atomic::{Ordering, AtomicU64, AtomicI64}},
    time::Duration
};
use serde::Serialize;
use anyhow::Result;
use hashbrown::HashMap;
use llq::broadcast::{Receiver, RecvError};
use tokio::{io::AsyncWriteExt, sync::{RwLock, oneshot}};
use tracing::{info, error};
use uuid::Uuid;

use crate::{
    server::ClientSession,
    request::{read_request, Request, RequestType, ListenRequest}, source::{self, IcyProperties, SourceStats, MoveClientsCommand, MoveClientsType}, response, utils, admin, api
};

pub struct Client {
    pub properties: ClientProperties,
    pub kill: Option<oneshot::Sender<()>>,
    pub stats: Arc<ClientStats>
}

#[derive(Serialize)]
pub struct ClientProperties {
    pub user_agent: Option<String>,
    pub metadata: bool
}

pub struct ClientStats {
    /// Here we need atomic so that we can update it when we change mountpoint
    pub start_time: AtomicI64,
    pub bytes_sent: AtomicU64
}

pub async fn handle_client<'a>(mut session: ClientSession, request: Request<'a>, req: ListenRequest) -> Result<()> {
    let (props, stream, meta_stream, mover, stats, mut clients) = match session.server.sources.read().await.get(&req.mountpoint) {
        Some(v) => {
            (
                v.properties.clone(),
                v.broadcast.clone(),
                v.meta_broadcast.clone(),
                v.move_listeners_receiver.clone(),
                v.stats.clone(),
                v.clients.clone()
            )
        },
        None => {
            response::not_found(&mut session.stream, &session.server.config.info.id).await?;
            error!("Source for {} suddenly vanished after client connection", req.mountpoint);
            return Ok(());
        }
    };

    // We increment active listeners
    // and check if we reached a new peak listeners here
    {
        // Server stats
        session.server.stats.listener_connections.fetch_add(1, Ordering::Relaxed);

        let new_count = session.server.stats.active_listeners.fetch_add(1, Ordering::Acquire) + 1;
        session.server.stats.peak_listeners.fetch_max(new_count, Ordering::Relaxed);
        // Mount stats
        let new_count = stats.active_listeners.fetch_add(1, Ordering::Acquire) + 1;
        stats.peak_listeners.fetch_max(new_count, Ordering::Relaxed);
    }
    
    let (kill, kill_rx) = oneshot::channel();
    let metadata        = utils::get_header("icy-metadata", &request.headers).unwrap_or(b"0") == b"1";
    let client_stats    = Arc::new(ClientStats {
        start_time: AtomicI64::new(chrono::offset::Utc::now().timestamp()),
        bytes_sent: AtomicU64::new(0)
    });
    let mut id;
    // We need to insert listener to list of active clients of mountpoint
    {
        let mut lock = clients.write().await;
        // We need to create a unique uuid for client
        loop {
            id = uuid::Uuid::new_v4();
            if lock.contains_key(&id) {
                continue;
            }
            lock.insert(id, Client {
                properties: ClientProperties {
                    // Retrieving user agent if there is one, a user agent will not be fully story
                    // if it's length is bigger than 100
                    user_agent: match utils::get_header("user-agent", &request.headers) {
                        Some(v) => match std::str::from_utf8(v) {
                            Ok(v) => Some(if v.len() > 100 {
                                v[..100].to_owned()
                            } else {
                                v.to_owned()
                            }),
                            Err(_) => None   
                        },
                        None => None
                    },
                    metadata
                },
                kill: Some(kill),
                stats: client_stats.clone()
            });
            break;
        };
    }

    drop(req);
    drop(request);

    let ret = tokio::select! {
        r = client_broadcast(&mut session, props, stream, meta_stream, mover, 
                             &stats, metadata, &mut id, &mut clients,
                             &client_stats) => {
            r
        },
        _ = kill_rx => {
            Err(anyhow::Error::msg("Client killed by admininstrator command"))
        }
    };

    // If it's a client error we must remove client from list of active clients
    // Otherwise that means source disconnected and we reached end
    if ret.is_err() {
        clients.write().await.remove(&id);
    }

    // End of connection
    session.server.stats.active_listeners.fetch_sub(1, Ordering::Release);
    stats.active_listeners.fetch_sub(1, Ordering::Release);

    ret
}

#[inline(always)]
pub async fn client_broadcast<'a>(session: &mut ClientSession,
                                  props: Arc<IcyProperties>, mut stream: Receiver<Arc<Vec<u8>>>,
                                  mut meta_stream: Receiver<Arc<Vec<u8>>>, mut mover: Receiver<Arc<MoveClientsCommand>>,
                                  stats: &SourceStats, metadata: bool, id: &mut Uuid,
                                  clients: &mut Arc<RwLock<HashMap<Uuid, Client>>>,
                                  client_stats: &ClientStats) -> Result<()> {
    if metadata {
        response::ok_200_icy_metadata(
            &mut session.stream,
            &session.server.config.info.id,
            &props,
            session.server.config.metaint
        ).await?;
    } else {
        response::ok_200(&mut session.stream, &session.server.config.info.id).await?;
    }

    drop(props);

    let mut fallback   = None;

    let mut metadata   = Arc::new(Vec::new());
    let mut metaint    = 0;
    let mut bytes_sent = 0;
    let mut stat_int   = tokio::time::interval(Duration::from_secs(30));

    loop {
        loop {
            tokio::select! {
                r = meta_stream.recv() => match r {
                    Ok(v) => metadata = v,
                    Err(RecvError::Lagged(_)) => (),
                    Err(RecvError::Closed) => break
                },
                r = stream.recv() => match r {
                    Ok(buf) => {
                        // We are checking here if we need to broadcast metadata
                        // Metadata needs to be sent inbetween ever metaint interval
                        if metaint + buf.len() >= session.server.config.metaint {
                            let diff          = (metaint + buf.len()) - session.server.config.metaint;
                            let first_buf_len = session.server.config.metaint - metaint;
                            
                            if first_buf_len > 0 {
                                session.stream.write_all(&buf[..first_buf_len]).await?;
                            }
                            // Now we write metadata
                            session.stream.write_all(&metadata).await?;
                            // Followed by what left in buffer if there is any
                            if diff > 0 {
                                session.stream.write_all(&buf[first_buf_len..]).await?;
                            }
                            metaint     = diff;
                            bytes_sent += metadata.len() + buf.len();
                        } else {
                            session.stream.write_all(&buf).await?;
                            metaint    += buf.len();
                            bytes_sent += buf.len();
                        }
                    },
                    Err(RecvError::Lagged(_)) => (),
                    Err(RecvError::Closed) => break
                },
                // We increment sent bytes count with interval in order not to have degraded performance
                // under contention if any
                _ = stat_int.tick() => {
                    let bytes = bytes_sent as u64;
                    stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                    client_stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                    bytes_sent = 0;
                }
                // Listening for move requests (ie. fallback or admin move clients)
                r = mover.recv() => match r {
                    Ok(v) => match &v.move_type {
                        MoveClientsType::Fallback => {
                            // If it is a fallback, we put it here until we reach closed state of
                            // current mount
                            fallback = Some(v);
                        },
                        MoveClientsType::Move => {
                            // Otherwise we move right away
                            change_mount(&mut stream, &mut meta_stream, &mut mover, client_stats, &v, id, clients).await;
                        }
                    },
                    Err(RecvError::Lagged(_)) => (),
                    Err(RecvError::Closed) => break
                }
            }
        };

        if let Some(v) = fallback.take() {
            change_mount(&mut stream, &mut meta_stream, &mut mover, client_stats, &v, id, clients).await;
            continue;
        }
        // If we don't have any fallback, it is fine to keep this client in list of active clients
        // of mount, because it will be cleaned up by source task dropping the whole hashmap
        break;
    };

    Ok(())
}

async fn change_mount(stream: &mut Receiver<Arc<Vec<u8>>>, meta_stream: &mut Receiver<Arc<Vec<u8>>>,
                      mover: &mut Receiver<Arc<MoveClientsCommand>>,
                      client_stats: &ClientStats,
                      new: &MoveClientsCommand, id: &mut Uuid,
                      clients: &mut Arc<RwLock<HashMap<Uuid, Client>>>) {
    // changing streams that we actually listen to
    *stream      = new.broadcast.clone();
    *meta_stream = new.meta_broadcast.clone();
    *mover       = new.move_listeners_receiver.clone();

    // We also need to remove ourselves from clients list of old mountpoint
    // And add ourselves to new one
    let client = clients.write().await.remove(id)
        .expect("Client should still by in mountpoint clients hashmap");
    
    // Now we move to new clients list
    *clients     = new.clients.clone();
    let mut lock = clients.write().await;
    loop {
        if lock.contains_key(&*id) {
            *id = uuid::Uuid::new_v4();
            continue;
        }
        lock.insert(*id, client);
        break;
    };

    // We need to also update client stats
    client_stats.start_time.store(chrono::offset::Utc::now().timestamp(), Ordering::Relaxed);
    client_stats.bytes_sent.store(0, Ordering::Relaxed);
}

pub async fn handle(mut session: ClientSession) {
    let mut request = Request {
        headers_buf: Vec::new(),
        headers: Vec::new(),
        method: ""
    };


    // Found no way to have fields of struct referencing each other without having borrow issue
    // for now we do direct access to pointer.
    // Safety: Valid pointer across the whole context of this function
    let _type;
    unsafe {
        let refm = (&mut request as *mut Request<'_>).as_mut().unwrap_unchecked();
        _type = match read_request(&mut session, refm).await {
            Ok(v) => v,
            Err(e) => {
                response::method_not_allowed(&mut session.stream, &session.server.config.info.id).await.ok();
                info!("Request coming from {} couldn't be handled: {}", session.addr, e);
                return;
            }
        };
    }

    _ = match _type {
        RequestType::AdminRequest(v)  => admin::handle_request(session, v).await,
        RequestType::ApiRequest(v)    => api::handle_request(session, v).await,
        RequestType::SourceRequest(v) => source::handle(session, &request, v).await,
        RequestType::ListenRequest(v) => handle_client(session, request, v).await,
    };
}
