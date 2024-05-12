use std::{
    sync::{Arc, atomic::{Ordering, AtomicU64, AtomicI64}},
    time::Duration
};
use serde::{Serialize, Deserialize};
use anyhow::Result;
use hashbrown::{HashMap, hash_map::Entry};
use qanat::{
    broadcast::{Receiver, RecvError, Sender},
    oneshot
};
use tokio::{
    io::{AsyncWriteExt, BufStream},
    sync::RwLock,
    net::TcpStream
};
use tracing::{info, error};
use uuid::Uuid;

use crate::{
    server::{ClientSession, Stream, Server, Session, AddrType},
    request::{read_request, Request, RequestType, ListenRequest},
    source::{self, SourceStats, MoveClientsCommand, MoveClientsType, IcyProperties, handle_source, SourceBroadcast, SourceAccessType},
    response, utils, admin, api,
    migrate::{MigrateClientInfo, MigrateClient, MigrateConnection, VersionedMigrateConnection, MigrateCommand},
    broadcast::read_media_broadcast
};

pub struct Client {
    pub properties: ClientProperties,
    pub kill: Option<oneshot::Sender<()>>,
    pub stats: Arc<ClientStats>
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct ClientProperties {
    pub user_agent: Option<String>,
    pub metadata: bool
}

pub struct ClientStats {
    /// Here we need atomic so that we can update it when we change mountpoint
    pub start_time: AtomicI64,
    pub bytes_sent: AtomicU64
}

pub async fn handle_listener_request(session: ClientSession, request: Request<'_>, req: ListenRequest) -> Result<()> {
    let metadata   = utils::get_header("icy-metadata", &request.headers).unwrap_or(b"0") == b"1";
    let user_agent = match utils::get_header("user-agent", &request.headers) {
        Some(v) => match std::str::from_utf8(v) {
            Ok(v) => Some(if v.len() > 100 {
                v[..100].to_owned()
            } else {
                v.to_owned()
            }),
            Err(_) => None   
        },
        None => None
    };

    drop(request);

    prepare_listener(
        session,
        ListenerInfo {
            mountpoint: req.mountpoint,
            migrated: None,
            properties: ClientProperties { user_agent, metadata }
        }
    ).await
}

async fn prepare_listener(mut session: ClientSession, info: ListenerInfo) -> Result<()> {
    let source_info = session.server.sources.read().await.get(&info.mountpoint)
        .map(|v| (
            v.properties.clone(),
            v.broadcast.clone(),
            v.meta_broadcast.clone(),
            v.move_listeners_receiver.clone(),
            v.stats.clone(),
            v.clients.clone(),
            v.on_demand_notify_reader.clone()
        ));

    let (props, mut stream, meta_stream,
         mover, stats, clients, on_demand_notify) = match source_info {
        Some(v) => v,
        None => {
            if info.migrated.is_none() {
                response::not_found(&mut session.stream, &session.server.config.info.id).await?;
            }
            error!("Source for {} suddenly vanished after client connection", info.mountpoint);
            return Ok(());
        }
    };

    // We increment active listeners
    // and check if we reached a new peak listeners here
    {
        // Server stats
        session.server.stats.listener_connections.fetch_add(1, Ordering::Relaxed);

        let new_count = session.server.stats.active_listeners.fetch_add(1, Ordering::Acquire) + 1;
        if new_count > session.server.config.limits.listeners {
            // We must not surpass limit of possible listeners
            session.server.stats.active_listeners.fetch_sub(1, Ordering::Release);
            if info.migrated.is_none() {
                response::internal_error(&mut session.stream, &session.server.config.info.id).await?;
            }
            return Ok(());
        }
        session.server.stats.peak_listeners.fetch_max(new_count, Ordering::Relaxed);
        // Mount stats
        let new_count = stats.active_listeners.fetch_add(1, Ordering::Acquire) + 1;
        stats.peak_listeners.fetch_max(new_count, Ordering::Relaxed);

        // Notify source reader if stream pull is on on_demand
        if new_count == 1 {
            if let Some(on_demand_notify) = on_demand_notify.as_ref() {
                on_demand_notify.notify();
            }
        }
    }
    
    let (kill, mut kill_rx) = oneshot::channel();
    let client_stats        = Arc::new(ClientStats {
        start_time: AtomicI64::new(chrono::offset::Utc::now().timestamp()),
        bytes_sent: AtomicU64::new(0)
    });
    let metadata = info.properties.metadata;
    let mut id;
    // We need to insert listener to list of active clients of mountpoint
    {
        let client = Client {
            properties: info.properties,
            kill: Some(kill),
            stats: client_stats.clone()
        };
        let mut lock = clients.write().await;
        // We need to create a unique uuid for client
        loop {
            id = uuid::Uuid::new_v4();
            if let Entry::Vacant(v) = lock.entry(id) {
                v.insert(client);
                break;
            }
        };
    }

    if info.migrated.is_none() {
        if metadata {
            response::ok_200_icy_metadata(
                &mut session.stream,
                &session.server.config.info.id,
                &props,
                session.server.config.metaint
            ).await?;
        } else {
            response::ok_200_listener(
                &mut session.stream,
                &session.server.config.info.id,
                &props
            ).await?;
        }
    }
    drop(props);

    let metaint = match info.migrated {
        Some(v) => {
            stream.restore(v.resume_point);
            v.metaint
        },
        None => 0
    };

    let mut broadcast = BroadcastInfo {
        stream,
        meta_stream,
        mover,
        stats,
        with_metadata: metadata,
        metaint,
        id,
        clients,
        client_stats,
        mountpoint: info.mountpoint
    };

    let server = session.server.clone();
    let ret    = tokio::select! {
        r = listener_broadcast(&mut session, &mut broadcast) => {
            r
        },
        _ = kill_rx.recv() => {
            Err(anyhow::Error::msg("Client killed by admininstrator command"))
        }
    };

    // Checking whether it's a normal exit or migration
    match ret {
        Ok(BroadcastStatus::Migrate(m)) => {
            // We got migration
            migrate_listener(session, &mut broadcast, m).await;
            // We don't need to update stats
            Ok(())
        },
        Ok(BroadcastStatus::EndofStream) | Err(_) => {
            // End of stream
            server.stats.active_listeners.fetch_sub(1, Ordering::Release);
            let new_count = broadcast.stats.active_listeners.fetch_sub(1, Ordering::Release);

            // Notify source reader if stream pull is on on_demand
            if new_count <= 1 {
                if let Some(on_demand_notify) = on_demand_notify.as_ref() {
                    on_demand_notify.notify();
                }
            }

            // If it's a client error we must remove client from list of active clients
            // Otherwise that means source disconnected and we reached end
            if let Err(e) = ret {
                info!("{session} no longer listening on {}: {e}", broadcast.mountpoint);
                broadcast.clients.write().await.remove(&id);
                return Err(e);
            }

            Ok(())
        }
    }
}

struct BroadcastInfo {
    stream: Receiver<Arc<Vec<u8>>>,
    meta_stream: Receiver<Arc<(u64, Vec<u8>)>>,
    mover: Receiver<Arc<MoveClientsCommand>>,
    stats: Arc<SourceStats>,
    with_metadata: bool,
    metaint: usize,
    id: Uuid,
    clients: Arc<RwLock<HashMap<Uuid, Client>>>,
    client_stats: Arc<ClientStats>,
    mountpoint: String
}

enum BroadcastStatus {
    Migrate(Result<Arc<MigrateCommand>, qanat::broadcast::RecvError>),
    EndofStream
}

#[inline(always)]
async fn listener_broadcast<'a>(session: &mut ClientSession, b: &mut BroadcastInfo)
    -> Result<BroadcastStatus> {
    info!("{session} listening on {}", b.mountpoint);
    let mut fallback   = None;
    // We must have an initial metadata to work with
    let mut metadata   = if let Ok(v) = b.meta_stream.recv().await {
        v
    } else {
        return Ok(BroadcastStatus::EndofStream);
    };
    let mut temp_metadata = None;
    let mut bytes_sent    = 0;
    let mut stat_int      = tokio::time::interval(Duration::from_secs(30));

    let mut migrate_comm = session.server.migrate.clone();

    loop {
        loop {
            tokio::select! {
                r = read_media_broadcast(&mut b.stream, &mut b.meta_stream, &mut metadata, &mut temp_metadata) => match r {
                    Ok(buf) => {
                        // We are checking here if we need to broadcast metadata
                        // Metadata needs to be sent inbetween ever metaint interval
                        if b.with_metadata {
                            if b.metaint + buf.len() >= session.server.config.metaint {
                                let diff          = (b.metaint + buf.len()) - session.server.config.metaint;
                                let first_buf_len = session.server.config.metaint - b.metaint;
                                
                                if first_buf_len > 0 {
                                    session.stream.write_all(&buf[..first_buf_len]).await?;
                                }
                                // Now we write metadata
                                session.stream.write_all(&metadata.1).await?;
                                // Followed by what left in buffer if there is any
                                if diff > 0 {
                                    session.stream.write_all(&buf[first_buf_len..]).await?;
                                }
                                b.metaint   = diff;
                                bytes_sent += metadata.1.len() + buf.len();
                            } else {
                                session.stream.write_all(&buf).await?;
                                b.metaint  += buf.len();
                                bytes_sent += buf.len();
                            }
                        } else {
                            session.stream.write_all(&buf).await?;
                            bytes_sent += buf.len();
                        }
                        session.stream.flush().await?;
                    },
                    Err(RecvError::Lagged(_)) => (),
                    Err(RecvError::Closed) => break
                },
                // We increment sent bytes count with interval in order not to have degraded performance
                // under contention if any
                _ = stat_int.tick() => {
                    let bytes = bytes_sent as u64;
                    b.stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                    b.client_stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                    bytes_sent = 0;
                }
                // Listening for move requests (ie. fallback or admin move clients)
                r = b.mover.recv() => match r {
                    Ok(v) => match &v.move_type {
                        MoveClientsType::Fallback => {
                            // If it is a fallback, we put it here until we reach closed state of
                            // current mount
                            fallback = Some(v);
                        },
                        MoveClientsType::Move => {
                            // Otherwise we move right away
                            change_mount(b, &v).await;
                        }
                    },
                    Err(RecvError::Lagged(_)) => (),
                    Err(RecvError::Closed) => break
                },
                migrate = migrate_comm.recv() => return Ok(BroadcastStatus::Migrate(migrate))
            }
        };

        if let Some(v) = fallback.take() {
            change_mount(b, &v).await;
            continue;
        }
        // If we don't have any fallback, it is fine to keep this client in list of active clients
        // of mount, because it will be cleaned up by source task dropping the whole hashmap
        break;
    };

    Ok(BroadcastStatus::EndofStream)
}

async fn migrate_listener(session: ClientSession,
                          b: &mut BroadcastInfo,
                          migrate: Result<Arc<MigrateCommand>, qanat::broadcast::RecvError>) {
    // Safety only task of client will ever remove itself
    let client = b.clients.write().await
        .remove(&b.id)
        .expect("Should be able to get Client");
    // Safety: migrate sender half is NEVER dropped until process exits
    let migrate = migrate
        .expect("Got migrate notice with closed mpsc");
    let info = MigrateConnection::Client {
        info: MigrateClient {
            mountpoint: b.mountpoint.clone(),
            properties: client.properties,
            resume_point: b.stream.read_position(),
            metaint: b.metaint as u64
        }
    };
    let info: VersionedMigrateConnection = info.into();
    // Well, can't do nothing if we ran out of memory here
    if let Ok(info) = serde_json::to_vec(&info) {
        _ = migrate.listener.send(MigrateClientInfo {
            info,
            mountpoint: b.mountpoint.clone(),
            sock: session.stream
        });
    }
}

async fn change_mount(b: &mut BroadcastInfo,
                      new: &MoveClientsCommand) {
    // changing streams that we actually listen to
    b.stream      = new.broadcast.clone();
    b.meta_stream = new.meta_broadcast.clone();
    b.mover       = new.move_listeners_receiver.clone();

    // We also need to remove ourselves from clients list of old mountpoint
    // And add ourselves to new one
    let client = b.clients.write().await.remove(&b.id)
        .expect("Client should still by in mountpoint clients hashmap");
    
    // Now we move to new clients list
    b.clients        = new.clients.clone();
    {
        let mut lock = b.clients.write().await;
        loop {
            if let Entry::Vacant(v) = lock.entry(b.id) {
                v.insert(client);
                break;
            }
            b.id = uuid::Uuid::new_v4();
        };
    }

    // We need to also update client stats
    b.client_stats.start_time.store(chrono::offset::Utc::now().timestamp(), Ordering::Relaxed);
    b.client_stats.bytes_sent.store(0, Ordering::Relaxed);
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
                info!("Request coming from {} couldn't be handled: {}", session, e);
                return;
            }
        };
    }

    _ = match _type {
        RequestType::Admin(v)  => admin::handle_request(session, v).await,
        RequestType::Api(v)    => api::handle_request(session, v).await,
        RequestType::Source(v) => source::handle_request(session, &request, v).await,
        RequestType::Listen(v) => handle_listener_request(session, request, v).await,
    };
}

pub struct ListenerRestoreInfo {
    pub resume_point: u64,
    pub metaint: usize
}

pub enum ClientInfo {
    Source(SourceInfo),
    Listener(ListenerInfo),
    MasterMountUpdates(MasterMountUpdatesInfo)
}

pub struct MasterMountUpdatesInfo {
    pub mounts: Vec<String>,
    pub user_id: String
}

pub struct SourceInfo {
    pub mountpoint: String,
    pub properties: IcyProperties,
    pub initial_bytes_read: usize,
    pub chunked: bool,
    pub fallback: Option<String>,
    pub queue_size: usize,
    pub broadcast: Option<(Sender<Arc<Vec<u8>>>, Receiver<Arc<Vec<u8>>>)>,
    pub metadata: Option<Vec<u8>>,
    pub relayed: Option<RelayStream>,
    pub access: SourceAccessType
}

pub struct RelayStream {
    pub on_demand: Option<StreamOnDemand>,
    pub info: RelayedInfo
}

pub struct StreamOnDemand {
    pub stats: Arc<SourceStats>,
    pub broadcast: SourceBroadcast,
    pub kill_notifier: oneshot::Receiver<()>,
    pub on_demand_notify: diatomic_waker::WakeSink
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Debug, Serialize, Deserialize))]
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct RelayedInfo {
    pub metaint: usize,
    pub metaint_position: usize,
    pub metadata_reading: bool,
    pub metadata_remaining: usize,
    pub metadata_buffer: Vec<u8>
}

pub struct ListenerInfo {
    pub mountpoint: String,
    pub migrated: Option<ListenerRestoreInfo>,
    pub properties: ClientProperties
}

pub async fn handle_migrated(sock: TcpStream, server: Arc<Server>, client: ClientInfo,
                             mut migrate_finished: Receiver<()>) {
    let addr = match sock.peer_addr() {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to fetch migrated connection address: {}", e);
            return;
        }
    };
    let stream: Stream = Box::new(BufStream::new(sock));

    match client {
        ClientInfo::Source(info) => {
            // We will only allow source clients that are still present in config
            if let SourceAccessType::SourceClient { username } = &info.access {
                if !server.config.account.contains_key(username) {
                    return;
                }
            }

            let session = Session {
                server,
                stream,
                addr
            };
            _ = handle_source(session, info).await;
        },
        ClientInfo::Listener(info) => {
            let session = ClientSession {
                addr_type: AddrType::Simple,
                server,
                stream,
                addr,
                user: None
            };
            _ = prepare_listener(session, info).await;
        },
        ClientInfo::MasterMountUpdates(mut info) => {
            // Same thing as source clients
            if !server.config.account.contains_key(&info.user_id) {
                return;
            }

            let mut mounts = HashMap::new();

            _ = migrate_finished.recv().await;

            {
                let lock = server.sources.read().await;
                for mount in info.mounts.drain(..) {
                    if let Some(source) = lock.get(&mount) {
                        mounts.insert(mount, source.meta_broadcast.clone());
                    }
                }
            }

            _ = crate::relay::master_mount_updates(
                ClientSession {
                    // Should be fine as we route directly this connection to mount updates
                    addr_type: AddrType::Admin,
                    server,
                    stream,
                    addr,
                    user: Some(crate::auth::UserRef { id: info.user_id })
                },
                mounts
            ).await;
        }
    }
}
