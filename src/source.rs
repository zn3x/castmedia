use std::{
    sync::{Arc, atomic::{AtomicUsize, AtomicU64, Ordering}},
    num::NonZeroUsize
};
use hashbrown::HashMap;
use serde::{Serialize, Deserialize};
use qanat::broadcast::{Receiver, Sender};

use anyhow::Result;
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::info;
use uuid::Uuid;

use crate::{
    server::{ClientSession, Session},
    request::{SourceRequest, Request},
    response, utils, stream::{self, BroadcastInfo, RelayBroadcastStatus}, auth,
    client::{Client, SourceInfo}, config::Account
};

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Debug, Serialize, Deserialize, Clone))]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcyProperties {
    pub uagent: Option<String>,
    pub public: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub genre: Option<String>,
    pub bitrate: Option<String>,
    pub content_type: String
}

impl IcyProperties {
    pub fn new(content_type: String) -> Self {
        IcyProperties {
            uagent: None,
            public: false,
            name: None,
            description: None,
            url: None,
            genre: None,
            bitrate: None,
            content_type
        }
    }

    fn populate_from_http_headers(&mut self, headers: &[httparse::Header<'_>]) {
        for header in headers {
            let name = header.name.to_lowercase();
            let val = match std::str::from_utf8(header.value) {
                Ok(v) => v,
                Err(_) => continue
            };

            // There's a nice list here: https://github.com/ben221199/MediaCast
            // Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
            match name.as_str() {
                "user-agent" => self.uagent = Some(val.to_string()),
                "ice-public" | "icy-pub" | "x-audiocast-public" | "icy-public" => self.public = val.parse::<usize>().unwrap_or(0) == 1,
                "ice-name" | "icy-name" | "x-audiocast-name" => self.name = Some(val.to_string()),
                "ice-description" | "icy-description" | "x-audiocast-description" => self.description = Some(val.to_string()),
                "ice-url" | "icy-url" | "x-audiocast-url" => self.url = Some(val.to_string()),
                "ice-genre" | "icy-genre" | "x-audiocast-genre" => self.genre = Some(val.to_string()),
                "ice-bitrate" | "icy-br" | "x-audiocast-bitrate" => self.bitrate = Some(val.to_string()),
                _ => (),
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcyMetadata {
    pub title: String,
    pub url: String
}

pub struct SourceStats {
    /// Time when source last mounted mountpoint as utc timestamp
    pub start_time: i64,
    /// Number of active listeners for mountpoint
    pub active_listeners: AtomicUsize,
    /// Peak number of listeners for mountpoint
    pub peak_listeners: AtomicUsize,
    /// Number of media bytes read from source client
    pub bytes_read: AtomicU64,
    /// Number of media bytes sent to all listeners of mountpoint
    pub bytes_sent: AtomicU64,
}

impl SourceStats {
    pub fn new() -> Self {
        Self {
            start_time: chrono::offset::Utc::now()
                .timestamp(),
            active_listeners: AtomicUsize::new(0),
            peak_listeners: AtomicUsize::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0)
        }
    }
}

#[derive(Debug, Clone)]
pub enum SourceAccessType {
    SourceClient {
        /// Account username used to mount source
        username: String
    },
    RelayedSource {
        /// URL stream source if this is a relayed stream
        relayed_source: String,
    }
}

pub struct Source {
    pub properties: Arc<IcyProperties>,
    pub metadata: Arc<RwLock<Option<IcyMetadata>>>,
    /// List of currently connected listeners to mount
    pub clients: Arc<RwLock<HashMap<Uuid, Client>>>,
    pub stats: Arc<SourceStats>,
    /// Fallback mountpoint in case this one is down
    pub fallback: Option<String>,
    pub access: SourceAccessType,
    /// Notify relay task to start reading stream
    /// Used when we get first listener for a source
    pub on_demand_notify_reader: Option<diatomic_waker::WakeSource>,
    /// The stream broadcast receiver
    pub broadcast: Receiver<Arc<Vec<u8>>>,
    /// Receiver stream for metadata broadcast
    pub meta_broadcast: Receiver<Arc<(u64, Vec<u8>)>>,
    /// Sender stream for metadata broadcast
    /// Needed so we don't create a new sender every time
    /// we get metadata update
    pub meta_broadcast_sender: Mutex<Sender<Arc<(u64, Vec<u8>)>>>,
    /// Broadcast to move all clients in mountpoint to another one
    pub move_listeners_receiver: Receiver<Arc<MoveClientsCommand>>,
    pub move_listeners_sender: Sender<Arc<MoveClientsCommand>>,
    /// Send a command to kill source
    pub kill: Option<oneshot::Sender<()>>
}

pub struct SourceBroadcast {
    pub audio: Sender<Arc<Vec<u8>>>,
    pub metadata: Sender<Arc<(u64, Vec<u8>)>>
}

/// We can remotely command clients to change mountpoint using a variant of move commands
pub struct MoveClientsCommand {
    pub broadcast: Receiver<Arc<Vec<u8>>>,
    pub meta_broadcast: Receiver<Arc<(u64, Vec<u8>)>>,
    pub move_listeners_receiver: Receiver<Arc<MoveClientsCommand>>,
    pub clients: Arc<RwLock<HashMap<Uuid, Client>>>,
    pub move_type: MoveClientsType
}

pub enum MoveClientsType {
    /// An admin move command, this should be instaneous.
    Move,
    /// When source disconnects, we can move clients to a fallback, unlike Move command, client
    /// need to read till end of stream of old mountpoint to move to new one.
    Fallback,
}


impl Source {
    pub fn new(properties: IcyProperties,
               fallback: Option<String>,
               access: SourceAccessType,
               migrated: Option<(Sender<Arc<Vec<u8>>>, Receiver<Arc<Vec<u8>>>)>) -> (Self, SourceBroadcast, oneshot::Receiver<()>) {
        let size: NonZeroUsize  = 1.try_into().expect("1 should be posetif");
        let (tx, rx)            = match migrated {
            Some(v)            => v,
            None               => qanat::broadcast::channel(size)
        };
        let (tx1, rx1)         = qanat::broadcast::channel(size);
        let (tx2, rx2)         = qanat::broadcast::channel(size);
        let (tx3, rx3)         = oneshot::channel();
        (Source {
            properties: Arc::new(properties),
            metadata: Arc::new(RwLock::new(None)),
            clients: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(SourceStats::new()),
            fallback,
            access,
            on_demand_notify_reader: None,
            broadcast: rx,
            meta_broadcast_sender: Mutex::new(tx1.clone()),
            meta_broadcast: rx1,
            move_listeners_receiver: rx2,
            move_listeners_sender: tx2,
            kill: Some(tx3)
        },
        SourceBroadcast {
            audio: tx,
            metadata: tx1
        },
        rx3)
    }
}

pub async fn handle_request<'a>(mut session: ClientSession, request: &Request<'a>, req: SourceRequest) -> Result<()> {
    if let Err(e) = auth::auth(&mut session, auth::AllowedAuthType::SourceMount, req.auth, &req.mountpoint).await {
        response::internal_error(&mut session.stream, &session.server.config.info.id).await?;
        return Err(anyhow::Error::msg(format!("Source authentication failed, cause: {}", e)));
    }
    
    info!("New source request from {} to mount on {}", session, req.mountpoint);

    let sid = &session.server.config.info.id;

    // Do not allow mounting on /admin and /api folders
    // and also files with same name to not confuse things
    if req.mountpoint.eq("/admin") || req.mountpoint.eq("/api")
        || req.mountpoint.starts_with("/admin/") || req.mountpoint.starts_with("/api/") {
        response::forbidden(&mut session.stream, sid, "You can't mount on paths reserved for api").await?;
        return Ok(());
    }

    // Check if mountpoint used
    if session.server.sources.read().await.contains_key(&req.mountpoint) {
        response::forbidden(&mut session.stream, sid, "Invalid mountpoint").await?;
        return Ok(());
    }

    // Instanciating stream properties
    let mut properties = IcyProperties::new(match utils::get_header("Content-Type", &request.headers) {
        Some(v) => std::str::from_utf8(v)?.to_owned(),
        None => {
            response::forbidden(&mut session.stream, sid, "Missing content type").await?;
            return Ok(());
        }
    });

    // We need to check stream type from content-type header
    if !["audio/mpeg"].contains(&properties.content_type.as_str()) {
        response::forbidden(&mut session.stream, sid, "Insuported stream codec").await?;
        return Ok(());
    }

    let chunked;
    if request.method == "SOURCE" {
        response::ok_200(&mut session.stream, sid).await?;
        chunked = false;
    } else {
        // PUT METHOD
        chunked = match utils::get_header("Transger-Encoding", &request.headers) {
            Some(b"identity") | None => false,
            Some(b"chunked") => true,
            _ => {
                response::bad_request(&mut session.stream, sid, "Invalid transfer encoding").await?;
                return Ok(());
            }
        };

        match utils::get_header("Expect", &request.headers) {
            Some(b"100-continue") => {},
            Some(_) => {
                response::bad_request(&mut session.stream, sid, "Expecting 100-continue in header expect").await?;
                return Ok(());
            },
            None => {
                response::bad_request(&mut session.stream, sid, "PUT method must have expect header").await?;
                return Ok(());
            }
        }

        response::ok_200(&mut session.stream, sid).await?;
    }

    properties.populate_from_http_headers(&request.headers);

    let user_id = session.user
        .as_ref()
        .expect("Should be identified at this point")
        .id
        .clone();

    let mut fallback = None;
    if let Some(account) = &session.server.config.account.get(&user_id) {
        match account {
            Account::Admin { .. } => (),
            Account::Source { mount, .. } => {
                for mount in mount {
                    if mount.path.eq(&req.mountpoint) {
                        fallback = mount.fallback.clone();
                        break;
                    }
                }
            },
            _ => ()
        }
    }

    handle_source(
        Session {
            server: session.server,
            stream: session.stream,
            addr: session.addr
        },
        SourceInfo {
            mountpoint: req.mountpoint,
            properties,
            initial_bytes_read: request.headers_buf.len(),
            chunked,
            fallback,
            queue_size: 0,
            broadcast: None,
            metadata: None,
            relayed: None,
            access: SourceAccessType::SourceClient { username: user_id }
        }
    ).await?;

    Ok(())
}

pub async fn handle_source(mut session: Session, info: SourceInfo) -> Result<Option<RelayBroadcastStatus>> {
    let (relayed, on_demand, stream_src_url) = match (info.relayed, &info.access) {
        (Some(v), SourceAccessType::RelayedSource { relayed_source }) => (
            Some(v.info), v.on_demand,
            Some(relayed_source.clone())
        ),
        (None, _) => (None, None, None),
        _         => unreachable!()
    };

    let mut on_demand_notify = None;
    let source_stats;
    let mut broadcast;
    let kill_notifier;
    let on_demand_flag;

    if let Some(on_demand) = on_demand {
        on_demand_flag   = true;
        source_stats     = on_demand.stats;
        broadcast        = on_demand.broadcast;
        kill_notifier    = on_demand.kill_notifier;
        on_demand_notify = Some(on_demand.on_demand_notify);
    } else {
        on_demand_flag = false;
        let source;
        (source, broadcast, kill_notifier) = Source::new(info.properties, info.fallback,
                                                         info.access, info.broadcast);

        // To prevent early readers from having no header
        if let Some(metadata) = info.metadata {
            broadcast.metadata.send(Arc::new((0, metadata)));
        }

        // We only ever need to keep track of media
        // keeping this here just for the looks
        /*
        // We must write initial read length to stats
        //source.stats.bytes_read.fetch_add(info.initial_bytes_read as u64, Ordering::Relaxed);
        */

        // If source never ever broadcasts metadata
        // we do this to respect metadata interval for reader
        crate::broadcast::broadcast_metadata(&source, &None, &None).await;

        source_stats = source.stats.clone();

        // Add this mountpoint to mountpoints hashmap
        {
            let mut lock = session.server.sources.write().await;
            if lock.len() >= session.server.config.limits.sources {
                if relayed.is_none() {
                    response::forbidden(
                        &mut session.stream,
                        &session.server.config.info.id,
                        "Too many sources connected").await?;
                    return Err(anyhow::Error::msg("Max sources limit reached"));
                } else {
                    return Ok(Some(RelayBroadcastStatus::LimitReached));
                }
            }

            if lock.try_insert(info.mountpoint.clone(), source).is_err() {
                if relayed.is_some() {
                    return Ok(Some(RelayBroadcastStatus::MountExists));
                } else {
                    return Err(anyhow::Error::msg(format!("Mountpoint {} already exists", info.mountpoint)));
                }
            }
        }

        // In case we are defined as a master server
        // we must notify slaves here that a new mount is present
        if session.server.relay_params.slave_auth_present {
            session.server.relay_params
                .new_source_event_tx
                .clone()
                .send(());
        }
    }

    let binfo = BroadcastInfo {
        mountpoint: &info.mountpoint,
        session,
        stats: source_stats,
        queue_size: info.queue_size,
        chunked: info.chunked,
        broadcast,
        kill_notifier,
        on_demand: on_demand_flag
    };

    match relayed {
        None => {
            // Server stats
            binfo.session.server.stats.source_client_connections.fetch_add(1, Ordering::Relaxed);

            binfo.session.server.stats.active_sources.fetch_add(1, Ordering::Acquire);
        

            // Then handle media stream coming for this mountpoint
            stream::broadcast(binfo).await;
        },
        Some(relay_info) => {
            binfo.session.server.stats.active_relay.fetch_add(1, Ordering::Relaxed);
            binfo.session.server.stats.active_relay_streams.fetch_add(1, Ordering::Acquire);

            return Ok(Some(stream::relay_broadcast(binfo, relay_info, stream_src_url.unwrap(), on_demand_notify).await));
        }
    }

    Ok(None)
}
