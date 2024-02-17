use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, net::SocketAddr, hash::BuildHasher, os::fd::AsRawFd, time::Duration};
use chrono::{DateTime, Local};
use futures::StreamExt;
use qanat::broadcast::{channel, Receiver, Sender};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet, io::{AsyncRead, AsyncWrite, BufStream}, sync::{Semaphore, RwLock, Mutex, mpsc}
};
use tokio_native_tls::{native_tls, TlsStream, TlsAcceptor};
use tracing::{info, error};
use hashbrown::{HashMap, hash_map::DefaultHashBuilder};
use inotify::{Inotify, WatchMask};
use anyhow::Result;

use crate::{
    config::{ServerSettings, TlsIdentity, Account}, 
    client, source::Source, migrate::MigrateCommand,
    auth::{self, HashCalculation}, ArgParse, relay
};

pub trait Socket: Send + Sync + AsyncRead + AsyncWrite + Unpin {
    fn fd(&self) -> i32;
    fn local_addr(&self) -> Result<SocketAddr>;
}

impl Socket for BufStream<TcpStream> {
    fn fd(&self) -> i32 {
        self.get_ref().as_raw_fd()
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().local_addr()?)
    }
}

impl Socket for BufStream<TlsStream<TcpStream>> {
    fn fd(&self) -> i32 {
        unreachable!("Can't migrate Tls connection")
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().get_ref().get_ref().get_ref().local_addr()?)
    }
}

pub type Stream = Box<dyn Socket>;

/// Struct holding all info related to server
pub struct Server {
    pub config: ServerSettings,
    /// argument to be used on restart
    pub args: ArgParse,
    /// Semaphore intended to cap concurrent connection to server
    pub max_clients: Arc<Semaphore>,
    /// List of all sources whereas the mountpoint is the key
    pub sources: RwLock<HashMap<String, Source>>,
    /// Server general stats (this excludes calls on /api and /admin)
    pub stats: ServerStats,
    /// Trigger to notify all tasks when live migration is done
    pub migrate_tx: Mutex<Option<Sender<Arc<MigrateCommand>>>>,
    /// Receiver to listener when migration happens
    pub migrate: Receiver<Arc<MigrateCommand>>,
    /// In order not to block async tasks, we have dedicated thread to calculate hashes
    pub hash_calculator: mpsc::UnboundedSender<HashCalculation>,
    /// Relay specific parameters
    pub relay_params: RelayParams
}

pub struct ServerStats {
    /// Server startup time as a utc timestamp
    pub start_time: i64,
    /// Number of connections since startup (accumulating counter)
    /// This includes number of failed connections (max clients reached, invalid request, ... etc)
    pub connections: AtomicUsize,
    ///// Number of total active clients
    //pub active_clients: AtomicUsize,
    /// Number of total active sources
    pub active_sources: AtomicUsize,
    /// Number of total active listening clients to mountpoints
    pub active_listeners: AtomicUsize,
    /// Number of total active outbound connections to master server
    pub active_relay: AtomicUsize,
    /// Number of peak listeners to mountpoints
    pub peak_listeners: AtomicUsize,
    /// Number of connections to mountpoints (accumulating counter)
    pub listener_connections: AtomicUsize,
    /// Number of connections made by source clients (accumulating counter)
    pub source_client_connections: AtomicUsize,
    /// Number of outbound connections made to master server (accumulating counter)
    pub source_relay_connections: AtomicUsize,
    /// Number of active streams that are relayed
    pub active_relay_streams: AtomicUsize,
    /// Number of connections made to admin api (accumulating counter)
    pub admin_api_connections: AtomicUsize,
    /// Number of admin api connections with successful authentication (accumulating counter)
    pub admin_api_connections_success: AtomicUsize,
    /// Number of public api connections (accumulating counter)
    pub api_connections: AtomicUsize
}

impl ServerStats {
    pub fn new(start_time: i64) -> Self {
        Self {
            start_time,
            connections: AtomicUsize::new(0),
            //active_clients: AtomicUsize::new(0),
            active_sources: AtomicUsize::new(0),
            active_listeners: AtomicUsize::new(0),
            active_relay: AtomicUsize::new(0),
            peak_listeners: AtomicUsize::new(0),
            listener_connections: AtomicUsize::new(0),
            source_client_connections: AtomicUsize::new(0),
            source_relay_connections: AtomicUsize::new(0),
            active_relay_streams: AtomicUsize::new(0),
            admin_api_connections: AtomicUsize::new(0),
            admin_api_connections_success: AtomicUsize::new(0),
            api_connections: AtomicUsize::new(0)
        }
    }
}

/// Helper struct used to store attributs solely needed
/// for relaying when master server is expected to receive
/// authenticated slave connection
pub struct RelayParams {
    /// Flag used to check if there is a slave auth configured
    pub slave_auth_present: bool,
    /// Notification for new source (sender)
    pub new_source_event_tx: Sender<()>,
    /// Notification for new source (receiver)
    pub new_source_event_rx: Receiver<()>
}

/// A client session
pub struct ClientSession {
    /// Is this an admin address
    pub admin_addr: bool,
    /// Server info
    pub server: Arc<Server>,
    /// Socket of this client session
    pub stream: Box<dyn Socket>,
    /// Address of our peer
    pub addr: SocketAddr
}

/// A generic session
pub struct Session {
    /// Server info
    pub server: Arc<Server>,
    /// Socket of this client session
    pub stream: Box<dyn Socket>,
    /// Address of our peer
    pub addr: SocketAddr
}

async fn accept_connections(serv: Arc<Server>, listener: TcpListener, admin_addr: bool) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let serv_clone = serv.clone();
                tokio::spawn(async move {
                    serv_clone.stats.connections.fetch_add(1, Ordering::Relaxed);
                    // Here we are trying to acquire the semaphore before handling connection
                    // If we can't, we already hit the max number of clients allowed and we can't
                    // do nothing
                    let sem = serv_clone.max_clients.clone();
                    let aq  = sem.try_acquire();
                    if let Ok(_guard) = aq {
                        client::handle(ClientSession {
                            admin_addr,
                            server: serv_clone,
                            // Use bufferer for socket to reduce syscalls we make
                            stream: Box::new(BufStream::new(stream)),
                            addr
                        }).await;
                    }
                });
            },
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn tls_accept_connections(serv: Arc<Server>, listener: TcpListener, admin_addr: bool) {
    // First we retrieve tls identity
    let mut tls_identity = None;
    if admin_addr {
        tls_identity = Some(serv.config.admin_access.address.tls.as_ref().unwrap());
    } else {
        let local_addr = listener.local_addr().expect("Should be able to get local address");
        for addr in &serv.config.address {
            if addr.bind.eq(&local_addr) {
                tls_identity = Some(addr.tls.as_ref().unwrap());
                break;
            }
        }
    }
    let tls_identity = tls_identity.expect("Should be able to find Tls identity");
    
    let inotify = Inotify::init()
        .expect("Should be able to initialize inotify");
    inotify.watches().add(std::path::Path::new(&tls_identity.cert), WatchMask::CREATE | WatchMask::MODIFY)
        .expect("Couldn't create inotify watch for tls identity file");
    let mut tls_update_stream = inotify.into_event_stream([0u8; 1024])
        .expect("Couldn't create inotify tls identity update stream");

    let mut tls_acceptor;
    let mut tls_id_hash         = 0u64;
    (tls_id_hash, tls_acceptor) = load_cert(tls_identity, tls_id_hash).await
        .expect("Failed loading Tls identity")
        .unwrap();

    loop {
        tokio::select! {
            _ = tls_connection_acceptor(&serv, &listener, admin_addr, &tls_acceptor) => {
                continue;
            },
            u = tls_update_stream.next() => {
                if u.is_none() {
                    panic!("Inotify notifications suddenly stopped.");
                } else if let Some(Err(e)) = u {
                    error!("Tls identity {} inotify error: {}", tls_identity.cert, e);
                    continue;
                }
            }
        }

        match load_cert(tls_identity, tls_id_hash).await {
            Ok(Some(v)) => {
                tls_id_hash  = v.0;
                tls_acceptor = v.1;
                info!("Tls identity from {} reloaded", tls_identity.cert);
            },
            Ok(None) => {},
            Err(e) => {
                error!("Couldn't reload Tls identity from {}, sticking to old identity. Reason: {}", tls_identity.cert, e);
            }
        }
    }
}

async fn load_cert(tls_identity: &TlsIdentity, old_hash: u64) -> Result<Option<(u64, TlsAcceptor)>> {
    let tls_slurp = tokio::fs::read(&tls_identity.cert).await?;
    // In order not to keep reloading same tls identity, we keep a hash for it
    let hash      = DefaultHashBuilder::default().hash_one(tls_slurp.as_slice());
    if hash.eq(&old_hash) {
        return Ok(None);
    }
    let tls_id    = native_tls::Identity::from_pkcs12(tls_slurp.as_slice(), &tls_identity.pass)?;
    
    Ok(Some((
        hash,
        tokio_native_tls::TlsAcceptor::from(native_tls::TlsAcceptor::new(tls_id)?)
    )))
}

#[inline]
async fn tls_connection_acceptor(serv: &Arc<Server>, listener: &TcpListener, admin_addr: bool, tls_acceptor: &TlsAcceptor) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let tls_acc_clone = tls_acceptor.clone();
                let serv_clone    = serv.clone();
                tokio::spawn(async move {
                    serv_clone.stats.connections.fetch_add(1, Ordering::Relaxed);
                    // Here we are trying to acquire the semaphore before handling connection
                    // If we can't, we already hit the max number of clients allowed and we can't
                    // do nothing
                    let sem = serv_clone.max_clients.clone();
                    let aq  = sem.try_acquire();
                    if let Ok(_guard) = aq {
                        let tls_stream = match tls_acc_clone.accept(stream).await {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Tls connection failed with {}: {}", addr, e);
                                return;
                            }
                        };
                        client::handle(ClientSession {
                            admin_addr,
                            server: serv_clone,
                            // Use bufferer for socket to reduce syscalls we make
                            stream: Box::new(BufStream::new(tls_stream)),
                            addr
                        }).await;
                    }
                });
            },
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

fn set_socket(bind: SocketAddr) -> Result<TcpListener> {
    let s = socket2::Socket::new(
        match bind {
            SocketAddr::V4(_) => socket2::Domain::IPV4,
            SocketAddr::V6(_) => socket2::Domain::IPV6
        },
        socket2::Type::STREAM,
        None
    )?;
    s.set_reuse_port(true)?;
    s.set_reuse_address(true)?;
    s.set_nonblocking(true)?;
    s.bind(&bind.into())?;
    s.listen(8192)?;

    let s = TcpListener::from_std(s.into())?;
    Ok(s)
}

async fn bind(bind: SocketAddr) -> TcpListener {
    match set_socket(bind) {
        Ok(v) => {
            v
        },
        Err(e) => {
            error!("Binding to {}:{} failed: {}", bind.ip(), bind.port(), e);
            std::process::exit(1);
        }
    }
}

pub async fn listener(config: ServerSettings, args: ArgParse, migrate_op: Option<String>) {
    let start_time  = chrono::offset::Utc::now();
    let init_size   = 1.try_into().expect("1 should not be 0");
    let (tx, rx)    = channel(init_size);
    let mut migrate = rx.clone();
    let (tx1, rx1)  = mpsc::unbounded_channel();
    let (tx2, rx2)  = channel(init_size);
    let serv        = Arc::new(Server {
        max_clients: Arc::new(Semaphore::new(config.limits.clients)),
        sources: RwLock::new(HashMap::new()),
        relay_params: RelayParams {
            slave_auth_present: config.account
                .iter()
                .any(|x| matches!(x.1, Account::Slave { .. })),
            new_source_event_tx: tx2,
            new_source_event_rx: rx2
        },
        config,
        args,
        stats: ServerStats::new(start_time.timestamp()),
        migrate_tx: Mutex::new(Some(tx)),
        migrate: rx,
        hash_calculator: tx1
    });

    let serv_clone = serv.clone();
    tokio::task::spawn_blocking(move || {
        auth::password_hasher_thread(serv_clone, rx1);
    });

    let mut set = JoinSet::new();

    if serv.config.admin_access.enabled {
        let bind_addr = serv.config.admin_access.address.bind;
        let listener  = bind(
            bind_addr,
        ).await;
        let tls = serv.config.admin_access.address.tls.as_ref().is_some_and(|x| x.enabled);
        info!("Listening on {}:{} (Admin interface) (Tls:{tls})", bind_addr.ip(), bind_addr.port());
        if tls {
            set.spawn(tls_accept_connections(serv.clone(), listener, true));
        } else {
            set.spawn(accept_connections(serv.clone(), listener, true));
        }
    }
    
    if serv.config.address.is_empty() {
        error!("At least one listening address must be specified in config file!");
        return;
    }

    for addr in &serv.config.address {
        let listener = bind(addr.bind).await;
        let tls = addr.tls.as_ref().is_some_and(|x| x.enabled);
        info!("Listening on {}:{} (Tls:{tls})", addr.bind.ip(), addr.bind.port());
        if tls {
            set.spawn(tls_accept_connections(serv.clone(), listener, false));
        } else {
            set.spawn(accept_connections(serv.clone(), listener, false));
        }
    }

    {
        let local: DateTime<Local> = DateTime::from(start_time);
        info!("Server started on {}", local);
    }
    
    if let Some(migrate_op) = migrate_op {
        // In case we are doing migration we need to spawn
        // slave server tasks while migrating
        crate::migrate::handle_successor(serv, migrate_op).await;
    } else if !serv.config.master.is_empty() {
        for i in 0..serv.config.master.len() {
            let serv_clone = serv.clone();
            tokio::spawn(async move {
                relay::slave_instance(serv_clone, i).await;
            });
        }
    }

    tokio::select! {
        _ = set.join_next() => {
            error!("A listener abrubtly exited, shutting down server");
            std::process::exit(1);
        },
        _ = migrate.recv() => {
            // Stopping accepting new connections by dropping listener futures
            set.abort_all();
            while !set.is_empty() {
                _ = set.join_next().await;
            }
            // We have a migrate operation, idling until the times come...
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            };
        }
    }
}
