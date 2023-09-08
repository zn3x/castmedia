use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, net::SocketAddr};
use chrono::{DateTime, Local};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet, io::{AsyncRead, AsyncWrite, BufStream}, sync::{Semaphore, RwLock}
};
use tracing::{info, error};
use hashbrown::HashMap;

use crate::{config::ServerSettings, client, source::Source};

pub trait Socket: Send + Sync + AsyncRead + AsyncWrite + Unpin {}
impl Socket for BufStream<TcpStream> {}
pub type Stream = Box<dyn Socket>;

/// Struct holding all info related to server
pub struct Server {
    pub config: ServerSettings,
    /// Semaphore intended to cap concurrent connection to server
    pub max_clients: Arc<Semaphore>,
    /// List of all sources whereas the mountpoint is the key
    pub sources: RwLock<HashMap<String, Source>>,
    /// Server general stats (this excludes calls on /api and /admin)
    pub stats: ServerStats
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
    /// Number of peak listeners to mountpoints
    pub peak_listeners: AtomicUsize,
    /// Number of connections to mountpoints (accumulating counter)
    pub listener_connections: AtomicUsize,
    /// Number of connections made by source clients (accumulating counter)
    pub source_client_connections: AtomicUsize,
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
            peak_listeners: AtomicUsize::new(0),
            listener_connections: AtomicUsize::new(0),
            source_client_connections: AtomicUsize::new(0),
            admin_api_connections: AtomicUsize::new(0),
            admin_api_connections_success: AtomicUsize::new(0),
            api_connections: AtomicUsize::new(0)
        }
    }
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

async fn bind(addr: &str, port: u16) -> TcpListener {
    match TcpListener::bind(&format!("{}:{}", addr, port)).await {
        Ok(v) => {
            info!("Listening on {}:{}", addr, port);
            v
        },
        Err(e) => {
            error!("Binding to {}:{} failed: {}", addr, port, e);
            std::process::exit(1);
        }
    }
}

pub async fn listener(config: ServerSettings) {
    let start_time = chrono::offset::Utc::now();
    let serv       = Arc::new(Server {
        max_clients: Arc::new(Semaphore::new(config.limits.clients)),
        sources: RwLock::new(HashMap::new()),
        config,
        stats: ServerStats::new(start_time.timestamp())
    });

    let mut set = JoinSet::new();

    if serv.config.admin_access.enabled {
        let listener = bind(
            &serv.config.admin_access.address.addr,
            serv.config.admin_access.address.port
        ).await;
        set.spawn(accept_connections(serv.clone(), listener, true));
    }

    if serv.config.address.is_empty() {
        error!("At least one listening address must be specified in config file!");
        return;
    }

    for addr in &serv.config.address {
        let listener = bind(&addr.addr, addr.port).await;
        set.spawn(accept_connections(serv.clone(), listener, false));
    }

    {
        let local: DateTime<Local> = DateTime::from(start_time);
        info!("Server started on {}", local);
    }

    set.join_next().await;
    error!("A listener abrubtly exited, shutting down server");
    std::process::exit(1);
}
