use std::{sync::Arc, net::SocketAddr};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet, io::{AsyncRead, AsyncWrite, BufStream}, sync::{Semaphore, RwLock}
};
use tracing::{info, error};
use hashbrown::HashMap;

use crate::{config::ServerSettings, connection, source::Source};

pub trait Socket: Send + Sync + AsyncRead + AsyncWrite + Unpin {}
impl Socket for BufStream<TcpStream> {}
pub type Stream = Box<dyn Socket>;

/// Struct holding all info related to server
pub struct Server {
    pub config: ServerSettings,
    /// Semaphore intended to cap concurrent connection to server
    pub max_clients: Arc<Semaphore>,
    /// List of all sources whereas the mountpoint is the key
    pub sources: RwLock<HashMap<String, Source>>
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
                    // Here we are trying to acquire the semaphore before handling connection
                    // If we can't, we already hit the max number of clients allowed and we can't
                    // do nothing
                    let sem = serv_clone.max_clients.clone();
                    let aq  = sem.try_acquire();
                    if let Ok(_guard) = aq {
                        connection::handle(ClientSession {
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
    let serv = Arc::new(Server {
        max_clients: Arc::new(Semaphore::new(config.limits.clients)),
        sources: RwLock::new(HashMap::new()),
        config
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

    set.join_next().await;
    error!("A listener abrubtly exited, shutting down server");
    std::process::exit(1);
}
