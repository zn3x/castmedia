use std::{
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    net::SocketAddr, hash::BuildHasher, os::fd::AsRawFd, time::Duration,
    ops::{Deref, DerefMut},
    pin::Pin, task::{Poll, Context}, io
};
use chrono::{DateTime, Local};
use futures::StreamExt;
use qanat::broadcast::{channel, Receiver, Sender};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet, io::{AsyncRead, AsyncWrite, BufReader, BufWriter, ReadBuf},
    sync::{Semaphore, RwLock}, signal
};
use tokio_native_tls::{native_tls, TlsStream, TlsAcceptor};
use tracing::{info, error, warn};
use hashbrown::{HashMap, hash_map::DefaultHashBuilder};
use inotify::{Inotify, WatchMask};
use anyhow::Result;
use pin_project::pin_project;

use crate::{
    config::{ServerSettings, TlsIdentity, Role}, 
    client, source::Source, migrate::MigrateCommand,
    relay,
    auth::UserRef
};

pub trait Socket: Send + Sync + AsyncRead + AsyncWrite + Unpin {
    fn fd(&self) -> i32;
    fn local_addr(&self) -> Result<SocketAddr>;
    fn peer_addr(&self) -> Result<SocketAddr>;
    fn consume(self: Box<Self>) -> (TcpStream, Vec<u8>);
}

impl Socket for BufReader<BufWriter<TcpStream>> {
    fn fd(&self) -> i32 {
        self.get_ref().get_ref().as_raw_fd()
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().get_ref().local_addr()?)
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().get_ref().peer_addr()?)
    }

    fn consume(self: Box<Self>) -> (TcpStream, Vec<u8>) {
        let read_buf = self.buffer().to_vec();
        let buf_writer: BufWriter<TcpStream> = self.into_inner();
        (buf_writer.into_inner(), read_buf)
    }
}

impl Socket for BufReader<BufWriter<TlsStream<TcpStream>>> {
    fn fd(&self) -> i32 { unreachable!("Can't migrate Tls connection") }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().get_ref().get_ref().get_ref().get_ref().local_addr()?)
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.get_ref().get_ref().get_ref().get_ref().get_ref().peer_addr()?)
    }

    fn consume(self: Box<Self>) -> (TcpStream, Vec<u8>) { unreachable!("Can't migrate Tls connection") }
}

#[pin_project]
pub struct PrefixedStream {
    stream: BufReader<BufWriter<TcpStream>>,
    prefix: Option<Vec<u8>>,
}

impl AsyncRead for PrefixedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        if let Some(ref mut prefix) = this.prefix {
            let n = prefix.len().min(buf.remaining());
            buf.put_slice(&prefix[..n]);
            prefix.drain(0..n);

            if prefix.is_empty() {
                *this.prefix = None;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(this.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.project();
        Pin::new(this.stream).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        Pin::new(this.stream).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.project();
        Pin::new(this.stream).poll_shutdown(cx)
    }
}

impl Socket for PrefixedStream {
    fn fd(&self) -> i32 {
        self.stream.get_ref().get_ref().as_raw_fd()
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.stream.get_ref().get_ref().local_addr()?)
    }

    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok(self.stream.get_ref().get_ref().peer_addr()?)
    }

    fn consume(self: Box<Self>) -> (TcpStream, Vec<u8>) {
        let mut read_buf = self.prefix.unwrap_or_default();
        read_buf.extend_from_slice(self.stream.buffer());
        let buf_writer: BufWriter<TcpStream> = self.stream.into_inner();
        (buf_writer.into_inner(), read_buf)
    }
}

pub struct Stream(pub Box<dyn Socket>);

impl Stream {
    pub fn new(s: TcpStream) -> Self {
        Self(Box::new(BufReader::new(BufWriter::new(s))))
    }

    pub fn new_tls(s: TlsStream<TcpStream>) -> Self {
        Self(Box::new(BufReader::new(BufWriter::new(s))))
    }

    pub fn new_migrated(stream: TcpStream, prefix: Vec<u8>) -> Self {
        match prefix.is_empty() {
            true => Self::new(stream),
            false => Self(Box::new(PrefixedStream { stream: BufReader::new(BufWriter::new(stream)), prefix: Some(prefix) }))
        }
    }

    pub fn prepare_for_migration(self) -> (TcpStream, Vec<u8>) {
        self.0.consume()
    }
}

impl Deref for Stream {
    type Target = Box<dyn Socket>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Stream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Struct holding all info related to server
pub struct Server {
    pub config: ServerSettings,
    /// Semaphore intended to cap concurrent connection to server
    pub max_clients: Arc<Semaphore>,
    /// List of all sources whereas the mountpoint is the key
    pub sources: RwLock<HashMap<String, Source>>,
    /// Server general stats (this excludes calls on /api and /admin)
    pub stats: ServerStats,
    /// Trigger to notify all tasks when live migration is done
    pub migrate_tx: Sender<Arc<MigrateCommand>>,
    /// Receiver to listener when migration happens
    pub migrate: Receiver<Arc<MigrateCommand>>,
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
    /// Number of total active listening clients to sources
    pub active_listeners: AtomicUsize,
    /// Number of total active outbound connections to master server
    pub active_relay: AtomicUsize,
    /// Number of peak listeners to sources
    pub peak_listeners: AtomicUsize,
    /// Number of connections to sources (accumulating counter)
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
    /// Flag used to check if there is a slave/yp auth configured
    pub slave_or_yp_auth_present: bool,
    /// Notification for new source (sender)
    pub new_source_event_tx: Sender<()>,
    /// Notification for new source (receiver)
    pub new_source_event_rx: Receiver<()>
}

/// A client session
pub struct ClientSession {
    /// Type of address: Is this an admin address or where auth is allowed
    pub addr_type: AddrType,
    /// Server info
    pub server: Arc<Server>,
    /// Socket of this client session
    pub stream: Stream,
    /// Address of our peer
    pub addr: SocketAddr,
    /// Assign account when user authentifies
    pub user: Option<UserRef>
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AddrType {
    Admin,
    AuthAllowed,
    Simple
}

impl std::fmt::Display for ClientSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.user {
            Some(v) => write!(f, "client:[{}, addr:{}]", v, self.addr),
            None => write!(f, "client:[unauthenticated, addr:{}]", self.addr)
        }
    }
}

/// A generic session
pub struct Session {
    /// Server info
    pub server: Arc<Server>,
    /// Socket of this client session
    pub stream: Stream,
    /// Address of our peer
    pub addr: SocketAddr
}

async fn accept_connections_plain(serv: Arc<Server>, listener: TcpListener, addr_type: AddrType) {
    accept_loop(serv, &listener, addr_type, None).await;
}

async fn accept_loop(serv: Arc<Server>, listener: &TcpListener, addr_type: AddrType, tls: Option<TlsAcceptor>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let serv_clone = serv.clone();
                match &tls {
                    Some(tls) => {
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            let tls_stream = match tls.accept(stream).await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Tls connection failed with {}: {}", addr, e);
                                    return;
                                }
                            };
                            handle_accepted(serv_clone, addr_type, Stream::new_tls(tls_stream), addr).await;
                        });
                    },
                    None => {
                        tokio::spawn(handle_accepted(serv_clone, addr_type, Stream::new(stream), addr));
                    }
                }
            },
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

async fn handle_accepted(serv: Arc<Server>, addr_type: AddrType, stream: Stream, addr: SocketAddr) {
    serv.stats.connections.fetch_add(1, Ordering::Relaxed);
    let sem = serv.max_clients.clone();
    let aq  = sem.try_acquire();
    if let Ok(_guard) = aq {
        client::handle(ClientSession { addr_type, server: serv, stream, addr, user: None }).await;
    }
}

async fn tls_accept_connections(serv: Arc<Server>, listener: TcpListener, addr_type: AddrType, tls_identity: Option<TlsIdentity>) {
    // First we retrieve tls identity
    let tls_identity = tls_identity.expect("Should be able to find Tls identity");
    
    let inotify = Inotify::init()
        .expect("Should be able to initialize inotify");
    inotify.watches().add(std::path::Path::new(&tls_identity.cert), WatchMask::CREATE | WatchMask::MODIFY)
        .expect("Couldn't create inotify watch for tls identity file");
    let mut tls_update_stream = inotify.into_event_stream([0u8; 1024])
        .expect("Couldn't create inotify tls identity update stream");

    let mut tls_acceptor;
    let mut tls_id_hash         = 0u64;
    (tls_id_hash, tls_acceptor) = load_cert(&tls_identity, tls_id_hash).await
        .expect("Failed loading Tls identity")
        .unwrap();

    loop {
        tokio::select! {
            _ = accept_loop(serv.clone(), &listener, addr_type, Some(tls_acceptor.clone())) => {
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

        match load_cert(&tls_identity, tls_id_hash).await {
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

async fn shutdown_signal() {
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .expect("Failed to install SIGTERM handler");

    tokio::select! {
        _ = sigterm.recv() => {},
        _ = signal::ctrl_c() => {},
    }
}

pub async fn listener(config: ServerSettings) {
    let start_time  = chrono::offset::Utc::now();
    let init_size   = 1.try_into().expect("1 should not be 0");
    let (tx, rx)    = channel(init_size);
    let mut migrate = rx.clone();
    let (tx2, rx2)  = channel(init_size);
    let serv        = Arc::new(Server {
        max_clients: Arc::new(Semaphore::new(config.limits.clients)),
        sources: RwLock::new(HashMap::new()),
        relay_params: RelayParams {
            slave_or_yp_auth_present: config.account
                .iter()
                .any(|x| matches!(x.1.role, Role::Slave | Role::YP)),
            new_source_event_tx: tx2,
            new_source_event_rx: rx2
        },
        config,
        stats: ServerStats::new(start_time.timestamp()),
        migrate_tx: tx,
        migrate: rx,
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
            set.spawn(
                tls_accept_connections(
                    serv.clone(),
                    listener,
                    AddrType::Admin,
                    serv.config.admin_access.address.tls.clone()
                )
            );
        } else {
            set.spawn(accept_connections_plain(serv.clone(), listener, AddrType::Admin));
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
        let is_auth = if addr.allow_auth { AddrType::AuthAllowed } else { AddrType::Simple };
        if tls {
            set.spawn(
                tls_accept_connections(
                    serv.clone(),
                    listener,
                    is_auth,
                    serv.config.admin_access.address.tls.clone()
                )
            );
        } else {
            set.spawn(accept_connections_plain(serv.clone(), listener, is_auth));
        }
    }

    {
        let local: DateTime<Local> = DateTime::from(start_time);
        info!("Server started on {}", local);
    }
    
    if serv.config.migrate.enabled {
        // Doing migration if there is an active instance
        crate::migrate::handle_successor(serv.clone()).await;
        // After it we start listening ourselves
        // Checking if all bind addresses are not tls
        let uses_tls = serv.config.address
            .iter()
            .any(|x| x.tls.is_some() && x.tls.as_ref().unwrap().enabled);
        if uses_tls || (serv.config.admin_access.address.tls.is_some()
            && serv.config.admin_access.address.tls.as_ref().unwrap().enabled) {
            warn!("Migration listener won't be started because Tls migration is not supported!");
        } else {
            crate::migrate::spawn_listener(serv.clone()).await;
        }
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
        },
        _ = shutdown_signal() => {
            info!("Received shutdown signal, starting graceful shutdown");
            set.abort_all();
            while !set.is_empty() {
                _ = set.join_next().await;
            }
            // Kill all sources to trigger listener cleanup (fallback movement, etc.)
            let kill_senders: Vec<_> = {
                let mut sources = serv.sources.write().await;
                sources.iter_mut().filter_map(|(_, s)| s.kill.take()).collect()
            };
            info!("Stopping {} active source(s)", kill_senders.len());
            for kill in kill_senders {
                _ = kill.send(());
            }
            // Give a short window for cleanup
            tokio::time::sleep(Duration::from_secs(2)).await;
            info!("Graceful shutdown complete");
            std::process::exit(0);
        }
    }
}
