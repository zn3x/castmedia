use std::{
    sync::Arc,
    os::{unix::net::{UnixListener, UnixStream}, fd::FromRawFd}, io::{Write, Read}, time::Duration
};
use anyhow::Result;
use llq::broadcast::{Sender, restore_channel_from_snapshot};
use tokio::{sync::mpsc, runtime::Handle, net::TcpStream};
use serde::{Serialize, Deserialize};
use passfd::FdPassingExt;
use tracing::{error, info};

use crate::{
    source::IcyProperties_v0_1_0, client::{ClientProperties_v0_1_0, handle_migrated, ListenerRestoreInfo},
    server::{Socket, Server}, stream::StreamReader
};

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateSource {
    pub mountpoint: String,
    #[obake(inherit)]
    pub properties: IcyProperties,
    /// Contains (number of frames in channel, channel size, last index)
    pub broadcast_snapshot: (u64, u64, u64),
    pub fallback: Option<String>,
    pub metadata: Vec<u8>,
    pub chunked: bool,
    pub queue_size: u64
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateClient {
    pub mountpoint: String,
    #[obake(inherit)]
    pub properties: ClientProperties,
    pub resume_point: u64,
    pub metaint: u64,
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub enum MigrateConnection {
    // Can't have unnamed fields here for obake to work correctly
    Source { info: MigrateSource },
    Client { info: MigrateClient }
}

pub struct MigrateClientInfo {
    /// Serialized MigrateConnection
    pub info: Vec<u8>,
    pub mountpoint: String,
    pub sock: Box<dyn Socket>
}

pub struct MigrateSourceInfo {
    /// Serialized MigrateConnection
    pub info: Vec<u8>,
    pub media: Vec<(u64, Arc<Vec<u8>>)>,
    pub sock: Box<dyn StreamReader>
}

pub struct MigrateCommand {
    /// mpsc channel where we will send connection info related to sources and listeners
    pub source: mpsc::UnboundedSender<MigrateSourceInfo>,
    pub listener: mpsc::UnboundedSender<MigrateClientInfo>
}

pub async fn handle(server: Arc<Server>, migrate: Sender<Arc<MigrateCommand>>) {
    // TODO: Can we remove all expect calls??

    // Spawning another task that will handle migration
    tokio::task::spawn_blocking(move || {
        if let Err(e) = migrate_operation(server, migrate) {
            error!("Migration failed: {}", e);
            error!("No recovery, exiting now...");
            std::process::exit(2);
        }
    });
}

pub async fn handle_successor(server: Arc<Server>, migrate: String) {
    // For us to spawn all client tasks we must also pass runtime handle to blocking task
    let runtime_handle = Handle::current();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = migrate_operation_successor(server, migrate, runtime_handle) {
            error!("Migration failed: {}", e);
            std::process::exit(2);
        }
    });
}

fn migrate_operation(server: Arc<Server>, mut migrate: Sender<Arc<MigrateCommand>>) -> Result<()> {
    let (tx, mut rx)   = mpsc::unbounded_channel();
    let (tx1, mut rx1) = mpsc::unbounded_channel();
    migrate.send(Arc::new(MigrateCommand {
        source: tx,
        listener: tx1
    }));

    info!("Preparing launch of new instance");

    let migrate_file = format!("/tmp/castradio_{}.migrate", std::process::id());
    let migrate_sock = UnixListener::bind(&migrate_file)
        .expect("We should be able to create a unix socket");

    // Now we actually spawn successor
    let exec_path    = std::env::current_exe()
        .expect("Should be able to fetch executable path");
    let successor_fh = std::process::Command::new(exec_path.to_str().unwrap())
        .args(["-m", &migrate_file, &std::env::args().last().unwrap()])
        .spawn()
        .expect("Should be able to spawn successor in migration");

    // Our job pretty much done
    // Our child will take care of the rest :')
    // It was a good life afterall
    let (mut successor, _) = migrate_sock.accept()
        .expect("Can't accept connection from spawned child");


    // The migration is done in the following way because we don't have any efficied way to
    // broadcast migration to all tasks, so we end not knewing when all senders are dropped
    // because a Sender is kept in broadcast channel.
    let mut source_count = 0;
    // We first read all sources
    loop {
        while let Ok(v) = rx.try_recv() {
            source_count += 1;

            // We first write source info
            successor.write_all(&(v.info.len() as u64).to_be_bytes())?;
            successor.write_all(&v.info)?;
            // Then media stream
            for frame in v.media {
                successor.write_all(&frame.0.to_be_bytes())?;
                successor.write_all(&frame.1.len().to_be_bytes())?;
                successor.write_all(&frame.1[..])?;
            }
            // Then pass on fd
            successor.send_fd(v.sock.fd())?;
        }

        if server.sources.blocking_read().len() == source_count {
            // We got all sources and now exiting
            break;
        }
    }
    successor.write_all(&0u64.to_be_bytes())?;

    // Then we go to clients
    // To make sure we read all sources
    // We iterate over each source and make sure no client is active
    loop {
        while let Ok(v) = rx1.try_recv() {
            // We send info first
            successor.write_all(&(v.info.len() as u64).to_be_bytes())?;
            successor.write_all(&v.info)?;
            // Then fd
            successor.send_fd(v.sock.fd())?;
        }

        // We check if all client have been removed
        // Then we remove source, if there is no source then we are finished
        let mut lock = server.sources.blocking_write();
        lock.retain(|_, x| !x.clients.blocking_read().is_empty());
        if lock.len() == 0 {
            break;
        }
    }
    successor.write_all(&0u64.to_be_bytes())?;

    // Next to you son :'') ...
    info!("Control now passed to new instance with pid {}, exiting.", successor_fh.id());
    std::process::exit(0);
}

fn migrate_operation_successor(server: Arc<Server>, migrate: String, runtime_handle: Handle) -> Result<()> {
    info!("Starting migration from old instance.");
    let mut predeseccor = UnixStream::connect(migrate)?;

    let mut buf = vec![0u8; 4092];

    // Reading all sources first
    // Then followed by listeners
    let mut sources = true;
    loop {
        let mut len_buf = [0u8; 8];
        predeseccor.read_exact(&mut len_buf)?;
        let len = u64::from_be_bytes(len_buf) as usize;
        if len == 0 {
            if sources {
                // Here we need to sleep sometime to let sources start
                sources = false;
                std::thread::sleep(Duration::from_millis(10));
                continue;
            } else {
                break;
            }
        } else if len > buf.len() {
            buf.reserve(len - buf.len());
        }
        predeseccor.read_exact(&mut buf[..len])?;
        let source_any: VersionedMigrateConnection = postcard::from_bytes(&buf[..len])?;
        let source: MigrateConnection = source_any.into();
        let cl_info = match source {
            MigrateConnection_v0_1_0::Source { info } => {
                // We now read media
                let mut snapshot_msgs = Vec::new();
                for _ in 0..info.broadcast_snapshot.0 {
                    let mut u64_buf = [0u8; 8];
                    predeseccor.read_exact(&mut u64_buf)?;
                    let index = u64::from_be_bytes(u64_buf);
                    u64_buf   = [0u8; 8];
                    predeseccor.read_exact(&mut u64_buf)?;
                    let len     = u64::from_be_bytes(u64_buf) as usize;
                    let mut vec = vec![0u8; len];
                    predeseccor.read_exact(&mut vec)?;
                    snapshot_msgs.push((index, Arc::new(vec)));
                }

                let snapshot = restore_channel_from_snapshot((snapshot_msgs, info.broadcast_snapshot.1, info.broadcast_snapshot.2));

                crate::client::ClientInfo::Source {
                    mountpoint: info.mountpoint,
                    properties: info.properties,
                    metadata: info.metadata,
                    chunked: info.chunked,
                    queue_size: info.queue_size as usize,
                    broadcast: snapshot
                }
            },
            MigrateConnection_v0_1_0::Client { info } => {
                crate::client::ClientInfo::Listener {
                    mountpoint: info.mountpoint,
                    migrated: ListenerRestoreInfo {
                        resume_point: info.resume_point,
                        metaint: info.metaint as usize,
                    },
                    properties: info.properties
                }
            }
        };

        let fd   = predeseccor.recv_fd()?;
        // Safety: We just got the fd from older instance, we are sure it's a tcp socket
        let std_sock = unsafe { std::net::TcpStream::from_raw_fd(fd) };
        if std_sock.set_nonblocking(true).is_err() {
            continue;
        }
        let sock = match TcpStream::from_std(std_sock) {
            Ok(v) => v,
            Err(_) => continue
        };
        let server_cl = server.clone();
        runtime_handle.spawn(async move {
            handle_migrated(sock, server_cl, cl_info).await;
        });
    };
    info!("Migration from old instance completed.");

    Ok(())
}
