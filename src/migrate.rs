use std::{
    sync::Arc,
    os::unix::net::{UnixListener, UnixStream}, io::{Write, Read}, time::Duration, num::NonZeroUsize
};
use anyhow::Result;
use qanat::broadcast::{Sender, restore_channel_from_snapshot, self};
use tokio::{sync::mpsc::{self, UnboundedSender, UnboundedReceiver}, runtime::Handle};
use serde::{Serialize, Deserialize};
use passfd::FdPassingExt;
use tracing::{error, info};
use url::Url;

use crate::{
    source::{IcyProperties_v0_1_0, SourceAccessType},
    client::{
        ClientProperties_v0_1_0, handle_migrated,
        ListenerRestoreInfo, SourceInfo,
        ListenerInfo, RelayedInfo_v0_1_0, RelayStream, MasterMountUpdatesInfo
    },
    server::{Socket, Server, Stream}, stream::StreamReader,
    config::MasterServerRelayScheme,
    utils::{read_socket_from_unix_socket, read_stream_from_unix_socket}, relay::RelaySourceMigrate
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
    pub queue_size: u64,
    #[obake(inherit)]
    pub is_relay: MigrateSourceConnectionType,
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub enum MigrateSourceConnectionType {
    SourceClient {
        username: String
    },
    RelayedSource {
        relayed_stream: String,
        #[obake(inherit)]
        relay_info: RelayedInfo,
        on_demand: bool
    },
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateInactiveOnDemandSource {
    pub mountpoint: String,
    #[obake(inherit)]
    pub properties: IcyProperties,
    pub master_url: String
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
pub struct MigrateMasterMountUpdates {
    pub mounts: Vec<String>,
    pub user_id: String
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateSlaveMountUpdates {
    pub master_url: String
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub enum MigrateConnection {
    // Can't have unnamed fields here for obake to work correctly
    Source { info: MigrateSource },
    Client { info: MigrateClient },
    MasterMountUpdates { info: MigrateMasterMountUpdates },
    SlaveMountUpdates { info: MigrateSlaveMountUpdates },
    SlaveInactiveOnDemandSource { info: MigrateInactiveOnDemandSource }
}

pub struct MigrateClientInfo {
    /// Serialized MigrateConnection::Client
    pub info: Vec<u8>,
    pub mountpoint: String,
    pub sock: Box<dyn Socket>
}

pub struct MigrateSourceInfo {
    /// Serialized MigrateConnection::Source
    pub info: Vec<u8>,
    pub active: Option<ActiveSourceInfo>
}

pub struct ActiveSourceInfo {
    pub media: Vec<(u64, Arc<Vec<u8>>)>,
    pub sock: Box<dyn StreamReader>
}

pub struct MigrateMasterMountUpdatesInfo {
    /// Serialized MigrateConnection::MasterMountUpdates
    pub info: Vec<u8>,
    pub sock: Stream
}

pub struct MigrateSlaveMountUpdatesInfo {
    /// Serialized MigrateConnection::SlaveMountUpdates
    pub info: Vec<u8>,
    pub sock: Stream
}

pub struct MigrateCommand {
    /// mpsc channel where we will send connection info related to sources and listeners
    pub source: mpsc::UnboundedSender<MigrateSourceInfo>,
    pub listener: mpsc::UnboundedSender<MigrateClientInfo>,
    /// /admin/mountpoint on master side
    pub master_mountupdates: mpsc::UnboundedSender<MigrateMasterMountUpdatesInfo>,
    /// /admin/mountpoint on slave side
    pub slave_mountupdates: mpsc::UnboundedSender<Option<MigrateSlaveMountUpdatesInfo>>,
}

pub async fn handle(server: Arc<Server>, migrate: Sender<Arc<MigrateCommand>>) {
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
    let (tx2, mut rx2) = mpsc::unbounded_channel();
    let (tx3, mut rx3) = mpsc::unbounded_channel();
    migrate.send(Arc::new(MigrateCommand {
        source: tx,
        listener: tx1,
        master_mountupdates: tx2,
        slave_mountupdates: tx3
    }));

    info!("Preparing launch of new instance");

    let migrate_file = format!("/tmp/castmedia_{}.migrate", std::process::id());
    let migrate_sock = UnixListener::bind(&migrate_file)
        .map_err(|_| anyhow::Error::msg("We should be able to create a unix socket"))?;

    // Now we actually spawn successor
    let name = std::env::args()
        .next()
        .ok_or(anyhow::Error::msg("Should be able to get current process name"))?;
    let mut successor_fh = std::process::Command::new(name);
    successor_fh.args(["-m", &migrate_file]);
    successor_fh.arg(&server.args.config_file);
    let successor_fh = successor_fh.spawn()
        .map_err(|_| anyhow::Error::msg("Should be able to spawn successor in migration"))?;

    // Our job pretty much done
    // Our child will take care of the rest :')
    // It was a good life afterall
    let (mut successor, _) = migrate_sock.accept()
        .map_err(|_| anyhow::Error::msg("Can't accept connection from spawned child"))?;


    // The migration is done in the following way because we don't have any efficied way to
    // broadcast migration to all tasks, so we end not knewing when all senders are dropped
    // because a Sender is kept in broadcast channel.

    // Migrating slave mountupdates
    {
        // First we count total auth mode master
        let mut count = 0;
        let total     = server.config.master
            .iter()
            .filter(|x| matches!(x.relay_scheme, MasterServerRelayScheme::Authenticated { .. }))
            .count();
        loop {
            while let Ok(v) = rx3.try_recv() {
                if let Some(v) = v {
                    // We first write mountupdates info
                    successor.write_all(&(v.info.len() as u64).to_be_bytes())?;
                    successor.write_all(&v.info)?;
                    // Then pass on fd
                    successor.send_fd(v.sock.fd())?;
                }
            }
            count += 1;
            if count >= total {
                break;
            }
        }
    }

    // Migrating sources
    let mut source_count = 0;
    // We first read all sources
    loop {
        write_sources(&mut successor, &mut rx, &mut source_count)?;

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

    // Doing best effort here as there is no way to knew if
    // there are other mountupdates streams we didn't pull
    while let Ok(v) = rx2.try_recv() {

        // We first write mountupdates info
        successor.write_all(&(v.info.len() as u64).to_be_bytes())?;
        successor.write_all(&v.info)?;
        // Then pass on fd
        successor.send_fd(v.sock.fd())?;
    }

    successor.write_all(&0u64.to_be_bytes())?;

    // Next to you son :'') ...
    info!("Control now passed to new instance with pid {}, exiting.", successor_fh.id());
    std::process::exit(0);
}

fn write_sources(successor: &mut UnixStream,
                rx: &mut UnboundedReceiver<MigrateSourceInfo>,
                source_count: &mut usize) -> Result<()> {
    while let Ok(v) = rx.try_recv() {
        *source_count += 1;

        // We first write source info
        successor.write_all(&(v.info.len() as u64).to_be_bytes())?;
        successor.write_all(&v.info)?;
        if let Some(v) = v.active {
            // Then media stream
            for frame in v.media {
                successor.write_all(&frame.0.to_be_bytes())?;
                successor.write_all(&frame.1.len().to_be_bytes())?;
                successor.write_all(&frame.1[..])?;
            }
            // Then pass on fd
            successor.send_fd(v.sock.fd())?;
        }
    }

    Ok(())
}

fn migrate_operation_successor(server: Arc<Server>, migrate: String, runtime_handle: Handle) -> Result<()> {
    info!("Starting migration from old instance.");
    let mut predeseccor = UnixStream::connect(migrate)?;

    let mut slave_id = match server.config.master.is_empty() {
        true => Vec::new(),
        false => (0..server.config.master.len()).collect::<Vec<_>>()
    };
    // TODO FIX ME: Master mounts are dropped when slave mountupdates
    // is not received in migration
    let mut slave_tx: Vec<(&Url, UnboundedSender<_>)> = Vec::new();

    let mut buf = vec![0u8; 4092];

    // A broadcast to signal when migration is finished
    let (mut tx, rx) = broadcast::channel(NonZeroUsize::MIN);

    // Reading all sources first
    // Then followed by listeners, master mountupdates
    let mut sources = true;
    'OUTER: loop {
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
            MigrateConnection::Source { info } => {
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
                let mut is_relay = None;
                let (access, relayed) = match info.is_relay {
                    MigrateSourceConnectionType::RelayedSource { relayed_stream, on_demand, relay_info } => {
                        is_relay = Some((relayed_stream.clone(), on_demand));
                        (
                            SourceAccessType::RelayedSource { relayed_source: relayed_stream },
                            Some(RelayStream {
                                info: relay_info,
                                on_demand: None
                            })
                        )
                    },
                    MigrateSourceConnectionType::SourceClient { username } => (
                        SourceAccessType::SourceClient { username: username.clone() },
                        None
                    )
                };
                let ret = SourceInfo {
                    mountpoint: info.mountpoint,
                    properties: info.properties,
                    initial_bytes_read: 0,
                    metadata: Some(info.metadata),
                    chunked: info.chunked,
                    fallback: info.fallback,
                    queue_size: info.queue_size as usize,
                    broadcast: Some(snapshot),
                    access,
                    relayed
                };
                
                if let Some((r, on_demand)) = is_relay {
                    let (stream, addr) = match read_stream_from_unix_socket(&mut predeseccor) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to fetch migrated connection: {}", e);
                            continue 'OUTER;
                        }
                    };
                    let url = match r.parse::<Url>() {
                        Ok(v) => v,
                        Err(_) => continue
                    };
                    for master in &slave_tx {
                        if master.0.host_str().eq(&url.host_str())
                            && master.0.port().eq(&url.port()) {
                            // When config is switched from on_demand mode or vice-versa
                            // we should comply
                            _ = match on_demand {
                                true  => master.1.send(RelaySourceMigrate::OnDemandActive { stream, addr, info: ret }),
                                false => master.1.send(RelaySourceMigrate::Normal { stream, addr, info: ret })
                            };
                            continue 'OUTER;
                        }
                    }
                    // If we can't find a master server for this relay source
                    // we will drop it here
                }

                crate::client::ClientInfo::Source(ret)
            },
            MigrateConnection::Client { info } => {
                crate::client::ClientInfo::Listener(ListenerInfo {
                    mountpoint: info.mountpoint,
                    migrated: Some(ListenerRestoreInfo {
                        resume_point: info.resume_point,
                        metaint: info.metaint as usize,
                    }),
                    properties: info.properties
                })
            },
            MigrateConnection::MasterMountUpdates { info } => {
                crate::client::ClientInfo::MasterMountUpdates(MasterMountUpdatesInfo {
                    mounts: info.mounts,
                    user_id: info.user_id
                })
            },
            MigrateConnection::SlaveMountUpdates { info } => {
                // Migrating slave mount update connections
                // This must always be called sent to us before
                // any SlaveInactiveOnDemandSource is sent
                for i in 0..server.config.master.len() {
                    let master = &server.config.master[i];
                    if let Ok(url) = info.master_url.parse::<Url>() {
                        if master.url.host_str().eq(&url.host_str())
                            && master.url.port().eq(&url.port()) {
                            let (stream, _) = match read_stream_from_unix_socket(&mut predeseccor) {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Failed to fetch migrated connection: {}", e);
                                    continue 'OUTER;
                                }
                            };
                            match master.relay_scheme {
                                MasterServerRelayScheme::Transparent { .. } => {
                                    // Oh no!! we just switched to only use Transparent mode
                                    // in this case we drop mountupdates connection
                                    continue 'OUTER;
                                },
                                MasterServerRelayScheme::Authenticated { .. } => {
                                    slave_id.retain(|x| x.ne(&i));
                                    let (tx, rx) = mpsc::unbounded_channel();
                                    slave_tx.push((&master.url, tx));
                                    let serv = server.clone();
                                    runtime_handle.spawn(async move {
                                        crate::relay::authenticated_mode(
                                            serv.clone(), i,
                                            Some((stream, rx))
                                        ).await;
                                    });
                                    continue 'OUTER;
                                }
                            }
                        }
                    }
                }
                continue;
            },
            MigrateConnection::SlaveInactiveOnDemandSource { info } => {
                // Here we only ever add this inactive ondemand source when
                // New loaded configuration states that we have same master
                // but with ondemand flag true
                if let Ok(url) = info.master_url.parse::<Url>() {
                    for master in &mut slave_tx {
                        if master.0.host_str().eq(&url.host_str())
                            && master.0.port().eq(&url.port()) {
                            _ = master.1.send(RelaySourceMigrate::OnDemandIdle { mount: info.mountpoint, properties: info.properties });
                            continue 'OUTER;
                        }
                    }
                }
                continue
            }
        };

        let sock = if let Ok(v) = read_socket_from_unix_socket(&mut predeseccor) {
            v
        } else {
            continue;
        };

        let server_cl = server.clone();
        let rx_cl     = rx.clone();
        runtime_handle.spawn(async move {
            // Acquiring semaphore to respect max connection limit set in config
            let sem = server_cl.max_clients.clone();
            let aq  = sem.try_acquire();
            if let Ok(_guard) = aq {
                handle_migrated(sock, server_cl, cl_info, rx_cl).await;
            }
        });
    };

    // Loading tasks for all new master relays
    for id in slave_id {
        let serv = server.clone();
        tokio::spawn(async move {
            crate::relay::slave_instance(serv, id).await;
        });
    }

    tx.send(());
    info!("Migration from old instance completed.");

    Ok(())
}
