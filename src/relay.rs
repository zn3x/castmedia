use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration, net::SocketAddr
};
use anyhow::Result;
use ::futures::{future::select_all, FutureExt};
use hashbrown::{HashMap, hash_map::OccupiedError};
use qanat::{
    mpsc,
    broadcast::{RecvError, Receiver}
};
use serde_json::json;
use tokio::{
    io::AsyncWriteExt, task::JoinHandle
};
use tracing::{info, error};
use url::Url;
use serde::{Deserialize, Serialize};

use crate::{
    broadcast::relay_broadcast_metadata, client::{RelayStream, SourceInfo, StreamOnDemand},
    config::MasterServerRelayScheme, http::{ChunkedResponseReader, HttpClient},
    internal_api::v1::{
        IcyProperties, MigrateConnection, MigrateInactiveOnDemandSource, MigrateMasterMountUpdates, MigrateSlaveMountUpdates, RelayedInfo
    },
    migrate::{MigrateCommand, MigrateMasterMountUpdatesInfo}, response::ChunkedResponse,
    server::{ClientSession, Server, Session, Stream},
    source::{Source, SourceAccessType},
    stream::RelayBroadcastStatus,
    utils::{basic_auth, concat_path, get_header}
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

pub async fn master_mount_updates(mut session: ClientSession,
                                  // map holding all mounts as tuple (mount, metadata_channel)
                                  mut mounts: HashMap<String, Receiver<Arc<(u64, Vec<u8>)>>>) -> Result<()> {
    info!("Mount updates stream initialized for {}", session);

    let user_id = session.user
        .as_ref()
        .expect("Should be identified at this point")
        .id
        .clone();
    let chunked_writer = ChunkedResponse::new_ready();

    let mut new_source_notify = session.server
        .relay_params
        .new_source_event_rx
        .clone();

    let mut migrate_comm = session.server.migrate.clone();
    // vec holding futures of reads we must perform on each source channel
    let mut reads;
    let mut ret;
    let mut remove     = None;
    let mut migrate_ch = None;

    'NO_SOURCE: loop {
        if let Some(m) = migrate_ch {
            migrate_master_mount_updates(
                m,
                user_id,
                session,
                mounts
            ).await;
        }

        tokio::select! {
            r = new_source_notify.recv() => ret = r,
            m = migrate_comm.recv() => {
                migrate_ch = Some(m);
                continue;
            }
        };
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
                // TODO: Can we eliminate the need to transmit metadata for source if slave
                // already listening to that source
                tokio::select! {
                    ((mount, metadata), _, _) = select_all(reads) => {
                        let ser = match metadata {
                            Ok(v) => {
                                // TODO: Can we use futures returned by select_all directly
                                // Holy borrowing hell
                                // We need to re-add recv future for this channel
                                serde_json::to_vec(&json!({
                                    "mount": mount,
                                    "metadata": &v.1,
                                    "type": "metadata"
                                }))?
                            },
                            Err(RecvError::Lagged) => continue,
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
                    },
                    m = migrate_comm.recv() => {
                        migrate_ch = Some(m);
                        continue 'NO_SOURCE;
                    }
                }
            };
        };
    };
}

async fn migrate_master_mount_updates(migrate: Result<Arc<MigrateCommand>, qanat::broadcast::RecvError>,
                                      user_id: String, session: ClientSession, 
                                      mounts: HashMap<String, Receiver<Arc<(u64, Vec<u8>)>>>) -> ! {
    let migrate = migrate
        .expect("Got migrate notice with closed mpsc");

    let info = MigrateConnection::MasterMountUpdates {
        info: MigrateMasterMountUpdates {
            mounts: mounts.into_keys().collect::<Vec<String>>(),
            user_id,
            client_addr: session.addr.to_string()
        }
    };

    if let Ok(info) = serde_json::to_vec(&info) {
        _ = migrate.master_mountupdates.send(MigrateMasterMountUpdatesInfo {
            info,
            sock: session.stream
        });
    }

    loop {
        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
    }
}

async fn fetch_available_mounts(server: &Server, url: &Url) -> Result<MasterMounts> {
    let timeout = Duration::from_millis(server.config.limits.master_timeout);
    tokio::time::timeout(
        timeout,
        async {
            let client     = HttpClient::connect(server, url, "/api/serverinfo").await?;
            let mut reader = client.get().await?;
            server.stats.source_relay_connections.fetch_add(1, Ordering::Relaxed);
            let body_buf   = reader.read_to_end("application/json").await?;
            // Now we go to parsing response
            let info: MasterMounts = serde_json::from_slice(&body_buf)?;

            Ok(info)
        }
    ).await?
}

async fn get_stream(serv: &Arc<Server>, url: &Url, mount: &str,
                    auth: Option<&str>)
    -> Result<(Stream, SocketAddr, usize, usize, IcyProperties, bool)> {
    // Fetching media stream from master
    let mut client = HttpClient::connect(serv.as_ref(), url, mount).await?;
    let addr       = client.peer_addr()?;
    client.add_header("Icy-Metadata: 1\r\n");
    if let Some(auth) = auth {
        client.add_header(auth);
    }
    let mut reader  = client.get().await?;
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
    if get_header("icy-name", resp.headers).is_none() {
        return Err(anyhow::Error::msg("not an icecast server"));
    }

    let chunked = match get_header("transfer-encoding", resp.headers) {
        // If nothing is set then it's identity
        Some(b"identity") | None => false,
        Some(b"chunked") => true,
        _ => return Err(anyhow::Error::msg("Unsupported transfer encoding"))
    };

    // Getting metaint
    let metaint = match get_header("icy-metaint", resp.headers) {
        Some(metaint) => std::str::from_utf8(metaint)?.parse::<usize>()?,
        None => {
            return Err(anyhow::Error::msg("missing icy-metaint header"));
        }
    };

    let properties = IcyProperties::new(match get_header("content-type", resp.headers) {
        Some(v) => std::str::from_utf8(v)?.to_owned(),
        None => {
            return Err(anyhow::Error::msg("missing content-type header"));
        }
    });
    Ok((reader.get_inner_stream(), addr, metaint, headers_buf.len(), properties, chunked))
}

pub async fn authenticated_mode(serv: Arc<Server>, master_ind: usize,
                                mut migrate_successor: Option<(Option<Stream>, mpsc::UnboundedReceiver<RelaySourceMigrate>)>) {
    let master_server     = &serv.config.master[master_ind];
    let reconnect_timeout = match &master_server.relay_scheme {
        MasterServerRelayScheme::Authenticated { reconnect_timeout, .. } => *reconnect_timeout,
        _ => unreachable!()
    };
    let timeout = Duration::from_millis(reconnect_timeout);
    loop {
        match migrate_successor.take() {
            Some((Some(stream), m)) => authenticated_mode_event_listener(&serv, stream, master_ind, Some(m)).await,
            v => {
                let m = match v {
                    Some((None, m)) => Some(m),
                    _ => None
                };
                let fut = async {
                    loop {
                        match authenticated_mode_fetch_updates_stream(&serv, master_ind).await {
                            Ok(v) => return v,
                            Err(e) => error!("Initial mountupdates connection to {} failed: {}", master_server, e)
                        }
                        tokio::time::sleep(timeout).await;
                    }
                };
                let mut migrate_comm = serv.migrate.clone();
                let stream = tokio::select! {
                    r = fut => r,
                    migrate = migrate_comm.recv() => {
                        let migrate = migrate
                            .expect("Got migrate notice with closed mpsc");
                        _ = migrate.slave_mountupdates.send(None);
                        loop {
                            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                        }
                    }
                };
                authenticated_mode_event_listener(&serv, stream, master_ind, m).await;
            }
        }
    }
}

async fn authenticated_mode_fetch_updates_stream(serv: &Arc<Server>, master_ind: usize) -> Result<Stream> {
    let url  = &serv.config.master[master_ind].url;
    let auth = match &serv.config.master[master_ind].relay_scheme {
        MasterServerRelayScheme::Authenticated { user, pass, .. } =>
            format!("Authorization: Basic {}\r\n", basic_auth(user, pass)),
        // Should not reach this as we did treat it already
        _ => unreachable!()
    };

    let mut client  = HttpClient::connect(serv.as_ref(), url, "/admin/mountupdates").await?;
    client.add_header(&auth);
    let mut reader  = client.get().await?;
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
    
    // TODO: check transfer-encoding??
    // Shouldn't matter anyway

    Ok(reader.get_inner_stream())
}

pub enum RelaySourceMigrate {
    OnDemandIdle {
        mount: String,
        properties: IcyProperties
    },
    OnDemandActive {
        stream: Stream,
        addr: SocketAddr,
        info: SourceInfo
    },
    Normal {
        stream: Stream,
        addr: SocketAddr,
        info: SourceInfo
    }
}

struct RelaySourceTask {
    handle: JoinHandle<()>,
    just_migrated: bool
}

async fn authenticated_mode_event_listener(serv: &Arc<Server>, mut stream: Stream,
                                           master_ind: usize,
                                           migrate_successor: Option<mpsc::UnboundedReceiver<RelaySourceMigrate>>) {
    let master            = &serv.config.master[master_ind];
    let (auth, on_demand) = match &serv.config.master[master_ind].relay_scheme {
        MasterServerRelayScheme::Authenticated { user, pass, stream_on_demand, .. } => (
            format!("Authorization: Basic {}\r\n", basic_auth(user, pass)),
            *stream_on_demand
        ),
        // Should not reach this as we did treat it already
        _ => unreachable!()
    };

    let mut sources = HashMap::new();
    if let Some(mut m) = migrate_successor {
        while let Ok(v) = m.recv().await {
            match v {
                RelaySourceMigrate::Normal { stream, addr, info } => {
                    let mount    = info.mountpoint.clone();
                    let serv_cl  = serv.clone();
                    let auth_cl  = auth.to_owned();
                    let task     = tokio::task::spawn(async move {
                        let cmount = info.mountpoint.clone();
                        let ret    = crate::source::handle_source(
                            Session {
                                server: serv_cl.clone(),
                                stream,
                                addr
                            },
                            info
                        ).await
                            .expect("Error shouldn't be returned on relay stream")
                            .expect("RelayBroadcastStatus should be returned on relay stream");

                        match ret {
                            RelayBroadcastStatus::StreamEndOrIdle(_) |
                                RelayBroadcastStatus::Unreachable(_) => {
                                relay_source(&serv_cl, master_ind, &cmount, Some(&auth_cl)).await;
                            },
                            _ => ()
                        }
                    });
                    sources.insert(mount, RelaySourceTask { handle: task, just_migrated: true });
                },
                RelaySourceMigrate::OnDemandIdle { mount, properties } => {
                    handle_mount_update(
                        &mut sources, serv,
                        master_ind, on_demand,
                        MountUpdate::New { mount, properties },
                        &auth
                    ).await;
                },
                RelaySourceMigrate::OnDemandActive { stream, addr, mut info } => {
                    let wake_sink                  = diatomic_waker::WakeSink::new();
                    let wake_src                   = wake_sink.source();
                    let (mut source, broadcast,
                         kill_notifier)            = Source::new(
                             info.properties.clone(), info.fallback.clone(),
                             info.access.clone(),
                             info.broadcast.take());
                    source.on_demand_notify_reader = Some(wake_src);

                    let stats = source.stats.clone();

                    if let Some(v) = &mut info.relayed {
                        v.on_demand = Some(StreamOnDemand {
                            stats,
                            broadcast,
                            kill_notifier,
                            on_demand_notify: wake_sink
                        })
                    }

                    crate::broadcast::broadcast_metadata(&source, &None, &None).await;

                    if serv.sources.write().await.try_insert(info.mountpoint.clone(), source).is_err() {
                        continue;
                    }

                    let mount    = info.mountpoint.clone();
                    let serv_cl  = serv.clone();
                    let auth_cl  = auth.to_owned();
                    let task     = tokio::task::spawn(async move {
                        let cmount = info.mountpoint.clone();
                        let ret    = crate::source::handle_source(
                            Session {
                                server: serv_cl.clone(),
                                stream,
                                addr
                            },
                            info
                        ).await
                            .expect("Error shouldn't be returned on relay stream")
                            .expect("RelayBroadcastStatus should be returned on relay stream");

                        if let Some(v) = relay_source_on_demand_exepction(ret).await {
                            relay_source_on_demand(&serv_cl, master_ind, cmount, v, auth_cl).await;
                        }
                    });

                    sources.insert(mount, RelaySourceTask { handle: task, just_migrated: true });
                }
            }
        }
    }

    info!("Starting mount updates from {}", master);

    serv.stats.active_relay.fetch_add(1, Ordering::Relaxed);

    let mut len_enc = [0u8; 4];
    let mut buf     = vec![0u8; 1024];
    let mut chunked = ChunkedResponseReader::new();

    let mut migrate_comm = serv.migrate.clone();
    let fut              = async {
        loop {
            match authenticated_mode_reader(&mut stream, &mut len_enc, &mut buf, &mut chunked).await {
                Err(e)    => {
                    error!("Mount updates from {} stopped: {}", master, e);
                    return;
                },
                Ok(event) => handle_mount_update(&mut sources, serv, master_ind, on_demand, event, &auth).await
            }
        }
    };

    tokio::select! {
        _ = fut => (),
        migrate = migrate_comm.recv() => {
            let migrate = migrate
                .expect("Got migrate notice with closed mpsc");

            let info = MigrateConnection::SlaveMountUpdates {
                info: MigrateSlaveMountUpdates {
                    master_url: master.url.to_string()
                }
            };
            if let Ok(info) = serde_json::to_vec(&info) {
                _ = migrate.slave_mountupdates.send(Some(
                        crate::migrate::MigrateSlaveMountUpdatesInfo { info, sock: stream }
                ));
            }
            loop {
                tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
            }
        }
    }
    
    serv.stats.active_relay.fetch_sub(1, Ordering::Relaxed);
}

async fn authenticated_mode_reader(stream: &mut Stream, len_enc: &mut [u8; 4],
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

async fn handle_mount_update(sources: &mut HashMap<String, RelaySourceTask>,
                             serv: &Arc<Server>, master_ind: usize,
                             on_demand: bool, event: MountUpdate, auth: &str) {
    let master = &serv.config.master[master_ind].url;

    // Now handling events for every source
    match event {
        MountUpdate::New { mount, properties } => {
            if let Some(mut task) = sources.remove(&mount) {
                if task.just_migrated {
                    task.just_migrated = false;
                    sources.insert(mount, task);
                    return;
                }
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
                    if let SourceAccessType::RelayedSource { relayed_source } = &source.access {
                        if relayed_source.starts_with(serv.config.master[master_ind].url.as_str()) {
                            // This is the same source
                            let kill = source.kill.take();
                            drop(lock);
                            if let Some(kill) = kill {
                                _ = kill.send(());
                                _ = task.handle.await;
                            }
                        } else {
                            // 💀 what can we do...
                            // Some asshat have same mount
                            return;
                        }
                    }
                }
            }

            // If we don't have on_demand enabled
            // we don't need to do all the following hassle
            // handle it like normal relay
            if !on_demand {
                let serv_cl  = serv.clone();
                let auth_cl  = auth.to_owned();
                let mount_cl = mount.clone();
                let task     = tokio::task::spawn(async move {
                    relay_source(&serv_cl, master_ind, &mount_cl, Some(&auth_cl)).await;
                });
                sources.insert(mount, RelaySourceTask { handle: task, just_migrated: false });
                return;
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
                 kill_notifier)            = Source::new(
                     properties, None,
                     SourceAccessType::RelayedSource { relayed_source: url },
                     None);
            source.on_demand_notify_reader = Some(wake_src);

            let stats = source.stats.clone();

            crate::broadcast::broadcast_metadata(&source, &None, &None).await;

            if lock.try_insert(mount.clone(), source).is_err() {
                panic!("Should be able to add relay source as we have lock");
            }

            let serv_cl  = serv.clone();
            let auth_cl  = auth.to_owned();
            let mount_cl = mount.clone();
            let task     = tokio::task::spawn(async move {
                relay_source_on_demand(
                    &serv_cl, master_ind,
                    mount_cl,
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

            sources.insert(mount, RelaySourceTask { handle: task, just_migrated: false });
        },
        MountUpdate::Metadata { mount, metadata } => {
            // we skip updating metadata here if either:
            // 1. on_demand is disabled
            // 2. on_demand is enable and we are currently listening
            // 3. no task is currently listening to mount
            if !on_demand || !sources.contains_key(&mount) {
                return;
            }

            let (icymeta, mut metadata_broadcast, last) = match serv.sources.read().await.get(&mount) {
                Some(source) => if on_demand && source.stats.active_listeners.load(Ordering::Acquire) != 0 {
                    return;
                } else {
                    (
                        source.metadata.clone(),
                        source.meta_broadcast_sender.clone(),
                        source.broadcast.last_index()
                    )
                }
                None => return
            };
            relay_broadcast_metadata(
                &icymeta,
                &mut metadata_broadcast,
                last,
                metadata
            ).await;
        },
        MountUpdate::Unmounted { mount } => {
            // If source disconnects from master then we should do the same
            // Send a kill notification to task
            if let Some(task) = sources.remove(&mount) {
                let mut lock = serv.sources.write().await;
                if let Some(source) = lock.get_mut(&mount) {
                    if let Some(kill) = source.kill.take() {
                        drop(lock);
                        _ = kill.send(());
                        if !on_demand {
                            // We might need to cancel task in case there was
                            // already an exception and relay_source didn't receive
                            // Kill signal properly
                            task.handle.abort();
                        }
                    }
                }
                // We should also try to unmount source
                // This is necesarry as source task may be slow when removing
                // source and we end up reading a new MountUpdate::New
                // Where source with same mount still hasn't been cleaned up
                crate::stream::unmount_source(serv, &mount).await;
            }
        }
    }
}

async fn relay_source(serv: &Arc<Server>, master_ind: usize, mount: &str, auth: Option<&str>) {
    loop {
        let status = fetch_source_from_master(
            serv, master_ind, mount.to_owned(),
            None,
            auth
        ).await;

        match status {
            RelayBroadcastStatus::Killed => break,
            RelayBroadcastStatus::StreamEndOrIdle(_) => (),
            RelayBroadcastStatus::Unreachable(_) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
            },
            // TODO: Better way to handle this
            RelayBroadcastStatus::MountExists |
                RelayBroadcastStatus::LimitReached => break
        }
    }
}

async fn relay_source_on_demand(serv: &Arc<Server>, master_ind: usize, mount: String,
                                mut on_demand: StreamOnDemand, auth: String) {
    // We here need to wait until we get notification that we have
    // more than 0 listener
    // ofc we should also handle also the event of killing ourselves
    // Here we migrate only the state of the stream not itself
    let mut migrate_comm = serv.migrate.clone();
    loop {
        tokio::select! {
            _ = on_demand.kill_notifier.recv() => break,
            _ = on_demand
                .on_demand_notify
                .wait_until(|| {
                    if on_demand.stats.active_listeners.load(Ordering::Relaxed) != 0 {
                        Some(())
                    } else {
                        None
                    }
                }) => (),
            migrate = migrate_comm.recv() => {
                let migrate = migrate
                    .expect("Got migrate notice with closed mpsc");

                let master = &serv.config.master[master_ind];
                if let Some(properties) = serv.sources.read().await.get(&mount).map(|x| (*x.properties).to_owned()) {
                    let info = MigrateConnection::SlaveInactiveOnDemandSource {
                        info: MigrateInactiveOnDemandSource {
                            master_url: master.url.to_string(),
                            mountpoint: mount,
                            properties
                        }
                    };
                    if let Ok(info) = serde_json::to_vec(&info) {
                        _ = migrate
                            .source
                            .send(crate::migrate::MigrateSourceInfo {
                                info,
                                active: None
                            });
                    }
                }

                loop {
                    tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                }
            }
        }

        let ret = fetch_source_from_master(serv, master_ind, mount.clone(), Some(on_demand), Some(&auth)).await;
        match relay_source_on_demand_exepction(ret).await {
            Some(v) => on_demand = v,
            None => break
        }
    }
}

async fn relay_source_on_demand_exepction(ret: RelayBroadcastStatus) -> Option<StreamOnDemand> {
    match ret {
        // Stream suddenly ended (Due to network problems or detecting end of stream before master
        // server sending unmount notification)
        // or we don't have any listener to continue pulling stream
        // In both cases we keep retrying until we get a notification from master to stop
        RelayBroadcastStatus::StreamEndOrIdle(v) => v,
        // Master server told us that source no longer exists
        RelayBroadcastStatus::Killed => None,
        // We can't seem to reach master server (reasons may be: network, master refusing
        // connection, ...)
        // we will keep trying until we do so or get a kill notification
        RelayBroadcastStatus::Unreachable(v) => {
            match v {
                Some(mut on_demand) => {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    match on_demand.kill_notifier.try_recv() {
                        // If we get kill notification or channel closed we stop
                        Ok(_) | Err(qanat::oneshot::TryRecvError::Closed) => None,
                        _ => Some(on_demand)
                    }
                },
                None => None
            }
        },
        _ => unreachable!()
    }
}

async fn fetch_source_from_master(serv: &Arc<Server>, master_ind: usize, mount: String,
                      on_demand: Option<StreamOnDemand>, auth: Option<&str>)
    -> RelayBroadcastStatus {
    let url = &serv.config.master[master_ind].url;

    match get_stream(serv, url, &mount, auth).await {
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
                    access: SourceAccessType::RelayedSource { relayed_source: url.clone() },
                    relayed: Some(RelayStream {
                        info: RelayedInfo {
                            metaint,
                            metaint_position: 0,
                            metadata_reading: false,
                            metadata_remaining: 0,
                            metadata_buffer: Vec::new()
                        },
                        on_demand
                    })
                }
            ).await;
            if let Err(e) = ret.as_ref() {
                error!(
                    "{} relaying for {} ended with: {}",
                    serv.config.master[master_ind],
                    mount,
                    e
                );
            }
            ret
                .expect("Error shouldn't be returned on relay stream")
                .expect("RelayBroadcastStatus should be returned on relay stream")
        },
        Err(e) => {
            error!(
                "{} relaying for {} failed: {}",
                serv.config.master[master_ind],
                mount,
                e
            );
            RelayBroadcastStatus::Unreachable(on_demand)
        }
    }
}

pub async fn slave_instance(serv: Arc<Server>, master_ind: usize) {
    // Here we should be fine a we did check bounds before
    let master_server = &serv.config.master[master_ind];

    match &master_server.relay_scheme {
        MasterServerRelayScheme::Transparent { update_interval } => {
            let mut interval = tokio::time::interval(Duration::from_millis(*update_interval));
            loop {
                interval.tick().await;

                match fetch_available_mounts(&serv, &master_server.url).await {
                    Ok(mounts) => {
                        let lock = serv.sources.read().await;
                        for mount in &mounts.mounts {
                            if !lock.contains_key(mount) {
                                let serv_clone  = serv.clone();
                                let mount_clone = mount.clone();
                                tokio::spawn(async move {
                                    fetch_source_from_master(
                                        &serv_clone, master_ind,
                                        mount_clone, None, None
                                    ).await
                                });
                            }
                        }
                    },
                    Err(e) => {
                        info!("Fetching mounts from {} failed: {}", master_server, e);
                    }
                }
            }
        },
        MasterServerRelayScheme::Authenticated { .. } => {
            authenticated_mode(serv, master_ind, None).await;
        }
    }
}
