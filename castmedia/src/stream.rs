use std::{
    time::Duration, collections::VecDeque, num::NonZeroUsize,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::Ordering
    },
    pin::Pin, task::{Poll, Context}
};
use futures::Future;
use qanat::broadcast::Sender;
use tracing::{error, info};
use anyhow::Result;
use qanat::oneshot;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::{
    utils,
    server::{Stream, Session, Server},
    source::{SourceBroadcast, SourceStats, MoveClientsCommand, MoveClientsType, SourceAccessType},
    migrate::{MigrateCommand, MigrateEntry},
    client::StreamOnDemand,
    http::ChunkedResponseReader,
    broadcast::relay_broadcast_metadata,
    internal_api::v1::{MigrateConnection, MigrateSource, MigrateSourceConnectionType, RelayedInfo},
    audio::{AudioReader, Mp3Reader, AdtsReader}
};

pub struct StreamReader {
    timeout: Duration,
    stream: Stream,
    stats: Arc<SourceStats>,
    chunked: Option<ChunkedResponseReader>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>
}

impl StreamReader {
    pub fn new(stream: Stream, timeout: u64, stats: Arc<SourceStats>, chunked: bool) -> Self {
        Self {
            stream,
            timeout: Duration::from_millis(timeout),
            stats,
            chunked: if chunked { Some(ChunkedResponseReader::new()) } else { None },
            sleep: None
        }
    }
}

impl AsyncRead for StreamReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.sleep.is_none() {
            this.sleep = Some(Box::pin(tokio::time::sleep(this.timeout)));
        }

        let unfilled = buf.initialize_unfilled();

        let mut inner: Pin<Box<dyn Future<Output = std::io::Result<usize>> + '_>> =
            if let Some(chunked) = this.chunked.as_mut() {
                Box::pin(chunked.read(&mut this.stream, unfilled))
            } else {
                Box::pin(this.stream.read(unfilled))
            };

        match inner.as_mut().poll(cx) {
            Poll::Ready(Ok(n)) => {
                drop(inner);
                this.sleep = None;
                if n == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "Reached end of connection",
                    )));
                }
                buf.advance(n);
                this.stats.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.sleep = None;
                Poll::Ready(Err(e))
            }
            Poll::Pending => {
                if this.sleep.as_mut().unwrap().as_mut().poll(cx).is_ready() {
                    this.sleep = None;
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "read timed out",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

pub struct BroadcastInfo<'a> {
    pub mountpoint: &'a str,
    pub session: Session,
    pub stats: Arc<SourceStats>,
    pub queue_size: usize,
    pub chunked: bool,
    pub broadcast: SourceBroadcast,
    pub kill_notifier: oneshot::Receiver<()>,
    pub on_demand: bool
}

pub enum RelayBroadcastStatus {
    Killed,
    StreamEndOrIdle(Option<StreamOnDemand>),
    Unreachable(Option<StreamOnDemand>),
    MountExists,
    LimitReached
}

pub async fn relay_broadcast(mut s: BroadcastInfo<'_>,
                             mut relay: RelayedInfo, master: String,
                             on_demand_notify: Option<diatomic_waker::WakeSink>) -> RelayBroadcastStatus {
    if !s.on_demand {
        info!("Mounted source on {} (from {})", s.mountpoint, master);
    } else {
        info!("Source on {} (from {}) is now active", s.mountpoint, master);
    }

    let mut reader = StreamReader::new(
        s.session.stream,
        s.session.server.config.limits.source_timeout,
        s.stats.clone(),
        s.chunked
    );

    let mut migrate_comm = s.session.server.migrate.clone();
    let mut data         = Vec::new();
    let mut chunk_size   = VecDeque::new();

    for i in s.broadcast.audio.snapshot().0 {
        chunk_size.push_front(i.1.len());
    }

    let fut = handle_relay_stream(
        &mut reader, &s.session.server,
        s.mountpoint, &mut s.broadcast,
        &mut s.queue_size, &mut relay,
        &mut chunk_size, &mut data,
        &s.stats, s.on_demand
    );

    let mut err    = Ok(());
    let mut killed = false;

    tokio::select! {
        r = fut => if s.on_demand {
            // If this is with on_demand option
            // we need to notify to close stream here
            if let Err(e) = r {
                err = Err(e);
            }
        },
        _ = s.kill_notifier.recv() => killed = true,
        migrate = migrate_comm.recv() => {
            // We should broadcast all media data currently held before migrating
            if !data.is_empty() {
                _ = push_to_queue(
                    &mut s.broadcast, s.mountpoint,
                    &mut s.queue_size, &mut chunk_size,
                    &s.session.server, data
                );
            }

            migrate_stream(
                MigrateStreamInfo {
                    server: &s.session.server,
                    migrate,
                    mountpoint: s.mountpoint,
                    media_broadcast: s.broadcast.audio,
                    chunked: s.chunked,
                    queue_size: s.queue_size,
                    stream: reader,
                    client_addr: s.session.addr
                },
                Some(relay),
                s.on_demand
            ).await;
        }
    }

    // Cleanup
    s.session.server.stats.active_relay.fetch_sub(1, Ordering::Release);
    s.session.server.stats.active_relay_streams.fetch_sub(1, Ordering::Release);

    if let Err(e) = err {
        info!("Abrupt end of stream for source on {} (from {}): {}", s.mountpoint, master, e);
    } else if !s.on_demand || killed {
        unmount_source(&s.session.server, s.mountpoint).await;
        info!("Unmounted source on {} (from {})", s.mountpoint, master);

        return if killed {
            RelayBroadcastStatus::Killed
        } else {
            RelayBroadcastStatus::StreamEndOrIdle(None)
        };
    } else {
        info!("Source on {} (from {}) is now inactive", s.mountpoint, master);
    }

    // We need to remove old channels
    let (tx, rx)         = qanat::broadcast::channel(NonZeroUsize::MIN);
    if let Some(source)  = s.session.server.sources.write().await.get_mut(s.mountpoint) {
        source.broadcast = rx;
    }

    RelayBroadcastStatus::StreamEndOrIdle(Some(StreamOnDemand {
        stats: s.stats,
        broadcast: SourceBroadcast {
            audio: tx,
            metadata: s.broadcast.metadata
        },
        kill_notifier: s.kill_notifier,
        on_demand_notify: on_demand_notify.unwrap()
    }))
}

pub async fn broadcast(mut s: BroadcastInfo<'_>) { 
    info!("Mounted source on {}", s.mountpoint);

    let omountpoint = s.mountpoint.to_owned();

    let mut reader      = StreamReader::new(s.session.stream, s.session.server.config.limits.source_timeout, s.stats, s.chunked);
    let media_broadcast = s.broadcast.audio.clone();
    let server          = s.session.server.clone();
    // Here we either wait for source to broadcast till it disconnects
    // Or we receive a command to kill it from admin
    // Or we receive command for a migration
    let mut migrate_comm = server.migrate.clone();
    tokio::select! {
        _ = handle_source_stream(&omountpoint, &mut reader, &s.session.server, s.broadcast, &mut s.queue_size, s.kill_notifier) => (),
        migrate = migrate_comm.recv() => {
            migrate_stream(
                MigrateStreamInfo {
                    server: &server,
                    migrate,
                    mountpoint: s.mountpoint,
                    media_broadcast,
                    chunked: s.chunked,
                    queue_size: s.queue_size,
                    stream: reader,
                    client_addr: s.session.addr
                },
                None,
                false
            ).await;
        }
    }

    // Cleanup
    server.stats.active_sources.fetch_sub(1, Ordering::Release);
    unmount_source(&server, s.mountpoint).await;

    info!("Unmounted source on {}", s.mountpoint);
}

pub async fn unmount_source(server: &Server, mountpoint: &str) {
    let mount = server.sources.write().await.remove(mountpoint);

    // Before closing, we need to give clients a fallback if one is configured
    if let Some(mut mount) = mount {
        if let Some(fallback) = mount.fallback {
            if let Some(fallback_mount) = server.sources.read().await.get(&fallback) {
                info!("Moving all listeners on {mountpoint} to {fallback}");
                mount.move_listeners_sender.send(Arc::new(MoveClientsCommand {
                    broadcast: fallback_mount.broadcast.clone(),
                    meta_broadcast: fallback_mount.meta_broadcast.clone(),
                    move_listeners_receiver: fallback_mount.move_listeners_receiver.clone(),
                    clients: fallback_mount.clients.clone(),
                    move_type: MoveClientsType::Fallback
                }));
            }
        }
    }
}

struct MigrateStreamInfo<'a> {
    server: &'a Server,
    migrate: Result<Arc<MigrateCommand>, qanat::broadcast::RecvError>,
    mountpoint: &'a str,
    media_broadcast: Sender<Arc<Vec<u8>>>,
    chunked: bool,
    queue_size: usize,
    stream: StreamReader,
    client_addr: SocketAddr
}

async fn migrate_stream(mut s: MigrateStreamInfo<'_>, relay: Option<RelayedInfo>,
                        on_demand: bool) -> ! {
    // Safety: migrate sender half is NEVER dropped until process exits
    let migrate = s.migrate
        .expect("Got migrate notice with closed mpsc");
    // Now we fetch all info needed
    let (properties, fallback, metadata, access) = s.server.sources.read().await
        .get(s.mountpoint)
        .map(|x| (
            x.properties.as_ref().clone(),
            x.fallback.clone(),
            x.metadata.clone(),
            x.access.clone()
        ))
        .expect("Source should still exist at this point");
    let metadata = metadata.read().await.clone();

    let mountpoint = s.mountpoint.to_owned();
    // We need to first take a snapshot of media channel
    // This is mainly the thing that will let us resume in new process
    let snapshot = s.media_broadcast.snapshot();
    let info     = MigrateConnection::Source(MigrateSource {
        mountpoint,
        properties,
        broadcast_snapshot: snapshot,
        fallback,
        metadata,
        queue_size: s.queue_size as u64,
        chunked: s.chunked,
        is_relay: match access {
            SourceAccessType::SourceClient { username } => MigrateSourceConnectionType::SourceClient { username },
            SourceAccessType::RelayedSource { relayed_source } => MigrateSourceConnectionType::RelayedSource {
                relayed_stream: relayed_source,
                relay_info: relay.expect("Should have non empty RelayInfo on source migration"),
                on_demand
            }
        }
    });

    match s.stream.stream.flush().await {
        Ok(()) => _ = migrate.source.send(MigrateEntry { info, sock: Some((s.stream.stream, s.client_addr)) }),
        Err(e) => tracing::error!("Failed migrating source stream: {e}")
    }

    // We have finished all preparations for migration
    utils::hang().await;
}

async fn handle_source_stream(mountpoint: &str, st: &mut StreamReader,
                              server: &Server, mut broadcast: SourceBroadcast,
                              queue_size: &mut usize, mut kill_notifier: oneshot::Receiver<()>) {
    let mut stream: Box<dyn AudioReader + Send> = match server.sources.read().await.get(mountpoint) {
        Some(v) => match v.properties.content_type.as_str() {
            "audio/mpeg" => Box::new(Mp3Reader::new(st)),
            "audio/aac" => Box::new(AdtsReader::new(st)),
            _ => unreachable!()
        },
        None => Box::new(Mp3Reader::new(st))
    };
    // Size of every chunk in broadcast queue
    let mut chunk_size = VecDeque::new();
    for i in broadcast.audio.snapshot().0 {
        chunk_size.push_front(i.1.len());
    }

    loop {
        tokio::select! {
            // Get the next packet from the media format.
            r = stream.read() => match r {
                Ok(slice) => {
                    if let Err(e) = push_to_queue(&mut broadcast, mountpoint, queue_size, &mut chunk_size, server, slice) {
                        error!("Failed pushing media block to queue for {mountpoint}: {e}");
                        break;
                    }
                },
                Err(e) => {
                    error!("Failed to read media stream for {mountpoint}: {e}");
                    break;
                }
            },
            r = kill_notifier.recv() => if r.is_ok() { break }
        }
    }
}

async fn handle_relay_stream(stream: &mut StreamReader,
                             server: &Server, mountpoint: &str,
                             broadcast: &mut SourceBroadcast,
                             queue_size: &mut usize,
                             relay: &mut RelayedInfo,
                             chunk_size: &mut VecDeque<usize>,
                             data: &mut Vec<u8>,
                             stats: &SourceStats,
                             on_demand: bool) -> Result<()> {
    let mut buf  = [0u8; 2048];
    let mut ret  = 0;

    let mut old_metadata = Vec::new();
    let src_metadata     = server.sources.read().await
        .get(mountpoint)
        .expect("Mount should be on sources")
        .metadata
        .clone();
    
    loop {
        // We need to properly read metadata at every metaint_position
        if relay.metaint_position + ret > relay.metaint {
            // We need to find at which position in buf the metadata starts
            let pos                = relay.metaint - relay.metaint_position;
            relay.metadata_reading = true;
            relay.metaint_position = 0;
            // Copying all media data befor metadata
            data.extend_from_slice(&buf[..pos]);
            if pos >= ret {
                // Next byte to read is the start of metadata
            } else {
                // We read first byte which is the length of the metadata
                relay.metadata_remaining = (buf[pos] as usize) << 4;
                // Checking if we also have some other bytes left of metadata
                // left to read
                if pos < ret - 1 {
                    let remaining = buf[pos+1..ret].len();
                    let last_pos  = if relay.metadata_remaining >= remaining {
                        ret
                    } else {
                        // Copying media data coming after metadata
                        let last                = pos + 1 + relay.metadata_remaining;
                        relay.metaint_position += buf[last..ret].len();
                        data.extend_from_slice(&buf[last..ret]);

                        last
                    };

                    relay.metadata_remaining -= buf[pos+1..last_pos].len();
                    relay.metadata_buffer.extend_from_slice(&buf[pos..last_pos]);
                    if relay.metadata_remaining == 0 {
                        let metadatabuf = std::mem::take(&mut relay.metadata_buffer);
                        if metadatabuf.ne(&old_metadata) {
                            old_metadata = metadatabuf.clone();
                            relay_broadcast_metadata(
                                src_metadata.as_ref(), &mut broadcast.metadata,
                                broadcast.audio.last_index(), metadatabuf
                            ).await;
                        }
                        relay.metadata_reading = false;
                    }
                }
            }
        } else if relay.metadata_reading {
            let start = if relay.metadata_remaining == 0 {
                // We still didn't read first byte yet
                relay.metadata_remaining = (buf[0] as usize) << 4;
                1
            } else {
                0
            };

            if start < ret {
                let remaining = buf[start..ret].len();
                let last_pos  = if relay.metadata_remaining >= remaining {
                    ret
                } else {
                    // Copying media data coming after metadata
                    let last                = start + relay.metadata_remaining;
                    relay.metaint_position += buf[last..ret].len();
                    data.extend_from_slice(&buf[last..ret]);

                    last
                };

                relay.metadata_remaining -= buf[start..last_pos].len();
                relay.metadata_buffer.extend_from_slice(&buf[start..last_pos]);
                if relay.metadata_remaining == 0 {
                    let metadatabuf = std::mem::take(&mut relay.metadata_buffer);
                    if metadatabuf.ne(&old_metadata) {
                        old_metadata = metadatabuf.clone();
                        relay_broadcast_metadata(
                            &src_metadata, &mut broadcast.metadata,
                            broadcast.audio.last_index(), metadatabuf
                        ).await;
                    }
                    relay.metadata_reading = false;
                }
            }
        } else if ret > 0 {
            relay.metaint_position += ret;
            data.extend_from_slice(&buf[..ret]);
        }

        if data.len() > 1024 {
            let v = std::mem::take(data);
            push_to_queue(broadcast, mountpoint, queue_size, chunk_size, server, v)?;

            if on_demand && stats.active_listeners.load(Ordering::Acquire) == 0 {
                break;
            } 
        }

        ret = stream.read(&mut buf).await?;
    };

    Ok(())
}

fn push_to_queue(broadcast: &mut SourceBroadcast, mountpoint: &str,
          size: &mut usize, chunk_size: &mut VecDeque<usize>,
          server: &Server, slice: Vec<u8>) -> Result<()> {
    *size += slice.len();
    chunk_size.push_front(slice.len());

    // We check if new buffer added will overflow broadcast
    if *size > server.config.limits.queue_size {
        // We will keep dropping elements in tail until
        // we have queue size not bigger than limit
        let mut count = 0;
        while *size > server.config.limits.queue_size {
            *size -= match chunk_size.pop_back() {
                Some(v) => v,
                None => {
                    // Source is sending too big of chunk to be read by us??
                    return Err(anyhow::Error::msg(format!("Source for {} is sending too big chunk of data to process", mountpoint)));
                }
            };
            count += 1;
        }
        // If number of elements is bigger than 1
        // We will resize broadcast queue by removing count-1
        // from capacity
        if count > 1 {
            let new_cap: NonZeroUsize = ((broadcast.audio.capacity()-(count-1)) as usize)
                .try_into()
                .expect("Can't have empty or negative size");
            broadcast.audio.set_capacity(new_cap);
        }
    } else if broadcast.audio.capacity() == broadcast.audio.len() {
        // If broadcast capacity is full
        // We need to resize it
        let new_cap: NonZeroUsize = ((broadcast.audio.capacity()+1) as usize)
            .try_into()
            .expect("Can't have empty or negative size");
        broadcast.audio.set_capacity(new_cap);
    }

    // Now we push buffer to broadcast queue
    broadcast.audio.send(Arc::new(slice));

    Ok(())
}
