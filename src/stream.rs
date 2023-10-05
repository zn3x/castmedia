use std::{time::Duration, future::Future, collections::VecDeque, io::Read, num::NonZeroUsize, sync::{Arc, atomic::{Ordering, AtomicBool}}};
use futures::executor::block_on;
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, meta::MetadataOptions, formats::FormatOptions, probe::Hint};
use tracing::{error, info};
use anyhow::Result;

use tokio::{time::timeout, io::AsyncReadExt, sync::oneshot};

use crate::{
    server::{Stream, ClientSession, Server},
    source::{SourceBroadcast, Source, IcyMetadata, SourceStats, MoveClientsCommand, MoveClientsType},
    migrate::{MigrateConnection, VersionedMigrateConnection, MigrateSource, MigrateSourceInfo}
};

pub trait StreamReader: Send + Sync + Read {
    fn fd(&self) -> i32;
}

pub struct SimpleReader {
    timeout: Duration,
    stream: Stream,
    stats: Arc<SourceStats>
}

impl SimpleReader {
    pub fn new(stream: Stream, timeout: u64, stats: Arc<SourceStats>) -> Self {
        SimpleReader { stream, timeout: Duration::from_millis(timeout), stats }
    }
}

impl StreamReader for SimpleReader {
    fn fd(&self) -> i32 {
        self.stream.fd()
    }
}

impl Read for SimpleReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let r = match block_on(timeout(self.timeout, self.stream.read(buf))) {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e.into()),
            Err(e) => return Err(e.into())
        };

        if r == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "Reached end of connection"));
        }

        self.stats.bytes_read.fetch_add(r as u64, Ordering::Relaxed);
        Ok(r)
    }
}

pub struct ChunkedReader {
    inner: SimpleReader,
    bytes_left: usize,
    reader: [u8; 1]
}

impl ChunkedReader {
    fn new(inner: SimpleReader) -> Self {
        Self {
            inner,
            bytes_left: 0,
            reader: [0u8]
        }
    }
}

impl StreamReader for ChunkedReader {
    fn fd(&self) -> i32 {
        self.inner.stream.fd()
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // We first need to read chunk length that is encoded as hex followed by \r\n
        if self.bytes_left == 0 {
            let mut hex_len = Vec::new();
            loop {
                match self.inner.read_exact(&mut self.reader) {
                    Ok(_) => {
                        hex_len.push(self.reader[0]);
                        // Avoiding a ddos here
                        if hex_len.len() > 12 {
                            return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Peer trying to send a big size chunk"))
                        } else if hex_len.ends_with(&[b'\r', b'\n']) {
                            break;
                        }
                    },
                    Err(e) => return Err(e)
                }
            }

            self.bytes_left = match std::str::from_utf8(&hex_len) {
                Ok(v) => match usize::from_str_radix(v, 16) {
                    Ok(v) => v,
                    Err(_) => return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Chunk length is not a valid number"))
                },
                Err(_) => return std::io::Result::Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "Chunk length is not a valid string"))
            };

            // if we get 0, this means it's the end
            if self.bytes_left == 0 {
                return Ok(0)
            }
        }

        // Now we read actual chunk
        // We make sure we are not reading more than bytes_left
        let read_len = if buf.len() > self.bytes_left {
            self.bytes_left
        } else {
            buf.len()
        };

        match self.inner.read(&mut buf[..read_len]) {
            Ok(r) => {
                self.bytes_left -= r;
                Ok(r)
            },
            Err(e) => Err(e)
        }
    }
}

pub fn metadata_encode(song: &Option<&str>, url: &Option<&str>) -> Vec<u8> {
    let mut vec = vec![0];
    vec.extend_from_slice(b"StreamTitle='");
    vec.extend_from_slice(song.unwrap_or("").as_bytes());
    vec.extend_from_slice(b"';StreamUrl='");
    vec.extend_from_slice(url.unwrap_or("").as_bytes());
    vec.extend_from_slice(b"';");

    // Black magic format https://thecodeartist.blogspot.com/2013/02/shoutcast-internet-radio-protocol.html
    let len = vec.len() - 1;
    vec[0] = {
        let down = len >> 4;
        let remainder = len & 0b1111;
        if remainder > 0 {
            // Pad with zeroes
            vec.append(&mut vec![0; 16 - remainder]);
            down + 1
        } else {
            down
        }
    } as u8;

    vec
}

pub async fn broadcast_metadata<'a>(source: &mut Source, song: &Option<&str>, url: &Option<&str>) {
    source.metadata.replace(IcyMetadata {
        title: song.unwrap_or("").to_string(),
        url: url.unwrap_or("").to_string()
    });


    source.meta_broadcast_sender
        .send(Arc::new(metadata_encode(song, url)));

}

// Getting a future lifetime error, a workaround for now
pub fn broadcast<'a>(mountpoint: &'a str, session: ClientSession,
                     queue_size: usize, chunked: bool, broadcast: SourceBroadcast,
                     kill_notifier: oneshot::Receiver<()>)
    -> impl Future<Output = ()> + 'a {
    async move {
        info!("Mounted source on {}", mountpoint);

        let omountpoint = mountpoint.to_owned();
        let stats       = match session.server.sources
            .read()
            .await
            .get(mountpoint) {
            Some(v) => v.stats.clone(),
            None => {
                error!("Can't find stats for {}, did it get removed?", mountpoint);
                return;
            }
        };

        let reader: Box<dyn StreamReader>;
        let base_reader = SimpleReader::new(session.stream, session.server.config.limits.source_timeout, stats);
        if chunked {
            reader      = Box::new(ChunkedReader::new(base_reader));
        } else {
            reader      = Box::new(base_reader);
        }
        let reader = Box::leak(reader);
        let rguard = unsafe { Box::from_raw(reader) };

        let media_broadcast = broadcast.audio.clone();
        let server          = session.server.clone();
        // For now we are using threads for source
        let abort_handle  = Arc::new(AtomicBool::new(false));
        let abort_cl      = abort_handle.clone();
        let mut fut       = tokio::task::spawn_blocking(move || {
            match blocking_broadcast(&omountpoint, reader, &session.server, broadcast, abort_cl, queue_size) {
                Ok(v) => Ok(v),
                Err(e) => {
                    error!("Source stream for {} closed: {}", omountpoint, e);
                    return Err(e);
                }
            }
        });
        
        // Here we either wait for source to broadcast till it disconnects
        // Or we receive a command to kill it from admin
        // Or we receive command for a migration
        let mut migrate_comm = server.migrate.clone();
        tokio::select! {
            _ = &mut fut => (),
            _ = kill_notifier => {
                abort_handle.store(true, Ordering::Relaxed);
            },
            migrate = migrate_comm.recv() => {
                abort_handle.store(true, Ordering::Relaxed);
                // Safety: migrate sender half is NEVER dropped until process exits
                let migrate = migrate
                    .expect("Got migrate notice with closed mpsc");
                // Now we fetch all info needed
                let (properties, fallback, metadata);
                {
                    let lock   = server.sources.read().await;
                    let source = lock.get(mountpoint)
                        .expect("Source should still exist at this point");
                    // Can we not clone here?
                    properties = source.properties.as_ref().clone();
                    fallback   = source.fallback.clone();
                    metadata   = match source.meta_broadcast.clone().try_recv().ok() {
                        Some(v) => v.as_ref().clone(),
                        None  => metadata_encode(&None, &None)
                    };
                }
                let mountpoint = mountpoint.to_owned();
                let queue_size = if let Ok(Ok(v)) = fut.await {
                    v
                } else {
                    0
                };

                // We need to first take a snapshot of media channel
                // This is mainly the thing that will let us resume in new process
                let snapshot = media_broadcast.snapshot();
                let info     = MigrateConnection::Source {
                    info: MigrateSource {
                        mountpoint,
                        properties,
                        broadcast_snapshot: (snapshot.0.len() as u64, snapshot.1, snapshot.2),
                        fallback,
                        metadata,
                        queue_size: queue_size as u64,
                        chunked
                    }
                };
                let info: VersionedMigrateConnection = info.into();
                if let Ok(info) = postcard::to_stdvec(&info) {
                    _ = migrate.source.send(MigrateSourceInfo {
                        info,
                        media: snapshot.0,
                        sock: rguard
                    });
                }
                return;
            }
        }

        // Cleanup
        let mount = server.sources.write().await.remove(mountpoint);

        // Before closing, we need to give clients a fallback if one is configured
        if let Some(mut mount) = mount {
            if let Some(fallback) = mount.fallback {
                if let Some(fallback_mount) = server.sources.read().await.get(&fallback) {
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

        info!("Unmounted source on {}", mountpoint);
        server.stats.active_sources.fetch_sub(1, Ordering::Release);
    }
}

fn blocking_broadcast(mountpoint: &str, st: &'static mut dyn StreamReader,
                      server: &Server, mut broadcast: SourceBroadcast,
                      abort_handle: Arc<AtomicBool>, queue_size: usize) -> Result<usize> {
    let mss  = MediaSourceStream::new(Box::new(ReadOnlySource::new(st)), Default::default());
    let hint = Hint::new();

    // Use the default options for metadata and format readers.
    let meta_opts: MetadataOptions  = Default::default();
    let mut fmt_opts: FormatOptions = Default::default();
    fmt_opts.enable_gapless         = true;

    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &fmt_opts, &meta_opts)?;

    //broadcast_metadata(&mut broadcast, probed.metadata.get());

    let mut format = probed.format;

    // Current total size we are holding in broadcast queue
    let mut size       = queue_size;
    // Size of every chunk in broadcast queue
    let mut chunk_size = VecDeque::new();

    'PACKET: loop {
        // Get the next packet from the media format.
        match format.next_packet() {
            Ok(packet) => {
                let slice = packet.data.into_vec();
                size += slice.len();
                chunk_size.push_front(slice.len());

                // We check if new buffer added will overflow broadcast
                if size > server.config.limits.queue_size {
                    // We will keep dropping elements in tail until
                    // we have queue size not bigger than limit
                    let mut count = 0;
                    while size > server.config.limits.queue_size {
                        size -= match chunk_size.pop_back() {
                            Some(v) => v,
                            None => {
                                // Source is sending too big of chunk to be read by us??
                                error!("Source for {} is sending too big chunk of data to process", mountpoint);
                                break 'PACKET;
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
            },
            Err(e) => {
                // A unrecoverable error occurred, halt reading
                return Err(e.into());
            }
        };

        // Consume any new metadata that has been read since the last packet.
        while !format.metadata().is_latest() {
            format.metadata().pop();
        }

        if abort_handle.load(Ordering::Relaxed) {
            break;
        }
    };

    Ok(size)
}
