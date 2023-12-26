use std::{
    time::Duration, collections::VecDeque, io::Read,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{Ordering, AtomicBool}
    }
};
use futures::executor::block_on;
use llq::broadcast::Sender;
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, meta::MetadataOptions, formats::FormatOptions, probe::Hint};
use tracing::{error, info};
use anyhow::Result;

use tokio::{time::timeout, io::AsyncReadExt, sync::oneshot};

use crate::{
    server::{Stream, Session, Server},
    source::{SourceBroadcast, Source, IcyMetadata, SourceStats, MoveClientsCommand, MoveClientsType},
    migrate::{MigrateConnection, VersionedMigrateConnection, MigrateSource, MigrateSourceInfo, MigrateCommand}, client::RelayedInfo
};

// TODO: Proper async interface...
// TODO: Remove async_trait when async traits stabilized
#[async_trait::async_trait]
pub trait StreamReader: Send + Sync + Read {
    fn fd(&self) -> i32;
    async fn async_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
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

#[async_trait::async_trait]
impl StreamReader for SimpleReader {
    fn fd(&self) -> i32 {
        self.stream.fd()
    }

    async fn async_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let r = match timeout(self.timeout, self.stream.read(buf)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(e.into())
        };

        if r == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "Reached end of connection"));
        }

        self.stats.bytes_read.fetch_add(r as u64, Ordering::Relaxed);
        Ok(r)
    }
}

impl Read for SimpleReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        block_on(self.async_read(buf))
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

#[async_trait::async_trait]
impl StreamReader for ChunkedReader {
    fn fd(&self) -> i32 {
        self.inner.stream.fd()
    }

    async fn async_read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // We first need to read chunk length that is encoded as hex followed by \r\n
        if self.bytes_left == 0 {
            let mut hex_len = Vec::new();
            loop {
                match self.inner.async_read(&mut self.reader).await {
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

        match self.inner.async_read(&mut buf[..read_len]).await {
            Ok(r) => {
                self.bytes_left -= r;
                Ok(r)
            },
            Err(e) => Err(e)
        }
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        block_on(self.async_read(buf))
    }
}

pub fn metadata_encode(title: &Option<&str>, url: &Option<&str>) -> Vec<u8> {
    let mut vec = vec![0];
    vec.extend_from_slice(b"StreamTitle='");
    vec.extend_from_slice(title.unwrap_or("").as_bytes());
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

fn metadata_decode(metadata: &str) -> Result<(Option<String>, Option<String>)> {
    let mut title = None;
    let mut url   = None;

    for kv in metadata.split(';') {
        let spl    = kv.split('=').collect::<Vec<_>>();
        let (k, v) = if spl.len() == 2 {
            (spl[0], spl[1])
        } else {
            return Err(anyhow::Error::msg("Invalid metadata"));
        };
        
        let v = if v.len() >= 2 && v.starts_with('\'') && v.ends_with('\'') {
            v[1..v.len()-1].to_string()
        } else {
            return Err(anyhow::Error::msg("Metadata value uncorrect formatting"));
        };

        // TODO: Are there other cases to handle?
        match k {
            "StreamTitle" => title = Some(v),
            "StreamUrl"   => url = Some(v),
            _             => ()
        }
    }

    Ok((title, url))
}

pub async fn broadcast_metadata<'a>(source: &mut Source, title: &Option<&str>, url: &Option<&str>) {
    source.metadata.replace(IcyMetadata {
        title: title.unwrap_or("").to_string(),
        url: url.unwrap_or("").to_string()
    });


    source.meta_broadcast_sender
        .send(Arc::new(metadata_encode(title, url)));

}

pub struct BroadcastInfo<'a> {
    pub mountpoint: &'a str,
    pub session: Session,
    pub stats: Arc<SourceStats>,
    pub queue_size: usize,
    pub chunked: bool,
    pub broadcast: SourceBroadcast,
    pub kill_notifier: oneshot::Receiver<()>
}

pub async fn relay_broadcast(mut s: BroadcastInfo<'_>, mut relay: RelayedInfo) {
    info!("Mounted source on {} from master", s.mountpoint);

    let reader = SimpleReader::new(
        s.session.stream,
        s.session.server.config.limits.source_timeout,
        s.stats
    );

    let mut reader: Box<dyn StreamReader> = match s.chunked {
        false => Box::new(reader),
        true  => Box::new(ChunkedReader::new(reader))
    };

    let mut migrate_comm = s.session.server.migrate.clone();
    let mut data         = Vec::new();
    let mut chunk_size   = VecDeque::new();

    let fut = handle_relay_stream(
        reader.as_mut(), &s.session.server,
        s.mountpoint, &mut s.broadcast,
        &mut s.queue_size, &mut relay,
        &mut chunk_size, &mut data
    );

    tokio::select! {
        _ = fut => (),
        _ = s.kill_notifier => (),
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
                MigrateStreamProps {
                    server: &s.session.server,
                    migrate,
                    mountpoint: s.mountpoint,
                    media_broadcast: s.broadcast.audio,
                    chunked: s.chunked,
                    queue_size: s.queue_size,
                    stream: reader
                },
                Some(relay)
            ).await;
        }
    }

    info!("Unmounted source on {}", s.mountpoint);
    s.session.server.stats.active_relay.fetch_sub(1, Ordering::Release);
    s.session.server.stats.active_relay_streams.fetch_sub(1, Ordering::Release);
}

pub async fn broadcast(s: BroadcastInfo<'_>) { 
    info!("Mounted source on {}", s.mountpoint);

    let omountpoint = s.mountpoint.to_owned();

    let reader: Box<dyn StreamReader>;
    let base_reader = SimpleReader::new(s.session.stream, s.session.server.config.limits.source_timeout, s.stats);
    if s.chunked {
        reader      = Box::new(ChunkedReader::new(base_reader));
    } else {
        reader      = Box::new(base_reader);
    }
    // Safety: rguard is valid as long as reader is valid
    // We explicitely await thread accessing reader before freeing
    // rguard
    let reader = Box::leak(reader);
    let rguard = unsafe { Box::from_raw(reader) };

    let media_broadcast = s.broadcast.audio.clone();
    let server          = s.session.server.clone();
    // For now we are using threads for source
    let abort_handle  = Arc::new(AtomicBool::new(false));
    let abort_cl      = abort_handle.clone();
    let mut fut       = tokio::task::spawn_blocking(move || {
        match handle_source_stream(&omountpoint, reader, &s.session.server, s.broadcast, abort_cl, s.queue_size) {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Source stream for {} closed: {}", omountpoint, e);
                Err(e)
            }
        }
    });
    
    // Here we either wait for source to broadcast till it disconnects
    // Or we receive a command to kill it from admin
    // Or we receive command for a migration
    let mut migrate_comm = server.migrate.clone();
    tokio::select! {
        _ = &mut fut => (),
        _ = s.kill_notifier => {
            abort_handle.store(true, Ordering::Relaxed);
            // We also need to wait here so we don't free rguard while other thread
            // may still be accessing it's pointer
            _ = fut.await;
        },
        migrate = migrate_comm.recv() => {
            abort_handle.store(true, Ordering::Relaxed);

            let queue_size = if let Ok(Ok(v)) = fut.await {
                v
            } else {
                0
            };

            migrate_stream(
                MigrateStreamProps {
                    server: &server,
                    migrate,
                    mountpoint: s.mountpoint,
                    media_broadcast,
                    chunked: s.chunked,
                    queue_size,
                    stream: rguard
                },
                None
            ).await;
            return;
        }
    }

    // Cleanup
    let mount = server.sources.write().await.remove(s.mountpoint);

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

    info!("Unmounted source on {}", s.mountpoint);
    server.stats.active_sources.fetch_sub(1, Ordering::Release);
}

struct MigrateStreamProps<'a> {
    server: &'a Server,
    migrate: Result<Arc<MigrateCommand>, llq::broadcast::RecvError>,
    mountpoint: &'a str,
    media_broadcast: Sender<Arc<Vec<u8>>>,
    chunked: bool,
    queue_size: usize,
    stream: Box<dyn StreamReader>
}

async fn migrate_stream(s: MigrateStreamProps<'_>, relay: Option<RelayedInfo>) {
    // Safety: migrate sender half is NEVER dropped until process exits
    let migrate = s.migrate
        .expect("Got migrate notice with closed mpsc");
    // Now we fetch all info needed
    let (properties, fallback, metadata);
    {
        let lock   = s.server.sources.read().await;
        let source = lock.get(s.mountpoint)
            .expect("Source should still exist at this point");
        // Can we not clone here?
        properties = source.properties.as_ref().clone();
        fallback   = source.fallback.clone();
        metadata   = match source.meta_broadcast.clone().try_recv().ok() {
            Some(v) => v.as_ref().clone(),
            None  => metadata_encode(&None, &None)
        };
    }

    let mountpoint = s.mountpoint.to_owned();
    // We need to first take a snapshot of media channel
    // This is mainly the thing that will let us resume in new process
    let (is_relay, relay) = match relay {
        Some(v) => (true, v),
        None => (false, RelayedInfo::default())
    };

    let snapshot = s.media_broadcast.snapshot();
    let info     = MigrateConnection::Source {
        info: MigrateSource {
            mountpoint,
            properties,
            broadcast_snapshot: (snapshot.0.len() as u64, snapshot.1, snapshot.2),
            fallback,
            metadata,
            queue_size: s.queue_size as u64,
            chunked: s.chunked,
            is_relay,
            relay
        }
    };
    let info: VersionedMigrateConnection = info.into();
    if let Ok(info) = postcard::to_stdvec(&info) {
        _ = migrate.source.send(MigrateSourceInfo {
            info,
            media: snapshot.0,
            sock: s.stream
        });
    }
}

fn handle_source_stream(mountpoint: &str, st: &'static mut dyn StreamReader,
                      server: &Server, mut broadcast: SourceBroadcast,
                      abort_handle: Arc<AtomicBool>, queue_size: usize) -> Result<usize> {
    let mss  = MediaSourceStream::new(Box::new(ReadOnlySource::new(st)), Default::default());
    let hint = Hint::new();

    // Use the default options for metadata and format readers.
    let meta_opts: MetadataOptions  = Default::default();
    let fmt_opts: FormatOptions     = Default::default();

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

                if let Err(e) = push_to_queue(&mut broadcast, mountpoint, &mut size, &mut chunk_size, server, slice) {
                    error!("Broadcast failed: {}", e);
                    break 'PACKET;
                }
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

async fn handle_relay_stream(stream: &mut dyn StreamReader,
                             server: &Server, mountpoint: &str,
                             broadcast: &mut SourceBroadcast,
                             queue_size: &mut usize,
                             relay: &mut RelayedInfo,
                             chunk_size: &mut VecDeque<usize>,
                             data: &mut Vec<u8>) -> Result<()> {
    let mut buf  = [0u8; 2048];
    let mut ret  = 0;
    
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
                    relay.metadata_buffer.extend_from_slice(&buf[pos+1..last_pos]);
                    if relay.metadata_remaining == 0 {
                        relay.metadata_reading = false;
                        relay.metadata_buffer  = Vec::new();
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
                    relay.metadata_reading = false;
                    relay.metadata_buffer  = Vec::new();
                }
            }
        } else if ret > 0 {
            relay.metaint_position += ret;
            data.extend_from_slice(&buf[..ret]);
        }

        if data.len() > 1024 {
            let v = std::mem::take(data);
            push_to_queue(broadcast, mountpoint, queue_size, chunk_size, server, v)?;
        }

        ret = stream.async_read(&mut buf).await?;

        if ret == 0 {
            return Err(anyhow::Error::msg("Reached end of stream"));
        }
    };
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
