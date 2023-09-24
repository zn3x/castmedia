use std::{time::Duration, future::Future, collections::VecDeque, io::Read, num::NonZeroUsize, sync::{Arc, atomic::Ordering}};
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, meta::MetadataOptions, formats::FormatOptions, probe::Hint};
use tracing::{error, info};
use anyhow::Result;

use tokio::{time::timeout, io::AsyncReadExt, sync::oneshot};

use crate::{server::{Stream, ClientSession}, source::{SourceBroadcast, Source, IcyMetadata, SourceStats, MoveClientsCommand, MoveClientsType}};

pub trait StreamReader: Send + Sync + Read {}

pub struct SimpleReader {
    timeout: Duration,
    stream: Stream,
    stats: Arc<SourceStats>,
    rt: tokio::runtime::Runtime
}

impl SimpleReader {
    pub fn new(stream: Stream, timeout: u64, stats: Arc<SourceStats>) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Should be able to get current thread");

        SimpleReader { stream, timeout: Duration::from_millis(timeout), stats, rt }
    }
}

impl StreamReader for SimpleReader {}

impl Read for SimpleReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let r = match self.rt.block_on(timeout(self.timeout, self.stream.read(buf))) {
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

impl StreamReader for ChunkedReader {}

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

pub async fn broadcast_metadata<'a>(source: &mut Source, song: &Option<&str>, url: &Option<&str>) {
    source.metadata.replace(IcyMetadata {
        title: song.unwrap_or("").to_string(),
        url: url.unwrap_or("").to_string()
    });

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

    source.meta_broadcast_sender
        .send(Arc::new(vec.clone()));
}

// Getting a future lifetime error, a workaround for now
pub fn broadcast<'a>(mountpoint: &'a str, session: ClientSession,
                     chunked: bool, broadcast: SourceBroadcast,
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

        let server = session.server.clone();
        // For now we are using threads for source
        let fut = tokio::task::spawn_blocking(move || {
            if let Err(e) = blocking_broadcast(&omountpoint, session, chunked, broadcast, stats) {
                error!("Source stream for {} closed: {}", omountpoint, e);
            }
        });
        
        // Here we either way for source to broadcast till it disconnects
        // Or we receive a command to kill it from admin
        let abort_handle = fut.abort_handle();
        tokio::select! {
            _ = kill_notifier => {
                abort_handle.abort();
            },
            _ = fut => {},
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

fn blocking_broadcast(mountpoint: &str, session: ClientSession, chunked: bool,
                      mut broadcast: SourceBroadcast, stats: Arc<SourceStats>) -> Result<()> {
    let reader: Box<dyn StreamReader>;
    let base_reader = SimpleReader::new(session.stream, session.server.config.limits.source_timeout, stats);
    if chunked {
        reader      = Box::new(ChunkedReader::new(base_reader));
    } else {
        reader      = Box::new(base_reader);
    }
    let mss         = MediaSourceStream::new(Box::new(ReadOnlySource::new(reader)), Default::default());

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
    let mut size       = 0;
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
                if size > session.server.config.limits.queue_size {
                    // We will keep dropping elements in tail until
                    // we have queue size not bigger than limit
                    let mut count = 0;
                    while size > session.server.config.limits.queue_size {
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
    };

    Ok(())
}
