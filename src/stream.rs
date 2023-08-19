use std::{time::Duration, future::Future, collections::VecDeque, io::Read, num::NonZeroUsize};
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, meta::MetadataOptions, formats::FormatOptions, probe::Hint};
use tracing::{error, info};
use anyhow::Result;

use tokio::{time::timeout, io::AsyncReadExt};

use crate::{server::{Stream, ClientSession}, source::{SourceBroadcast, Source, IcyMetadata}};

pub trait StreamReader: Send + Sync + Read {
}

pub struct SimpleReader {
    timeout: Duration,
    stream: Stream,
    rt: tokio::runtime::Runtime
}

impl SimpleReader {
    pub fn new(stream: Stream, timeout: u64) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Should be able to get current thread");

        SimpleReader { stream, timeout: Duration::from_millis(timeout), rt }
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
        
        Ok(r)
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
        .send(vec.clone());
}

// Getting a future lifetime error, a workaround for now
pub fn broadcast<'a>(mountpoint: &'a str, session: ClientSession, chunked: bool, broadcast: SourceBroadcast)
    -> impl Future<Output = ()> + 'a {
    async move {
        info!("Mounted source on {}", mountpoint);

        let mountpoint = mountpoint.to_owned();
        // For now we are using threads for source
        tokio::task::spawn_blocking(move || {
            let server = session.server.clone();
            if let Err(e) = blocking_broadcast(&mountpoint, session, chunked, broadcast) {
                error!("Source stream for {} closed: {}", mountpoint, e);
            }

            // Cleanup
            server.sources.blocking_write().remove(&mountpoint);

            info!("Unmounted source on {}", mountpoint);
        });

    }
}

fn blocking_broadcast(mountpoint: &str, session: ClientSession, chunked: bool, broadcast: SourceBroadcast) -> Result<()> {
    let reader = Box::new(SimpleReader::new(session.stream, session.server.config.limits.source_timeout));
    let mss = MediaSourceStream::new(Box::new(ReadOnlySource::new(reader)), Default::default());

    let hint = Hint::new();

    // Use the default options for metadata and format readers.
    let meta_opts: MetadataOptions = Default::default();
    let mut fmt_opts: FormatOptions = Default::default();
    fmt_opts.enable_gapless = true;

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
                        println!("reiiiiiiizzizizizizzz");
                    }
                } else if broadcast.audio.capacity() == broadcast.audio.len() {
                    // If broadcast capacity is full
                    // We need to resize it
                    let new_cap: NonZeroUsize = ((broadcast.audio.capacity()+1) as usize)
                        .try_into()
                        .expect("Can't have empty or negative size");
                    broadcast.audio.set_capacity(new_cap);
                        println!("reiiiiiiizzizizizizzz");
                }

                // Now we push buffer to broadcast queue
                broadcast.audio.send(slice);
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

    // TODO: Checking if there is a fallback

    Ok(())
}
