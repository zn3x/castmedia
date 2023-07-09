use std::{time::Duration, future::Future, sync::Arc, collections::VecDeque, io::Read};
use futures::executor::block_on;
use symphonia::core::{io::{MediaSourceStream, ReadOnlySource}, meta::MetadataOptions, formats::FormatOptions, probe::Hint};
use tracing::error;

use tokio::{time::timeout, io::AsyncReadExt};

use crate::{server::{Stream, ClientSession}, source::{SourceBroadcast, Source, IcyMetadata}};

pub trait StreamReader: Send + Sync + Read {
}

pub struct SimpleReader {
    timeout: Duration,
    stream: Stream
}

impl SimpleReader {
    pub fn new(stream: Stream, timeout: u64) -> Self {
        SimpleReader { stream, timeout: Duration::from_millis(timeout) }
    }
}

impl StreamReader for SimpleReader {}

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
        .broadcast(Arc::new(vec.clone()))
        .await
        .expect("Should be able to broadcast metadata");
}

// Getting a future lifetime error, a workaround for now
pub fn broadcast<'a>(mountpoint: &'a str, session: ClientSession, chunked: bool, mut broadcast: SourceBroadcast)
    -> impl Future<Output = ()> + 'a {
    async move {
        let mountpoint = mountpoint.to_owned();
        // For now we are using threads for source
        tokio::task::spawn_blocking(move || {
            let reader = Box::new(SimpleReader::new(session.stream, session.server.config.limits.source_timeout));
            let mss = MediaSourceStream::new(Box::new(ReadOnlySource::new(reader)), Default::default());

            let hint = Hint::new();

            broadcast.audio.set_overflow(true);
            broadcast.metadata.set_overflow(true);
            // Use the default options for metadata and format readers.
            let meta_opts: MetadataOptions = Default::default();
            let mut fmt_opts: FormatOptions = Default::default();
            fmt_opts.enable_gapless = true;

            let probed = symphonia::default::get_probe()
                .format(&hint, mss, &fmt_opts, &meta_opts)
                .expect("unsupported format");

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
                                broadcast.audio.set_capacity(broadcast.audio.capacity()-(count-1));
                            }
                        } else if broadcast.audio.capacity() == broadcast.audio.len() {
                            // If broadcast capacity is full
                            // We need to resize it
                            broadcast.audio.set_capacity(broadcast.audio.capacity()+1);
                        }

                        // Now we push buffer to broadcast queue
                        block_on(broadcast.audio.broadcast(Arc::new(slice)))
                            .expect("Should be able to write to broadcast queue");
                    },
                    Err(err) => {
                        // A unrecoverable error occurred, halt decoding.
                        panic!("{}", err);
                    }
                };

                // Consume any new metadata that has been read since the last packet.
                while !format.metadata().is_latest() {
                    format.metadata().pop();
                }
            };

            // TODO: Checking if there is a fallback
            
            // Cleanup
            session.server.sources.blocking_write().remove(&mountpoint);
        }).await.ok();

    }
}
