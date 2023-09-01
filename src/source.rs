use std::{
    sync::{Arc, atomic::{AtomicUsize, AtomicU64}},
    num::NonZeroUsize
};
use serde::Serialize;
use llq::broadcast::{Receiver, Sender};

use anyhow::Result;
use tracing::info;

use crate::{server::ClientSession, request::{SourceRequest, Request}, response, utils, stream, auth};

#[derive(Serialize)]
pub struct IcyProperties {
    pub uagent: Option<String>,
    pub public: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub genre: Option<String>,
    pub bitrate: Option<String>,
    pub content_type: String
}

impl IcyProperties {
    fn new(content_type: String) -> Self {
        IcyProperties {
            uagent: None,
            public: false,
            name: None,
            description: None,
            url: None,
            genre: None,
            bitrate: None,
            content_type
        }
    }

    fn populate_from_http_headers(&mut self, headers: &[httparse::Header<'_>]) {
        for header in headers {
            let name = header.name.to_lowercase();
            let val = match std::str::from_utf8(header.value) {
                Ok(v) => v,
                Err(_) => continue
            };

            // There's a nice list here: https://github.com/ben221199/MediaCast
            // Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
            match name.as_str() {
                "user-agent" => self.uagent = Some(val.to_string()),
                "ice-public" | "icy-pub" | "x-audiocast-public" | "icy-public" => self.public = val.parse::<usize>().unwrap_or(0) == 1,
                "ice-name" | "icy-name" | "x-audiocast-name" => self.name = Some(val.to_string()),
                "ice-description" | "icy-description" | "x-audiocast-description" => self.description = Some(val.to_string()),
                "ice-url" | "icy-url" | "x-audiocast-url" => self.url = Some(val.to_string()),
                "ice-genre" | "icy-genre" | "x-audiocast-genre" => self.genre = Some(val.to_string()),
                "ice-bitrate" | "icy-br" | "x-audiocast-bitrate" => self.bitrate = Some(val.to_string()),
                _ => (),
            }
        }
    }
}

#[derive(Serialize)]
pub struct IcyMetadata {
    pub title: String,
    pub url: String
}

pub struct SourceStats {
    // Utc timestamp
    pub start_time: i64,
    pub active_listeners: AtomicUsize,
    pub peak_listeners: AtomicUsize,
    pub bytes_read: AtomicU64
}

impl SourceStats {
    pub fn new() -> Self {
        Self {
            start_time: chrono::offset::Utc::now()
                .timestamp(),
            active_listeners: AtomicUsize::new(0),
            peak_listeners: AtomicUsize::new(0),
            bytes_read: AtomicU64::new(0)
        }
    }
}

pub struct Source {
    pub properties: Arc<IcyProperties>,
    pub metadata: Option<IcyMetadata>,
    pub stats: Arc<SourceStats>,
    /// Fallback mountpoint in case this one is down
    pub fallback: Option<String>,
    /// The stream broadcast receiver
    pub broadcast: Receiver<Vec<u8>>,
    /// Receiver stream for metadata broadcast
    pub meta_broadcast: Receiver<Vec<u8>>,
    /// Sender stream for metadata broadcast
    /// Needed so we don't create a new sender every time
    /// we get metadata update
    pub meta_broadcast_sender: Sender<Vec<u8>>
}

pub struct SourceBroadcast {
    pub audio: Sender<Vec<u8>>,
    pub metadata: Sender<Vec<u8>>
}

impl Source {
    pub fn new(properties: IcyProperties) -> (Self, SourceBroadcast) {
        let size: NonZeroUsize = 1.try_into().expect("1 should be posetif");
        let (tx, rx)           = llq::broadcast::channel(size);
        let (tx1, rx1)         = llq::broadcast::channel(size);
        (Source {
            properties: Arc::new(properties),
            metadata: None,
            stats: Arc::new(SourceStats::new()),
            fallback: None,
            broadcast: rx,
            meta_broadcast_sender: tx1.clone(),
            meta_broadcast: rx1
        },
        SourceBroadcast {
            audio: tx,
            metadata: tx1
        })
    }
}

pub async fn handle<'a>(mut session: ClientSession, request: &Request<'a>, req: SourceRequest) -> Result<()> {
    let sid = &session.server.config.info.id;
    match auth::source_auth(req.auth).await {
        Ok(v) => if !v {
            response::authentication_needed(&mut session.stream, sid).await?;
            info!("Source request from {} with wrong authentication", session.addr);
            return Ok(());
        },
        Err(e) => {
            response::internal_error(&mut session.stream, sid).await?;
            return Err(anyhow::Error::msg(format!("Source authentication failed, cause {}", e)));
        }
    }

    // TODO: CHECK PATH

    // Check if mountpoint used
    if session.server.sources.read().await.contains_key(&req.mountpoint) {
        response::forbidden(&mut session.stream, sid, "Invalid mountpoint").await?;
        return Ok(());
    }

    // Instanciating stream properties
    let mut properties = IcyProperties::new(match utils::get_header("Content-Type", &request.headers) {
        Some(v) => std::str::from_utf8(v)?.to_owned(),
        None => {
            response::forbidden(&mut session.stream, sid, "Missing content type").await?;
            return Ok(());
        }
    });

    let chunked;
    if request.method == "SOURCE" {
        response::ok_200(&mut session.stream, sid).await?;
        chunked = false;
    } else {
        // PUT METHOD
        // No support for encoding
        // TODO Add support for transfer encoding options as specified here: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
        match utils::get_header("Transger-Encoding", &request.headers) {
            Some(b"identity") | None => {},
            Some(b"chunked") => {},
            _ => {
                response::bad_request(&mut session.stream, sid, "Invalid transfer encoding").await?;
                return Ok(());
            }
        }

        match utils::get_header("Expect", &request.headers) {
            Some(b"100-continue") => {},
            Some(_) => {
                response::bad_request(&mut session.stream, sid, "Expecting 100-continue in header expect").await?;
                return Ok(());
            },
            None => {
                response::bad_request(&mut session.stream, sid, "PUT method must have expect header").await?;
                return Ok(());
            }
        }

        response::ok_200(&mut session.stream, sid).await?;
        chunked = true;
    }

    properties.populate_from_http_headers(&request.headers);

    let (source, broadcast) = Source::new(properties);

    // We must write initial read length to stats
    source.stats.bytes_read.fetch_add(request.headers_buf.len() as u64, std::sync::atomic::Ordering::Relaxed);

    // Add this mountpoint to mountpoints hashmap
    let mountpoint = req.mountpoint.clone();
    session.server.sources.write().await.insert(req.mountpoint, source);
    
    // Then handle media stream coming for this mountpoint
    stream::broadcast(&mountpoint, session, chunked, broadcast).await;

    Ok(())
}
