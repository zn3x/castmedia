use std::sync::Arc;
use qanat::broadcast::{Sender, Receiver, RecvError};
use tokio::sync::RwLock;
use anyhow::Result;

use crate::{
    source::{Source, MetadataMsg}, internal_api::v1::IcyMetadata
};

pub fn metadata_encode(title: &str, url: &str) -> Vec<u8> {
    let mut vec = vec![0];
    vec.extend_from_slice(b"StreamTitle='");
    vec.extend_from_slice(title.as_bytes());
    vec.extend_from_slice(b"';StreamUrl='");
    vec.extend_from_slice(url.as_bytes());
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

pub fn metadata_decode(metadata: &str) -> Result<(String, String)> {
    let mut title = None;
    let mut url   = None;

    for kv in metadata.split(';').take(2) {
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

    Ok((title.unwrap_or_default(), url.unwrap_or_default()))
}

pub async fn broadcast_metadata(source: &Source, metadata: Option<IcyMetadata>) {
    let (title, url) = match metadata.as_ref() {
        Some(v) => (v.title.as_str(), v.url.as_str()),
        None => ("", "")
    };
    source.meta_broadcast_sender
        .clone()
        .send(Arc::new(MetadataMsg {
            pos: source.broadcast.last_index(),
            bin: metadata_encode(title, url),
            obj: metadata.clone().unwrap_or_default()
        }));

    *source.metadata.write().await = metadata.unwrap_or_default();
}

pub async fn relay_broadcast_metadata(src_metadata: &RwLock<IcyMetadata>,
                                      broadcast: &mut Sender<Arc<MetadataMsg>>,
                                      media_last_index: u64,
                                      metadatabuf: Vec<u8>) {
    let metadata = match std::str::from_utf8(&metadatabuf[1..]) {
        Ok(v) => v,
        Err(_) => return
    };

    if let Ok((title, url)) = metadata_decode(metadata) {
        let metadata = IcyMetadata { title, url };
        broadcast
            .send(Arc::new(MetadataMsg {
                pos: media_last_index,
                bin: metadatabuf,
                obj: metadata.clone()
            }));
        *src_metadata.write().await = metadata;
    }
}

pub async fn read_media_broadcast(stream: &mut Receiver<Arc<Vec<u8>>>,
                                  media_buf: &mut Vec<Arc<Vec<u8>>>,
                                  meta_stream: &mut Receiver<Arc<MetadataMsg>>,
                                  metadata: &mut Arc<MetadataMsg>,
                                  temp_metadata: &mut Option<Arc<MetadataMsg>>)
    -> Result<(), RecvError> {
    let ret = stream.recv_batched(media_buf, 32, false).await;
    
    match meta_stream.try_recv() {
        Ok(v) => {
            temp_metadata.replace(v);
        },
        Err(qanat::broadcast::TryRecvError::Empty) => (),
        Err(qanat::broadcast::TryRecvError::Closed) => return Err(qanat::broadcast::RecvError::Closed),
        Err(qanat::broadcast::TryRecvError::Lagged) => return Err(qanat::broadcast::RecvError::Lagged),
    }

    if temp_metadata.as_ref().is_some_and(|x| stream.read_position() >= x.pos) {
        *metadata = temp_metadata
            .take()
            .unwrap();
    }

    ret
}
