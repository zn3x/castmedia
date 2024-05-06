use std::sync::Arc;
use qanat::broadcast::{Sender, Receiver, RecvError};
use tokio::sync::RwLock;
use anyhow::Result;

use crate::source::{IcyMetadata, Source};

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

pub fn metadata_decode(metadata: &str) -> Result<(Option<String>, Option<String>)> {
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

    Ok((title, url))
}

pub async fn broadcast_metadata<'a>(source: &Source, title: &Option<&str>, url: &Option<&str>) {
    source.metadata.write().await.replace(IcyMetadata {
        title: title.unwrap_or("").to_string(),
        url: url.unwrap_or("").to_string()
    });


    
    source.meta_broadcast_sender
        .lock()
        .await
        .send(Arc::new((
            source.broadcast.last_index(),
            metadata_encode(title, url))
        ));
}

pub async fn relay_broadcast_metadata<'a>(src_metadata: &RwLock<Option<IcyMetadata>>,
                                          broadcast: &mut Sender<Arc<(u64, Vec<u8>)>>,
                                          media_last_index: u64,
                                          metadatabuf: Vec<u8>) {
    let metadata = match std::str::from_utf8(&metadatabuf) {
        Ok(v) => v,
        Err(_) => return
    };

    if let Ok((title, url)) = metadata_decode(metadata) {
        src_metadata.write().await.replace(IcyMetadata {
            title: title.unwrap_or("".to_string()),
            url: url.unwrap_or("".to_string())
        });


        broadcast
            .send(Arc::new((media_last_index, metadatabuf)));
    }
}

pub async fn read_media_broadcast(stream: &mut Receiver<Arc<Vec<u8>>>,
                                  meta_stream: &mut Receiver<Arc<(u64, Vec<u8>)>>,
                                  metadata: &mut Arc<(u64, Vec<u8>)>,
                                  temp_metadata: &mut Option<Arc<(u64, Vec<u8>)>>)
    -> Result<Arc<Vec<u8>>, RecvError> {
    let ret = stream.recv().await;
    
    match meta_stream.try_recv() {
        Ok(v) => {
            temp_metadata.replace(v);
        },
        Err(qanat::broadcast::TryRecvError::Empty) => (),
        Err(qanat::broadcast::TryRecvError::Closed) => return Err(qanat::broadcast::RecvError::Closed),
        Err(qanat::broadcast::TryRecvError::Lagged(v)) => return Err(qanat::broadcast::RecvError::Lagged(v)),
    }

    if temp_metadata.as_ref().is_some_and(|x| stream.read_position() >= x.0) {
        *metadata = temp_metadata
            .take()
            .unwrap();
    }

    ret
}
