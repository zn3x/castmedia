use std::{cell::RefCell, sync::{Arc, atomic::{Ordering, AtomicU64}}};
use anyhow::Result;
use llq::{errors::{RecvError, TryRecvError}, broadcast::Receiver};
use tokio::io::AsyncWriteExt;
use tracing::{info, error};

use crate::{
    server::ClientSession,
    request::{read_request, Request, RequestType, ListenRequest}, source::{self, IcyProperties}, response, utils, admin
};

pub struct Client {
    properties: ClientProperties,
    stats: ClientStats
}

pub struct ClientProperties {
    user_agent: Option<String>,
    metadata: bool
}

pub struct ClientStats {
    pub start_time: i64,
    pub bytes_sent: AtomicU64
}

pub async fn handle_client<'a>(mut session: ClientSession, request: &Request<'a>, req: ListenRequest) -> Result<()> {
    let (props, stream, meta_stream, stats) = match session.server.sources.read().await.get(&req.mountpoint) {
        Some(v) => {
            (
            v.properties.clone(),
            v.broadcast.clone(),
            v.meta_broadcast.clone(),
            v.stats.clone())
        },
        None => {
            response::not_found(&mut session.stream, &session.server.config.info.id).await?;
            error!("Source for {} suddenly vanished after client connection", req.mountpoint);
            return Ok(());
        }
    };

    // We increment active listeners
    // and check if we reached a new peak listeners here
    let new_count = stats.active_listeners.fetch_add(1, Ordering::Relaxed) + 1;
    stats.peak_listeners.fetch_max(new_count, Ordering::Relaxed);

    drop(req);

    let ret = client_broadcast(session, request, props, stream, meta_stream).await;

    // End of connection
    stats.active_listeners.fetch_sub(1, Ordering::Relaxed);

    ret
}

#[inline]
pub async fn client_broadcast<'a>(mut session: ClientSession, request: &Request<'a>,
                                  props: Arc<IcyProperties>, mut stream: Receiver<Vec<u8>>,
                                  mut meta_stream: Receiver<Vec<u8>>) -> Result<()> {
    if utils::get_header("icy-metadata", &request.headers).unwrap_or(b"0") == b"1" {
        response::ok_200_icy_metadata(
            &mut session.stream,
            &session.server.config.info.id,
            &props,
            session.server.config.metaint
        ).await?;
    } else {
        response::ok_200(&mut session.stream, &session.server.config.info.id).await?;
    }

    drop(props);

    let mut metadata = Arc::new(Vec::new());
    let mut metaint = 0;

    loop {
        match meta_stream.try_recv() {
            Ok(v) => metadata = v,
            Err(TryRecvError::Empty | TryRecvError::Lagged(_)) => (),
            Err(TryRecvError::Closed) => break
        }

        match stream.recv().await {
            Ok(buf) => {
                // We are checking here if we need to broadcast metadata
                // Metadata needs to be sent inbetween ever metaint interval
                if metaint + buf.len() >= session.server.config.metaint {
                    let diff          = (metaint + buf.len()) - session.server.config.metaint;
                    let first_buf_len = session.server.config.metaint - metaint;
                    
                    if first_buf_len > 0 {
                        session.stream.write_all(&buf[..first_buf_len]).await?;
                    }
                    // Now we write metadata
                    session.stream.write_all(&metadata).await?;
                    // Followed by what left in buffer if there is any
                    if diff > 0 {
                        session.stream.write_all(&buf[first_buf_len..]).await?;
                    }
                    metaint = diff;
                } else {
                    metaint += buf.len();
                    session.stream.write_all(&buf).await?;
                }
            },
            Err(RecvError::Lagged(_)) => (),
            Err(RecvError::Closed) => break
        }
    };

    Ok(())
}

pub async fn handle(mut session: ClientSession) {
    let request = RefCell::new(Request {
        headers_buf: Vec::new(),
        headers: Vec::new(),
        method: ""
    });


    // I don't really knew why borrowing later is impossible without this hack
    let _type;
    {
        let refe = unsafe { request.as_ptr().as_mut() };
        _type = match read_request(&mut session, refe.unwrap()).await {
            Ok(v) => v,
            Err(e) => {
                response::method_not_allowed(&mut session.stream, &session.server.config.info.id).await.ok();
                info!("Request coming from {} couldn't be handled: {}", session.addr, e);
                return;
            }
        };
    }

    let request = request.into_inner();

    match _type {
        RequestType::AdminRequest(v) => admin::handle_request(session, &request, v).await,
        RequestType::SourceRequest(v) => source::handle(session, &request, v).await,
        RequestType::ListenRequest(v) => handle_client(session, &request, v).await
    }.ok();
}
