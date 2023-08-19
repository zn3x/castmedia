use std::{cell::RefCell, sync::Arc};
use anyhow::Result;
use llq::errors::{RecvError, TryRecvError};
use tokio::io::AsyncWriteExt;
use tracing::{info, error};

use crate::{
    server::ClientSession,
    request::{read_request, Request, RequestType, ListenRequest}, source, response, utils, admin
};

pub async fn client_broadcast<'a>(mut session: ClientSession, request: &Request<'a>, req: ListenRequest) -> Result<()> {
    let (props, mut stream, mut meta_stream) = match session.server.sources.read().await.get(&req.mountpoint) {
        Some(v) => {
            (
            v.properties.clone(),
            v.broadcast.clone(),
            v.meta_broadcast.clone())
        },
        None => {
            response::not_found(&mut session.stream, &session.server.config.info.id).await?;
            error!("Source for {} suddenly vanished after client connection", req.mountpoint);
            return Ok(());
        }
    };

    drop(req);

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
        RequestType::ListenRequest(v) => client_broadcast(session, &request, v).await
    }.ok();
}
