use std::sync::atomic::Ordering;

use anyhow::Result;
use serde_json::json;

use crate::{server::ClientSession, response, request::ApiRequest, utils};

async fn server_info(session: &mut ClientSession) -> Result<()> {
    let sid = &session.server.config.info.id;    
        
    let resp = json!({
        "mounts": session.server.sources.read().await.keys().cloned().collect::<Vec<String>>(),
        "properties": session.server.config.info,
        "stats": {
            "start_time": session.server.stats.start_time,
            "active_listeners": session.server.stats.active_listeners.load(Ordering::Relaxed),
            "peak_listeners": session.server.stats.peak_listeners.load(Ordering::Relaxed),
        },
    });

    match serde_json::to_vec(&resp) {
        Ok(v) => response::ok_200_json_body(&mut session.stream, sid, &v).await,
        Err(_) => response::internal_error(&mut session.stream, sid).await
    }
}

async fn mount_info(session: &mut ClientSession, req: ApiRequest) -> Result<()> {
    let sid = &session.server.config.info.id;

    let mount = match utils::get_queries_val_for_keys(&["mount"], &req.queries).as_slice() {
        [Some(mount)] => *mount,
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount not specified").await?;
            return Ok(());
        }
    };

    let source = match session.server.sources.read().await.get(mount) {
        Some(mount) => {
            let prop_ref = mount.properties.as_ref();
            let metadata = mount.metadata.read().await;
            json!({
                "metadata": metadata.as_ref(),
                "properties": prop_ref,
                "stats": {
                    "active_listeners": mount.stats.active_listeners.load(Ordering::Relaxed),
                    "peak_listeners": mount.stats.peak_listeners.load(Ordering::Relaxed),
                    "start_time": mount.stats.start_time
                }
            })
        },
        None => {
            response::bad_request(&mut session.stream, sid, "Mount not found").await?;
            return Ok(());
        }
    };

    match serde_json::to_vec(&source) {
        Ok(v) => response::ok_200_json_body(&mut session.stream, sid, &v).await,
        Err(_) => response::internal_error(&mut session.stream, sid).await
    }
}

pub async fn handle_request<'a>(mut session: ClientSession, req: ApiRequest) -> Result<()> {
    session.server.stats.api_connections.fetch_add(1, Ordering::Relaxed);

    match req.path.as_str() {
        "/api/serverinfo" => server_info(&mut session).await,
        "/api/mountinfo" => mount_info(&mut session, req).await,
        _ => response::not_found(&mut session.stream, &session.server.config.info.id).await
    }
}
