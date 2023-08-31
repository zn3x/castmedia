use anyhow::Result;
use hashbrown::HashMap;
use tracing::info;
use serde_json::json;

use crate::{
    server::ClientSession,
    request::{Request, AdminRequest}, response, auth, utils, stream::broadcast_metadata
};

async fn update_metadata(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let sid = &session.server.config.info.id;

    if !auth::admin_or_source_auth(req.auth).await? {
        response::authentication_needed(&mut session.stream, sid).await?;
        return Ok(());
    }

    match utils::get_queries_val_for_keys(&["mode", "mount", "song", "url"], &req.queries).as_slice() {
        &[Some(mode), Some(mount), song, url] => {
            if !mode.eq("updinfo") {
                response::bad_request(&mut session.stream, sid, "Metadata update request only supports updinfo mode").await?;
                return Ok(());
            }

            if let Some(mut source) = session.server.sources.write().await.get_mut(mount) {
                broadcast_metadata(&mut source, &song, &url).await;
            } else {
                response::bad_request(&mut session.stream, sid, "Invalid mountpoint").await?;
                return Ok(());
            }
            response::ok_200(&mut session.stream, sid).await?;
            info!("Updated mountpoint metadata for {}", mount);
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Metadata update request need valid queries").await?;
        }
    }

    Ok(())
}

async fn list_mounts(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let sid = &session.server.config.info.id;

    if !auth::admin_auth(req.auth).await? {
        response::authentication_needed(&mut session.stream, sid).await?;
        return Ok(());
    }

    let mut sources = HashMap::new();

    session.server.sources.read().await
        .iter()
        .for_each(|source| {
        let prop_ref = source.1.properties.as_ref();
        sources.insert(source.0.to_owned(), json!({
            "fallback": source.1.fallback,
            "metadata": source.1.metadata,
            "properties": prop_ref,
            // TODO: Add missing
        }));
    });

    match serde_json::to_vec(&sources) {
        Ok(v) => response::ok_200_json_body(&mut session.stream, sid, &v).await?,
        Err(_) => response::internal_error(&mut session.stream, sid).await?
    }

    Ok(())
}

async fn update_fallback(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let sid = &session.server.config.info.id;

    if !auth::admin_or_source_auth(req.auth).await? {
        response::authentication_needed(&mut session.stream, sid).await?;
        return Ok(());
    }

    match utils::get_queries_val_for_keys(&["mount", "fallback"], &req.queries).as_slice() {
        &[Some(mount), fallback] => {
            match session.server.sources.write().await.get_mut(mount) {
                Some(mount) => {
                    mount.fallback = match fallback {
                        Some(v) => Some(v.to_owned()),
                        None => None
                    };
                },
                None => {
                    response::bad_request(&mut session.stream, sid, "Invalid mountpoint").await?;
                }
            }
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Invalid query").await?;
        }
    }

    Ok(())
}

pub async fn handle_request<'a>(mut session: ClientSession, _request: &Request<'a>, req: AdminRequest) -> Result<()> {
    // Handling /admin requests
    // In each path we must first check identity before proceeding todo anything
    match req.path.as_str() {
        // Update metadata for a mount point
        "/admin/metadata" => update_metadata(&mut session, req).await?,
        // Fetch all mounts with their info
        "/admin/listmounts" => list_mounts(&mut session, req).await?,
        // Changing fallback for a mount
        "/admin/fallback" => update_fallback(&mut session, req).await?,
        _ => response::not_found(&mut session.stream, &session.server.config.info.id).await?
    }

    Ok(())
}
