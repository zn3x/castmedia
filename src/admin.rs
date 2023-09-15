use std::sync::atomic::Ordering;

use anyhow::Result;
use hashbrown::HashMap;
use tracing::info;
use serde_json::json;

use crate::{
    server::ClientSession,
    request::{Request, AdminRequest}, response, auth, utils, stream::broadcast_metadata, source::{MoveClientsCommand, MoveClientsType}
};

async fn update_metadata(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_or_source_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

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

async fn update_fallback(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_or_source_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

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

async fn list_mounts(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

    let mut sources = HashMap::new();

    session.server.sources.read().await
        .iter()
        .for_each(|source| {
        let prop_ref = source.1.properties.as_ref();
        sources.insert(source.0.to_owned(), json!({
            "fallback": source.1.fallback,
            "metadata": source.1.metadata,
            "properties": prop_ref,
            "stats": {
                "active_listeners": source.1.stats.active_listeners.load(Ordering::Relaxed),
                "peak_listeners": source.1.stats.peak_listeners.load(Ordering::Relaxed),
                "bytes_read": source.1.stats.bytes_read.load(Ordering::Relaxed),
                "bytes_sent": source.1.stats.bytes_sent.load(Ordering::Relaxed),
                "start_time": source.1.stats.start_time
            }
        }));
    });

    match serde_json::to_vec(&sources) {
        Ok(v) => response::ok_200_json_body(&mut session.stream, sid, &v).await?,
        Err(_) => response::internal_error(&mut session.stream, sid).await?
    }

    Ok(())
}

async fn stats(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

    let stats = json!({
        "start_time": session.server.stats.start_time,
        "connections": session.server.stats.connections.load(Ordering::Relaxed),
        "active_clients": session.server.config.limits.clients - session.server.max_clients.available_permits(),
        "active_sources": session.server.stats.active_sources.load(Ordering::Relaxed),
        "active_listeners": session.server.stats.active_listeners.load(Ordering::Relaxed),
        "peak_listeners": session.server.stats.peak_listeners.load(Ordering::Relaxed),
        "listener_connections": session.server.stats.listener_connections.load(Ordering::Relaxed),
        "source_client_connections": session.server.stats.source_client_connections.load(Ordering::Relaxed),
        "admin_api_connections": session.server.stats.admin_api_connections.load(Ordering::Relaxed),
        "admin_api_connections_success": session.server.stats.admin_api_connections_success.load(Ordering::Relaxed),
        "api_connections": session.server.stats.api_connections.load(Ordering::Relaxed)
    });

    match serde_json::to_vec(&stats) {
        Ok(v) => response::ok_200_json_body(&mut session.stream, sid, &v).await?,
        Err(_) => response::internal_error(&mut session.stream, sid).await?
    }

    Ok(())
}

async fn move_clients(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

    match utils::get_queries_val_for_keys(&["mount", "destination"], &req.queries).as_slice() {
        &[Some(mount), Some(destination)] => {
            let move_comm;
            match session.server.sources.read().await.get(destination) {
                Some(destination) => {
                    move_comm = MoveClientsCommand {
                        broadcast: destination.broadcast.clone(),
                        meta_broadcast: destination.meta_broadcast.clone(),
                        move_listeners_receiver: destination.move_listeners_receiver.clone(),
                        clients: destination.clients.clone(),
                        move_type: MoveClientsType::Move
                    };
                },
                None => {
                    response::bad_request(&mut session.stream, sid, "Destination not found").await?;
                    return Ok(());
                }
            }

            match session.server.sources.read().await.get(mount) {
                Some(mount) => {
                    mount.move_listeners_sender.clone().send(move_comm);
                },
                None => {
                    response::bad_request(&mut session.stream, sid, "Mount not found").await?;
                }
            }
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount and destination are both needed").await?;
        }
    }

    Ok(())
}

async fn kill_source(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    match utils::get_queries_val_for_keys(&["mount"], &req.queries).as_slice() {
        &[Some(mount)] => {
            let kill_switch = match session.server.sources.write().await.get_mut(mount) {
                Some(mount) => mount.kill.take(),
                None => {
                    response::bad_request(&mut session.stream, sid, "Mount not found").await?;
                    return Ok(())
                }
            };
            
            match kill_switch {
                Some(kill_switch) => {
                    _ = kill_switch.send(());
                    info!("Source killed for mount {} by admin {}", mount, user_id);
                },
                None => {
                    // This might really only happen in a small interval between a killsource
                    // command and before source being removed from hash of active sources
                    response::bad_request(&mut session.stream, sid, "Source for mountpoint already killed").await?;
                    return Ok(())
                }
            }
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount not specified").await?;
        }
    }

    Ok(())
}


pub async fn handle_request<'a>(mut session: ClientSession, _request: &Request<'a>, req: AdminRequest) -> Result<()> {
    session.server.stats.admin_api_connections.fetch_add(1, Ordering::Relaxed);
    // Handling /admin requests
    // In each path we must first check identity before proceeding todo anything
    match req.path.as_str() {
        // Mount specific requests that an admin or source with it's own mountpoint can perform:
        //
        // Update metadata for a mount point
        "/admin/metadata" => update_metadata(&mut session, req).await?,
        // Changing fallback for a mount
        "/admin/fallbacks" => update_fallback(&mut session, req).await?,

        // Admin only access:
        //
        // General server stats
        "/admin/stats" => stats(&mut session, req).await?,
        // Fetch all mounts with their info
        "/admin/listmounts" => list_mounts(&mut session, req).await?,
        // Move clients from one mount to another
        "/admin/moveclients" => move_clients(&mut session, req).await?,
        // Kill a source mountpoint
        "/admin/killsource" => kill_source(&mut session, req).await?,
        _ => response::not_found(&mut session.stream, &session.server.config.info.id).await?
    }

    Ok(())
}
