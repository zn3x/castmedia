use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use hashbrown::HashMap;
use tracing::info;
use serde_json::json;
use uuid::Uuid;

use crate::{
    server::ClientSession,
    request::AdminRequest,
    response::{self, ChunkedResponse},
    auth::{self, AllowedAuthType}, utils,
    broadcast::broadcast_metadata,
    source::{MoveClientsCommand, MoveClientsType, SourceAccessType},
    migrate
};

async fn update_metadata(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    match utils::get_queries_val_for_keys(&["mode", "mount", "song", "url"], &req.queries).as_slice() {
        &[Some(mode), Some(mount), song, url] => {
            let user_id = auth::auth(session, AllowedAuthType::SourceApi, req.auth, mount).await?;
            let sid     = &session.server.config.info.id;

            if !mode.eq("updinfo") {
                response::bad_request(&mut session.stream, sid, "Metadata update request only supports updinfo mode").await?;
                return Ok(());
            }

            let success = if let Some(source) = session.server.sources.read().await.get(mount) {
                broadcast_metadata(source, &song, &url).await;
                true
            } else {
                false
            };

            if success {
                info!("Updated mountpoint metadata for {} by {}", mount, user_id);
                response::ok_200(&mut session.stream, sid).await?;
            } else {
                response::bad_request(&mut session.stream, sid, "Invalid mountpoint").await?;
                return Ok(());
            }
        },
        _ => {
            response::bad_request(&mut session.stream, &session.server.config.info.id, "Metadata update request need valid queries").await?;
        }
    }

    Ok(())
}

async fn update_fallback(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    match utils::get_queries_val_for_keys(&["mount", "fallback"], &req.queries).as_slice() {
        &[Some(mount), fallback] => {
            let user_id = auth::auth(session, AllowedAuthType::SourceApi, req.auth, mount).await?;
            let sid     = &session.server.config.info.id;

            let success = match session.server.sources.write().await.get_mut(mount) {
                Some(source) => {
                    source.fallback = fallback.map(|x| x.to_owned());
                    true
                },
                None => false
            };

            if success {
                info!("New fallback {:?} set for {} by {}", fallback, mount, user_id);
                response::ok_200(&mut session.stream, sid).await?;
            } else {
                response::bad_request(&mut session.stream, sid, "Invalid mountpoint").await?;
            }
        },
        _ => {
            response::bad_request(&mut session.stream, &session.server.config.info.id, "Invalid query").await?;
        }
    }

    Ok(())
}

async fn list_mounts(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

    let mut sources = HashMap::new();

    for source in session.server.sources.read().await.iter() {
        let prop_ref  = source.1.properties.as_ref();
        let metadata  = source.1.metadata.read().await;
        let mut sinfo = json!({
            "fallback": source.1.fallback,
            "metadata": metadata.as_ref(),
            "properties": prop_ref,
            "stats": {
                "active_listeners": source.1.stats.active_listeners.load(Ordering::Relaxed),
                "peak_listeners": source.1.stats.peak_listeners.load(Ordering::Relaxed),
                "bytes_read": source.1.stats.bytes_read.load(Ordering::Relaxed),
                "bytes_sent": source.1.stats.bytes_sent.load(Ordering::Relaxed),
                "start_time": source.1.stats.start_time
            }
        });

        let s = sinfo.as_object_mut()
            .expect("Listmounts json response should be an object");
        match &source.1.access {
            SourceAccessType::SourceClient { username } => {
                s.insert("source_username".to_string(), serde_json::to_value(username)?);
            },
            SourceAccessType::RelayedSource { relayed_source } => {
                s.insert("stream_source".to_string(), serde_json::to_value(relayed_source)?);
            }
        }
        sources.insert(source.0.to_owned(), sinfo);
    }

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
        "active_relay": session.server.stats.active_relay.load(Ordering::Relaxed),
        "peak_listeners": session.server.stats.peak_listeners.load(Ordering::Relaxed),
        "listener_connections": session.server.stats.listener_connections.load(Ordering::Relaxed),
        "source_client_connections": session.server.stats.source_client_connections.load(Ordering::Relaxed),
        "source_relay_connections": session.server.stats.source_relay_connections.load(Ordering::Relaxed),
        "active_relay_streams": session.server.stats.active_relay_streams.load(Ordering::Relaxed),
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
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    match utils::get_queries_val_for_keys(&["mount", "destination"], &req.queries).as_slice() {
        &[Some(mount), Some(destination)] => {
            let move_comm = match session.server.sources.read().await.get(destination) {
                Some(destination) => Some(MoveClientsCommand {
                        broadcast: destination.broadcast.clone(),
                        meta_broadcast: destination.meta_broadcast.clone(),
                        move_listeners_receiver: destination.move_listeners_receiver.clone(),
                        clients: destination.clients.clone(),
                        move_type: MoveClientsType::Move
                }),
                None => None
            };

            let move_comm = match move_comm {
                Some(v) => v,
                None => {
                    response::bad_request(&mut session.stream, sid, "Destination not found").await?;
                    return Ok(());
                }
            };

            match session.server.sources.read().await.get(mount) {
                Some(source) => {
                    source.move_listeners_sender.clone().send(Arc::new(move_comm));
                    info!("Clients in {} moved to {} by admin {}", mount, destination, user_id);
                    response::ok_200(&mut session.stream, sid).await?;
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

enum SourceKillSwitchState {
    SourceFound(Option<tokio::sync::oneshot::Sender<()>>),
    SourceIsRelayed,
    SourceNotFound
}

async fn kill_source(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    match utils::get_queries_val_for_keys(&["mount"], &req.queries).as_slice() {
        &[Some(mount)] => {
            let mut kill_switch = SourceKillSwitchState::SourceNotFound;
            if let Some(source) = session.server.sources.write().await.get_mut(mount) {
                kill_switch = if let SourceAccessType::SourceClient { .. } = &source.access {
                    SourceKillSwitchState::SourceFound(source.kill.take())
                } else {
                    SourceKillSwitchState::SourceIsRelayed
                }
            };

            match kill_switch {
                SourceKillSwitchState::SourceFound(Some(kill_switch)) => {
                    _ = kill_switch.send(());
                    info!("Source killed for mount {} by admin {}", mount, user_id);
                    response::ok_200(&mut session.stream, sid).await?;
                },
                SourceKillSwitchState::SourceFound(None) => {
                    // This might really only happen in a small interval between a killsource
                    // command and before source being removed from hash of active sources
                    response::bad_request(&mut session.stream, sid, "Source for mountpoint already killed").await?;
                },
                SourceKillSwitchState::SourceIsRelayed => {
                    response::forbidden(&mut session.stream, sid, "Can't kill relayed source").await?;
                },
                SourceKillSwitchState::SourceNotFound => {
                    response::bad_request(&mut session.stream, sid, "Mount not found").await?;
                }
            }
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount not specified").await?;
        }
    }

    Ok(())
}

async fn list_clients(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let _   = auth::admin_auth(session, req.auth).await?;
    let sid = &session.server.config.info.id;

    let mount = match utils::get_queries_val_for_keys(&["mount"], &req.queries).as_slice() {
        &[Some(mount)] => mount,
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount not specified").await?;
            return Ok(());
        }
    };

    let clients = {
        let clients = session.server.sources.read().await
            .get(mount)
            .and_then(|x| Some(x.clients.clone()));
        match clients {
            Some(v) => v,
            None => {
                response::bad_request(&mut session.stream, sid, "Mount not found").await?;
                return Ok(())
            }
        }
    };

    let chunked = ChunkedResponse::new(&mut session.stream, sid).await?;
    chunked.send(&mut session.stream, b"{").await?;
    {
        let lock     = clients.read().await;
        let mut list = lock.iter().peekable();
        while let Some((key, val)) = list.next() {
            chunked.send(&mut session.stream, format!("\"{}\":", key).as_bytes()).await?;
            chunked.send(&mut session.stream, serde_json::to_vec(&json!({
                "properties": val.properties,
                "stats": {
                    "start_time": val.stats.start_time.load(Ordering::Relaxed),
                    "bytes_sent": val.stats.bytes_sent.load(Ordering::Relaxed)
                }
            }))?.as_slice()).await?;
            if list.peek().is_some() {
                chunked.send(&mut session.stream, b",").await?;
            }
        }
    }
    chunked.send(&mut session.stream, b"}").await?;
    chunked.flush(&mut session.stream).await?;

    Ok(())
}

enum ClientKillSwitchState {
    SourceNotFound,
    ClientFound(Option<tokio::sync::oneshot::Sender<()>>),
    ClientNotFound
}

async fn kill_client(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    match utils::get_queries_val_for_keys(&["mount", "id"], &req.queries).as_slice() {
        &[Some(mount), Some(id)] => {
            let id = match Uuid::parse_str(id) {
                Ok(v) => v,
                Err(_) => {
                    response::bad_request(&mut session.stream, sid, "id is not a uuid").await?;
                    return Ok(());
                }
            };

            let clients = session.server.sources.read().await
                .get(mount)
                .and_then(|x| Some(x.clients.clone()));

            let kill_switch = match clients {
                Some(clients) => match clients.write().await.get_mut(&id) {
                    Some(v) => ClientKillSwitchState::ClientFound(v.kill.take()),
                    None => ClientKillSwitchState::ClientNotFound
                },
                None => ClientKillSwitchState::SourceNotFound
            };

            match kill_switch {
                ClientKillSwitchState::ClientFound(Some(kill_switch)) => {
                    _ = kill_switch.send(());
                    info!("Client {} killed for mount {} by admin {}", id, mount, user_id);
                    response::ok_200(&mut session.stream, sid).await?;
                },
                ClientKillSwitchState::ClientFound(None) => {
                    response::bad_request(&mut session.stream, sid, "Client already killed").await?;
                },
                ClientKillSwitchState::ClientNotFound => {
                    response::bad_request(&mut session.stream, sid, "Mount not found").await?;
                },
                ClientKillSwitchState::SourceNotFound => {
                    response::bad_request(&mut session.stream, sid, "Client with specified id not found").await?;
                }
            }
        },
        _ => {
            response::bad_request(&mut session.stream, sid, "Mount or client id not specified").await?;
        }
    }

    Ok(())
}

async fn server_restart(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    // Checking if all bind addresses are not tls
    let uses_tls = session.server.config.address
        .iter()
        .any(|x| x.tls.is_some() && x.tls.as_ref().unwrap().enabled);
    if uses_tls || (session.server.config.admin_access.address.tls.is_some()
        && session.server.config.admin_access.address.tls.as_ref().unwrap().enabled) {
        response::forbidden(&mut session.stream, sid, "Migration with Tls not supported").await?;
        return Ok(());
    }

    match session.server.migrate_tx.lock().await.take() {
        Some(migrate) => {
            info!("Starting zero downtime migration requested by admin {}", user_id);
            migrate::handle(session.server.clone(), migrate).await;
            response::ok_200(&mut session.stream, sid).await?;
        },
        None => {
            response::bad_request(&mut session.stream, sid, "Migration in progress...").await?;
        }
    }

    Ok(())
}

async fn server_shutdown(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::admin_auth(session, req.auth).await?;
    let sid     = &session.server.config.info.id;

    _ = response::ok_200(&mut session.stream, sid).await;

    info!("Stopping server as requested by admin {}", user_id);

    std::process::exit(0);
}

async fn mount_updates(mut session: ClientSession, req: AdminRequest) -> Result<()> {
    let user_id = auth::auth(&mut session, AllowedAuthType::Slave, req.auth, "").await?;

    ChunkedResponse::new(&mut session.stream, &session.server.config.info.id).await?;

    crate::relay::master_mount_updates(session, user_id, HashMap::new()).await
}

pub async fn handle_request<'a>(mut session: ClientSession, req: AdminRequest) -> Result<()> {
    session.server.stats.admin_api_connections.fetch_add(1, Ordering::Relaxed);
    // Handling /admin requests
    // In each path we must first check identity before proceeding todo anything
    match req.path.as_str() {
        // Mount specific requests that an admin or source with it's own mountpoint can perform:
        //
        // Update metadata for a mount point
        "/admin/metadata" => update_metadata(&mut session, req).await,
        // Changing fallback for a mount
        "/admin/fallbacks" => update_fallback(&mut session, req).await,

        // Admin only access:
        //
        // General server stats
        "/admin/stats" => stats(&mut session, req).await,
        // Fetch all mounts with their info
        "/admin/listmounts" => list_mounts(&mut session, req).await,
        // Move clients from one mount to another
        "/admin/moveclients" => move_clients(&mut session, req).await,
        // Kill a source mountpoint
        "/admin/killsource" => kill_source(&mut session, req).await,
        // List listeners of a specific mountpoint
        "/admin/listclients" => list_clients(&mut session, req).await,
        // Kill a specific listener
        "/admin/killclient" => kill_client(&mut session, req).await,
        // Do a zero downtime server restart
        "/admin/restart" => server_restart(&mut session, req).await,
        // Shutdown server
        "/admin/shutdown" => server_shutdown(&mut session, req).await,
        
        // New api for castmedia
        //
        // Send mount updates to slave server
        "/admin/mountupdates" => mount_updates(session, req).await,

        _ => response::not_found(&mut session.stream, &session.server.config.info.id).await
    }
}
