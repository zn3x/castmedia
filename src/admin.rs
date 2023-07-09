use anyhow::Result;
use tracing::info;

use crate::{
    server::ClientSession,
    request::{Request, AdminRequest}, response, auth, utils, stream::broadcast_metadata
};

async fn update_metadata<'a>(session: &mut ClientSession, req: AdminRequest) -> Result<()> {
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

pub async fn handle_request<'a>(mut session: ClientSession, _request: &Request<'a>, req: AdminRequest) -> Result<()> {
    match req.path.as_str() {
        "/admin/metadata" => update_metadata(&mut session, req).await?,
        _ => response::not_found(&mut session.stream, &session.server.config.info.id).await?
    }

    Ok(())
}
