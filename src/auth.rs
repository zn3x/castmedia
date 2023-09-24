use std::sync::atomic::Ordering;

use anyhow::Result;

use crate::{server::ClientSession, response};

// TODO: HOW TO DEAL WITH AUTH?
// TODO: Source auth with mountpoint

pub async fn source_mount_auth(auth: Option<(String, String)>) -> Result<bool> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn admin_or_source_auth(session: &mut ClientSession, auth: Option<(String, String)>) -> Result<String> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
            return Ok(v.0);
        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Admin or Source authentication failed"))
}

pub async fn admin_auth(session: &mut ClientSession, auth: Option<(String, String)>) -> Result<String> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
            return Ok(v.0);
        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Admin authentication failed"))
}
