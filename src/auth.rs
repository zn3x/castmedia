use anyhow::Result;

// TODO: HOW TO DEAL WITH AUTH?
// TODO: Source auth with mountpoint

pub async fn source_auth(auth: Option<(String, String)>) -> Result<bool> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn admin_or_source_auth(auth: Option<(String, String)>) -> Result<bool> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn admin_auth(auth: Option<(String, String)>) -> Result<bool> {
    if let Some(v) = auth {
        if v.0.eq("1") && v.1.eq("2") {
            return Ok(true);
        }
    }
    Ok(false)
}
