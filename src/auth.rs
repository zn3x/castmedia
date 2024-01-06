use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use scrypt::password_hash::PasswordVerifier;
use tokio::sync::{mpsc, oneshot};

use crate::{server::{ClientSession, Server}, response, config::Account};

pub struct HashCalculation {
    pub user_id: usize,
    pub pass: String,
    pub resp: oneshot::Sender<bool>
}

pub enum Hash<'a> {
    Scrypt(scrypt::password_hash::PasswordHash<'a>)
}

pub fn password_hasher_thread(server: Arc<Server>, mut rx: mpsc::UnboundedReceiver<HashCalculation>) {
    // We first load all hash password and parse them to their correct types
    let mut hashes = Vec::new();
    for account in &server.config.account {
        let pass = match account {
            Account::Admin { pass, .. }  => pass,
            Account::Source { pass, .. } => pass,
            Account::Slave { pass, .. }  => pass
        };
        // We have already guarded againt this in phase of config load
        let hash  = scrypt::password_hash::PasswordHash::new(pass.split_at(2).1)
            .expect("Should be able to parse scrypt hash");
        hashes.push(Hash::Scrypt(hash));
    }

    while let Some(req) = rx.blocking_recv() {
        match &hashes[req.user_id] {
            Hash::Scrypt(hash) => {
                _ = req.resp.send(scrypt::Scrypt.verify_password(req.pass.as_bytes(), hash).is_ok());
            }
        }
    }
}

pub async fn admin_or_source_auth(session: &mut ClientSession, auth: Option<(String, String)>, req_mount: &str) -> Result<String> {
    // Making sure we are receiving this through admin interface
    if session.admin_addr {
        if let Some(v) = auth {
            // we are retrieving both index of account and if it can access mount
            // for a source it can only access mount it has permission to
            let mut has_permission = false;
            let user_id            = session.server.config.account.iter()
                .position(|x| {
                    v.0.eq(match x {
                        Account::Source { user, mount, .. } => {
                            has_permission = mount.iter().any(|x| x.path.eq("*") || x.path.eq(req_mount));
                            user.as_str()
                        },
                        Account::Admin { user, .. } => {
                            has_permission = true;
                            user.as_str()
                        },
                        _ => {
                            has_permission = false;
                            ""
                        }
                    })
                });

            // User not found or no permission
            if let Some(user_id) = user_id {
                if has_permission {
                    let (tx, rx) = oneshot::channel();
                    if session.server.hash_calculator.send(HashCalculation { user_id, pass: v.1, resp: tx }).is_ok() {
                        if let Ok(true) = rx.await {
                            session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                            return Ok(v.0);
                        }
                    }
                }
            }

        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Admin or Source authentication failed"))
}

pub async fn admin_auth(session: &mut ClientSession, auth: Option<(String, String)>) -> Result<String> {
    // Making sure we are receiving this through admin interface
    if session.admin_addr {
        if let Some(v) = auth {
            let user_id = session.server.config.account.iter()
                .position(|x| {
                    v.0.eq(match x {
                        Account::Admin { user, .. } => {
                            user.as_str()
                        },
                        _ => ""
                    })
                });

            // User not found or no permission
            if let Some(user_id) = user_id {
                let (tx, rx) = oneshot::channel();
                if session.server.hash_calculator.send(HashCalculation { user_id, pass: v.1, resp: tx }).is_ok() {
                    if let Ok(true) = rx.await {
                        session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                        return Ok(v.0);
                    }
                }
            }
        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Admin authentication failed"))
}
