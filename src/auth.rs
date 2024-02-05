use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use hashbrown::HashMap;
use scrypt::password_hash::PasswordVerifier;
use tokio::sync::{mpsc, oneshot};

use crate::{server::{ClientSession, Server}, response, config::Account};

#[derive(Debug, PartialEq, Eq)]
pub enum AllowedAuthType {
    Source,
    Slave
}

pub struct HashCalculation {
    pub user: String,
    pub pass: String,
    pub resp: oneshot::Sender<bool>
}

pub enum Hash<'a> {
    Scrypt(scrypt::password_hash::PasswordHash<'a>)
}

pub fn password_hasher_thread(server: Arc<Server>, mut rx: mpsc::UnboundedReceiver<HashCalculation>) {
    // We first load all hash password and parse them to their correct types
    let mut hashes = HashMap::new();
    for (user, account) in &server.config.account {
        let pass = match account {
            Account::Admin { pass, .. }  => pass,
            Account::Source { pass, .. } => pass,
            Account::Slave { pass, .. }  => pass
        };
        // We have already guarded againt this in phase of config load
        let hash  = scrypt::password_hash::PasswordHash::new(pass.split_at(2).1)
            .expect("Should be able to parse scrypt hash");
        hashes.insert(user.clone(), Hash::Scrypt(hash));
    }

    while let Some(req) = rx.blocking_recv() {
        match &hashes.get(&req.user) {
            Some(Hash::Scrypt(hash)) => {
                _ = req.resp.send(scrypt::Scrypt.verify_password(req.pass.as_bytes(), hash).is_ok());
            },
            None => unreachable!()
        }
    }
}

pub async fn auth(session: &mut ClientSession, allowed: AllowedAuthType, auth: Option<(String, String)>, req_mount: &str) -> Result<String> {
    // Making sure we are receiving this through admin interface
    if session.admin_addr {
        if let Some((user, pass)) = auth {
            // we are retrieving both index of account and if it can access mount
            // In AllowedAuthType::Source: a source can only access mount it has permission to
            // In AllowedAuthType::Slave: slave has access
            // Admin has access to everything
            // While others can only get access in their respective mode
            let mut has_permission = false;
            
            if let Some(x) = session.server.config.account.get(&user) {
                // Check if we have necessary permissions before proceeding
                match x {
                    Account::Source { mount, .. } => {
                        if allowed == AllowedAuthType::Source {
                            has_permission = mount.iter().any(|x| x.path.eq("*") || x.path.eq(req_mount));
                        }
                    },
                    Account::Admin { .. } => {
                        has_permission = true;
                    },
                    Account::Slave { .. } => {
                        if allowed == AllowedAuthType::Slave {
                            has_permission = true;
                        }
                    }
                }

                // Doing authentication here
                if has_permission {
                    let (tx, rx) = oneshot::channel();
                    if session.server.hash_calculator.send(HashCalculation { user: user.clone(), pass, resp: tx }).is_ok() {
                        if let Ok(true) = rx.await {
                            session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                            return Ok(user);
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
            if let Some(Account::Admin { .. }) = session.server.config.account.get(&v.0) {
                let (tx, rx) = oneshot::channel();
                if session.server.hash_calculator.send(HashCalculation { user: v.0.clone(), pass: v.1, resp: tx }).is_ok() {
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
