use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use futures::executor::block_on;
use hashbrown::HashMap;
use scrypt::password_hash::PasswordVerifier;
use qanat::{mpsc, oneshot};

use crate::{server::{ClientSession, Server, AddrType}, response, config::Account};

#[derive(Debug, PartialEq, Eq)]
pub enum AllowedAuthType {
    SourceApi,
    SourceMount,
    SlaveOrYP
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
            Account::Slave { pass, .. }  => pass,
            Account::YP { pass, .. }     => pass
        };
        // We have already guarded againt this in phase of config load
        let hash  = scrypt::password_hash::PasswordHash::new(pass.split_at(2).1)
            .expect("Should be able to parse scrypt hash");
        hashes.insert(user.clone(), Hash::Scrypt(hash));
    }

    while let Ok(req) = block_on(rx.recv()) {
        match &hashes.get(&req.user) {
            Some(Hash::Scrypt(hash)) => {
                _ = req.resp.send(scrypt::Scrypt.verify_password(req.pass.as_bytes(), hash).is_ok());
            },
            None => unreachable!()
        }
    }
}

pub async fn verify_auth_enabled(session: &mut ClientSession) -> Result<()> {
    let enabled = match session.addr_type {
        AddrType::Admin => session.server.config.admin_access.address.allow_auth,
        AddrType::AuthAllowed => true,
        AddrType::Simple => false
    };

    if !enabled {
        response::forbidden(&mut session.stream, &session.server.config.info.id, "Access not allowed").await?;
        return Err(anyhow::Error::msg("Attempt to access public interface with disabled auth"));
    }

    Ok(())
}

pub struct UserRef {
    pub id: String
}

impl std::fmt::Display for UserRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "auth:{}", self.id)
    }
}

pub async fn auth(session: &mut ClientSession, allowed: AllowedAuthType,
                      auth: Option<(String, String)>, req_mount: &str)
    -> Result<()> {
    // Making sure we are receiving this through admin interface
    if let Some((user, pass)) = auth {
        verify_auth_enabled(session).await?;

        // we are retrieving both index of account and if it can access mount
        // In AllowedAuthType::Source: a source can only access mount it has permission to
        // In AllowedAuthType::SlaveOrYP: slave/YP has access
        // Admin has access to everything
        // While others can only get access in their respective mode
        let mut has_permission = false;
        
        if let Some(x) = session.server.config.account.get(&user) {
            // Check if we have necessary permissions before proceeding
            match x {
                Account::Source { mount, .. } => {
                    // First we check if user has access to specific mount
                    if allowed == AllowedAuthType::SourceApi ||
                       allowed == AllowedAuthType::SourceMount {
                        has_permission = mount.iter().any(|x| x.path.eq("*") || x.path.eq(req_mount));
                    }
                    // Then we verifiy if it's owned by user
                    if allowed == AllowedAuthType::SourceApi {
                        has_permission = has_permission && match session.server.sources.read().await.get(req_mount) {
                            Some(v) => match &v.access {
                                crate::source::SourceAccessType::SourceClient { username } => username.eq(&user),
                                _ => false
                            },
                            None => false
                        }
                    }
                },
                Account::Admin { .. } => {
                    if session.addr_type == AddrType::Admin {
                        has_permission = true;
                    }
                },
                Account::Slave { .. } | Account::YP { .. } => {
                    if allowed == AllowedAuthType::SlaveOrYP {
                        has_permission = true;
                    }
                }
            }

            // Doing authentication here
            if has_permission {
                let (tx, mut rx) = oneshot::channel();
                if session.server.hash_calculator.send(HashCalculation { user: user.clone(), pass, resp: tx }).is_ok() {
                    if let Ok(true) = rx.recv().await {
                        session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                        session.user = Some(UserRef { id: user });
                        return Ok(());
                    }
                }
            }
        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Authentication failed"))
}

pub async fn admin_auth(session: &mut ClientSession, auth: Option<(String, String)>) -> Result<()> {
    // Making sure we are receiving this through admin interface
    if session.addr_type == AddrType::Admin {
        if !session.server.config.admin_access.address.allow_auth {
            response::forbidden(&mut session.stream, &session.server.config.info.id, "Access not allowed").await?;
            return Err(anyhow::Error::msg("Attempt to access admin interface with disabled auth"));
        }

        if let Some(v) = auth {
            if let Some(Account::Admin { .. }) = session.server.config.account.get(&v.0) {
                let (tx, mut rx) = oneshot::channel();
                if session.server.hash_calculator.send(HashCalculation { user: v.0.clone(), pass: v.1, resp: tx }).is_ok() {
                    if let Ok(true) = rx.recv().await {
                        session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                        session.user = Some(UserRef { id: v.0 });
                        return Ok(());
                    }
                }
            }
        }
    }

    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Admin authentication failed"))
}
