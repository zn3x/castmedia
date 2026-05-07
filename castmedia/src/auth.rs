use std::sync::atomic::Ordering;

use anyhow::Result;
use scrypt::password_hash::{PasswordHash, PasswordVerifier};

use crate::{
    server::{ClientSession, AddrType},
    response, config::Account,
};

pub enum RequiredRole {
    Admin,
    SourceApi { mount: String },
    SourceMount { mount: String },
    SlaveOrYP,
}

pub struct UserRef {
    pub id: String,
}

impl std::fmt::Display for UserRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "auth:{}", self.id)
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

pub async fn authenticate(
    session: &mut ClientSession,
    auth: Option<(String, String)>,
    role: RequiredRole,
) -> Result<()> {
    if let Some((user, pass)) = auth {
        verify_auth_enabled(session).await?;
        let mut has_permission = false;
        let mut password_hash  = None::<String>;
        if let Some(account) = session.server.config.account.get(&user) {
            // Admin accounts on admin interface have universal access
            if matches!(account, Account::Admin { .. }) && session.addr_type == AddrType::Admin {
                has_permission = true;
                if let Account::Admin { pass } = account {
                    password_hash = Some(pass.clone());
                }
            } else {
                match role {
                    RequiredRole::Admin => {}
                    RequiredRole::SourceApi { ref mount } => {
                        if let Account::Source { pass, mount: mounts } = account {
                            has_permission = mounts.iter().any(|x| x.path == "*" || x.path == *mount);
                            if has_permission {
                                password_hash = Some(pass.clone());
                            }
                            has_permission = has_permission
                                && match session.server.sources.read().await.get(mount) {
                                    Some(v) => match &v.access {
                                        crate::source::SourceAccessType::SourceClient { username } => username == &user,
                                        _ => false,
                                    },
                                    None => false,
                                };
                        }
                    }
                    RequiredRole::SourceMount { ref mount } => {
                        if let Account::Source { pass, mount: mounts } = account {
                            has_permission = mounts.iter().any(|x| x.path == "*" || x.path == *mount);
                            if has_permission {
                                password_hash = Some(pass.clone());
                            }
                        }
                    }
                    RequiredRole::SlaveOrYP => {
                        if matches!(account, Account::Slave { .. } | Account::YP { .. }) {
                            has_permission = true;
                            password_hash = Some(match account {
                                Account::Slave { pass } => pass.clone(),
                                Account::YP { pass } => pass.clone(),
                                _ => unreachable!(),
                            });
                        }
                    }
                }
            }
        }

        if has_permission {
            if let Some(hash_str) = password_hash {
                let hash_str = hash_str.split_at(2).1.to_string();
                let result = tokio::task::spawn_blocking(move || {
                    let hash = PasswordHash::new(&hash_str);
                    hash.ok()
                        .map(|h| scrypt::Scrypt.verify_password(pass.as_bytes(), &h).is_ok())
                        .unwrap_or(false)
                }).await;

                if let Ok(true) = result {
                    session.server.stats.admin_api_connections_success.fetch_add(1, Ordering::Relaxed);
                    session.user = Some(UserRef { id: user });
                    return Ok(());
                }
            }
        }
    }
    response::authentication_needed(&mut session.stream, &session.server.config.info.id).await?;
    Err(anyhow::Error::msg("Authentication failed"))
}
