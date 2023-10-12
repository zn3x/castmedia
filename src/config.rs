use std::net::SocketAddr;

use serde::{Serialize, Deserialize};
use tracing::{error, info, warn};

use scrypt::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, SaltString
    },
    Scrypt
};

// Sane defaults for CastRadio
const BIND: &str             = "127.0.0.1:9000";
const METAINT: usize         = 32000;

const SERVER_ID: &str        = "CastRadio 0.1.0";
const SERVER_ADMIN: &str     = "admin@localhost";
const LOCATION: &str         = "1.064646";
const DESCRIPTION: &str      = "Internet radio!";

const MAX_CLIENTS: usize     = 400;
const MAX_SOURCES: usize     = 4;
const MAX_LISTENERS: usize   = 370;
const QUEUE_SIZE: usize      = 102400;
const HEADER_TIMEOUT: u64    = 15000;
const SOURCE_TIMEOUT: u64    = 10000;
const HTTP_MAX_LEN: usize    = 8192;

const ADMINACC_ENABLED: bool = true;
const ADMINACC_BIND: &str    = "127.0.0.1:9100";

/// Server configuration
#[derive(Serialize, Deserialize)]
pub struct ServerSettings {
    /// List of every address:port couple we want to bind to
    #[serde(default = "default_val_address")]
    pub address: Vec<ServerAddress>,
    /// Interval in millis when we resend icy metadata of mountpoint to clients
    /// https://thecodeartist.blogspot.com/2013/02/shoutcast-internet-radio-protocol.html
    #[serde(default = "default_val_metaint", rename = "metadata_interval")]
    pub metaint: usize,
    /// Icy metadata related to our radio instance
    #[serde(default = "default_val_info")]
    pub info: ServerInfo,
    /// Predefined limits that server shall not surpass
    #[serde(default = "default_val_limits")]
    pub limits: ServerLimits,
    /// Access for admin accounts
    #[serde(default = "default_val_admin_access")]
    pub admin_access: AdminAccess,
    /// Accounts credentials
    #[serde(default = "default_val_accounts")]
    pub account: Vec<Account>
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "role")]
#[serde(rename_all = "lowercase")]
pub enum Account {
    Admin {
        user: String,
        pass: String,
    },
    Source {
        user: String,
        pass: String,
        mount: Vec<Mount>
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Mount {
    pub path: String,
    pub fallback: Option<String>
}

#[derive(Serialize, Deserialize)]
pub struct ServerAddress {
    /// Address to bind to, must be a valid ipv4/ipv6 of an interface
    pub bind: SocketAddr,
    pub tls: Option<TlsIdentity>
}

#[derive(Serialize, Deserialize)]
pub struct TlsIdentity {
    pub enabled: bool, 
    pub cert: String,
    pub pass: String
}

#[derive(Serialize, Deserialize)]
pub struct ServerInfo {
    /// Radio unique name
    #[serde(default = "default_val_info_id")]
    pub id: String,
    /// Radio admin, usually an email address is given
    #[serde(default = "default_val_info_admin")]
    pub admin: String,
    /// Radio gps location
    #[serde(default = "default_val_info_location")]
    pub location: String,
    /// Radio description, genre... etc
    #[serde(default = "default_val_info_description")]
    pub description: String
}

#[derive(Serialize, Deserialize)]
pub struct ServerLimits {
    /// Max number of concurrent clients
    #[serde(default = "default_val_limit_clients")]
    pub clients: usize,
    /// Max number of concurrent sources
    #[serde(default = "default_val_limit_sources")]
    pub sources: usize,
    /// Max number of concurrent listeners
    #[serde(default = "default_val_limit_listeners")]
    pub listeners: usize,
    /// The max size the queue for every mountpoint
    /// When a user falls behind this queue, they are moved to the tail of the queue
    /// Or they is disconnected if we can't send data to them
    /// This is also the value used as burst value for new clients
    /// Burst is usefull so that we do not put new clients a the top of the queue
    /// where there may or may not be some new data in queue to buffer
    #[serde(default = "default_val_limit_queue_size")]
    pub queue_size: usize,
    /// Max time in millis we wait for a client to send his header
    #[serde(default = "default_val_limit_header_timeout")]
    pub header_timeout: u64,
    /// Max time in millis we wait to receive a chunk of audio stream from a source
    #[serde(default = "default_val_limit_source_timeout")]
    pub source_timeout: u64,
    /// Max http request size in bytes sent by client that we are willing to accept
    #[serde(default = "default_val_limit_http_max_len")]
    pub http_max_len: usize
}

#[derive(Serialize, Deserialize)]
pub struct AdminAccess {
    #[serde(default = "default_val_adminacc_enabled")]
    pub enabled: bool,
    #[serde(default = "default_val_adminacc_address")]
    pub address: ServerAddress,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            address: default_val_address(),
            metaint: default_val_metaint(),
            info: default_val_info(),
            limits: default_val_limits(),
            admin_access: default_val_admin_access(),
            account: default_val_accounts()
        }
    }
}

impl Default for ServerInfo {
    fn default() -> Self {
        ServerInfo {
            id: default_val_info_id(),
            admin: default_val_info_admin(),
            location: default_val_info_location(),
            description: default_val_info_description()
        }
    }
}

impl Default for ServerLimits {
    fn default() -> Self {
        ServerLimits {
            clients: default_val_limit_clients(),
            sources: default_val_limit_sources(),
            listeners: default_val_limit_listeners(),
            queue_size: default_val_limit_queue_size(),
            header_timeout: default_val_limit_header_timeout(),
            source_timeout: default_val_limit_source_timeout(),
            http_max_len: default_val_limit_http_max_len()
        }
    }
}

impl Default for AdminAccess {
    fn default() -> Self {
        AdminAccess {
            enabled: default_val_adminacc_enabled(),
            address: default_val_adminacc_address()
        }
    }
}

fn default_val_address() -> Vec<ServerAddress> { vec![ ServerAddress { bind: BIND.parse().expect("Should be a valid socket address"), tls: None } ] }
fn default_val_metaint() -> usize { METAINT }
fn default_val_info() -> ServerInfo { ServerInfo::default() }
fn default_val_limits() -> ServerLimits { ServerLimits::default() }
fn default_val_admin_access() -> AdminAccess { AdminAccess::default() }

fn default_val_info_id() -> String { SERVER_ID.to_owned() }
fn default_val_info_admin() -> String { SERVER_ADMIN.to_owned() }
fn default_val_info_location() -> String { LOCATION.to_owned() }
fn default_val_info_description() -> String { DESCRIPTION.to_owned() }

fn default_val_limit_clients() -> usize { MAX_CLIENTS }
fn default_val_limit_sources() -> usize { MAX_SOURCES }
fn default_val_limit_listeners() -> usize { MAX_LISTENERS }
fn default_val_limit_queue_size() -> usize { QUEUE_SIZE }
fn default_val_limit_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_val_limit_source_timeout() -> u64 { SOURCE_TIMEOUT }
fn default_val_limit_http_max_len() -> usize { HTTP_MAX_LEN }

fn default_val_adminacc_enabled() -> bool { ADMINACC_ENABLED }
fn default_val_adminacc_address() -> ServerAddress { ServerAddress { bind: ADMINACC_BIND.parse().expect("Should be a valid socket address"), tls: None } }

fn default_val_accounts() -> Vec<Account> { Vec::new() }

impl ServerSettings {
    pub fn load(config_path: &str) -> Self {
        match std::fs::read_to_string(config_path) {
            Ok(v) => {
                match serde_yaml::from_str::<ServerSettings>(&v) {
                    Ok(v) => {
                        info!("Loaded configuration from {}", config_path);
                        v
                    },
                    Err(e) => {
                        error!("Loading config file {} failed: {}", config_path, e);
                        std::process::exit(1);
                    }
                }
            },
            Err(e) => {
                error!("Reading config file {} failed: {}", config_path, e);
                std::process::exit(1);
            }
        }
    }

    pub fn hash_passwords(config: &mut ServerSettings) {
        // Converting plaintext passwords to hash
        for account in &mut config.account {
            let pass = match account {
                Account::Source { pass, .. } => pass,
                Account::Admin { pass, .. } => pass
            };
            
            match pass.split_at(2) {
                ("1$", _) => (),
                ("0$", rawpass) => {
                    let salt = SaltString::generate(&mut OsRng);
                    // Speeding up debug
                    // Should never be used for prod
                    #[cfg(debug_assertions)]
                    let hash = Scrypt.hash_password_customized(rawpass.as_bytes(), None, None, scrypt::Params::new(1, 1, 1, 10).unwrap(), &salt)
                        .expect("Should be able to hash password")
                        .to_string();
                    #[cfg(not(debug_assertions))]
                    let hash = Scrypt.hash_password(rawpass.as_bytes(), &salt)
                        .expect("Should be able to hash password")
                        .to_string();
                    *pass    = "1$".to_string();
                    pass.push_str(&hash);
                },
                _ => ()
            }
        }
    }

    pub fn create_default(config_path: &str) {
        let settings = serde_yaml::to_string(&Self::default()).expect("Can't serialize server settings");
        match std::fs::write(config_path, settings) {
            Ok(_) => info!("Default config file written to {}", config_path),
            Err(e) => error!("Creating default config at {} failed: {}", config_path, e)
        }
    }

    /// Method to verify if current settings are sane returning number of errors found
    pub fn verify(config: &ServerSettings, unsafe_pass: bool) -> usize {
        let mut errors   = 0;
        let mut with_tls = false;
        // First we verify no duplicate addresses are supplied to us
        let mut addresses = config.address.iter().collect::<Vec<_>>();
        if config.admin_access.enabled {
            addresses.push(&config.admin_access.address);
        }
        for address in &addresses {
            let address = *address;
            for address1 in &addresses {
                let address1 = *address1;
                if &address.bind as *const _ != &address1.bind as *const _
                    && address.bind.eq(&address1.bind) {
                    error!("Two addresses can't have same bind tuple [{}].", address.bind);
                    errors += 1;
                }
            }
            if let Some(tls) = address.tls.as_ref() {
                with_tls = true;
                if !std::path::Path::new(&tls.cert).is_file() {
                    error!("Tls identity {} for [{}] not found.", tls.cert, address.bind);
                    errors += 1;
                }
            }
        }

        if with_tls {
            warn!("Migration not supported with tls, an tls termination proxy must be set if needed");
        }

        // Verifying accounts credentials
        for account in &config.account {
            let (user, pass, mounts) = match account {
                Account::Admin { user, pass } => (user, pass, None),
                Account::Source { user, pass, mount } => (user, pass, Some(mount))
            };

            // Checking if we don't have duplicates
            for raccount in &config.account {
                let (ruser, rmounts) = match raccount {
                    Account::Admin { user, .. } => (user, None),
                    Account::Source { user, mount, .. } => (user, Some(mount))
                };
                // Skip if we are identic
                if std::ptr::eq(user, ruser) {
                    continue;
                }

                if user.eq(ruser) {
                    error!("Two accounts with identical username: {}", user);
                    errors += 1;
                }

                if let (Some(mounts), Some(rmounts)) = (mounts, rmounts) {
                    for mount in mounts {
                        for rmount in rmounts {
                            if mount.path.eq(&rmount.path) && mount.path.ne("*") {
                                warn!("Sources {} and {} have access to same mountpoint {}", user, ruser, mount.path);
                            }
                        }
                    }
                }
            }

            if !unsafe_pass {
                // Checking if we have strong password if it's plaintext
                match pass.split_at(2) {
                    ("0$", rawpass) => {
                        let estimate = zxcvbn::zxcvbn(rawpass, &[user])
                            .expect("Should be able to calculate password entropy");

                        if estimate.score() <= 3 {
                            error!("Password for {} is not strong with a score of {}/4", user, estimate.score());
                            errors += 1;
                            continue;
                        }
                    },
                    ("1$", hash) => {
                        if let Err(e) = PasswordHash::new(hash) {
                            error!("Invalid scrypt password hash for {}: {}", user, e);
                            errors += 1;
                        }
                    },
                    _ => {
                        error!("Invalid password prefix for {}", user);
                        errors += 1;
                        continue;
                    }
                }
            }
        }

        // Verifying also if other settings are sane
        if config.metaint < 10000 {
            warn!("metadata_interval [value:{}] interval is too small, this may degrade performance.", config.metaint);
        }
        if config.limits.http_max_len > 16000 {
            warn!("http_max_len [value:{}] is too big, this may be used to deny service.", config.limits.http_max_len);
        }
        if config.limits.sources + config.limits.listeners > config.limits.clients {
            error!("limits.sources + limits.listeners should never be bigger than limits.clients");
            errors += 1;
        }

        errors
    }
}
