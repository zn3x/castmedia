use std::net::SocketAddr;
use anyhow::Result;

use hashbrown::HashMap;
use serde::{Serialize, Deserialize};
use tracing::{error, info, warn};

use scrypt::{
    password_hash::{
        rand_core::OsRng,
        PasswordHash, PasswordHasher, SaltString
    },
    Scrypt
};
use url::Url;

// Sane defaults for CastRadio
const BIND: &str                         = "127.0.0.1:9000";
const METAINT: usize                     = 32000;

pub const SERVER_ID: &str                = "CastMedia 0.1.0";
const SERVER_ADMIN: &str                 = "admin@localhost";
const LOCATION: &str                     = "1.064646";
const DESCRIPTION: &str                  = "Internet radio!";

const MAX_CLIENTS: usize                 = 400;
const MAX_SOURCES: usize                 = 4;
const MAX_LISTENERS: usize               = 370;
const QUEUE_SIZE: usize                  = 102400;
const HEADER_TIMEOUT: u64                = 15000;
const SOURCE_TIMEOUT: u64                = 10000;
const HTTP_MAX_LEN: usize                = 8192;
const MASTER_HTTP_MAX_LEN: usize         = 16384;
const MASTER_TIMEOUT: u64                = 20000;
const MASTER_MOUNTS_LIMIT: usize         = usize::MAX;
const MASTER_TRANS_UP_INTERVAL: u64      = 120000;
const MASTER_AUTH_STREAM_ON_DEMAND: bool = false;
const MASTER_AUTH_RECONNECT_TIMEOUT: u64 = 20000;

const SERVERADDR_ALLOW_AUTH: bool        = true;

const ADMINACC_ENABLED: bool             = true;
const ADMINACC_BIND: &str                = "127.0.0.1:9100";

const MIGRATE_ENABLED: bool              = false;

const MISC_UNSAFE_PASS: bool             = false;
const MISC_CHECK_FORWARDEDFOR: bool      = false;

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
    /// Migration related
    #[serde(default = "default_val_migrate")]
    pub migrate: Migrate,
    /// Other misc settings
    #[serde(default = "default_val_misc")]
    pub misc: MiscSettings,
    /// Access for admin accounts
    #[serde(default = "default_val_admin_access")]
    pub admin_access: AdminAccess,
    /// Accounts credentials
    #[serde(default = "default_val_accounts")]
    #[serde(with = "::serde_with::rust::maps_duplicate_key_is_error")]
    pub account: HashMap<String, Account>,
    /// Master server relaying for slave instance
    #[serde(default = "default_val_master")]
    pub master: Vec<MasterServer>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Migrate {
    #[serde(default = "default_val_migrate_enabled")]
    pub enabled: bool,
    pub bind: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MiscSettings {
    #[serde(default = "default_val_misc_unsafe_pass")]
    /// Allow unsafe passwords, this is highly discouraged and should only be used for testing!!
    pub unsafe_pass: bool,
    #[serde(default = "default_val_misc_check_forwardedfor")]
    /// Check if `X-Forwarded-For` header is present and set it as default IP address for client
    /// Usefull when castmedia is sitting behind a reverse proxy like haproxy
    pub check_forwardedfor: bool
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "role")]
#[serde(rename_all = "lowercase")]
pub enum Account {
    Admin {
        pass: String,
    },
    Source {
        pass: String,
        #[serde(default = "default_source_mount")]
        mount: Vec<Mount>
    },
    Slave {
        pass: String
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub struct MasterServer {
    pub url: Url,
    #[serde(default = "default_val_master_mounts_limit")]
    pub mounts_limit: usize,
    pub relay_scheme: MasterServerRelayScheme
}

impl std::fmt::Display for MasterServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "master:[url:{}]", self.url)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum MasterServerRelayScheme {
    Transparent {
        #[serde(default = "default_val_master_transparent_update_interval")]
        update_interval: u64
    },
    Authenticated {
        user: String,
        pass: String,
        #[serde(default = "default_val_master_auth_stream_on_demand")]
        stream_on_demand: bool,
        #[serde(default = "default_val_master_auth_reconnect_timeout")]
        reconnect_timeout: u64
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
    pub tls: Option<TlsIdentity>,
    #[serde(default = "default_val_serveraddr_allow_auth")]
    pub allow_auth: bool
}

#[derive(Clone, Serialize, Deserialize)]
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
    /// Max number of concurrent sources including relays
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
    pub http_max_len: usize,
    /// Max http response size in bytes sent by master server in master/slave replication
    #[serde(default = "default_val_limit_master_http_max_len")]
    pub master_http_max_len: usize,
    #[serde(default = "default_val_limit_master_timeout")]
    /// Max time in millis we wait for master response including media stream read
    pub master_timeout: u64
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
            migrate: default_val_migrate(),
            admin_access: default_val_admin_access(),
            account: default_val_accounts(),
            master: default_val_master(),
            misc: default_val_misc()
        }
    }
}

impl Default for MiscSettings {
    fn default() -> Self {
        Self {
            unsafe_pass: default_val_misc_unsafe_pass(),
            check_forwardedfor: default_val_misc_check_forwardedfor()
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
            http_max_len: default_val_limit_http_max_len(),
            master_http_max_len: default_val_limit_master_http_max_len(),
            master_timeout: default_val_limit_master_timeout()
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

impl Default for Migrate {
    fn default() -> Self {
        Self {
            enabled: default_val_migrate_enabled(),
            bind: None
        }
    }
}

fn default_val_address() -> Vec<ServerAddress> {
    vec![ ServerAddress { bind: BIND.parse().expect("Should be a valid socket address"), tls: None, allow_auth: true } ]
}
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
fn default_val_limit_master_http_max_len() -> usize { MASTER_HTTP_MAX_LEN }
fn default_val_limit_master_timeout() -> u64 { MASTER_TIMEOUT }

fn default_val_serveraddr_allow_auth() -> bool { SERVERADDR_ALLOW_AUTH }

fn default_val_adminacc_enabled() -> bool { ADMINACC_ENABLED }
fn default_val_adminacc_address() -> ServerAddress {
    ServerAddress { bind: ADMINACC_BIND.parse().expect("Should be a valid socket address"), tls: None, allow_auth: true }
}

fn default_val_accounts() -> HashMap<String, Account> { HashMap::new() }

fn default_val_master() -> Vec<MasterServer> { Vec::new() }

fn default_source_mount() -> Vec<Mount> { Vec::new() }

fn default_val_master_mounts_limit() -> usize { MASTER_MOUNTS_LIMIT }
fn default_val_master_transparent_update_interval() -> u64 { MASTER_TRANS_UP_INTERVAL }
fn default_val_master_auth_stream_on_demand() -> bool { MASTER_AUTH_STREAM_ON_DEMAND }
fn default_val_master_auth_reconnect_timeout() -> u64 { MASTER_AUTH_RECONNECT_TIMEOUT }

fn default_val_migrate() -> Migrate { Migrate::default() }
fn default_val_migrate_enabled() -> bool { MIGRATE_ENABLED }

fn default_val_misc() -> MiscSettings { MiscSettings::default() }
fn default_val_misc_unsafe_pass() -> bool { MISC_UNSAFE_PASS }
fn default_val_misc_check_forwardedfor() -> bool { MISC_CHECK_FORWARDEDFOR }

impl ServerSettings {
    pub fn load(config_path: &str) -> Self {
        match std::fs::read_to_string(config_path) {
            Ok(v) => {
                match Self::from_string(&v) {
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

    pub fn from_string(config: &str) -> Result<Self> {
        Ok(serde_yaml::from_str::<ServerSettings>(config)?)
    }

    pub fn hash_passwords(config: &mut ServerSettings) {
        // Converting plaintext passwords to hash
        for (_, account) in &mut config.account {
            let pass = match account {
                Account::Source { pass, .. } => pass,
                Account::Admin { pass, .. }  => pass,
                Account::Slave { pass, .. }  => pass
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

        if addresses.is_empty() {
            error!("At least one public bind address must be specified");
            errors += 1;
        }

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

        if with_tls && config.migrate.enabled {
            error!("Migration not supported with tls, a tls termination proxy must be set if needed");
            errors += 1;
        }

        // Verifying accounts credentials
        for (user, account) in &config.account {
            let (pass, mounts) = match account {
                Account::Admin { pass } => (pass, None),
                Account::Source { pass, mount } => {
                    if mount.is_empty() {
                        warn!("Source {} has no defined mount, this means it can't mount any stream", user);
                    }
                    (pass, Some(mount))
                },
                Account::Slave { pass } => (pass, None)
            };

            // Checking if we don't have duplicates
            for (ruser, raccount) in &config.account {
                let rmounts = match raccount {
                    Account::Admin { .. } => None,
                    Account::Source { mount, .. } => Some(mount),
                    Account::Slave { .. } => None
                };
                // Skip if we are identic
                if std::ptr::eq(user, ruser) {
                    continue;
                }

                if let (Some(mounts), Some(rmounts)) = (mounts, rmounts) {
                    for mount in mounts {
                        for rmount in rmounts {
                            if mount.path.eq(&rmount.path) && mount.path.ne("*") {
                                warn!("Source users {} and {} have access to same mountpoint {}", user, ruser, mount.path);
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

        // Verifying if relaying settings are sane
        for master in &config.master {
            match (master.url.host(), master.url.port_or_known_default()) {
                (Some(_), Some(_)) => {
                    if !["http", "https"].contains(&master.url.scheme()) {
                        error!("Master instance url {} is invalid!", master.url);
                        errors += 1;
                    }
                },
                (None, _) => {
                    error!("Master instance url {} missing hostname!", master.url);
                    errors += 1;
                },
                (_, None) => {
                    error!("Master instance url {} has no port", master.url);
                    errors += 1;
                }
            }

            if let MasterServerRelayScheme::Transparent { update_interval } = &master.relay_scheme {
                if *update_interval > 200000 {
                    warn!("Update interval {} for {} may be too big", master.url, update_interval);
                }
            }
        }

        if config.migrate.enabled {
            match config.migrate.bind.as_ref() {
                Some(f) => {
                    let path = std::path::Path::new(f);
                    
                    if !path.is_file() && path.parent().map(|x| x.exists()).ne(&Some(true)) {
                        error!("Path {} for migration socket doesn't look valid", f);
                        errors += 1;
                    }
                },
                None => {
                    error!("Migration socket path must be specified when migration is enabled");
                    errors += 1;
                }
            }
        } else {
            warn!("Migration is disabled, zero downtimes won't be possible");
        }

        if config.misc.check_forwardedfor {
            warn!("check_forwardedfor is enabled!! make sure only reverse proxy can access listeners");
        }

        errors
    }
}
