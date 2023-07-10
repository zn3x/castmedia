use serde::{Serialize, Deserialize};
use tracing::{error, info};

// Sane defaults for CastRadio
const ADDR: &str             = "127.0.0.1";
const PORT: u16              = 9000;
const METAINT: usize         = 32000;

const SERVER_ID: &str        = "CastRadio 0.1.0";
const SERVER_ADMIN: &str     = "admin@localhost";
const LOCATION: &str         = "1.064646";
const DESCRIPTION: &str      = "Internet radio!";

const MAX_CLIENTS: usize     = 400;
const MAX_SOURCES: usize     = 4;
const QUEUE_SIZE: usize      = 102400;
const HEADER_TIMEOUT: u64    = 15000;
const SOURCE_TIMEOUT: u64    = 10000;
const HTTP_MAX_LEN: usize    = 8192;

const ADMINACC_ENABLED: bool = true;
const ADMINACC_ADDR: &str    = ADDR;
const ADMINACC_PORT: u16     = 9100;

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
    pub admin_access: AdminAccess
}

#[derive(Serialize, Deserialize)]
pub struct ServerAddress {
    /// Address to bind to, must be a valid ipv4/ipv6 of an interface
    pub addr: String,
    pub port: u16
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
            admin_access: default_val_admin_access()
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

fn default_val_address() -> Vec<ServerAddress> { vec![ ServerAddress { addr: ADDR.to_owned(), port: PORT } ] }
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
fn default_val_limit_queue_size() -> usize { QUEUE_SIZE }
fn default_val_limit_header_timeout() -> u64 { HEADER_TIMEOUT }
fn default_val_limit_source_timeout() -> u64 { SOURCE_TIMEOUT }
fn default_val_limit_http_max_len() -> usize { HTTP_MAX_LEN }

fn default_val_adminacc_enabled() -> bool { ADMINACC_ENABLED }
fn default_val_adminacc_address() -> ServerAddress { ServerAddress { addr: ADMINACC_ADDR.to_owned(), port: ADMINACC_PORT } }

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
    pub fn create_default(config_path: &str) {
        let settings = serde_yaml::to_string(&Self::default()).expect("Can't serialize server settings");
        match std::fs::write(config_path, &settings) {
            Ok(_) => info!("Default config file written to {}", config_path),
            Err(e) => error!("Creating default config at {} failed: {}", config_path, e)
        }
    }
}
