pub mod config;
pub mod server;
pub mod client;
pub mod request;
pub mod source;
pub mod response;
pub mod stream;
pub mod admin;
pub mod api;
pub mod auth;
pub mod utils;
pub mod migrate;
pub mod relay;
pub mod http;
pub mod broadcast;

use arg::Args;

#[derive(Debug, Args)]
pub struct ArgParse {
    #[arg(short = "g", long = "gen")]
    /// Generate a config file with default values
    pub gen: bool,
    #[arg(short = "v", long = "verify")]
    /// Verify if specified config file is valid
    pub verify: bool,
    #[arg(short = "m", long = "migrate")]
    /// For migration purposes only, this command shouldn't be used by user
    pub migrate: Option<String>,
    #[arg(short = "u", long = "unsafe-password")]
    /// Allow unsafe passwords, this is highly discouraged and should only be used for testing!!
    pub unsafe_pass: bool,
    /// Configuration file path
    pub config_file: String
}
