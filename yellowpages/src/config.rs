use serde::Deserialize;
use tracing::error;
use url::Url;

#[derive(Deserialize)]
pub struct Config {
    /// castmedia server with authentication
    pub server: Url,
    /// Public url where clients will be able to reach streams
    /// It is worth to note that the stream mount is appended to the end of this url
    pub public_server: Url,
    /// Timeout in millis when trying to fetch mountupdates stream from server
    pub timeout: u64,
    /// List of directories when streams will be published
    pub directories: Vec<Directory>,
    /// Path where to store state info
    pub state: String
}

#[derive(Deserialize)]
pub struct Directory {
    /// Url of YP directory
    pub yp_url: Url,
    /// Timeout in millis to send request to the YP directory
    pub timeout: u64
}

impl Config {
    pub fn load(config_path: &str) -> Self {
        match std::fs::read_to_string(config_path) {
            Ok(v) => match serde_yaml::from_str::<Self>(&v) {
                Ok(v) => v,
                Err(e) => {
                    error!("Loading config file {} failed: {}", config_path, e);
                    std::process::exit(1);
                }
            },
            Err(e) => {
                error!("Reading config file {} failed: {}", config_path, e);
                std::process::exit(1);
            }
        }
    }
}
