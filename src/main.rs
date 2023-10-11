mod config;
mod server;
mod client;
mod request;
mod source;
mod response;
mod stream;
mod admin;
mod api;
mod auth;
mod utils;
mod migrate;

use arg::{Args, ParseError, ParseKind};

#[derive(Debug, Args)]
struct ArgParse {
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

#[tokio::main]
async fn main() {
    let args     = Vec::from_iter(std::env::args());
    let mut args = args.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    // Remove executable
    args.remove(0);
    let args = match ArgParse::from_args(args) {
        Ok(v) => v,
        Err(e) => {
            if let ParseKind::Top(ParseError::HelpRequested(help)) = e {
                eprintln!("{}", help);
                std::process::exit(1);
            }
            eprintln!("Error parsing cmd line args: {}", e);
            std::process::exit(1);
        }
    };

    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();

    if args.gen {
        config::ServerSettings::create_default(&args.config_file);
        std::process::exit(0);
    }
    if args.verify {
        let config = config::ServerSettings::load(&args.config_file);
        config::ServerSettings::verify(&config, args.unsafe_pass);
        std::process::exit(0);
    }
    let migrate = args.migrate.clone();
    
    let mut config = config::ServerSettings::load(&args.config_file);
    config::ServerSettings::verify(&config, args.unsafe_pass);
    config::ServerSettings::hash_passwords(&mut config);
    drop(args);
    server::listener(config, migrate).await;
}
