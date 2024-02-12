use castmedia::{
    ArgParse,
    config, server
};

use arg::{Args, ParseError, ParseKind};

use tracing::{error, warn};

async fn parse_args() -> ArgParse {
    let args                = Vec::from_iter(std::env::args());
    let mut args: Vec<&str> = args.iter().map(AsRef::as_ref).collect::<Vec<_>>();
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

    args
}

#[tokio::main]
async fn main() {
    let args = parse_args().await;

    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();

    if std::env::args()
        .next()
        .and_then(|path| if path.starts_with('/') { None } else { Some(()) })
        .is_some() {
        warn!("Executed with a relative path, restarting may fail if we can't resolve path");
    }

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
    {
        let e = config::ServerSettings::verify(&config, args.unsafe_pass);
        if e > 0 {
            error!("{} errors found in configuration, exiting...", e);
            std::process::exit(1);
        }
    }
    config::ServerSettings::hash_passwords(&mut config);

    server::listener(config, args, migrate).await;
}
