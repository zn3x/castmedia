use castmedia::{config, server};

use tracing::error;

fn parse_conf_path() -> String {
    let mut args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Help: {} <config path>", args[0]);
        std::process::exit(1);
    }

    args.remove(1)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();

    let mut config = config::ServerSettings::load(&parse_conf_path());
    {
        let e = config::ServerSettings::verify(&config, config.misc.unsafe_pass);
        if e > 0 {
            error!("{} errors found in configuration, exiting...", e);
            std::process::exit(1);
        }
    }
    config::ServerSettings::hash_passwords(&mut config);

    server::listener(config).await;
}
