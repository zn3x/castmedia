use castmedia::{config, server};

use tracing::{error, info};

const HELP_MESSAGE: &str = "\
Usage: castmedia [-c] [configuration_file]\n\
\n\
Optional arguments:\n\
\t-c, --check\tOnly parse and check if configuration file is valid\n\
\n\
castmedia is a media broadcast server inspired by icecast.\n\
Any issues or bugs should be reported via https://codeberg.org/zesty/castmedia/.\n\
";

struct Args {
    config: String,
    check: bool
}

fn parse_args() -> Args {
    let mut args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprint!("{}", HELP_MESSAGE);
        std::process::exit(1);
    }

    let mut parsed_args = Args {
        config: args.pop().unwrap(),
        check: false
    };

    args.pop()
        .is_some_and(|arg| arg.eq("-c") || arg.eq("--check"))
        .then(|| parsed_args.check = true);

    parsed_args
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();

    let args       = parse_args();
    let mut config = config::ServerSettings::load(&args.config);
    {
        let e = config::ServerSettings::verify(&config, config.misc.unsafe_pass);
        if e > 0 {
            error!("{} errors found in configuration, exiting...", e);
            std::process::exit(1);
        }
    }
    if args.check {
        info!("Configuration is valid");
        std::process::exit(0);
    }
    drop(args);
    config::ServerSettings::hash_passwords(&mut config);

    server::listener(config).await;
}
