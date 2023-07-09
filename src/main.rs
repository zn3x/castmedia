mod config;
mod server;
mod connection;
mod request;
mod source;
mod response;
mod stream;
mod utils;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_thread_names(true).with_max_level(tracing::Level::DEBUG).init();

    let config = config::ServerSettings::load("config.yaml");

    server::listener(config).await;
}
