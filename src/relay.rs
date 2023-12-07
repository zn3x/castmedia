use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::{
    net::TcpStream,
    io::{BufStream, AsyncWriteExt, AsyncReadExt}
};
use tokio_native_tls::native_tls::TlsConnector;
use tracing::info;
use url::Url;
use serde::Deserialize;

use crate::{server::{Server, Stream}, utils::get_header, config::MasterServer, http::ResponseReader};

#[derive(Debug, Deserialize)]
struct MasterMounts {
    mounts: Vec<String>
}

async fn http_get_request(url: &Url, path: &str, headers: &str) -> Result<Stream> {
    // We did already check before running
    let host = url.host()
        .expect("Should be able to fetch master host")
        .to_string();
    let port = url.port()
        .expect("Should be able to fetch master port");
    let stream     = TcpStream::connect(format!("{}:{}", host, port)).await?;
    let mut stream = if url.scheme().eq("https") {
        Box::new(BufStream::new(stream)) as Stream
    } else {
        let cx = tokio_native_tls::TlsConnector::from(TlsConnector::builder().build()?);
        Box::new(BufStream::new(cx.connect(&host, stream).await?))
    };

    stream.write_all(format!("GET {} HTTP/1.1\r\n
Host: {}:{}\r\n\
User-Agent: \r\n{}\
Connection: close\r\n\r\n",
        path,
        host, port,
        headers
    ).as_bytes()).await?;

    Ok(stream)
}

async fn fetch_available_sources(server: &Server, url: &Url) -> Result<MasterMounts> {
    let timeout = Duration::from_millis(server.config.limits.master_timeout);
    tokio::time::timeout(
        timeout,
        async {
            let mut stream = http_get_request(url, "/api/serverinfo", "").await?;

            let mut reader = ResponseReader::new(&mut stream, server.config.limits.master_http_max_len);
            let body_buf   = reader.read_to_end("application/json").await?;
            // Now we go to parsing response
            let info: MasterMounts = serde_json::from_slice(&body_buf)?;

            Ok(info)
        }
    ).await?
}

async fn relay_mountpoint(serv: Arc<Server>, master_ind: usize, mount: &str) -> Result<()> {
    let url = {
        let master_server  = &serv.config.master[master_ind];
        match master_server {
            MasterServer::Transparent { url, .. } => url
        }
    };

    let mut stream = http_get_request(url, mount, "Icy-Metadata:1\r\n").await?;
    let mut reader = ResponseReader::new(&mut stream, serv.config.limits.master_http_max_len);

    reader.read_headers().await?;

    Ok(())
}

pub async fn slave_instance(serv: Arc<Server>, master_ind: usize) {
    // Here we should be fine a we did check bounds before
    let master_server  = &serv.config.master[master_ind];

    match master_server {
        MasterServer::Transparent { url, update_interval } => {
            let mut interval = tokio::time::interval(Duration::from_millis(*update_interval));
            loop {
                interval.tick().await;

                match fetch_available_sources(&serv, url).await {
                    Ok(mounts) => {
                        let lock = serv.sources.read().await;
                        for mount in &mounts.mounts {
                            if !lock.contains_key(mount) {
                                tokio::spawn(async move {
                                });
                            }
                        }
                    },
                    Err(e) => {
                        info!("Fetching mounts from master server failed: {}", e);
                    }
                }
            }
        }
    }
}
