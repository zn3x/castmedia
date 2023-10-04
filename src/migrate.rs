use std::{sync::{Arc, RwLock}, time::Duration};
use hashbrown::HashMap;
use llq::broadcast::Sender;
use tokio::{sync::mpsc, net::UnixListener, io::AsyncWriteExt};
use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::{
    source::IcyProperties_v0_1_0, client::{ClientProperties_v0_1_0, Client},
    server::{Socket, Server}
};

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateSource {
    pub mountpoint: String,
    #[obake(inherit)]
    pub properties: IcyProperties,
    /// Contains (number of frames in channel, channel size, last index)
    pub broadcast_snapshot: (u64, u64, u64),
    pub fallback: Option<String>,
    pub chunked: bool,
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub struct MigrateClient {
    #[obake(inherit)]
    pub properties: ClientProperties,
}

#[obake::versioned]
#[obake(version("0.1.0"))]
#[obake(derive(Serialize, Deserialize))]
#[derive(Serialize, Deserialize)]
pub enum MigrateConnection {
    // Can't have unnamed fields here for obake to work correctly
    Source { info: MigrateSource },
    Client { info: MigrateClient }
}

pub struct MigrateInfo {
    /// Serialized MigrateConnection
    pub info: Vec<u8>,
    pub mountpoint: String,
    pub sock: Box<dyn Socket>
}

pub struct MigrateCommand {
    /// mpsc channel where we will send connection info related to sources and listeners
    pub source: mpsc::UnboundedSender<MigrateInfo>,
    pub listener: mpsc::UnboundedSender<MigrateInfo>
}

pub async fn handle(server: Arc<Server>, mut migrate: Sender<Arc<MigrateCommand>>) {
    // TODO: Can we remove all expect calls??

    // Spawning another task that will handle migration
    tokio::spawn(async move {
        let migrate_file = format!("/tmp/{}.migrate", std::process::id());
        let migrate_sock = UnixListener::bind(&migrate_file)
            .expect("We should be able to create a unix socket");

        let (tx, mut rx)   = mpsc::unbounded_channel();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        migrate.send(Arc::new(MigrateCommand {
            source: tx,
            listener: tx1
        }));

        // Our job pretty much done
        // Our child will take care of the rest :')
        // It was a good life afterall
        let (mut successor, _) = migrate_sock.accept().await
            .expect("Can't accept connection from spawned child");


        // The migration is done in the following way because we don't have any efficied way to
        // broadcast migration to all tasks, so we end not knewing when all senders are dropped
        // because a Sender is kept in broadcast channel.
        let mut source_count = 0;
        // We first read all sources
        loop {
            loop {
                match rx.try_recv() {
                    Ok(v) => {
                        source_count += 1;
                    },
                    _ => break
                }
            };

            if server.sources.read().await.len() == source_count {
                // We got all sources and now exiting
                break;
            }
            tokio::time::sleep(Duration::from_millis(1));
        }

        // Then we go to clients
        // To make sure we read all sources
        // We iterate over each source and make sure no client is active
        loop {
            loop {
                match rx1.try_recv() {
                    Ok(v) => {},
                    _ => break
                }
            };

            //if mount.is_empty() {
            //    break;
            //}
        }

        successor.write_u64(0).await.ok();
        // Next to you son :'') ...
        std::process::exit(0);
    });
}
