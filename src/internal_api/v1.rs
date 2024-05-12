use serde::{Serialize, Deserialize};

pub const INTERNAL_API_VERSION: u64 = 1;

#[derive(Serialize, Deserialize)]
pub struct MigrateSource {
    pub mountpoint: String,
    pub properties: IcyProperties,
    /// Contains (number of frames in channel, channel size, last index)
    pub broadcast_snapshot: (u64, u64, u64),
    pub fallback: Option<String>,
    pub metadata: Vec<u8>,
    pub chunked: bool,
    pub queue_size: u64,
    pub is_relay: MigrateSourceConnectionType,
    pub client_addr: String
}

#[derive(Serialize, Deserialize)]
pub enum MigrateSourceConnectionType {
    SourceClient {
        username: String
    },
    RelayedSource {
        relayed_stream: String,
        relay_info: RelayedInfo,
        on_demand: bool
    },
}

#[derive(Serialize, Deserialize)]
pub struct MigrateInactiveOnDemandSource {
    pub mountpoint: String,
    pub properties: IcyProperties,
    pub master_url: String
}

#[derive(Serialize, Deserialize)]
pub struct MigrateClient {
    pub mountpoint: String,
    pub properties: ClientProperties,
    pub resume_point: u64,
    pub metaint: u64
}

#[derive(Serialize, Deserialize)]
pub struct MigrateMasterMountUpdates {
    pub mounts: Vec<String>,
    pub user_id: String,
    pub client_addr: String
}

#[derive(Serialize, Deserialize)]
pub struct MigrateSlaveMountUpdates {
    pub master_url: String
}

#[derive(Serialize, Deserialize)]
pub enum MigrateConnection {
    Source { info: MigrateSource },
    Client { info: MigrateClient },
    MasterMountUpdates { info: MigrateMasterMountUpdates },
    SlaveMountUpdates { info: MigrateSlaveMountUpdates },
    SlaveInactiveOnDemandSource { info: MigrateInactiveOnDemandSource }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IcyProperties {
    pub uagent: Option<String>,
    pub public: bool,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub genre: Option<String>,
    pub bitrate: Option<String>,
    pub content_type: String
}

impl IcyProperties {
    pub fn new(content_type: String) -> Self {
        IcyProperties {
            uagent: None,
            public: false,
            name: None,
            description: None,
            url: None,
            genre: None,
            bitrate: None,
            content_type
        }
    }

    pub fn populate_from_http_headers(&mut self, headers: &[httparse::Header<'_>]) {
        for header in headers {
            let name = header.name.to_lowercase();
            let val = match std::str::from_utf8(header.value) {
                Ok(v) => v,
                Err(_) => continue
            };

            // There's a nice list here: https://github.com/ben221199/MediaCast
            // Although, these were taken directly from Icecast's source: https://github.com/xiph/Icecast-Server/blob/master/src/source.c
            match name.as_str() {
                "user-agent" => self.uagent = Some(val.to_string()),
                "ice-public" | "icy-pub" | "x-audiocast-public" | "icy-public" => self.public = val.parse::<usize>().unwrap_or(0) == 1,
                "ice-name" | "icy-name" | "x-audiocast-name" => self.name = Some(val.to_string()),
                "ice-description" | "icy-description" | "x-audiocast-description" => self.description = Some(val.to_string()),
                "ice-url" | "icy-url" | "x-audiocast-url" => self.url = Some(val.to_string()),
                "ice-genre" | "icy-genre" | "x-audiocast-genre" => self.genre = Some(val.to_string()),
                "ice-bitrate" | "icy-br" | "x-audiocast-bitrate" => self.bitrate = Some(val.to_string()),
                _ => (),
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ClientProperties {
    pub user_agent: Option<String>,
    pub metadata: bool,
    pub addr: String
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct RelayedInfo {
    pub metaint: usize,
    pub metaint_position: usize,
    pub metadata_reading: bool,
    pub metadata_remaining: usize,
    pub metadata_buffer: Vec<u8>
}
