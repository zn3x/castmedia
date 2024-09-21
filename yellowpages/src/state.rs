use std::sync::Arc;
use castmedia::{relay::MountUpdate, source::IcyProperties};
use hashbrown::HashMap;
use qanat::{broadcast, mpsc};
use serde::{Deserialize, Serialize};
use url::Url;


pub struct State {
    pub state: HashMap<String, StreamState>,
    pub channel: HashMap<String, Channel>,
    pub update_ch: mpsc::UnboundedReceiver<Update>,
    pub update_ch_tx: mpsc::UnboundedSender<Update>
}

pub enum Update {
    Sid { mount: String, dir: Url, sid: String, touch_freq: u64 }
}

pub struct Channel {
    pub tx: broadcast::Sender<Arc<MountUpdate>>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamState {
    pub properties: IcyProperties,
    pub dirs: HashMap<Url, DirProperties>
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DirProperties {
    pub sid: String,
    pub touch_freq: u64
}
