//! pigeonhole in one process for the site's demo (wasm32-wasip1 in a Web
//! Worker, or natively): a `pigeonhole.toml` read by pigeonhole-config, and
//! the node the binary assembles from it (pigeonhole-node), without HTTP.
//! Telegram and Discord
//! stores are the real ones, on fake channels (pigeonhole-fake-channels:
//! each a tablet of its own, in memory or under a local_disk store).
//!
//! The channels and memory stores live in a [`World`] that outlives the
//! [`Node`] built over it: a new config is a restart, and the tablet replays
//! its log. The S3 calls go straight to the server's handlers
//! (`pigeonhole_gateway_s3::S3Gateway`).

use anyhow::Result;
use pigeonhole_admin::{Channels, StoreRef};
use pigeonhole_blob::{InstanceInfo, SharedBackend};
use pigeonhole_config::instances::InstanceConfig;
use pigeonhole_config::{Config, LogSettings, SnapshotSettings};
use pigeonhole_fake_channels::Fakes;
use pigeonhole_meta::{LogStore, SnapshotStore};
use pigeonhole_node::{namespaces, open_meta_stores, open_stack, open_tablet, Opener, Stack};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub mod cluster;
#[cfg(target_family = "wasm")]
mod web;

/// The demo's `pigeonhole.toml`.
pub const DEMO_CONFIG: &str = include_str!("../demo.toml");

/// Parses a `pigeonhole.toml` as the binary would.
pub fn parse_config(text: &str) -> Result<Config> {
    Config::from_toml(text, Path::new("pigeonhole.toml"))
}

/// Parses a cluster's `pigeonhole.toml` as node `node` reads it.
pub fn parse_config_as(text: &str, node: u64) -> Result<Config> {
    Config::from_toml_as(text, Path::new("pigeonhole.toml"), node)
}

/// What a server's restart keeps in this process: the fake channels (each a
/// tablet of its own, in memory or in a directory) and the memory stores.
/// The node's stores are opened on it ([`Opener`]): a real channel is refused.
#[derive(Default)]
pub struct World {
    pub fakes: Arc<pigeonhole_fake_channels::World>,
    memory: Mutex<HashMap<String, Arc<MemoryBlobStore>>>,
    /// How long a frame takes over a direct link between the nodes, each
    /// way (`[fake] link_*` at a boot, changed as they run: the `fake` op).
    pub link: pigeonhole_storage_api::fake::Latency,
}

impl World {
    fn opener(&self) -> Fakes {
        Fakes(self.fakes.clone())
    }

    /// Stops every fake channel's tablet (a process exit).
    pub async fn close(&self) -> Result<()> {
        self.fakes.close().await
    }
}

#[async_trait::async_trait]
impl Opener for World {
    async fn log(&self, cfg: &Config, place: &LogSettings) -> Result<Arc<dyn LogStore>> {
        Opener::log(&self.opener(), cfg, place).await
    }

    async fn snapshots(&self, cfg: &Config, place: &SnapshotSettings) -> Result<Arc<dyn SnapshotStore>> {
        Opener::snapshots(&self.opener(), cfg, place).await
    }

    async fn member(&self, cfg: &Config, inst: &InstanceConfig) -> Result<SharedBackend> {
        Opener::member(&self.opener(), cfg, inst).await
    }

    /// Kept for the next node on this world, as a server's restart would
    /// find a store that outlives it.
    fn memory(&self, id: &str, info: Option<&InstanceInfo>) -> Arc<MemoryBlobStore> {
        let mut m = self.memory.lock().unwrap();
        m.entry(id.to_string()).or_insert_with(|| pigeonhole_node::fresh_memory(info)).clone()
    }

    fn view(&self, stores: &[StoreRef], key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        Opener::view(&self.opener(), stores, key)
    }
}

/// A node as the server's (pigeonhole-node): the admin ops over it
/// (`Deref`: `node.put(…)`, `node.verify(…)`, `node.admin`), and the
/// background loops: repair, sweep, cool.
pub struct Node {
    stack: Stack,
}

impl std::ops::Deref for Node {
    type Target = Stack;
    fn deref(&self) -> &Stack {
        &self.stack
    }
}

impl Node {
    /// Boots over `world`: claims the log, loads its snapshot, replays the rest.
    pub async fn open(world: &Arc<World>, cfg: &Config) -> Result<Self> {
        Self::boot(world, cfg, None).await
    }

    /// Boots node `[cluster] node` of a cluster whose other nodes boot in
    /// this process too, over `net` (all at once: a tablet waits for a leader).
    pub async fn open_in(world: &Arc<World>, cfg: &Config, net: &cluster::Net) -> Result<Self> {
        Self::boot(world, cfg, Some(net)).await
    }

    async fn boot(world: &Arc<World>, cfg: &Config, net: Option<&cluster::Net>) -> Result<Self> {
        let stores = open_meta_stores(cfg, world.as_ref()).await?;
        let cluster = match (&cfg.cluster, net) {
            (Some(c), Some(net)) => Some(net.cluster(world, cfg, c).await?),
            _ => None,
        };
        let meta = open_tablet(cfg, &stores, namespaces(), cluster).await?;
        let mut stack = open_stack(cfg, world.as_ref(), meta, stores.log, &[]).await?;
        stack.start();
        Ok(Self { stack })
    }
}
