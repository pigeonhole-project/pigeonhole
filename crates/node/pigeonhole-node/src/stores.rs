//! The stores a node opens: the metadata log and snapshots, and the blob
//! stores of the placement. A local disk and memory are opened here; a
//! channel (Telegram, Discord, real or fake) by the root's [`Opener`].

use anyhow::{Context, Result};
use async_trait::async_trait;
use pigeonhole_admin::{Channels, StoreRef, LOG_STORE, SNAPSHOT_STORE};
use pigeonhole_blob::{erase, CheapestFirst, InstanceInfo, MetricsBackend, Replicated, SharedBackend};
use pigeonhole_config::instances::{InstanceConfig, InstanceKind};
use pigeonhole_config::{Config, LogSettings, SnapshotSettings};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, Lease, LogStore, SnapshotStore};
use pigeonhole_storage_api::{Chain, LeaseChain};
use pigeonhole_storage_disk::DiskBlobStore;
use pigeonhole_storage_memory::MemoryBlobStore;
use std::sync::Arc;
use tracing::warn;

/// What a root reaches the channels with: the stores the node cannot open
/// itself (a channel's, real or fake), and how they are seen.
#[async_trait]
pub trait Opener: Send + Sync {
    /// `[meta] log` in a channel (`place`: telegram, discord or fake): a
    /// [`chain_log`] on it, a Discord one [`leased_log`].
    async fn log(&self, cfg: &Config, place: &LogSettings) -> Result<Arc<dyn LogStore>>;

    /// `[meta] snapshot` in a channel: a [`snapshot_store`] on it.
    async fn snapshots(&self, cfg: &Config, place: &SnapshotSettings) -> Result<Arc<dyn SnapshotStore>>;

    /// Blob store `inst` of the placement, a telegram or discord one (real
    /// or fake, `inst.fake`); the node wraps it for its metrics.
    async fn member(&self, cfg: &Config, inst: &InstanceConfig) -> Result<SharedBackend>;

    /// Memory store `id` (with `info`, a blob store's): a new one, gone
    /// with the node; a root whose process outlives its node (the site's
    /// demo, restarted on a new config) keeps it.
    fn memory(&self, id: &str, info: Option<&InstanceInfo>) -> Arc<MemoryBlobStore> {
        let _ = id;
        fresh_memory(info)
    }

    /// How the channels look to the admin's UI, where the root knows more
    /// than their list (fake channels: what they hold); `key`: a cluster's.
    fn view(&self, stores: &[StoreRef], key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        let _ = (stores, key);
        None
    }
}

/// A new memory store, `info`'s if given.
pub fn fresh_memory(info: Option<&InstanceInfo>) -> Arc<MemoryBlobStore> {
    let store = MemoryBlobStore::new();
    Arc::new(match info {
        Some(info) => store.with_instance_info(info.clone()),
        None => store,
    })
}

/// The log as a chain of blobs in `chain` (fenced by their order).
pub fn chain_log<C: Chain>(chain: Arc<C>) -> Arc<dyn LogStore> {
    Arc::new(ChainLogStore::new(chain))
}

/// [`chain_log`] held by `[meta] lease` (a Discord log: docs/discord-lease.md).
pub fn leased_log<C: LeaseChain>(chain: Arc<C>, cfg: &Config) -> Arc<dyn LogStore> {
    let log = ChainLogStore::new(chain);
    Arc::new(match cfg.meta.lease {
        Some(lease) => log.with_lease(Lease::new(lease)),
        None => log,
    })
}

/// Snapshots as blobs in `chain`.
pub fn snapshot_store<C: Chain>(chain: Arc<C>) -> Arc<dyn SnapshotStore> {
    Arc::new(BlobSnapshotStore::new(chain))
}

/// The metadata log and its snapshots.
#[derive(Clone)]
pub struct MetaStores {
    pub log: Arc<dyn LogStore>,
    pub snapshots: Arc<dyn SnapshotStore>,
}

/// The configured `[meta] log` and `snapshot`. A local_disk store holds
/// the log in `log/` and snapshots in `snapshots/` (one chain each), so one
/// store may serve both.
pub async fn open_meta_stores(cfg: &Config, opener: &dyn Opener) -> Result<MetaStores> {
    let disk = |path: &std::path::Path, sub: &str| {
        DiskBlobStore::open(path.join(sub)).with_context(|| format!("open local {sub} {}", path.display()))
    };
    let log = match &cfg.meta.log {
        LogSettings::Memory => {
            warn!("metadata log in memory: everything is gone when the process exits");
            chain_log(opener.memory(LOG_STORE, None))
        }
        LogSettings::LocalDisk { path } => chain_log(Arc::new(disk(path, "log")?)),
        place => opener.log(cfg, place).await.context("[meta] log")?,
    };
    let snapshots = match &cfg.meta.snapshot {
        SnapshotSettings::Memory => snapshot_store(opener.memory(SNAPSHOT_STORE, None)),
        SnapshotSettings::LocalDisk { path } => snapshot_store(Arc::new(disk(path, "snapshots")?)),
        place => opener.snapshots(cfg, place).await.context("[meta] snapshot")?,
    };
    Ok(MetaStores { log, snapshots })
}

/// `[chunk] store`: its shards, each a replica group of blob stores.
pub async fn placement(cfg: &Config, opener: &dyn Opener) -> Result<Arc<Replicated>> {
    let mut shards = Vec::new();
    for shard in &cfg.placement.shards {
        let mut members = Vec::new();
        for id in &shard.group {
            let inst = cfg
                .instances
                .iter()
                .find(|i| i.info.id == *id)
                .with_context(|| format!("blob store {id:?} not resolved"))?;
            members.push(member(cfg, opener, inst).await.with_context(|| format!("store {id:?}"))?);
        }
        shards.push((members, shard.write_quorum));
    }
    let replicated = Replicated::sharded(shards, Arc::new(CheapestFirst::new())).context("blob placement")?;
    Ok(Arc::new(replicated))
}

/// One blob store as a placement member, with its metrics.
async fn member(cfg: &Config, opener: &dyn Opener, inst: &InstanceConfig) -> Result<SharedBackend> {
    let store: SharedBackend = match inst.kind {
        InstanceKind::Memory => {
            warn!(store = %inst.info.id, "a memory blob store: its data does not outlive the process");
            Arc::new(erase(opener.memory(&inst.info.id, Some(&inst.info))))
        }
        InstanceKind::LocalDisk => Arc::new(erase(Arc::new(
            DiskBlobStore::with_instance(&inst.scope_id, inst.info.clone())
                .with_context(|| format!("open local_disk store {}", inst.scope_id))?,
        ))),
        InstanceKind::Telegram | InstanceKind::Discord => opener.member(cfg, inst).await?,
    };
    Ok(MetricsBackend::wrap(store))
}
