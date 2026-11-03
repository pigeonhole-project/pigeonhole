//! A pigeonhole node assembled from its [`Config`], once for every root:
//! the binary, the site's in-browser build (crates/web) and the binary's
//! fake channels build it here, and add only what they reach the
//! channels with ([`Opener`]: real Telegram and Discord, or fakes).
//!
//! 1. [`open_meta_stores`]: the metadata log and snapshots (a local disk
//!    or memory here; a channel through the [`Opener`]);
//! 2. [`open_tablet`]: the tablet on them, alone or as a node of its
//!    Raft cluster ([`cluster`] has the parts both roots' networks share);
//! 3. [`open_stack`]: the chunk store on the placement, the S3 handlers,
//!    the dynamic config, the admin; [`Stack::start`]: the background loops.
//!
//! Builds for wasm: no network client, no server, no thread.

use anyhow::{Context, Result};
use pigeonhole_admin::{Admin, Channels, StoreRef};
use pigeonhole_chunk_store::{BlobDb, BlockCache, ChunkStore, IngestOptions};
use pigeonhole_config::dynamic::Task;
use pigeonhole_config::instances::check_fingerprints;
use pigeonhole_config::{Config, DynConfig};
use pigeonhole_gateway_s3::{Index, S3Gateway};
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{Cluster, LogStore, MetaOptions, MetaStore, Namespaces};
use std::sync::Arc;

pub mod cluster;
mod loops;
pub mod state_metrics;
mod stores;

pub use stores::{chain_log, fresh_memory, leased_log, open_meta_stores, placement, snapshot_store};
pub use stores::{MetaStores, Opener};

/// The loops every node runs (the snapshot ticker is the tablet's own); a
/// root adds its own to [`open_stack`]'s `extra` ([`Task::Metrics`] runs
/// here too, the REAPI's GC in its server).
pub const LOOPS: [Task; 3] = [Task::Sweep, Task::Repair, Task::Cool];

/// The namespaces every node's tablet serves: the dynamic config's, the S3
/// index's, the chunks'. A root registers its own on top (REAPI's CAS index).
pub fn namespaces() -> Namespaces {
    DynConfig::register(Index::register(BlobDb::register(Namespaces::new())))
}

/// The tablet's knobs (`[meta]`; a cluster's node, its timing too).
pub fn meta_options(cfg: &Config) -> MetaOptions {
    let opts = MetaOptions {
        snapshot_every: cfg.meta.snapshot_every_entries,
        snapshot_interval: cfg.meta.snapshot_interval,
        snapshots_outlive_log: cfg.meta.snapshots_outlive_log(),
        keep_snapshots: cfg.meta.keep_snapshots,
        keep_log: cfg.meta.keep_log_entries,
        ..MetaOptions::default()
    };
    match &cfg.cluster {
        Some(c) => MetaOptions { heartbeat: c.heartbeat, election_timeout: c.election_timeout, ..opts },
        None => opts,
    }
}

/// Boots the tablet on `stores`: a cluster of one, or node `[cluster]
/// node` of its cluster over `cluster` (the root's network: a tablet of a
/// cluster waits for a leader, so its peers boot at once).
pub async fn open_tablet<N: openraft::RaftNetworkFactory<TypeConfig>>(
    cfg: &Config,
    stores: &MetaStores,
    namespaces: Namespaces,
    cluster: Option<Cluster<N>>,
) -> Result<Arc<MetaStore>> {
    let (log, snapshots, opts) = (stores.log.clone(), stores.snapshots.clone(), meta_options(cfg));
    match (&cfg.cluster, cluster) {
        (None, None) => MetaStore::open(log, snapshots, namespaces, opts).await,
        (Some(_), Some(cluster)) => MetaStore::open_in(log, snapshots, namespaces, opts, cluster).await,
        (Some(c), None) => anyhow::bail!("[cluster]: node {} boots with its cluster's network", c.node),
        (None, Some(_)) => anyhow::bail!("a network for a tablet without [cluster]"),
    }
    .context("boot metadata tablet")
}

/// A running node: its admin (and, through it, the tablet, the chunk store,
/// the S3 handlers, the dynamic config) and its background loops.
pub struct Stack {
    pub admin: Arc<Admin>,
    loops: Vec<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for Stack {
    type Target = Admin;
    fn deref(&self) -> &Admin {
        &self.admin
    }
}

impl Stack {
    /// Starts the background loops: those of [`LOOPS`], and the tablet's
    /// gauges if [`Task::Metrics`] is among the loops it runs (the root's
    /// others, it runs itself). In the caller's span: a demo running
    /// several nodes tells them apart.
    pub fn start(&mut self) {
        if self.loops.is_empty() {
            let metrics = self.admin.dynamic.runs().any(|t| t == Task::Metrics);
            self.loops = loops::spawn(&self.admin, metrics);
        }
    }

    /// Stops the loops and the tablet (a lease log is given back), as a
    /// process exit would.
    pub async fn close(&self) -> Result<()> {
        for task in &self.loops {
            task.abort();
        }
        self.admin.meta.close().await
    }
}

/// The node on its booted tablet `meta` (whose log is `log`): the chunk
/// store on the placement `opener` reaches, the S3 handlers, the dynamic
/// config (driving the loops of [`LOOPS`] and `extra`, the root's), the
/// admin. Its loops start with [`Stack::start`]. On an error the tablet is
/// closed: it leaves nothing running.
pub async fn open_stack(
    cfg: &Config,
    opener: &dyn Opener,
    meta: Arc<MetaStore>,
    log: Arc<dyn LogStore>,
    extra: &[Task],
) -> Result<Stack> {
    match build(cfg, opener, meta.clone(), log, extra).await {
        Ok(admin) => Ok(Stack { admin, loops: Vec::new() }),
        Err(e) => {
            if let Err(close) = meta.close().await {
                tracing::warn!(error = %format!("{close:#}"), "closing the tablet of a node that did not open");
            }
            Err(e)
        }
    }
}

async fn build(
    cfg: &Config,
    opener: &dyn Opener,
    meta: Arc<MetaStore>,
    log: Arc<dyn LogStore>,
    extra: &[Task],
) -> Result<Arc<Admin>> {
    let db = BlobDb::open(meta.clone()).await.context("open chunk metadata")?;
    let stored = db.list_instance_fingerprints().await.context("list instance fingerprints")?;
    check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;
    let replicated = placement(cfg, opener).await.context("build placement group")?;
    let mut ingest = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    ingest.block_size = cfg.block_size;
    ingest.memory_budget = cfg.ingest_budget.clone();
    let store = ChunkStore::open_replicated(db, replicated, ingest).await.context("open ChunkStore")?;
    let store = Arc::new(with_caches(store, cfg));
    let index = Index::open(meta.clone()).await.context("open s3 index")?;
    let s3 = S3Gateway::new(cfg.s3_options(), index.clone(), store.clone());
    let runs: Vec<Task> = LOOPS.iter().chain(extra).copied().collect();
    let dynamic = Arc::new(DynConfig::open(cfg.clone(), meta.clone(), &runs));
    let stores = pigeonhole_admin::stores::of(cfg);
    Ok(Arc::new(Admin {
        meta,
        log,
        store,
        s3,
        index,
        dynamic,
        inline_on: cfg.inline.max_object_size.is_some(),
        snapshot_every: cfg.meta.snapshot_every_entries,
        channels: Some(channels(cfg, opener, &stores)),
        config_text: (!cfg.text.is_empty()).then(|| cfg.text.clone()),
        cluster: cfg.cluster.clone(),
        stores,
        limiters: vec![("telegram", cfg.limiters.clone()), ("discord", cfg.discord_limiters.clone())],
    }))
}

fn with_caches(store: ChunkStore, cfg: &Config) -> ChunkStore {
    if !cfg.cache.enabled {
        return store;
    }
    let l1 = Arc::new(BlockCache::new("l1", cfg.cache.l1_bytes));
    let l2 = cfg.cache.l2_bytes.map(|bytes| Arc::new(BlockCache::new("l2", bytes)));
    store.with_caches(Some(l1), l2, cfg.cache.write_through)
}

/// The node's stores as the admin's UI sees them, each drawn by the first
/// view that holds it: the opener's (fake channels, with what they hold),
/// local disks with their files, the rest listed.
fn channels(cfg: &Config, opener: &dyn Opener, order: &[StoreRef]) -> Arc<dyn Channels> {
    use pigeonhole_admin::{DiskView, ListedStores, Views};
    use pigeonhole_storage_disk::{inspect_dir, DiskId};
    let mut views: Vec<Arc<dyn Channels>> =
        opener.view(order, cfg.cluster.as_ref().map(|c| c.key)).into_iter().collect();
    views.push(Arc::new(DiskView::<DiskId>::new(order, inspect_dir)));
    views.push(Arc::new(ListedStores(order.to_vec())));
    Arc::new(Views { order: order.to_vec(), views })
}
