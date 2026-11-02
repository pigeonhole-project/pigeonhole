//! pigeonhole in one process for the site's demo (wasm32-wasip1 in a Web
//! Worker, or natively): a `pigeonhole.toml` read by pigeonhole-config, and
//! the stack the binary builds from it, without HTTP. Telegram and Discord
//! stores are the real ones, on fake channels (pigeonhole-fake-channels:
//! each a tablet of its own, in memory or under a local_disk store).
//!
//! The channels and memory stores live in a [`World`] that outlives the
//! [`Node`] built over it: a new config is a restart, and the tablet replays
//! its log. The S3 calls go straight to [`S3Gateway`], the server's handlers.

use anyhow::{bail, Context, Result};
use pigeonhole_admin::{stores, Admin, DiskView, StoreRef, Views};
use pigeonhole_blob::{erase, CheapestFirst, InstanceInfo, MetricsBackend, Replicated, SharedBackend};
use pigeonhole_chunk_store::{BlobDb, BlockCache, ChunkStore, IngestOptions};
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_config::instances::{check_fingerprints, InstanceConfig, InstanceKind};
use pigeonhole_config::{Config, DynConfig, LogSettings, SnapshotSettings};
use pigeonhole_fake_channels::world::meta_info;
use pigeonhole_fake_channels::FakeView;
use pigeonhole_gateway_s3::{Index, S3Gateway};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, LogStore, MetaOptions, MetaStore, Namespaces, SnapshotStore};
use pigeonhole_storage_disk::DiskId;
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

/// What a server's restart keeps in this process: the fake channels (each a
/// tablet of its own, in memory or in a directory) and the memory stores.
#[derive(Default)]
pub struct World {
    pub fakes: Arc<pigeonhole_fake_channels::World>,
    memory: Mutex<HashMap<String, Arc<MemoryBlobStore>>>,
}

impl World {
    fn memory(&self, id: &str, info: Option<&InstanceInfo>) -> Arc<MemoryBlobStore> {
        let mut m = self.memory.lock().unwrap();
        m.entry(id.to_string())
            .or_insert_with(|| {
                let store = MemoryBlobStore::new();
                Arc::new(match info {
                    Some(info) => store.with_instance_info(info.clone()),
                    None => store,
                })
            })
            .clone()
    }

    /// Stops every fake channel's tablet (a process exit).
    pub async fn close(&self) -> Result<()> {
        self.fakes.close().await
    }
}

pub struct Node {
    /// The admin ops over this node (`Deref`: `node.put(…)`, `node.verify(…)`).
    pub admin: Admin,
    /// The stores the config uses, data, log and snapshots.
    pub stores: Vec<StoreRef>,
    /// The background loops, as the server's: repair, sweep, cool.
    loops: Vec<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for Node {
    type Target = Admin;
    fn deref(&self) -> &Admin {
        &self.admin
    }
}

/// The cooler's loop, as the server's (`[inline] cool_every`).
async fn cooler(
    mut schedule: pigeonhole_types::schedule::Schedule<pigeonhole_config::Cooling>,
    s3: S3Gateway,
    meta: Arc<MetaStore>,
    inline_on: bool,
) {
    loop {
        let cooling = schedule.next(|c| c.every).await;
        if meta.is_stopped() {
            return;
        }
        if !meta.is_leader() {
            schedule.done();
            continue;
        }
        match s3.cool_pass(&cooling.policy.with_inlining(inline_on)).await {
            Ok(s) => tracing::info!(by_age = s.by_age, by_budget = s.by_budget, "cooled inline objects"),
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "cooler pass failed"),
        }
        schedule.done();
    }
}

fn no_real_channels() -> anyhow::Error {
    anyhow::anyhow!("real Telegram and Discord channels are not reachable from here: add `fake = \"memory\"`")
}

async fn open_log(world: &World, cfg: &Config) -> Result<Arc<dyn LogStore>> {
    Ok(match &cfg.meta.log {
        LogSettings::Memory => Arc::new(ChainLogStore::new(world.memory("meta-log", None))),
        LogSettings::Fake(f) if f.kind == InstanceKind::Telegram => {
            let info = meta_info(f);
            Arc::new(ChainLogStore::new(Arc::new(world.fakes.telegram_store(cfg, f, info).await?)))
        }
        LogSettings::Fake(f) if f.kind == InstanceKind::Discord => {
            let info = meta_info(f);
            let log = ChainLogStore::new(Arc::new(world.fakes.discord_store(cfg, f, None, info).await?));
            Arc::new(match cfg.meta.lease {
                Some(lease) => log.with_lease(pigeonhole_meta::Lease::new(lease)),
                None => log,
            })
        }
        LogSettings::Fake(_) => bail!("[meta] log: a fake channel is telegram or discord"),
        LogSettings::LocalDisk { path } => Arc::new(ChainLogStore::new(Arc::new(
            pigeonhole_storage_disk::DiskBlobStore::open(path.join("log"))
                .with_context(|| format!("open local log {}", path.display()))?,
        ))),
        LogSettings::Telegram { .. } | LogSettings::Discord { .. } => return Err(no_real_channels()),
    })
}

async fn open_snapshots(world: &World, cfg: &Config) -> Result<Arc<dyn SnapshotStore>> {
    Ok(match &cfg.meta.snapshot {
        SnapshotSettings::Memory => Arc::new(BlobSnapshotStore::new(world.memory("meta-snap", None))),
        SnapshotSettings::Fake(f) if f.kind == InstanceKind::Telegram => {
            let info = meta_info(f);
            Arc::new(BlobSnapshotStore::new(Arc::new(world.fakes.telegram_store(cfg, f, info).await?)))
        }
        SnapshotSettings::Fake(f) if f.kind == InstanceKind::Discord => {
            let info = meta_info(f);
            Arc::new(BlobSnapshotStore::new(Arc::new(world.fakes.discord_store(cfg, f, None, info).await?)))
        }
        SnapshotSettings::Fake(_) => bail!("[meta] snapshot: a fake channel is telegram or discord"),
        SnapshotSettings::LocalDisk { path } => Arc::new(BlobSnapshotStore::new(Arc::new(
            pigeonhole_storage_disk::DiskBlobStore::open(path.join("snapshots"))
                .with_context(|| format!("open local snapshots {}", path.display()))?,
        ))),
        SnapshotSettings::Telegram { .. } | SnapshotSettings::Discord { .. } => return Err(no_real_channels()),
    })
}

/// One blob store of the placement, as the binary's `build_member`.
async fn member(world: &World, cfg: &Config, inst: &InstanceConfig) -> Result<SharedBackend> {
    let id = &inst.info.id;
    let store: SharedBackend = match (inst.kind, &inst.fake) {
        (InstanceKind::Memory, _) => Arc::new(erase(world.memory(id, Some(&inst.info)))),
        (InstanceKind::Telegram, Some(f)) => {
            Arc::new(erase(Arc::new(world.fakes.telegram_store(cfg, f, inst.info.clone()).await?)))
        }
        (InstanceKind::Discord, Some(f)) => {
            Arc::new(erase(Arc::new(world.fakes.discord_store(cfg, f, inst.max_blob_size, inst.info.clone()).await?)))
        }
        (InstanceKind::LocalDisk, _) => Arc::new(erase(Arc::new(
            pigeonhole_storage_disk::DiskBlobStore::with_instance(&inst.scope_id, inst.info.clone())
                .with_context(|| format!("open local_disk store {}", inst.scope_id))?,
        ))),
        (InstanceKind::Telegram | InstanceKind::Discord, None) => return Err(no_real_channels()),
    };
    Ok(MetricsBackend::wrap(store))
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
        let namespaces = DynConfig::register(Index::register(BlobDb::register(Namespaces::new())));
        let opts = MetaOptions {
            snapshot_every: cfg.meta.snapshot_every_entries,
            snapshot_interval: cfg.meta.snapshot_interval,
            snapshots_outlive_log: cfg.meta.snapshots_outlive_log(),
            keep_snapshots: cfg.meta.keep_snapshots,
            ..MetaOptions::default()
        };
        let log = open_log(world, cfg).await?;
        let snapshots = open_snapshots(world, cfg).await?;
        let meta = match (&cfg.cluster, net) {
            (Some(c), Some(net)) => {
                let opts = MetaOptions { heartbeat: c.heartbeat, election_timeout: c.election_timeout, ..opts };
                MetaStore::open_in(log.clone(), snapshots, namespaces, opts, net.cluster(world, cfg, c).await?).await
            }
            (Some(_), None) => bail!("[cluster]: a node of a cluster boots with the others (Node::open_in)"),
            (None, _) => MetaStore::open(log.clone(), snapshots, namespaces, opts).await,
        }
        .context("boot metadata tablet")?;
        let db = BlobDb::open(meta.clone()).await.context("open chunk metadata")?;
        let stored = db.list_instance_fingerprints().await.context("list instance fingerprints")?;
        check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;

        let mut shards = Vec::new();
        for shard in &cfg.placement.shards {
            let mut members = Vec::new();
            for id in &shard.group {
                let inst = cfg.instances.iter().find(|i| i.info.id == *id).with_context(|| format!("store {id:?}"))?;
                members.push(member(world, cfg, inst).await?);
            }
            shards.push((members, shard.write_quorum));
        }
        let replicated = Replicated::sharded(shards, Arc::new(CheapestFirst::new())).context("blob placement")?;

        let mut ingest = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
        ingest.block_size = cfg.block_size;
        ingest.memory_budget = cfg.ingest_budget.clone();
        let mut store = ChunkStore::open_replicated(db, Arc::new(replicated), ingest).await?;
        if cfg.cache.enabled {
            let l1 = Arc::new(BlockCache::new("l1", cfg.cache.l1_bytes));
            let l2 = cfg.cache.l2_bytes.map(|bytes| Arc::new(BlockCache::new("l2", bytes)));
            store = store.with_caches(Some(l1), l2, cfg.cache.write_through);
        }
        let store = Arc::new(store);
        let index = Index::open(meta.clone()).await?;
        let s3 = S3Gateway::new(cfg.s3_options(), index.clone(), store.clone());
        let dynamic = Arc::new(DynConfig::open(cfg.clone(), meta.clone()));
        let inline_on = cfg.inline.max_object_size.is_some();
        let spawn = |f| tokio::spawn(tracing::Instrument::in_current_span(f));
        let repairer = Repairer::new(store.db().clone(), store.replicated().clone(), dynamic.repair.get());
        let members = store.replicated().members().to_vec();
        let pins = store.replicated().pins().clone();
        let sweeper = Sweeper::new(store.db().clone(), members, dynamic.sweep.get(), pins);
        let loops = vec![
            spawn(Box::pin(repairer.run_loop(dynamic.repair.schedule()))
                as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>),
            spawn(Box::pin(sweeper.run_loop(dynamic.sweep.schedule()))),
            spawn(Box::pin(cooler(dynamic.cool.schedule(), s3.clone(), meta.clone(), inline_on))),
            spawn(Box::pin(dynamic.clone().follow())),
            // In a cluster the chunk stores are the leader's: taken over when it leads.
            spawn(Box::pin(store.clone().follow_leadership())),
        ];
        let stores = stores::of(cfg);
        let view: Arc<dyn pigeonhole_admin::Channels> = Arc::new(Views {
            order: stores.clone(),
            views: vec![
                Arc::new(FakeView::new(world.fakes.clone(), &stores)),
                Arc::new(DiskView::<DiskId>::new(&stores, pigeonhole_storage_disk::inspect_dir)),
            ],
        });
        let admin = Admin {
            meta,
            log,
            store,
            s3,
            index,
            dynamic,
            inline_on,
            snapshot_every: cfg.meta.snapshot_every_entries,
            channels: Some(view),
            config_text: None,
            cluster: cfg.cluster.clone(),
        };
        Ok(Self { admin, stores, loops })
    }

    /// Stops the tablet, as a process exit would.
    pub async fn close(&self) -> Result<()> {
        for task in &self.loops {
            task.abort();
        }
        self.admin.meta.close().await
    }
}
