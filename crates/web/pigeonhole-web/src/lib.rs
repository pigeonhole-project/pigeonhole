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
use pigeonhole_admin::Admin;
use pigeonhole_blob::{erase, CheapestFirst, InstanceInfo, MetricsBackend, Replicated, SharedBackend};
use pigeonhole_chunk_store::{BlobDb, BlockCache, ChunkStore, IngestOptions};
use pigeonhole_chunk_store::{Repairer, Sweeper};
use pigeonhole_config::instances::{check_fingerprints, InstanceConfig, InstanceKind};
use pigeonhole_config::stores::{FakeBacking, FakeChannel};
use pigeonhole_config::{Config, DynConfig, LogSettings, SnapshotSettings};
use pigeonhole_fake_channels::{self as fake, DiscordChannel, TelegramChannel};
use pigeonhole_gateway_s3::{Index, S3Gateway};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, LogStore, MetaOptions, MetaStore, Namespaces, SnapshotStore};
use pigeonhole_storage_discord::fake::bot::FakeBot as DiscordBot;
use pigeonhole_storage_discord::pool::PooledDiscord;
use pigeonhole_storage_discord::DiscordBlobStore;
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_storage_telegram::fake::bot::FakeBot as TelegramBot;
use pigeonhole_storage_telegram::pool::PooledChannel;
use pigeonhole_storage_telegram::TelegramBlobStore;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub mod channels;
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
    telegram: Mutex<HashMap<(i64, String), Arc<TelegramChannel>>>,
    discord: Mutex<HashMap<(u64, String), Arc<DiscordChannel>>>,
    memory: Mutex<HashMap<String, Arc<MemoryBlobStore>>>,
    /// Channels taken down (`kind:scope`): every bot's call fails.
    outages: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

/// The stores a fake channel's tablet keeps itself in.
fn channel_stores(backing: &FakeBacking) -> Result<fake::Stores> {
    match backing {
        FakeBacking::Memory => Ok(fake::Stores::memory()),
        FakeBacking::Dir(dir) => fake::Stores::in_dir(dir),
        FakeBacking::Store(id) => bail!("fake = {id:?} was not resolved"),
    }
}

impl World {
    async fn telegram(&self, chat: &str, backing: &FakeBacking) -> Result<Arc<TelegramChannel>> {
        let key = (chat.parse().with_context(|| format!("chat id {chat:?}"))?, format!("{backing:?}"));
        if let Some(c) = self.telegram.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let channel = Arc::new(fake::telegram(key.0, channel_stores(backing)?).await?);
        self.telegram.lock().unwrap().insert(key, channel.clone());
        Ok(channel)
    }

    async fn discord(&self, channel: &str, backing: &FakeBacking) -> Result<Arc<DiscordChannel>> {
        let key = (channel.parse().with_context(|| format!("channel id {channel:?}"))?, format!("{backing:?}"));
        if let Some(c) = self.discord.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let ch = Arc::new(fake::discord(key.0, channel_stores(backing)?).await?);
        self.discord.lock().unwrap().insert(key, ch.clone());
        Ok(ch)
    }

    /// The down switch of fake channel `scope`, shared by its bots.
    fn outage(&self, kind: InstanceKind, scope: &str) -> Arc<AtomicBool> {
        let key = format!("{}:{scope}", kind.as_str());
        self.outages.lock().unwrap().entry(key).or_default().clone()
    }

    /// Takes channel `c` down (every call fails) or brings it back.
    pub fn set_down(&self, c: &channels::ChannelRef, down: bool) {
        self.outage(c.kind, &c.scope).store(down, Ordering::SeqCst);
    }

    pub fn is_down(&self, c: &channels::ChannelRef) -> bool {
        self.outage(c.kind, &c.scope).load(Ordering::SeqCst)
    }

    /// Channel `c` loses message `id` (someone deleted it in the app).
    pub async fn delete_message(&self, c: &channels::ChannelRef, id: &str) -> Result<()> {
        match c.kind {
            InstanceKind::Telegram => {
                use pigeonhole_storage_telegram::ChannelApi;
                let id = id.parse().with_context(|| format!("message id {id:?}"))?;
                self.telegram(&c.scope, &c.backing).await?.delete_messages(&[id]).await
            }
            InstanceKind::Discord => {
                use pigeonhole_storage_discord::DiscordApi;
                let id = id.parse().with_context(|| format!("message id {id:?}"))?;
                self.discord(&c.scope, &c.backing).await?.delete_messages(&[id]).await
            }
            _ => bail!("{}: not a fake channel", c.store),
        }
    }

    /// Channel `c` loses every message (an admin cleared it); how many.
    pub async fn wipe(&self, c: &channels::ChannelRef) -> Result<usize> {
        use pigeonhole_storage_api::fake::FakeHost;
        match c.kind {
            InstanceKind::Telegram => {
                use pigeonhole_storage_telegram::ChannelApi;
                let ch = self.telegram(&c.scope, &c.backing).await?;
                let ids = ch.host().read(|s: &pigeonhole_storage_telegram::fake::state::TelegramChannelState| s.ids());
                for part in ids.chunks(100) {
                    ch.delete_messages(part).await?;
                }
                Ok(ids.len())
            }
            InstanceKind::Discord => {
                use pigeonhole_storage_discord::DiscordApi;
                let ch = self.discord(&c.scope, &c.backing).await?;
                let ids = ch.host().read(|s: &pigeonhole_storage_discord::fake::state::DiscordChannelState| s.ids());
                for part in ids.chunks(100) {
                    ch.delete_messages(part).await?;
                }
                Ok(ids.len())
            }
            _ => bail!("{}: not a fake channel", c.store),
        }
    }

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

    /// A Telegram store on fake channel `f`, through its bots: each with
    /// its own budget (`[telegram.send]` is per bot and chat), as a server's.
    async fn telegram_store(
        &self,
        cfg: &Config,
        f: &FakeChannel,
        info: InstanceInfo,
    ) -> Result<TelegramBlobStore<PooledChannel<TelegramBot<TelegramChannel>>>> {
        let channel = self.telegram(&f.scope, &f.backing).await?;
        let bots = (0..f.bots)
            .map(|i| {
                let limiter = cfg.chat_limiter(InstanceKind::Telegram, &fake_bot(i), &f.scope);
                (
                    TelegramBot::new(channel.clone(), limiter.clone(), i).with_switch(self.outage(f.kind, &f.scope)),
                    limiter,
                )
            })
            .collect();
        let pooled = PooledChannel::new(&f.scope, bots);
        let limiter = pooled.limiter().clone();
        Ok(TelegramBlobStore::new(Arc::new(pooled), limiter, info))
    }

    /// A Discord store on fake channel `f`, through its bots.
    async fn discord_store(
        &self,
        cfg: &Config,
        f: &FakeChannel,
        max_blob_size: Option<usize>,
        info: InstanceInfo,
    ) -> Result<DiscordBlobStore<PooledDiscord<DiscordBot<DiscordChannel>>>> {
        let channel = self.discord(&f.scope, &f.backing).await?;
        let bots = (0..f.bots)
            .map(|i| {
                let limiter = cfg.chat_limiter(InstanceKind::Discord, &fake_bot(i), &f.scope);
                (
                    DiscordBot::new(channel.clone(), limiter.clone(), i).with_switch(self.outage(f.kind, &f.scope)),
                    limiter,
                )
            })
            .collect();
        let pooled = PooledDiscord::new(&f.scope, bots);
        let limiter = pooled.limiter().clone();
        Ok(DiscordBlobStore::new(Arc::new(pooled), limiter, max_blob_size, info))
    }
}

pub struct Node {
    /// The admin ops over this node (`Deref`: `node.put(…)`, `node.verify(…)`).
    pub admin: Admin,
    /// The fake channels the config uses, data, log and snapshots.
    pub channels: Vec<channels::ChannelRef>,
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

/// The store of the log or the snapshots in fake channel `f`, by its id.
fn meta_info(f: &FakeChannel) -> InstanceInfo {
    let location = format!("fake:{}:{}", f.kind.as_str(), f.scope);
    InstanceInfo {
        id: f.id.clone(),
        kind: f.kind.as_str(),
        fingerprint: location.clone(),
        location,
        role: pigeonhole_blob::InstanceRole::ReadWrite,
    }
}

/// Fake bot `i`'s name: its budget is shared by every channel it posts to.
fn fake_bot(i: usize) -> String {
    format!("fake-bot-{}", i + 1)
}

fn no_real_channels() -> anyhow::Error {
    anyhow::anyhow!("real Telegram and Discord channels are not reachable from here: add `fake = \"memory\"`")
}

async fn open_log(world: &World, cfg: &Config) -> Result<Arc<dyn LogStore>> {
    Ok(match &cfg.meta.log {
        LogSettings::Memory => Arc::new(ChainLogStore::new(world.memory("meta-log", None))),
        LogSettings::Fake(f) if f.kind == InstanceKind::Telegram => {
            let info = meta_info(f);
            Arc::new(ChainLogStore::new(Arc::new(world.telegram_store(cfg, f, info).await?)))
        }
        LogSettings::Fake(f) if f.kind == InstanceKind::Discord => {
            let info = meta_info(f);
            let log = ChainLogStore::new(Arc::new(world.discord_store(cfg, f, None, info).await?));
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
        SnapshotSettings::Memory => Arc::new(BlobSnapshotStore::new(world.memory("meta-snapshots", None))),
        SnapshotSettings::Fake(f) if f.kind == InstanceKind::Telegram => {
            let info = meta_info(f);
            Arc::new(BlobSnapshotStore::new(Arc::new(world.telegram_store(cfg, f, info).await?)))
        }
        SnapshotSettings::Fake(f) if f.kind == InstanceKind::Discord => {
            let info = meta_info(f);
            Arc::new(BlobSnapshotStore::new(Arc::new(world.discord_store(cfg, f, None, info).await?)))
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
            Arc::new(erase(Arc::new(world.telegram_store(cfg, f, inst.info.clone()).await?)))
        }
        (InstanceKind::Discord, Some(f)) => {
            Arc::new(erase(Arc::new(world.discord_store(cfg, f, inst.max_blob_size, inst.info.clone()).await?)))
        }
        (InstanceKind::LocalDisk, _) => Arc::new(erase(Arc::new(
            pigeonhole_storage_disk::DiskBlobStore::with_instance(&inst.scope_id, inst.info.clone())
                .with_context(|| format!("open local_disk store {}", inst.scope_id))?,
        ))),
        (InstanceKind::Telegram | InstanceKind::Discord, None) => return Err(no_real_channels()),
    };
    Ok(MetricsBackend::wrap(store))
}

/// Stops every fake channel's tablet (a process exit).
impl World {
    pub async fn close(&self) -> Result<()> {
        let tg: Vec<_> = self.telegram.lock().unwrap().drain().map(|(_, c)| c).collect();
        let dc: Vec<_> = self.discord.lock().unwrap().drain().map(|(_, c)| c).collect();
        for c in tg {
            c.host().close().await?;
        }
        for c in dc {
            c.host().close().await?;
        }
        Ok(())
    }
}

impl Node {
    /// Boots over `world`: claims the log, loads its snapshot, replays the rest.
    pub async fn open(world: &Arc<World>, cfg: &Config) -> Result<Self> {
        let namespaces = DynConfig::register(Index::register(BlobDb::register(Namespaces::new())));
        let opts = MetaOptions {
            snapshot_every: cfg.meta.snapshot_every_entries,
            snapshot_interval: cfg.meta.snapshot_interval,
            snapshots_outlive_log: cfg.meta.snapshots_outlive_log(),
            keep_snapshots: cfg.meta.keep_snapshots,
            ..MetaOptions::default()
        };
        let log = open_log(world, cfg).await?;
        let meta = MetaStore::open(log.clone(), open_snapshots(world, cfg).await?, namespaces, opts)
            .await
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
        ];
        let channels = channels::of(cfg);
        let view: Arc<dyn pigeonhole_admin::Channels> =
            Arc::new(channels::DemoChannels { world: world.clone(), channels: channels.clone() });
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
        };
        Ok(Self { admin, channels, loops })
    }

    /// Stops the tablet, as a process exit would.
    pub async fn close(&self) -> Result<()> {
        for task in &self.loops {
            task.abort();
        }
        self.admin.meta.close().await
    }
}
