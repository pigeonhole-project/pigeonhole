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
use bytes::{Bytes, BytesMut};
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole_blob::{erase, CheapestFirst, InstanceInfo, MetricsBackend, Replicated, SharedBackend};
use pigeonhole_chunk_store::{BlobDb, BlockCache, ChunkStore, IngestOptions};
use pigeonhole_chunk_store::{RepairStats, Repairer, SweepStats, Sweeper};
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
use s3s::dto::{CopyObjectInput, CreateBucketInput, DeleteObjectInput, GetObjectInput, ListObjectsV2Input};
use s3s::dto::{PutObjectInput, StreamingBlob};
use s3s::{S3Request, S3};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub mod channels;
mod log_view;
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
    /// The fake channels the config uses, data, log and snapshots.
    pub channels: Vec<channels::ChannelRef>,
    /// The store the node's metadata log lives in (read by the inspector).
    pub log: Arc<dyn LogStore>,
    pub meta: Arc<MetaStore>,
    pub store: Arc<ChunkStore>,
    pub s3: S3Gateway,
    /// The background loops' knobs, the file's with the tablet's overrides.
    pub dynamic: Arc<DynConfig>,
    /// Repair passes in the background, as the server's (`[repair]`).
    repair: tokio::task::JoinHandle<()>,
    snapshot_every: Option<u64>,
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
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
    pub async fn open(world: &World, cfg: &Config) -> Result<Self> {
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
        let s3 = S3Gateway::new(cfg.s3_options(), Index::open(meta.clone()).await?, store.clone());
        let dynamic = Arc::new(DynConfig::open(cfg.clone(), meta.clone()));
        let repairer = Repairer::new(store.db().clone(), store.replicated().clone(), dynamic.repair.get());
        let repair = tokio::spawn(tracing::Instrument::in_current_span(repairer.run_loop(dynamic.repair.schedule())));
        Ok(Self {
            channels: channels::of(cfg),
            log,
            meta,
            store,
            s3,
            dynamic,
            repair,
            snapshot_every: cfg.meta.snapshot_every_entries,
        })
    }

    /// The log's entries as they lie in its store, commands decoded: the last
    /// `limit`, or `limit` from position `from`.
    pub async fn log_entries(&self, from: Option<u64>, limit: usize) -> Result<serde_json::Value> {
        log_view::read(self.log.as_ref(), from, limit).await
    }

    /// Stops the tablet, as a process exit would.
    pub async fn close(&self) -> Result<()> {
        self.repair.abort();
        self.meta.close().await
    }

    /// What runs in the background: the repair queue and when its next pass
    /// is due; how far the log is from its next snapshot.
    pub async fn background(&self) -> Result<serde_json::Value> {
        let jobs = self.store.db().repair_jobs().await;
        let every = self.dynamic.repair.get().interval;
        let due = self.dynamic.repair.due(|c| c.interval);
        let next_ms = due.map(|at| at.saturating_duration_since(tokio::time::Instant::now()).as_millis() as u64);
        let found = self.log.discover().await?;
        let (head, base) = found.map_or((0, 0), |d| (d.head.position, d.base.map_or(0, |b| b.at)));
        Ok(serde_json::json!({
            "repair": {
                "queue": jobs.iter().map(|(chunk, store)| serde_json::json!({ "chunk": chunk, "store": store })).collect::<Vec<_>>(),
                "every_ms": every.map(|d| d.as_millis() as u64),
                "next_ms": next_ms,
            },
            "snapshot": { "head": head, "base": base, "every": self.snapshot_every },
        }))
    }

    /// Entries the boot replayed from the log after its snapshot.
    pub fn replayed(&self) -> u64 {
        self.meta.applied_position().saturating_sub(self.meta.base_position())
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.s3.create_bucket(req(CreateBucketInput { bucket: bucket.into(), ..Default::default() })).await?;
        Ok(())
    }

    pub async fn put(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        let input = PutObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(body)),
            ..Default::default()
        };
        self.s3.put_object(req(input)).await?;
        Ok(())
    }

    /// The object, or the bytes `first..=last` of it.
    pub async fn get(&self, bucket: &str, key: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let input = GetObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            range: range.map(|(first, last)| s3s::dto::Range::Int { first, last: Some(last) }),
            ..Default::default()
        };
        let out = self.s3.get_object(req(input)).await?;
        let mut body = out.output.body.context("no body")?;
        let mut buf = BytesMut::new();
        while let Some(chunk) = futures_next(&mut body).await {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    }

    pub async fn copy(&self, bucket: &str, from: &str, to: &str) -> Result<()> {
        let input = CopyObjectInput::builder()
            .bucket(bucket.into())
            .key(to.into())
            .copy_source(s3s::dto::CopySource::Bucket { bucket: bucket.into(), key: from.into(), version_id: None })
            .build()?;
        self.s3.copy_object(req(input)).await?;
        Ok(())
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let input = DeleteObjectInput { bucket: bucket.into(), key: key.into(), ..Default::default() };
        self.s3.delete_object(req(input)).await?;
        Ok(())
    }

    /// The keys in `bucket` with their sizes.
    pub async fn list(&self, bucket: &str) -> Result<Vec<(String, i64)>> {
        let input = ListObjectsV2Input { bucket: bucket.into(), ..Default::default() };
        let out = self.s3.list_objects_v2(req(input)).await?;
        Ok(out
            .output
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|o| (o.key.unwrap_or_default(), o.size.unwrap_or(0)))
            .collect())
    }

    /// One repair pass: copy chunks that lost a replica back to full.
    pub async fn repair(&self) -> Result<RepairStats> {
        let repairer =
            Repairer::new(self.store.db().clone(), self.store.replicated().clone(), self.dynamic.repair.get());
        repairer.repair_once().await
    }

    /// Repair jobs waiting.
    pub async fn repair_queue(&self) -> Result<u64> {
        self.store.db().repair_queue_len().await
    }

    /// Probe every copy store `id` should hold; queue a repair for each one
    /// gone. How many it checked, and how many were gone.
    pub async fn verify(&self, id: &str) -> Result<(u64, u64)> {
        let repairer =
            Repairer::new(self.store.db().clone(), self.store.replicated().clone(), self.dynamic.repair.get());
        repairer.verify_instance(id).await
    }

    /// One sweeper pass: delete posts that no chunk points to.
    pub async fn sweep(&self) -> Result<SweepStats> {
        let replicated = self.store.replicated();
        let sweeper = Sweeper::new(
            self.store.db().clone(),
            replicated.members().to_vec(),
            self.dynamic.sweep.get(),
            replicated.pins().clone(),
        );
        sweeper.sweep_once().await
    }
}

async fn futures_next(body: &mut StreamingBlob) -> Option<Result<Bytes>> {
    use futures::StreamExt;
    body.next().await.map(|r| r.map_err(|e| anyhow::anyhow!("{e}")))
}
