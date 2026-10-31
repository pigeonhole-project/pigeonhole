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
use pigeonhole_chunk_store::{RepairConfig, RepairStats, Repairer, SweepConfig, SweepStats, Sweeper};
use pigeonhole_config::instances::{check_fingerprints, InstanceConfig, InstanceKind};
use pigeonhole_config::stores::FakeBacking;
use pigeonhole_config::{Config, LogSettings, SnapshotSettings};
use pigeonhole_fake_channels::{self as fake, DiscordChannel, TelegramChannel};
use pigeonhole_gateway_s3::{Index, S3Gateway};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, LogStore, MetaOptions, MetaStore, Namespaces, SnapshotStore};
use pigeonhole_storage_discord::DiscordBlobStore;
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_storage_telegram::TelegramBlobStore;
use s3s::dto::{CopyObjectInput, CreateBucketInput, DeleteObjectInput, GetObjectInput, ListObjectsV2Input};
use s3s::dto::{PutObjectInput, StreamingBlob};
use s3s::{S3Request, S3};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

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

    /// A Telegram store on fake channel `chat`.
    async fn telegram_store(
        &self,
        cfg: &Config,
        chat: &str,
        backing: &FakeBacking,
        info: InstanceInfo,
    ) -> Result<TelegramBlobStore<Arc<TelegramChannel>>> {
        let limiter = cfg.chat_limiter(InstanceKind::Telegram, "fake", chat);
        Ok(TelegramBlobStore::new(Arc::new(self.telegram(chat, backing).await?), limiter, info))
    }

    /// A Discord store on fake channel `channel`.
    async fn discord_store(
        &self,
        cfg: &Config,
        channel: &str,
        backing: &FakeBacking,
        max_blob_size: Option<usize>,
        info: InstanceInfo,
    ) -> Result<DiscordBlobStore<Arc<DiscordChannel>>> {
        let limiter = cfg.chat_limiter(InstanceKind::Discord, "fake", channel);
        Ok(DiscordBlobStore::new(Arc::new(self.discord(channel, backing).await?), limiter, max_blob_size, info))
    }
}

pub struct Node {
    pub meta: Arc<MetaStore>,
    pub store: Arc<ChunkStore>,
    pub s3: S3Gateway,
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

/// The store of the log or the snapshots in a fake channel.
fn meta_info(kind: InstanceKind, scope: &str, purpose: &str) -> InstanceInfo {
    let location = format!("fake:{}:{scope}", kind.as_str());
    InstanceInfo {
        id: format!("meta-{purpose}"),
        kind: kind.as_str(),
        fingerprint: location.clone(),
        location,
        role: pigeonhole_blob::InstanceRole::ReadWrite,
    }
}

fn no_real_channels() -> anyhow::Error {
    anyhow::anyhow!("real Telegram and Discord channels are not reachable from here: add `fake = \"memory\"`")
}

async fn open_log(world: &World, cfg: &Config) -> Result<Arc<dyn LogStore>> {
    Ok(match &cfg.meta.log {
        LogSettings::Memory => Arc::new(ChainLogStore::new(world.memory("meta-log", None))),
        LogSettings::Fake { kind: InstanceKind::Telegram, scope, backing } => {
            let info = meta_info(InstanceKind::Telegram, scope, "log");
            Arc::new(ChainLogStore::new(Arc::new(world.telegram_store(cfg, scope, backing, info).await?)))
        }
        LogSettings::Fake { kind: InstanceKind::Discord, scope, backing } => {
            let info = meta_info(InstanceKind::Discord, scope, "log");
            let log = ChainLogStore::new(Arc::new(world.discord_store(cfg, scope, backing, None, info).await?));
            Arc::new(match cfg.meta.lease {
                Some(lease) => log.with_lease(pigeonhole_meta::Lease::new(lease)),
                None => log,
            })
        }
        LogSettings::Fake { .. } => bail!("[meta] log: a fake channel is telegram or discord"),
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
        SnapshotSettings::Fake { kind: InstanceKind::Telegram, scope, backing } => {
            let info = meta_info(InstanceKind::Telegram, scope, "snapshots");
            Arc::new(BlobSnapshotStore::new(Arc::new(world.telegram_store(cfg, scope, backing, info).await?)))
        }
        SnapshotSettings::Fake { kind: InstanceKind::Discord, scope, backing } => {
            let info = meta_info(InstanceKind::Discord, scope, "snapshots");
            Arc::new(BlobSnapshotStore::new(Arc::new(world.discord_store(cfg, scope, backing, None, info).await?)))
        }
        SnapshotSettings::Fake { .. } => bail!("[meta] snapshot: a fake channel is telegram or discord"),
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
        (InstanceKind::Telegram, Some(backing)) => {
            Arc::new(erase(Arc::new(world.telegram_store(cfg, &inst.scope_id, backing, inst.info.clone()).await?)))
        }
        (InstanceKind::Discord, Some(backing)) => Arc::new(erase(Arc::new(
            world.discord_store(cfg, &inst.scope_id, backing, inst.max_blob_size, inst.info.clone()).await?,
        ))),
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
        let namespaces = Index::register(BlobDb::register(Namespaces::new()));
        let opts = MetaOptions {
            snapshot_every: cfg.meta.snapshot_every_entries,
            snapshot_interval: cfg.meta.snapshot_interval,
            snapshots_outlive_log: cfg.meta.snapshots_outlive_log(),
            keep_snapshots: cfg.meta.keep_snapshots,
            ..MetaOptions::default()
        };
        let meta = MetaStore::open(open_log(world, cfg).await?, open_snapshots(world, cfg).await?, namespaces, opts)
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
        Ok(Self { meta, store, s3 })
    }

    /// Stops the tablet, as a process exit would.
    pub async fn close(&self) -> Result<()> {
        self.meta.close().await
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
        let repairer = Repairer::new(self.store.db().clone(), self.store.replicated().clone(), RepairConfig::default());
        repairer.repair_once().await
    }

    /// One sweeper pass: delete posts that no chunk points to.
    pub async fn sweep(&self) -> Result<SweepStats> {
        let replicated = self.store.replicated();
        let sweeper = Sweeper::new(
            self.store.db().clone(),
            replicated.members().to_vec(),
            SweepConfig::default(),
            replicated.pins().clone(),
        );
        sweeper.sweep_once().await
    }
}

async fn futures_next(body: &mut StreamingBlob) -> Option<Result<Bytes>> {
    use futures::StreamExt;
    body.next().await.map(|r| r.map_err(|e| anyhow::anyhow!("{e}")))
}
