//! Binary-only: boot the metadata tablet, build the placement group, and wire the `ChunkStore`.

use crate::channel_log;
use anyhow::Context;
use pigeonhole::config::Config;
use pigeonhole::config::{LogSettings, SnapshotSettings};
use pigeonhole::index::Index;
use pigeonhole::instances::{check_fingerprints, InstanceConfig, InstanceKind, TelegramLogin};
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::telegram::{Lane, PooledChannel, TelegramBlobStore};
use pigeonhole_blob::{erase, CheapestFirst, MetricsBackend, Replicated, SharedBackend};
use pigeonhole_blob::{InstanceInfo, InstanceRole};
use pigeonhole_chunk_store::{BlobDb, BlockCache, ChunkStore, IngestOptions};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, SnapshotStore};
use pigeonhole_meta::{LogStore, MetaOptions, MetaStore, Namespaces};
use pigeonhole_storage_disk::DiskBlobStore;
use std::sync::Arc;
use tracing::{info, warn};

pub struct Runtime {
    /// The one metadata store every layer registers its namespace on.
    pub meta: Arc<MetaStore>,
    pub store: Arc<ChunkStore>,
}

/// The configured metadata log (`[meta] log`).
pub async fn open_log(cfg: &Config) -> anyhow::Result<Arc<dyn LogStore>> {
    Ok(match &cfg.meta.log {
        LogSettings::Memory => {
            warn!("metadata log in memory: everything is gone when the process exits");
            Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())))
        }
        // A local_disk store holds the log in `log/` and snapshots in
        // `snapshots/` (one chain each), so one store may serve both.
        LogSettings::LocalDisk { path } => Arc::new(ChainLogStore::new(Arc::new(
            DiskBlobStore::open(path.join("log")).with_context(|| format!("open local log {}", path.display()))?,
        ))),
        LogSettings::Telegram { chat_id, logins } => {
            info!(chat_id = %chat_id, bots = logins.len(), "metadata log in a Telegram channel");
            Arc::new(ChainLogStore::new(meta_channel_store(cfg, logins, chat_id, "log").await?))
        }
        #[cfg(not(feature = "discord"))]
        LogSettings::Discord { .. } => anyhow::bail!("[meta] log is discord: build with `--features discord`"),
        #[cfg(feature = "discord")]
        LogSettings::Discord { channel_id, bot_tokens, max_blob_size } => {
            info!(channel_id = %channel_id, bots = bot_tokens.len(), "metadata log in a Discord channel");
            let lease = cfg.meta.lease.map(pigeonhole_meta::Lease::new);
            meta_discord(cfg, channel_id, bot_tokens, *max_blob_size, "log", |s| {
                let log = ChainLogStore::new(s);
                Arc::new(match lease {
                    Some(lease) => log.with_lease(lease),
                    None => log,
                })
            })
            .await?
        }
    })
}

/// The configured snapshot store (`[meta] snapshot`).
pub async fn open_snapshots(cfg: &Config) -> anyhow::Result<Arc<dyn SnapshotStore>> {
    Ok(match &cfg.meta.snapshot {
        SnapshotSettings::LocalDisk { path } => Arc::new(BlobSnapshotStore::new(Arc::new(
            DiskBlobStore::open(path.join("snapshots"))
                .with_context(|| format!("open local snapshots {}", path.display()))?,
        ))),
        SnapshotSettings::Memory => Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new()))),
        SnapshotSettings::Telegram { chat_id, logins } => {
            info!(chat_id = %chat_id, bots = logins.len(), "metadata snapshots in a Telegram channel");
            Arc::new(BlobSnapshotStore::new(meta_channel_store(cfg, logins, chat_id, "snapshots").await?))
        }
        #[cfg(not(feature = "discord"))]
        SnapshotSettings::Discord { .. } => {
            anyhow::bail!("[meta] snapshot is discord: build with `--features discord`")
        }
        #[cfg(feature = "discord")]
        SnapshotSettings::Discord { channel_id, bot_tokens, max_blob_size } => {
            info!(channel_id = %channel_id, bots = bot_tokens.len(), "metadata snapshots in a Discord channel");
            meta_discord(cfg, channel_id, bot_tokens, *max_blob_size, "snapshots", |s| {
                Arc::new(BlobSnapshotStore::new(s))
            })
            .await?
        }
    })
}

/// Channel `chat_id` through every bot of `logins` (the primary first), on
/// `lane`: one pool.
async fn telegram_channel(
    cfg: &Config,
    logins: &[TelegramLogin],
    chat_id: &str,
    lane: Lane,
) -> anyhow::Result<PooledChannel> {
    let chat = parse_chat(chat_id)?;
    let mut members = Vec::new();
    for login in logins {
        let limiter = cfg.chat_limiter(InstanceKind::Telegram, &login.bot_token, chat_id);
        let bot = pigeonhole::telegram::bot_id_of(&login.bot_token)?;
        let channel = channel_log::client(login)
            .await?
            .channel(chat, lane, limiter)
            .await
            .with_context(|| format!("bot {bot}"))?;
        members.push(channel);
    }
    Ok(PooledChannel::of(members))
}

/// A channel holding the log or the snapshots, as a blob store whose blobs
/// chain (on the metadata connection, apart from blob uploads).
async fn meta_channel_store(
    cfg: &Config,
    logins: &[TelegramLogin],
    chat_id: &str,
    purpose: &str,
) -> anyhow::Result<Arc<TelegramBlobStore<PooledChannel>>> {
    let channel =
        telegram_channel(cfg, logins, chat_id, Lane::Meta).await.with_context(|| format!("{purpose} channel"))?;
    let info = InstanceInfo {
        id: format!("meta-{purpose}"),
        kind: InstanceKind::Telegram.as_str(),
        fingerprint: format!("tg:meta:{chat_id}"),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    let limiter = channel.limiter().clone();
    Ok(Arc::new(TelegramBlobStore::new(Arc::new(channel), limiter, info)))
}

/// Channel `channel_id` through every bot of `bot_tokens` (the primary
/// first), each checked for its rights there (`chain`: the thread rights
/// the log and snapshots need); with the primary's application id.
#[cfg(feature = "discord")]
async fn discord_channel(
    cfg: &Config,
    bot_tokens: &[String],
    channel_id: &str,
    chain: bool,
) -> anyhow::Result<(pigeonhole_storage_discord::PooledDiscord, String)> {
    use pigeonhole_storage_discord::{DiscordClient, PooledDiscord};
    let id: u64 = channel_id.parse().with_context(|| format!("channel id {channel_id:?}"))?;
    let mut members = Vec::new();
    for token in bot_tokens {
        let limiter = cfg.chat_limiter(InstanceKind::Discord, token, channel_id);
        let dc = DiscordClient::new(token.clone()).context("discord client")?;
        dc.ensure_permissions(id, chain, &limiter).await.with_context(|| format!("bot {}", dc.app_id()))?;
        members.push(dc.channel(id, limiter));
    }
    let app_id = members[0].client().app_id();
    Ok((PooledDiscord::of(members), app_id))
}

/// A Discord channel holding the log or the snapshots: its store, handed to
/// `wrap` (a chain over threads; the bots need thread rights there).
#[cfg(feature = "discord")]
async fn meta_discord<T: ?Sized>(
    cfg: &Config,
    channel_id: &str,
    bot_tokens: &[String],
    max_blob_size: Option<usize>,
    purpose: &str,
    wrap: impl FnOnce(
        Arc<pigeonhole_storage_discord::DiscordBlobStore<pigeonhole_storage_discord::PooledDiscord>>,
    ) -> Arc<T>,
) -> anyhow::Result<Arc<T>> {
    use pigeonhole_storage_discord::{discord_fingerprint, discord_location, DiscordBlobStore};
    let (channel, app_id) =
        discord_channel(cfg, bot_tokens, channel_id, true).await.with_context(|| format!("{purpose} channel"))?;
    let info = InstanceInfo {
        id: format!("meta-{purpose}"),
        kind: InstanceKind::Discord.as_str(),
        fingerprint: discord_fingerprint(&app_id, channel_id),
        location: discord_location(channel_id),
        role: InstanceRole::ReadWrite,
    };
    let limiter = channel.limiter().clone();
    Ok(wrap(Arc::new(DiscordBlobStore::new(Arc::new(channel), limiter, max_blob_size, info))))
}

fn parse_chat(chat_id: &str) -> anyhow::Result<i64> {
    chat_id.parse().with_context(|| format!("chat id {chat_id:?}"))
}

pub async fn open_runtime(cfg: &Config) -> anyhow::Result<Runtime> {
    let namespaces = Index::register(BlobDb::register(Namespaces::new()));
    #[cfg(feature = "bytestream")]
    let namespaces = pigeonhole_gateway_bytestream::cas_index::CasIndex::register(namespaces);
    let log = open_log(cfg).await?;
    let snapshots = open_snapshots(cfg).await?;
    let opts = MetaOptions {
        snapshot_every: cfg.meta.snapshot_every_entries,
        snapshot_interval: cfg.meta.snapshot_interval,
        snapshots_outlive_log: cfg.meta.snapshots_outlive_log(),
        keep_snapshots: cfg.meta.keep_snapshots,
        ..MetaOptions::default()
    };
    let meta = MetaStore::open(log, snapshots, namespaces, opts).await.context("boot metadata tablet")?;
    let blob_db = BlobDb::open(meta.clone()).await.context("open chunk metadata")?;

    let stored = blob_db.list_instance_fingerprints().await.context("list instance fingerprints")?;
    check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;

    let replicated = build_placement(cfg).await.context("build placement group")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let layer = ChunkStore::open_replicated(blob_db, replicated, opts).await.context("open ChunkStore")?;
    let store = Arc::new(with_optional_caches(layer, cfg));
    Ok(Runtime { meta, store })
}

async fn build_placement(cfg: &Config) -> anyhow::Result<Arc<Replicated>> {
    let mut shards = Vec::new();
    for shard in &cfg.placement.shards {
        let mut members: Vec<SharedBackend> = Vec::new();
        for id in &shard.group {
            let inst = cfg
                .instances
                .iter()
                .find(|i| i.info.id == *id)
                .with_context(|| format!("blob store {id:?} not resolved"))?;
            members.push(build_member(cfg, inst).await?);
        }
        shards.push((members, shard.write_quorum));
    }
    let replicated = Replicated::sharded(shards, Arc::new(CheapestFirst::new())).context("blob placement")?;
    Ok(Arc::new(replicated))
}

/// One blob store as a placement member.
async fn build_member(cfg: &Config, inst: &InstanceConfig) -> anyhow::Result<SharedBackend> {
    let id = &inst.info.id;
    Ok(match inst.kind {
        InstanceKind::Memory => {
            warn!("memory instance {id}: in-process MemoryBlobStore (data does not survive the process)");
            let mem = Arc::new(MemoryBlobStore::new().with_instance_info(inst.info.clone()));
            MetricsBackend::wrap(Arc::new(erase(mem)))
        }
        InstanceKind::LocalDisk => {
            let store = DiskBlobStore::with_instance(&inst.scope_id, inst.info.clone())
                .with_context(|| format!("open local_disk store {}", inst.scope_id))?;
            MetricsBackend::wrap(Arc::new(erase(Arc::new(store))))
        }
        InstanceKind::Telegram => {
            if inst.telegram.is_empty() {
                anyhow::bail!("store {id:?}: no MTProto login");
            }
            let channel = telegram_channel(cfg, &inst.telegram, &inst.scope_id, Lane::Blobs)
                .await
                .with_context(|| format!("store {id:?}"))?;
            let limiter = channel.limiter().clone();
            let store = TelegramBlobStore::new(Arc::new(channel), limiter, inst.info.clone());
            MetricsBackend::wrap(Arc::new(erase(Arc::new(store))))
        }
        InstanceKind::Discord => {
            #[cfg(feature = "discord")]
            {
                use pigeonhole_storage_discord::DiscordBlobStore;
                let (channel, _) = discord_channel(cfg, &inst.bot_tokens, &inst.scope_id, false)
                    .await
                    .with_context(|| format!("store {id:?}: permission check"))?;
                let limiter = channel.limiter().clone();
                let store = DiscordBlobStore::new(Arc::new(channel), limiter, inst.max_blob_size, inst.info.clone());
                MetricsBackend::wrap(Arc::new(erase(Arc::new(store))))
            }
            #[cfg(not(feature = "discord"))]
            {
                anyhow::bail!("store {id:?} is discord but this binary was built without `--features discord`");
            }
        }
    })
}

fn with_optional_caches(layer: ChunkStore, cfg: &Config) -> ChunkStore {
    if cfg.cache.enabled {
        let l1 = Arc::new(BlockCache::new("l1", cfg.cache.l1_bytes));
        let l2 = cfg.cache.l2_bytes.map(|bytes| Arc::new(BlockCache::new("l2", bytes)));
        layer.with_caches(Some(l1), l2, cfg.cache.write_through)
    } else {
        layer
    }
}
