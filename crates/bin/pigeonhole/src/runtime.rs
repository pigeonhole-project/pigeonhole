//! Binary-only: boot the metadata tablet, build the placement group, and wire the `ChunkStore`.

use anyhow::Context;
use pigeonhole::config::Config;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole_blob::{
    erase_sweep, ChatLimiter, CheapestFirst, InstanceKind, MetricsBackend, Replicated, SharedBackend,
};
use pigeonhole_chunk_store::{
    check_fingerprints, BlockCache, BlobDb, ChunkStore, IngestOptions, InstanceConfig, LogSettings,
    SnapshotSettings,
};
use pigeonhole::index::Index;
use crate::channel_log;
use pigeonhole::telegram::{Lane, MtprotoChannel, TelegramBlobStore};
use pigeonhole_blob::{InstanceInfo, InstanceRole};
use pigeonhole_chunk_store::TelegramLogin;
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
        LogSettings::Telegram { chat_id, login } => {
            info!(chat_id = %chat_id, "metadata log in a Telegram channel");
            Arc::new(ChainLogStore::new(meta_channel_store(cfg, login, chat_id, "log").await?))
        }
        #[cfg(not(feature = "discord"))]
        LogSettings::Discord { .. } => anyhow::bail!("[meta] log is discord: build with `--features discord`"),
        #[cfg(feature = "discord")]
        LogSettings::Discord { channel_id, bot_token, max_blob_size } => {
            info!(channel_id = %channel_id, "metadata log in a Discord channel");
            meta_discord(cfg, channel_id, bot_token, *max_blob_size, "log", |s| Arc::new(ChainLogStore::new(s))).await?
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
        SnapshotSettings::Telegram { chat_id, login } => {
            info!(chat_id = %chat_id, "metadata snapshots in a Telegram channel");
            Arc::new(BlobSnapshotStore::new(meta_channel_store(cfg, login, chat_id, "snapshots").await?))
        }
        #[cfg(not(feature = "discord"))]
        SnapshotSettings::Discord { .. } => anyhow::bail!("[meta] snapshot is discord: build with `--features discord`"),
        #[cfg(feature = "discord")]
        SnapshotSettings::Discord { channel_id, bot_token, max_blob_size } => {
            info!(channel_id = %channel_id, "metadata snapshots in a Discord channel");
            meta_discord(cfg, channel_id, bot_token, *max_blob_size, "snapshots", |s| Arc::new(BlobSnapshotStore::new(s)))
                .await?
        }
    })
}

/// A channel holding the log or the snapshots, as a blob store whose blobs
/// chain (on the metadata connection, apart from blob uploads).
async fn meta_channel_store(
    cfg: &Config,
    login: &TelegramLogin,
    chat_id: &str,
    purpose: &str,
) -> anyhow::Result<Arc<TelegramBlobStore<MtprotoChannel>>> {
    let limiter = cfg.chat_limiter(&login.bot_token, chat_id);
    let channel = channel_log::client(login)
        .await?
        .channel(parse_chat(chat_id)?, Lane::Meta, limiter.clone())
        .await
        .with_context(|| format!("{purpose} channel"))?;
    let info = InstanceInfo {
        id: format!("meta-{purpose}"),
        kind: InstanceKind::Telegram,
        fingerprint: format!("tg:meta:{chat_id}"),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    Ok(Arc::new(TelegramBlobStore::new(Arc::new(channel), limiter, info)))
}

/// A Discord channel holding the log or the snapshots: its store, handed to
/// `wrap` (a chain over threads; the bot needs thread rights there).
#[cfg(feature = "discord")]
async fn meta_discord<T: ?Sized>(
    cfg: &Config,
    channel_id: &str,
    bot_token: &str,
    max_blob_size: Option<usize>,
    purpose: &str,
    wrap: impl FnOnce(Arc<pigeonhole_storage_discord::DiscordBlobStore<pigeonhole_storage_discord::DiscordChannel>>) -> Arc<T>,
) -> anyhow::Result<Arc<T>> {
    use pigeonhole_storage_discord::{discord_fingerprint, discord_location, DiscordBlobStore, DiscordClient};
    let limiter = cfg.chat_limiter(bot_token, channel_id);
    let dc = DiscordClient::new(bot_token.to_string()).context("discord client")?;
    let id: u64 = channel_id.parse().with_context(|| format!("{purpose} channel id {channel_id:?}"))?;
    dc.ensure_permissions(id, true, &limiter).await.with_context(|| format!("{purpose} channel"))?;
    let info = InstanceInfo {
        id: format!("meta-{purpose}"),
        kind: InstanceKind::Discord,
        fingerprint: discord_fingerprint(&dc.app_id(), channel_id),
        location: discord_location(channel_id),
        role: InstanceRole::ReadWrite,
    };
    let channel = Arc::new(dc.channel(id, limiter.clone()));
    Ok(wrap(Arc::new(DiscordBlobStore::new(channel, limiter, max_blob_size, info))))
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
    };
    let meta = MetaStore::open(log, snapshots, namespaces, opts)
        .await
        .context("boot metadata tablet")?;
    info!(generation = meta.generation(), "metadata tablet active");
    let blob_db = BlobDb::open(meta.clone())
        .await
        .context("open chunk metadata")?;

    let stored = blob_db
        .list_instance_fingerprints()
        .await
        .context("list instance fingerprints")?;
    check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;

    let replicated = build_placement(cfg).await.context("build placement group")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let layer = ChunkStore::open_replicated(blob_db, replicated, opts)
        .await
        .context("open ChunkStore")?;
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
            let limiter = cfg.chat_limiter(&inst.bot_token, &inst.scope_id);
            members.push(build_member(inst, &limiter).await?);
        }
        shards.push((members, shard.write_quorum));
    }
    let replicated = Replicated::sharded(shards, Arc::new(CheapestFirst::new())).context("blob placement")?;
    Ok(Arc::new(replicated))
}

/// One blob store as a placement member.
async fn build_member(inst: &InstanceConfig, limiter: &Arc<ChatLimiter>) -> anyhow::Result<SharedBackend> {
    let id = &inst.info.id;
    Ok(match inst.info.kind {
        InstanceKind::Memory => {
            warn!("memory instance {id}: in-process MemoryBlobStore (data does not survive the process)");
            let mem = Arc::new(
                MemoryBlobStore::new().with_instance_info(inst.info.clone()),
            );
            MetricsBackend::wrap(Arc::new(erase_sweep(mem)))
        }
        InstanceKind::LocalDisk => {
            let store = DiskBlobStore::with_instance(&inst.scope_id, inst.info.clone())
                .with_context(|| format!("open local_disk store {}", inst.scope_id))?;
            MetricsBackend::wrap(Arc::new(erase_sweep(Arc::new(store))))
        }
        InstanceKind::Telegram => {
            let login = inst.telegram.as_ref().with_context(|| format!("store {id:?}: no MTProto login"))?;
            let channel = channel_log::client(login)
                .await?
                .channel(parse_chat(&inst.scope_id)?, Lane::Blobs, limiter.clone())
                .await
                .with_context(|| format!("store {id:?}"))?;
            let store = TelegramBlobStore::new(Arc::new(channel), limiter.clone(), inst.info.clone());
            MetricsBackend::wrap(Arc::new(erase_sweep(Arc::new(store))))
        }
        InstanceKind::Discord => {
            #[cfg(feature = "discord")]
            {
                use pigeonhole_storage_discord::{DiscordBlobStore, DiscordClient};
                let dc = DiscordClient::new(inst.bot_token.clone()).context("discord client")?;
                let channel_id: u64 = inst.scope_id.parse().with_context(|| format!("store {id:?}: channel id"))?;
                dc.ensure_permissions(channel_id, false, limiter)
                    .await
                    .with_context(|| format!("store {id:?}: permission check"))?;
                let channel = Arc::new(dc.channel(channel_id, limiter.clone()));
                let store = DiscordBlobStore::new(channel, limiter.clone(), inst.max_blob_size, inst.info.clone());
                MetricsBackend::wrap(Arc::new(erase_sweep(Arc::new(store))))
            }
            #[cfg(not(feature = "discord"))]
            {
                anyhow::bail!(
                    "store {id:?} is discord but this binary was built without `--features discord`"
                );
            }
        }
    })
}

fn with_optional_caches(layer: ChunkStore, cfg: &Config) -> ChunkStore {
    if cfg.cache.enabled {
        let l1 = Arc::new(BlockCache::new(
            cfg.cache.block_memory_bytes,
            cfg.cache.readahead_blocks,
        ));
        layer.with_caches(Some(l1), None)
    } else {
        layer
    }
}
