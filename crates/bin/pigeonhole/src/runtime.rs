//! Binary-only: boot the metadata tablet, build the placement group, and wire the `ChunkStore`.

use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::telegram::{TelegramBlobStore, TelegramClient};
use pigeonhole_blob::{
    erase_sweep, CheapestFirst, InstanceKind, MetricsBackend, Replicated, SharedBackend,
};
use pigeonhole_chunk_store::{
    check_fingerprints, BlockCache, BlobDb, ChunkStore, IngestOptions, LogSettings, SnapshotSettings,
};
use pigeonhole::index::Index;
use crate::telegram_log::{self, TelegramChat};
use pigeonhole_meta::log::chat::{ChatLogOptions, ChatLogStore};
use pigeonhole_meta::snapshot::ChatSnapshotStore;
use pigeonhole_meta::{LocalSnapshotStore, SnapshotStore};
use pigeonhole_meta::{LocalLogStore, LogStore, MetaOptions, MetaStore, Namespaces};
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
        LogSettings::Dir { path } => Arc::new(
            LocalLogStore::open(path).with_context(|| format!("open local log {}", path.display()))?,
        ),
        LogSettings::Telegram { chat_id, bot_token, straggler_window } => {
            if *straggler_window <= telegram_log::CALL_DEADLINE {
                bail!(
                    "[meta] log_straggler_window_secs must exceed the {:?} call deadline",
                    telegram_log::CALL_DEADLINE
                );
            }
            let tg = TelegramClient::new(bot_token.clone()).context("telegram client for the log")?;
            tg.ensure_chat_admin(chat_id).await.context("log chat access check")?;
            let chat = TelegramChat::new(tg, chat_id.clone(), cfg.chat_limiter());
            let opts = ChatLogOptions { straggler_window: *straggler_window, ..ChatLogOptions::default() };
            info!(chat_id = %chat_id, "metadata log in a Telegram chat");
            Arc::new(ChatLogStore::new(Arc::new(chat), opts))
        }
    })
}

/// The configured snapshot store (`[meta] snapshot`).
pub async fn open_snapshots(cfg: &Config) -> anyhow::Result<Arc<dyn SnapshotStore>> {
    Ok(match &cfg.meta.snapshot {
        SnapshotSettings::Dir { path } => Arc::new(LocalSnapshotStore::new(path)),
        SnapshotSettings::Telegram { chat_id, bot_token } => {
            let tg = TelegramClient::new(bot_token.clone()).context("telegram client for snapshots")?;
            tg.ensure_chat_admin(chat_id).await.context("snapshot chat access check")?;
            let chat = TelegramChat::new(tg, chat_id.clone(), cfg.chat_limiter());
            info!(chat_id = %chat_id, "metadata snapshots in a Telegram chat");
            Arc::new(ChatSnapshotStore::new(Arc::new(chat)))
        }
    })
}

pub async fn open_runtime(cfg: &Config) -> anyhow::Result<Runtime> {
    let namespaces = Index::register(BlobDb::register(Namespaces::new()));
    #[cfg(feature = "bytestream")]
    let namespaces = pigeonhole_gateway_bytestream::cas_index::CasIndex::register(namespaces);
    let log = open_log(cfg).await?;
    let snapshots = open_snapshots(cfg).await?;
    let opts = MetaOptions {
        batch_max_delay: cfg.meta.batch_max_delay,
        batch_max_entries: cfg.meta.batch_max_entries,
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
    let limiter = cfg.chat_limiter();
    let mut members: Vec<SharedBackend> = Vec::new();

    for id in &cfg.placement.group {
        let inst = cfg
            .instances
            .iter()
            .find(|i| i.info.id == *id)
            .with_context(|| format!("placement member {id:?} missing from instances"))?;

        match inst.info.kind {
            InstanceKind::Memory => {
                warn!("memory instance {id}: in-process MemoryBlobStore (data does not survive the process)");
                let mem = Arc::new(
                    MemoryBlobStore::new().with_instance_info(inst.info.clone()),
                );
                let backend = MetricsBackend::wrap(Arc::new(erase_sweep(mem)));
                members.push(backend);
            }
            InstanceKind::Telegram => {
                let tg = TelegramClient::new(inst.bot_token.clone()).context("telegram client")?;
                tg.ensure_chat_admin(&inst.scope_id)
                    .await
                    .context("chat_id access check")?;
                let store = Arc::new(TelegramBlobStore::with_instance(
                    tg,
                    inst.scope_id.clone(),
                    limiter.clone(),
                    inst.info.clone(),
                ));
                let backend = MetricsBackend::wrap(Arc::new(erase_sweep(store)));
                members.push(backend);
            }
            InstanceKind::Discord => {
                #[cfg(feature = "discord")]
                {
                    use pigeonhole_storage_discord::{DiscordBlobStore, DiscordClient};
                    let dc = DiscordClient::new(inst.bot_token.clone()).context("discord client")?;
                    dc.ensure_channel_permissions(&inst.scope_id, Some(limiter.as_ref()))
                        .await
                        .context("channel_id permission check")?;
                    let store = Arc::new(DiscordBlobStore::with_instance(
                        dc,
                        inst.scope_id.clone(),
                        limiter.clone(),
                        inst.max_blob_size,
                        inst.info.clone(),
                    ));
                    let backend = MetricsBackend::wrap(Arc::new(erase_sweep(store)));
                    members.push(backend);
                }
                #[cfg(not(feature = "discord"))]
                {
                    bail!(
                        "instance {id:?} is discord but this binary was built without `--features discord`"
                    );
                }
            }
        }
    }

    if members.is_empty() {
        bail!("placement.group produced no backends");
    }
    let replicated = Replicated::new(
        members,
        cfg.placement.write_quorum,
        Arc::new(CheapestFirst::new()),
    )
    .context("Replicated::new")?;
    Ok(Arc::new(replicated))
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
