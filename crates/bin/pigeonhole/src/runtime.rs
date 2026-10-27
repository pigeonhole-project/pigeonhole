//! Binary-only: boot the metadata tablet, build the placement group, and wire `ChunkStore` + `Durability`.

use anyhow::{bail, Context};
use pigeonhole::config::Config;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::telegram::{TelegramBlobStore, TelegramClient};
use pigeonhole_blob::{
    erase_sweep, CheapestFirst, InstanceKind, MetricsBackend, Replicated, SharedBackend,
    TypedBootstrapPointer,
};
use pigeonhole_chunk_store::{
    check_fingerprints, BlockCache, BlobDb, LogSettings,
    ChunkStore, Durability, IngestOptions, PinTarget, Superblock, WatermarkBackend,
};
use pigeonhole::index::Index;
use pigeonhole_meta::{LocalLogStore, LogStore, MetaOptions, MetaStore, Namespaces};
use std::sync::Arc;
use tracing::{info, warn};

pub struct Runtime {
    /// The one metadata store every layer registers its namespace on.
    pub meta: Arc<MetaStore>,
    pub store: Arc<ChunkStore>,
    pub durability: Arc<Durability>,
    pub primary_fingerprint: String,
}

pub async fn open_runtime(cfg: &Config) -> anyhow::Result<Runtime> {
    let namespaces = Namespaces::new()
        .with_sql(BlobDb::namespace())
        .with_sql(Index::namespace());
    #[cfg(feature = "bytestream")]
    let namespaces = pigeonhole_gateway_bytestream::cas_index::CasIndex::register(namespaces);
    let log: Arc<dyn LogStore> = match &cfg.meta.log {
        LogSettings::Local { path } => Arc::new(
            LocalLogStore::open(path)
                .with_context(|| format!("open local log {}", path.display()))?,
        ),
    };
    let opts = MetaOptions {
        batch_max_delay: cfg.meta.batch_max_delay,
        batch_max_entries: cfg.meta.batch_max_entries,
    };
    let meta = MetaStore::open(&cfg.meta.db_url(), log, namespaces, opts)
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

    let (replicated, pins, primary_fingerprint, primary_id) =
        build_placement(cfg, &blob_db).await.context("build placement group")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let layer = ChunkStore::open_replicated(blob_db, replicated.clone(), opts)
        .await
        .context("open ChunkStore")?;
    let store = Arc::new(with_optional_caches(layer, cfg));

    let genesis = Superblock::new(0, primary_id, primary_fingerprint.clone());
    let durability = Arc::new(Durability::new_replicated(replicated, pins, genesis));

    Ok(Runtime {
        meta,
        store,
        durability,
        primary_fingerprint,
    })
}

async fn build_placement(
    cfg: &Config,
    blob_db: &BlobDb,
) -> anyhow::Result<(Arc<Replicated>, Vec<PinTarget>, String, String)> {
    let limiter = cfg.chat_limiter();
    let mut members: Vec<SharedBackend> = Vec::new();
    let mut pins: Vec<PinTarget> = Vec::new();
    let grace = cfg.sweep.grace;

    for id in &cfg.placement.group {
        let inst = cfg
            .instances
            .iter()
            .find(|i| i.info.id == *id)
            .with_context(|| format!("placement member {id:?} missing from instances"))?;

        match inst.info.kind {
            InstanceKind::Memory => {
                warn!("memory instance {id}: in-process MemoryBlobStore (no durable pin across processes)");
                let mem = Arc::new(
                    MemoryBlobStore::new().with_instance_info(inst.info.clone()),
                );
                let pin: Arc<dyn TypedBootstrapPointer> = mem.clone();
                let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                    Arc::new(erase_sweep(mem)),
                    blob_db.clone(),
                    grace,
                ));
                pins.push(PinTarget {
                    instance_id: inst.info.id.clone(),
                    fingerprint: inst.info.fingerprint.clone(),
                    pin,
                    backend: backend.clone(),
                });
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
                let pin: Arc<dyn TypedBootstrapPointer> = store.clone();
                let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                    Arc::new(erase_sweep(store)),
                    blob_db.clone(),
                    grace,
                ));
                pins.push(PinTarget {
                    instance_id: inst.info.id.clone(),
                    fingerprint: inst.info.fingerprint.clone(),
                    pin,
                    backend: backend.clone(),
                });
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
                    let pin: Arc<dyn TypedBootstrapPointer> = store.clone();
                    let backend = MetricsBackend::wrap(WatermarkBackend::wrap(
                        Arc::new(erase_sweep(store)),
                        blob_db.clone(),
                        grace,
                    ));
                    pins.push(PinTarget {
                        instance_id: inst.info.id.clone(),
                        fingerprint: inst.info.fingerprint.clone(),
                        pin,
                        backend: backend.clone(),
                    });
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
    let replicated = Arc::new(
        Replicated::new(
            members,
            cfg.placement.write_quorum,
            Arc::new(CheapestFirst::new()),
        )
        .context("Replicated::new")?,
    );
    let primary = cfg.primary_instance()?;
    Ok((
        replicated,
        pins,
        primary.info.fingerprint.clone(),
        primary.info.id.clone(),
    ))
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
