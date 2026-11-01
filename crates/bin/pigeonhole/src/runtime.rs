//! Binary-only: boot the metadata tablet, build the placement group, and wire the `ChunkStore`.

use crate::channel_log;
use anyhow::Context;
use pigeonhole::config::cluster::ClusterSettings;
use pigeonhole::config::Config;
use pigeonhole::config::{DynConfig, LogSettings, SnapshotSettings};
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
    /// The background loops' knobs: the file's, with the tablet's overrides.
    pub dynamic: Arc<DynConfig>,
    /// The store the metadata log lives in (the admin's log inspector).
    pub log: Arc<dyn LogStore>,
}

/// The configured metadata log (`[meta] log`).
pub async fn open_log(cfg: &Config) -> anyhow::Result<Arc<dyn LogStore>> {
    Ok(match &cfg.meta.log {
        LogSettings::Fake(_) => anyhow::bail!("[meta] log is a fake channel: this build has no fake channels"),
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
        SnapshotSettings::Fake(_) => {
            anyhow::bail!("[meta] snapshot is a fake channel: this build has no fake channels")
        }
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
    Ok(pigeonhole::telegram::pooled(members))
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
) -> anyhow::Result<(pigeonhole_discord_http::PooledDiscord, String)> {
    use pigeonhole_discord_http::DiscordClient;
    let id: u64 = channel_id.parse().with_context(|| format!("channel id {channel_id:?}"))?;
    let mut members = Vec::new();
    for token in bot_tokens {
        let limiter = cfg.chat_limiter(InstanceKind::Discord, token, channel_id);
        let dc = DiscordClient::new(token.clone()).context("discord client")?;
        dc.ensure_permissions(id, chain, &limiter).await.with_context(|| format!("bot {}", dc.app_id()))?;
        members.push(dc.channel(id, limiter));
    }
    let app_id = members[0].client().app_id();
    Ok((pigeonhole_discord_http::pooled(members), app_id))
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
    wrap: impl FnOnce(Arc<pigeonhole_storage_discord::DiscordBlobStore<pigeonhole_discord_http::PooledDiscord>>) -> Arc<T>,
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
    let namespaces = DynConfig::register(Index::register(BlobDb::register(Namespaces::new())));
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
    let meta = match &cfg.cluster {
        None => MetaStore::open(log.clone(), snapshots, namespaces, opts).await,
        Some(cluster) => open_node(cluster, log.clone(), snapshots, namespaces, opts).await,
    }
    .context("boot metadata tablet")?;
    let blob_db = BlobDb::open(meta.clone()).await.context("open chunk metadata")?;

    let stored = blob_db.list_instance_fingerprints().await.context("list instance fingerprints")?;
    check_fingerprints(&cfg.instances, &stored).context("fingerprint check")?;

    let replicated = build_placement(cfg).await.context("build placement group")?;

    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    opts.memory_budget = cfg.ingest_budget.clone();

    let layer = ChunkStore::open_replicated(blob_db, replicated, opts).await.context("open ChunkStore")?;
    let store = Arc::new(with_optional_caches(layer, cfg));
    let dynamic = Arc::new(DynConfig::open(cfg.clone(), meta.clone()));
    Ok(Runtime { meta, store, dynamic, log })
}

/// The tablet as node `cluster.node` of its cluster: Raft over gRPC to the
/// other nodes, served on this node's `grpc` address.
async fn open_node(
    cluster: &ClusterSettings,
    log: Arc<dyn LogStore>,
    snapshots: Arc<dyn SnapshotStore>,
    namespaces: Namespaces,
    opts: MetaOptions,
) -> anyhow::Result<Arc<MetaStore>> {
    use pigeonhole::config::cluster::Link;
    use pigeonhole_meta::raft::TypeConfig;
    use pigeonhole_meta::{Cluster, Serve};
    if let Some((peer, _)) = cluster.links().into_iter().find(|(_, l)| *l == Link::Chat) {
        anyhow::bail!("[[node]] {peer} is reached over the chat: this build has no chat mailbox yet; give both `grpc`");
    }
    let me = cluster.me();
    let addr = me.grpc.as_deref().context("this [[node]] has no `grpc` address to serve Raft on")?;
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind Raft gRPC {addr}"))?;
    let peers = cluster.nodes.iter().filter(|n| n.id != me.id).filter_map(|n| Some((n.id, n.grpc.clone()?))).collect();
    let identity = openraft_grpc::Identity { me: me.id, cluster: cluster.id, key: cluster.key };
    let network = openraft_grpc::GrpcNetwork::<TypeConfig>::new(identity.clone(), peers);
    info!(node = me.id, %addr, nodes = cluster.nodes.len(), "a node of a Raft cluster over gRPC");
    let serve: Serve = Box::new(move |raft| {
        Box::pin(async move {
            tokio::spawn(async move {
                if let Err(e) = openraft_grpc::serve::<TypeConfig>(listener, identity, Arc::new(raft)).await {
                    tracing::error!(error = %format!("{e:#}"), "Raft gRPC server stopped");
                }
            });
        })
    });
    let members = cluster.nodes.iter().map(|n| n.id).collect();
    let opts = MetaOptions { heartbeat: cluster.heartbeat, election_timeout: cluster.election_timeout, ..opts };
    let cluster = Cluster { node: me.id, members, network, serve: Some(serve) };
    MetaStore::open_in(log, snapshots, namespaces, opts, cluster).await
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
    if inst.fake.is_some() {
        anyhow::bail!("store {id:?} is a fake channel: this build has no fake channels");
    }
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
