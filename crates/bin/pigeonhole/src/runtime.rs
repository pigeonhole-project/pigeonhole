//! Binary-only: the node (pigeonhole-node) on the real channels: Telegram
//! over MTProto, Discord over HTTP, and their Raft outboxes; fake ones
//! with the feature `fake` (crate::fakes).

use crate::channel_log;
use anyhow::Context;
use async_trait::async_trait;
use pigeonhole::config::cluster::{ChatSettings, ClusterSettings};
use pigeonhole::config::dynamic::Task;
use pigeonhole::config::{Config, LogSettings, SnapshotSettings};
use pigeonhole::instances::{InstanceConfig, InstanceKind, TelegramLogin};
use pigeonhole::telegram::{Lane, PooledChannel, TelegramBlobStore};
use pigeonhole_admin::{Channels, StoreRef, LOG_STORE, SNAPSHOT_STORE};
use pigeonhole_blob::{erase, InstanceInfo, InstanceRole, SharedBackend};
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{Cluster, LogStore, Namespaces, SnapshotStore};
use pigeonhole_node::{chain_log, snapshot_store, MetaStores, Opener, Stack};
use std::sync::Arc;
use tracing::info;

/// The node's stores on real channels (and fakes, with the feature `fake`).
pub struct RealChannels;

#[async_trait]
impl Opener for RealChannels {
    async fn log(&self, cfg: &Config, place: &LogSettings) -> anyhow::Result<Arc<dyn LogStore>> {
        Ok(match place {
            LogSettings::Fake(f) => crate::fakes::log(cfg, f).await?,
            LogSettings::Telegram { chat_id, logins } => {
                info!(chat_id = %chat_id, bots = logins.len(), "metadata log in a Telegram channel");
                chain_log(meta_channel_store(cfg, logins, chat_id, LOG_STORE).await?)
            }
            #[cfg(feature = "discord")]
            LogSettings::Discord { channel_id, bot_tokens, max_blob_size } => {
                info!(channel_id = %channel_id, bots = bot_tokens.len(), "metadata log in a Discord channel");
                let store = meta_discord(cfg, channel_id, bot_tokens, *max_blob_size, LOG_STORE).await?;
                pigeonhole_node::leased_log(store, cfg)
            }
            #[cfg(not(feature = "discord"))]
            LogSettings::Discord { .. } => anyhow::bail!("a discord log: build with `--features discord`"),
            LogSettings::Memory | LogSettings::LocalDisk { .. } => unreachable!("the node opens these"),
        })
    }

    async fn snapshots(&self, cfg: &Config, place: &SnapshotSettings) -> anyhow::Result<Arc<dyn SnapshotStore>> {
        Ok(match place {
            SnapshotSettings::Fake(f) => crate::fakes::snapshots(cfg, f).await?,
            SnapshotSettings::Telegram { chat_id, logins } => {
                info!(chat_id = %chat_id, bots = logins.len(), "metadata snapshots in a Telegram channel");
                snapshot_store(meta_channel_store(cfg, logins, chat_id, SNAPSHOT_STORE).await?)
            }
            #[cfg(feature = "discord")]
            SnapshotSettings::Discord { channel_id, bot_tokens, max_blob_size } => {
                info!(channel_id = %channel_id, bots = bot_tokens.len(), "metadata snapshots in a Discord channel");
                snapshot_store(meta_discord(cfg, channel_id, bot_tokens, *max_blob_size, SNAPSHOT_STORE).await?)
            }
            #[cfg(not(feature = "discord"))]
            SnapshotSettings::Discord { .. } => anyhow::bail!("discord snapshots: build with `--features discord`"),
            SnapshotSettings::Memory | SnapshotSettings::LocalDisk { .. } => unreachable!("the node opens these"),
        })
    }

    async fn member(&self, cfg: &Config, inst: &InstanceConfig) -> anyhow::Result<SharedBackend> {
        if let Some(f) = &inst.fake {
            return crate::fakes::member(cfg, inst, f).await;
        }
        match inst.kind {
            InstanceKind::Telegram => telegram_member(cfg, inst).await,
            InstanceKind::Discord => discord_member(cfg, inst).await,
            InstanceKind::Memory | InstanceKind::LocalDisk => unreachable!("the node opens these"),
        }
    }

    fn view(&self, stores: &[StoreRef], key: Option<[u8; 32]>) -> Option<Arc<dyn Channels>> {
        crate::fakes::view(stores, key)
    }
}

/// The node `cfg` describes, its tablet booted (alone, or as a node of its
/// cluster), its loops running: those of pigeonhole-node and `extra`
/// (`namespaces`: the root's own on top of the node's).
pub async fn open(
    cfg: &Config,
    namespaces: impl FnOnce(Namespaces) -> Namespaces,
    extra: &[Task],
) -> anyhow::Result<Stack> {
    let stores = open_meta_stores(cfg).await?;
    let cluster = match &cfg.cluster {
        Some(c) => Some(node_network(cfg, c).await?),
        None => None,
    };
    let meta = pigeonhole_node::open_tablet(cfg, &stores, namespaces(pigeonhole_node::namespaces()), cluster).await?;
    pigeonhole_node::open_stack(cfg, &RealChannels, meta, stores.log, extra).await
}

/// The configured metadata log and snapshots.
pub async fn open_meta_stores(cfg: &Config) -> anyhow::Result<MetaStores> {
    pigeonhole_node::open_meta_stores(cfg, &RealChannels).await
}

/// Channel `chat_id` through every bot of `logins` (the primary first), on
/// `lane`: one pool.
async fn telegram_channel(
    cfg: &Config,
    logins: &[TelegramLogin],
    chat_id: &str,
    lane: Lane,
) -> anyhow::Result<PooledChannel> {
    let chat = chat_id.parse().with_context(|| format!("chat id {chat_id:?}"))?;
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

/// A channel holding the log or the snapshots (store `id`, as the admin
/// lists it), as a blob store whose blobs chain (on the metadata
/// connection, apart from blob uploads).
async fn meta_channel_store(
    cfg: &Config,
    logins: &[TelegramLogin],
    chat_id: &str,
    id: &str,
) -> anyhow::Result<Arc<TelegramBlobStore<PooledChannel>>> {
    let channel = telegram_channel(cfg, logins, chat_id, Lane::Meta).await.with_context(|| format!("{id} channel"))?;
    let info = InstanceInfo {
        id: id.to_string(),
        kind: InstanceKind::Telegram.as_str(),
        fingerprint: format!("tg:meta:{chat_id}"),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    let limiter = channel.limiter().clone();
    Ok(Arc::new(TelegramBlobStore::new(Arc::new(channel), limiter, info)))
}

/// A Telegram blob store of the placement.
async fn telegram_member(cfg: &Config, inst: &InstanceConfig) -> anyhow::Result<SharedBackend> {
    if inst.telegram.is_empty() {
        anyhow::bail!("no MTProto login");
    }
    let channel = telegram_channel(cfg, &inst.telegram, &inst.scope_id, Lane::Blobs).await?;
    let limiter = channel.limiter().clone();
    let store = TelegramBlobStore::new(Arc::new(channel), limiter, inst.info.clone());
    Ok(Arc::new(erase(Arc::new(store))))
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

#[cfg(feature = "discord")]
type DiscordStore = pigeonhole_storage_discord::DiscordBlobStore<pigeonhole_discord_http::PooledDiscord>;

/// A Discord channel holding the log or the snapshots (store `id`): a
/// chain over threads; the bots need thread rights there.
#[cfg(feature = "discord")]
async fn meta_discord(
    cfg: &Config,
    channel_id: &str,
    bot_tokens: &[String],
    max_blob_size: Option<usize>,
    id: &str,
) -> anyhow::Result<Arc<DiscordStore>> {
    use pigeonhole_storage_discord::{discord_fingerprint, discord_location};
    let (channel, app_id) =
        discord_channel(cfg, bot_tokens, channel_id, true).await.with_context(|| format!("{id} channel"))?;
    let info = InstanceInfo {
        id: id.to_string(),
        kind: InstanceKind::Discord.as_str(),
        fingerprint: discord_fingerprint(&app_id, channel_id),
        location: discord_location(channel_id),
        role: InstanceRole::ReadWrite,
    };
    let limiter = channel.limiter().clone();
    Ok(Arc::new(DiscordStore::new(Arc::new(channel), limiter, max_blob_size, info)))
}

/// A Discord blob store of the placement.
#[cfg(feature = "discord")]
async fn discord_member(cfg: &Config, inst: &InstanceConfig) -> anyhow::Result<SharedBackend> {
    let (channel, _) =
        discord_channel(cfg, &inst.discord_tokens, &inst.scope_id, false).await.context("permission check")?;
    let limiter = channel.limiter().clone();
    let store = DiscordStore::new(Arc::new(channel), limiter, inst.max_blob_size, inst.info.clone());
    Ok(Arc::new(erase(Arc::new(store))))
}

#[cfg(not(feature = "discord"))]
async fn discord_member(_cfg: &Config, _inst: &InstanceConfig) -> anyhow::Result<SharedBackend> {
    anyhow::bail!("a discord store: this binary was built without `--features discord`")
}

type Network = openraft_grpc::Mixed<
    openraft_grpc::GrpcNetwork<TypeConfig>,
    openraft_chat::ChatNetwork<TypeConfig>,
    pigeonhole_meta::raft::NodeId,
>;

/// The tablet as node `cluster.node` of its cluster: Raft over gRPC to the
/// nodes both have an address for, over the chat (Telegram outboxes) to the
/// others (docs/cluster.md § Transports).
async fn node_network(cfg: &Config, cluster: &ClusterSettings) -> anyhow::Result<Cluster<Network>> {
    let me = cluster.me();
    let via_chat = pigeonhole_node::cluster::via_chat(cluster);
    let grpc_peers = cluster
        .nodes
        .iter()
        .filter(|n| n.id != me.id && !via_chat.contains(&n.id))
        .filter_map(|n| Some((n.id, n.grpc.clone()?)))
        .collect();
    let identity = openraft_grpc::Identity { me: me.id, cluster: cluster.id, key: cluster.key };
    let grpc = openraft_grpc::GrpcNetwork::<TypeConfig>::new(identity.clone(), grpc_peers);
    let listener = match &me.grpc {
        Some(addr) => {
            Some(tokio::net::TcpListener::bind(addr).await.with_context(|| format!("bind Raft gRPC {addr}"))?)
        }
        None => None,
    };
    let chat = chat_network(cfg, cluster, &via_chat).await?;
    info!(node = me.id, grpc = ?me.grpc, chat_peers = ?via_chat, nodes = cluster.nodes.len(), "a node of a Raft cluster");
    Ok(pigeonhole_node::cluster::cluster(cluster, grpc, chat, move |raft| {
        let Some(listener) = listener else { return };
        tokio::spawn(async move {
            if let Err(e) = openraft_grpc::serve::<TypeConfig>(listener, identity, raft).await {
                tracing::error!(error = %format!("{e:#}"), "Raft gRPC server stopped");
            }
        });
    }))
}

/// The chat side of the node's network: its outbox and the outboxes of the
/// peers in `peers`, each a Telegram channel read through this node's bots
/// and woken by the posts Telegram pushes. No peers: an idle network.
async fn chat_network(
    cfg: &Config,
    cluster: &ClusterSettings,
    peers: &std::collections::HashSet<u64>,
) -> anyhow::Result<openraft_chat::ChatNetwork<TypeConfig>> {
    if let Some(ChatSettings::Fake) = &cluster.chat {
        anyhow::bail!("[cluster] fake: fake outboxes run in the site's demo (pigeonhole-web), not in the binary");
    }
    let Some(ChatSettings::Telegram { logins }) = (!peers.is_empty()).then_some(cluster.chat.as_ref()).flatten() else {
        return Ok(pigeonhole_node::cluster::idle_chat(cluster));
    };
    let outbox = |node: u64| -> anyhow::Result<i64> {
        let n = cluster.nodes.iter().find(|n| n.id == node).expect("a member");
        n.outbox.with_context(|| format!("[[node]] {node} has no outbox"))
    };
    let own = telegram_outbox(cfg, logins, cluster.node, outbox(cluster.node)?, None).await?;
    let mut others = std::collections::HashMap::new();
    for &peer in peers {
        others.insert(peer, telegram_outbox(cfg, logins, peer, outbox(peer)?, Some(cluster.poll)).await?);
    }
    Ok(pigeonhole_node::cluster::chat_network(cluster, own, others))
}

/// Node `node`'s Raft outbox `chat` through `logins`' bots. A peer's
/// (`poll`: read the chat after that long without a push) is read from
/// what Telegram pushes; our own is only posted to.
async fn telegram_outbox(
    cfg: &Config,
    logins: &[TelegramLogin],
    node: u64,
    chat: i64,
    poll: Option<std::time::Duration>,
) -> anyhow::Result<Arc<dyn openraft_chat::Mailbox>> {
    use pigeonhole_raft_telegram::TelegramMailbox;
    let channel = telegram_channel(cfg, logins, &chat.to_string(), Lane::Meta)
        .await
        .with_context(|| format!("Raft outbox {chat}"))?;
    let mailbox = TelegramMailbox::new(Arc::new(channel)).named(pigeonhole_admin::outbox_store(node));
    let Some(poll) = poll else { return Ok(Arc::new(mailbox)) };
    let mailbox = Arc::new(mailbox.polling(poll));
    let pushed = mailbox.clone();
    channel_log::on_post(pigeonhole::telegram::channel_id_of(chat)?, move |id, message| {
        pushed.pushed(id, Some(message));
    });
    Ok(mailbox)
}
