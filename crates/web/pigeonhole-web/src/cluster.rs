//! The demo's Raft cluster: every node of the config's `[cluster]` in this
//! one process, as the binary runs one per host. Their gRPC links are
//! openraft-grpc's [`Direct`] (no network in a browser), their chat links
//! the fake Telegram outboxes, read through pigeonhole-raft-telegram's
//! mailbox as a server reads real ones. A node can be cut off.

use crate::World;
use anyhow::{Context, Result};
use openraft_chat::{ChatNetwork, FakeChat, Mailbox, Settings};
use openraft_grpc::{Direct, DirectNetwork, Mixed};
use pigeonhole_config::cluster::{ClusterSettings, Link};
use pigeonhole_config::Config;
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{Cluster, Serve};
use pigeonhole_raft_telegram::TelegramMailbox;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A node's network: Direct to the nodes it has a gRPC link with, the chat to the rest.
pub type Network = Mixed<DirectNetwork<TypeConfig>, ChatNetwork<TypeConfig>, u64>;

/// The links between the nodes of one boot.
pub struct Net {
    direct: Direct<TypeConfig>,
    /// Each node's outbox, one mailbox for the process: a post wakes its
    /// readers at once. Made under the lock: the nodes boot at once.
    outboxes: tokio::sync::Mutex<HashMap<u64, Arc<dyn Mailbox>>>,
}

impl Default for Net {
    fn default() -> Self {
        Self { direct: Direct::new(), outboxes: Default::default() }
    }
}

impl Net {
    async fn outbox(
        &self,
        world: &World,
        cfg: &Config,
        cluster: &ClusterSettings,
        node: u64,
    ) -> Result<Arc<dyn Mailbox>> {
        let mut made = self.outboxes.lock().await;
        if let Some(m) = made.get(&node) {
            return Ok(m.clone());
        }
        let channel = cluster.outbox_channel(node).with_context(|| format!("[[node]] {node}: no fake outbox"))?;
        let bot = world.fakes.telegram_bot(cfg, &channel).await?;
        let mailbox: Arc<dyn Mailbox> = Arc::new(TelegramMailbox::new(Arc::new(bot)));
        made.insert(node, mailbox.clone());
        Ok(mailbox)
    }

    /// Node `cluster.node`'s side of the cluster, for its tablet.
    pub async fn cluster(&self, world: &World, cfg: &Config, cluster: &ClusterSettings) -> Result<Cluster<Network>> {
        let me = cluster.node;
        let via_chat: HashSet<u64> =
            cluster.links().into_iter().filter(|(_, l)| *l == Link::Chat).map(|(id, _)| id).collect();
        let settings = Settings {
            cluster: cluster.id,
            key: cluster.key,
            tick: cluster.tick,
            poll: cluster.poll,
            keep: Some(2 * cluster.election_timeout.1),
        };
        let chat = if via_chat.is_empty() {
            let idle = FakeChat::new();
            ChatNetwork::new(me, settings, idle.mailbox(me, me), HashMap::new())
        } else {
            let mut peers = HashMap::new();
            for &peer in &via_chat {
                peers.insert(peer, self.outbox(world, cfg, cluster, peer).await?);
            }
            ChatNetwork::new(me, settings, self.outbox(world, cfg, cluster, me).await?, peers)
        };
        let (hub, serving) = (self.direct.clone(), chat.clone());
        let serve: Serve = Box::new(move |raft| {
            Box::pin(async move {
                let raft = Arc::new(raft);
                hub.serve(me, raft.clone());
                serving.serve(raft).await;
            })
        });
        let network = Mixed { a: self.direct.network(me), b: chat, via_b: via_chat };
        let members = cluster.nodes.iter().map(|n| n.id).collect();
        Ok(Cluster { node: me, members, network, serve: Some(serve) })
    }

    /// Cuts node `node` off from the others, or brings it back: its direct
    /// links, and its outbox (the channel down: its frames do not go out).
    pub fn isolate(&self, world: &World, cluster: &ClusterSettings, node: u64, cut: bool) {
        self.direct.isolate(node, cut);
        if let Some(outbox) = cluster.outbox_channel(node) {
            world.fakes.set_down(&outbox, cut);
        }
    }
}

/// The nodes of one boot: one, or every node of a cluster, and their links.
pub struct Nodes {
    pub nodes: std::collections::BTreeMap<u64, crate::Node>,
    net: Net,
    cluster: Option<ClusterSettings>,
}

impl Nodes {
    /// Boots the config `text` describes: one node (node 1), or every
    /// `[[node]]` of its cluster, each reading the one file as itself, all
    /// at once (a tablet waits for a leader).
    pub async fn open(world: &Arc<World>, text: &str) -> Result<Self> {
        let net = Net::default();
        let ids = Config::node_ids(text)?;
        if ids.is_empty() {
            let node = crate::Node::open(world, &crate::parse_config(text)?).await?;
            return Ok(Self { nodes: [(1, node)].into(), net, cluster: None });
        }
        let configs = ids.iter().map(|&id| crate::parse_config_as(text, id)).collect::<Result<Vec<_>>>()?;
        let cluster = configs[0].cluster.clone();
        let opened = futures::future::join_all(configs.iter().map(|cfg| crate::Node::open_in(world, cfg, &net))).await;
        let nodes = ids.into_iter().zip(opened).map(|(id, n)| Ok((id, n?))).collect::<Result<_>>()?;
        Ok(Self { nodes, net, cluster })
    }

    /// The node that leads, if one does. A leader cut off still thinks it
    /// leads until it hears otherwise: the newest term's is the one.
    pub fn leader(&self) -> Option<u64> {
        let leaders = self.nodes.iter().filter(|(_, n)| n.admin.meta.is_leader());
        leaders.max_by_key(|(_, n)| n.admin.meta.cluster_view().term).map(|(id, _)| *id)
    }

    /// Node `id`; without one, the leader (or the first, while none leads).
    pub fn node(&self, id: Option<u64>) -> Result<&crate::Node> {
        let id = id.or_else(|| self.leader()).or_else(|| self.nodes.keys().next().copied());
        id.and_then(|id| self.nodes.get(&id)).with_context(|| format!("no node {id:?}"))
    }

    /// Cuts node `node` off from the others, or brings it back.
    pub fn isolate(&self, world: &World, node: u64, cut: bool) -> Result<()> {
        let cluster = self.cluster.as_ref().context("isolate: one node, no cluster")?;
        anyhow::ensure!(self.nodes.contains_key(&node), "no node {node}");
        self.net.isolate(world, cluster, node, cut);
        Ok(())
    }

    /// Stops every node, as their processes' exits would.
    pub async fn close(&self) -> Result<()> {
        for n in self.nodes.values() {
            n.close().await?;
        }
        Ok(())
    }
}
