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
        let settings = Settings { cluster: cluster.id, key: cluster.key, tick: cluster.tick, poll: cluster.poll };
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
