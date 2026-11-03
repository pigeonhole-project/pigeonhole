//! The parts of a cluster's network every root builds alike
//! (docs/cluster.md § Transports): the chat's settings, which peers go
//! over it, and the tablet's [`Cluster`] mixing a direct transport (gRPC,
//! or the demo's in-process one) with the chat. The outboxes are the
//! root's: Telegram channels, or fakes.

use openraft_chat::{ChatNetwork, FakeChat, Mailbox, Settings};
use openraft_grpc::Mixed;
use pigeonhole_config::cluster::{ClusterSettings, Link};
use pigeonhole_meta::raft::{NodeId, Raft, TypeConfig};
use pigeonhole_meta::{Cluster, Serve};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The chat transport of this node's cluster: its tick and poll, and how
/// long frames are kept (past twice the longest election timeout no call
/// waits for one).
pub fn chat_settings(c: &ClusterSettings) -> Settings {
    Settings { cluster: c.id, key: c.key, tick: c.tick, poll: c.poll, keep: Some(2 * c.election_timeout.1) }
}

/// The peers this node reaches over the chat (the others: directly).
pub fn via_chat(c: &ClusterSettings) -> HashSet<NodeId> {
    c.links().into_iter().filter(|(_, l)| *l == Link::Chat).map(|(id, _)| id).collect()
}

/// The chat side of this node's network: its own outbox and its chat
/// peers'. Without such peers, an idle one (`own`, `peers` unused).
pub fn chat_network(
    c: &ClusterSettings,
    own: Arc<dyn Mailbox>,
    peers: HashMap<NodeId, Arc<dyn Mailbox>>,
) -> ChatNetwork<TypeConfig> {
    ChatNetwork::new(c.node, chat_settings(c), own, peers)
}

/// [`chat_network`] for a node with no chat peer.
pub fn idle_chat(c: &ClusterSettings) -> ChatNetwork<TypeConfig> {
    chat_network(c, FakeChat::new().mailbox(c.node, c.node), HashMap::new())
}

/// The tablet's place in cluster `c`: `direct` to the peers not in
/// [`via_chat`], `chat` to those; `serve_direct` hands the node's Raft to
/// the direct transport's receiving side (the chat's is served here).
pub fn cluster<A>(
    c: &ClusterSettings,
    direct: A,
    chat: ChatNetwork<TypeConfig>,
    serve_direct: impl FnOnce(Arc<Raft>) + Send + 'static,
) -> Cluster<Mixed<A, ChatNetwork<TypeConfig>, NodeId>> {
    let serving = chat.clone();
    let serve: Serve = Box::new(move |raft| {
        Box::pin(async move {
            let raft = Arc::new(raft);
            serve_direct(raft.clone());
            serving.serve(raft).await;
        })
    });
    let members = c.nodes.iter().map(|n| n.id).collect();
    let network = Mixed { a: direct, b: chat, via_b: via_chat(c) };
    Cluster { node: c.node, members, network, serve: Some(serve) }
}
