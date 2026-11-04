//! The demo's Raft cluster: every node of the config's `[cluster]` in this
//! one process, as the binary runs one per host. Their gRPC links are
//! openraft-grpc's [`Direct`] (no network in a browser), their chat links
//! the fake Telegram outboxes, read through pigeonhole-raft-telegram's
//! mailbox as a server reads real ones, each node with bots of its own
//! (`[[node]] bot_ids`, else one each). A node can be cut off.

use crate::World;
use anyhow::{Context, Result};
use openraft_chat::{ChatNetwork, Mailbox};
use openraft_grpc::{Direct, DirectNetwork, Mixed};
use pigeonhole_config::cluster::ClusterSettings;
use pigeonhole_config::Config;
use pigeonhole_fake_channels::TelegramChannel;
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::Cluster;
use pigeonhole_node::cluster::{chat_network, idle_chat, via_chat};
use pigeonhole_raft_telegram::TelegramMailbox;
use pigeonhole_storage_api::fake::FakeCalls;
use pigeonhole_storage_telegram::fake::bot::FakeBot;
use pigeonhole_storage_telegram::ChannelMessage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

type Outbox = TelegramMailbox<FakeBot<TelegramChannel>>;
/// The readers' copies of one outbox.
type Copies = Arc<Mutex<Vec<Reader>>>;

/// A reader's copy of an outbox, and the way a push reaches it: one after
/// another, each a fake call ("push") taking the fakes' latency, as
/// Telegram's update takes its time to reach a bot.
struct Reader {
    calls: FakeCalls,
    /// When the last push queued reaches the reader: the next one no sooner.
    last: Mutex<Instant>,
    queue: tokio::sync::mpsc::UnboundedSender<(u64, Instant, i64, Option<ChannelMessage>)>,
}

impl Reader {
    fn new(copy: Arc<Outbox>, calls: FakeCalls) -> Self {
        let (queue, mut pushes) = tokio::sync::mpsc::unbounded_channel();
        let done = calls.clone();
        tokio::spawn(async move {
            while let Some((id, when, at, message)) = pushes.recv().await {
                tokio::time::sleep_until(when).await;
                done.done(id, true);
                copy.pushed(at, message);
            }
        });
        Self { calls, last: Mutex::new(Instant::now()), queue }
    }

    /// Post `at` (`bytes` of it) on its way to the reader.
    fn push(&self, at: i64, message: Option<ChannelMessage>, bytes: u64) {
        let (id, delay) = self.calls.draw();
        let when = {
            let mut last = self.last.lock().unwrap();
            *last = (*last).max(Instant::now() + delay);
            *last
        };
        self.calls.begin(id, "push", Some(bytes), when - Instant::now());
        let _ = self.queue.send((id, when, at, message));
    }
}
/// The outboxes as each node sees them, by `(owner, user)`; and by owner,
/// the readers' copies.
type Outboxes = (HashMap<(u64, u64), Arc<dyn Mailbox>>, HashMap<u64, Copies>);

/// One outbox as one node sees it, through that node's bot. A post through
/// any node's copy is pushed to the others at once, message and all, as
/// Telegram pushes a new post to every bot in the channel.
struct Copy {
    own: Arc<Outbox>,
    /// The readers' copies of this outbox (the other nodes').
    all: Copies,
}

#[async_trait::async_trait]
impl Mailbox for Copy {
    async fn post(&self, frame: bytes::Bytes) -> Result<u64> {
        let bytes = frame.len() as u64;
        let (at, message) = self.own.post_message(frame).await?;
        for reader in self.all.lock().unwrap().iter() {
            reader.push(at as i64, message.clone(), bytes);
        }
        Ok(at)
    }
    async fn read_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, bytes::Bytes)>> {
        self.own.read_after(after, limit).await
    }
    async fn head(&self) -> Result<u64> {
        self.own.head().await
    }
    async fn wait(&self, after: u64, timeout: Duration) {
        self.own.wait(after, timeout).await;
    }
    async fn prune(&self, keep_from: u64) -> Result<()> {
        self.own.prune(keep_from).await
    }
}

/// A node's network: Direct to the nodes it has a gRPC link with, the chat to the rest.
pub type Network = Mixed<DirectNetwork<TypeConfig>, ChatNetwork<TypeConfig>, u64>;

/// The links between the nodes of one boot.
pub struct Net {
    direct: Direct<TypeConfig>,
    /// Made under the lock: the nodes boot at once.
    outboxes: tokio::sync::Mutex<Outboxes>,
}

impl Default for Net {
    fn default() -> Self {
        Self { direct: Direct::new(), outboxes: Default::default() }
    }
}

impl Net {
    /// Node `owner`'s outbox as node `user` sees it: through `user`'s bot.
    async fn outbox(
        &self,
        world: &World,
        cfg: &Config,
        cluster: &ClusterSettings,
        owner: u64,
        user: u64,
    ) -> Result<Arc<dyn Mailbox>> {
        let mut made = self.outboxes.lock().await;
        let (copies, all) = &mut *made;
        if let Some(m) = copies.get(&(owner, user)) {
            return Ok(m.clone());
        }
        let channel = cluster.outbox_channel(owner).with_context(|| format!("[[node]] {owner}: no fake outbox"))?;
        let bot = cluster.nodes.iter().find(|n| n.id == user).and_then(|n| n.bot_ids.first().copied());
        let bot = bot.with_context(|| format!("[[node]] {user}: no bot"))?;
        let own = Arc::new(
            TelegramMailbox::new(Arc::new(world.fakes.telegram_bot(cfg, &channel, bot, user).await?))
                .named(pigeonhole_admin::outbox_store(owner))
                .polling(cluster.poll),
        );
        let all = all.entry(owner).or_default().clone();
        if user != owner {
            let calls = FakeCalls::new(&channel.id, Some(user), world.fakes.latency.clone());
            all.lock().unwrap().push(Reader::new(own.clone(), calls));
        }
        let copy: Arc<dyn Mailbox> = Arc::new(Copy { own, all });
        copies.insert((owner, user), copy.clone());
        Ok(copy)
    }

    /// Node `cluster.node`'s side of the cluster, for its tablet.
    pub async fn cluster(&self, world: &World, cfg: &Config, cluster: &ClusterSettings) -> Result<Cluster<Network>> {
        let me = cluster.node;
        let peers = via_chat(cluster);
        let chat: ChatNetwork<TypeConfig> = if peers.is_empty() {
            idle_chat(cluster)
        } else {
            let mut outboxes = HashMap::new();
            for &peer in &peers {
                outboxes.insert(peer, self.outbox(world, cfg, cluster, peer, me).await?);
            }
            chat_network(cluster, self.outbox(world, cfg, cluster, me, me).await?, outboxes)
        };
        let hub = self.direct.clone();
        let direct = self.direct.network(me);
        Ok(pigeonhole_node::cluster::cluster(cluster, direct, chat, move |raft| hub.serve(me, raft)))
    }

    /// Cuts node `node` off from the others, or brings it back: its direct
    /// links, and its outbox (the channel down: its frames do not go out).
    /// A direct link's frame takes `range` each way.
    pub fn set_link(&self, range: (Duration, Duration)) {
        self.direct.set_delay(Some(range));
    }

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
        // The fakes' delays as the file has them (the page may change them as the nodes run).
        let fakes = |cfg: &Config| {
            world.fakes.latency.set(cfg.fake.latency);
            world.link.set(cfg.fake.link);
            net.set_link(cfg.fake.link);
        };
        let ids = Config::node_ids(text)?;
        if ids.is_empty() {
            let cfg = crate::parse_config(text)?;
            fakes(&cfg);
            let node = crate::Node::open(world, &cfg).await?;
            return Ok(Self { nodes: [(1, node)].into(), net, cluster: None });
        }
        let configs = ids.iter().map(|&id| crate::parse_config_as(text, id)).collect::<Result<Vec<_>>>()?;
        fakes(&configs[0]);
        let cluster = configs[0].cluster.clone();
        // The lowest id starts a fresh cluster's first election: polled
        // last, it finds the others serving already. Polled first, its votes
        // went to nodes not up yet, and a lost first election costs an
        // election timeout (seconds).
        let opened =
            futures::future::join_all(configs.iter().rev().map(|cfg| crate::Node::open_in(world, cfg, &net))).await;
        let (mut nodes, mut failed) = (std::collections::BTreeMap::new(), None);
        for (id, node) in ids.into_iter().zip(opened.into_iter().rev()) {
            match node {
                Ok(node) => {
                    nodes.insert(id, node);
                }
                Err(e) => {
                    failed.get_or_insert_with(|| e.context(format!("node {id}")));
                }
            }
        }
        let nodes = Self { nodes, net, cluster };
        match failed {
            None => Ok(nodes),
            // The nodes that did open let their logs go: the next boot claims them at once.
            Some(e) => {
                if let Err(close) = nodes.close().await {
                    tracing::warn!(error = %format!("{close:#}"), "closing the nodes of a cluster that did not open");
                }
                Err(e)
            }
        }
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

    /// A direct link's frame takes `range` each way, from now on.
    pub fn set_link(&self, range: (Duration, Duration)) {
        self.net.set_link(range);
    }

    /// Cuts node `node` off from the others, or brings it back.
    pub fn isolate(&self, world: &World, node: u64, cut: bool) -> Result<()> {
        let cluster = self.cluster.as_ref().context("isolate: one node, no cluster")?;
        anyhow::ensure!(self.nodes.contains_key(&node), "no node {node}");
        self.net.isolate(world, cluster, node, cut);
        Ok(())
    }

    /// Stops every node, as their processes' exits would; the first
    /// error, once every node was asked.
    pub async fn close(&self) -> Result<()> {
        let mut first = Ok(());
        for n in self.nodes.values() {
            let closed = n.close().await;
            if first.is_ok() {
                first = closed;
            }
        }
        first
    }
}
