//! `[cluster]` and `[[node]]`: the metadata tablet as one node of a Raft
//! cluster (docs/cluster.md § Transports). Without them a process is
//! a cluster of one.
//!
//! Every node runs with the same `[cluster]` and `[[node]]` list (and the
//! same blob stores); each has a `[meta]` log and snapshots of its own, and
//! knows which node it is from `[cluster] node` or `PIGEONHOLE_NODE`. Two
//! nodes talk over gRPC when both have `grpc`, else over the chat when both
//! have an `outbox`: a channel each posts its frames into, and every other
//! node reads, all with their own bots (`[cluster] bot_token_envs`, its
//! values set on each host apart). The file names every node's bots by id
//! (`[[node]] bot_ids`): no two nodes share one, none is a store's, and a
//! node starts only if its tokens are the bots the file says.

use crate::instances::TelegramLogin;
use crate::stores::{check_bot_ids, telegram_bot_id, FakeBacking, FakeChannel, TelegramBots};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// How two nodes talk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Link {
    Grpc,
    /// Through their outboxes (channels).
    Chat,
}

impl Link {
    pub fn as_str(self) -> &'static str {
        match self {
            Link::Grpc => "grpc",
            Link::Chat => "chat",
        }
    }
}

/// One member of the cluster.
#[derive(Clone, Debug)]
pub struct NodeSettings {
    pub id: u64,
    /// `host:port` it serves Raft on and the others call it at.
    pub grpc: Option<String>,
    /// The channel it posts its Raft frames into (`-100…`).
    pub outbox: Option<i64>,
    /// Its bots in the chat of the outboxes, by id (fake outboxes: its
    /// fake bots, by default one of its own, [`fake_raft_bot`]).
    pub bot_ids: Vec<u64>,
}

/// A node's fake bot on the fake outboxes when `bot_ids` names none: one
/// per node, far from a fake channel's (1, 2, …).
pub fn fake_raft_bot(node: u64) -> u64 {
    1_000_000 + node
}

/// The chat the outboxes are in, and this node's bots there.
#[derive(Clone, Debug)]
pub enum ChatSettings {
    /// The primary bot first; each reads and posts for the node.
    Telegram { logins: Vec<TelegramLogin> },
    /// Fake Telegram channels ([`ClusterSettings::outbox_channel`]; the
    /// feature `fake`): no bots, the site's demo.
    Fake,
}

/// The cluster as this node sees it.
#[derive(Clone, Debug)]
pub struct ClusterSettings {
    /// This node.
    pub node: u64,
    /// Frames of another cluster sharing a channel are dropped.
    pub id: u128,
    /// The secret every frame is sealed with.
    pub key: [u8; 32],
    pub nodes: Vec<NodeSettings>,
    /// Set when this node has a chat link.
    pub chat: Option<ChatSettings>,
    pub heartbeat: Duration,
    pub election_timeout: (Duration, Duration),
    /// The chat transport's tick and poll.
    pub tick: Duration,
    pub poll: Duration,
    /// Where fake outboxes keep themselves (`[cluster] fake`).
    pub fake: Option<FakeBacking>,
}

impl ClusterSettings {
    /// Node `node`'s outbox as a fake Telegram channel, with `[cluster] fake`.
    pub fn outbox_channel(&self, node: u64) -> Option<FakeChannel> {
        let fake = self.fake.as_ref()?;
        let owner = self.nodes.iter().find(|n| n.id == node)?;
        let chat = owner.outbox?;
        let backing = match fake {
            FakeBacking::Dir(dir) => FakeBacking::Dir(dir.join(format!("telegram-{chat}"))),
            other => other.clone(),
        };
        Some(FakeChannel {
            id: format!("outbox-{node}"),
            kind: crate::instances::InstanceKind::Telegram,
            scope: chat.to_string(),
            backing,
            bots: owner.bot_ids.len(),
            bot_ids: owner.bot_ids.clone(),
        })
    }

    pub fn me(&self) -> &NodeSettings {
        self.nodes.iter().find(|n| n.id == self.node).expect("checked when read")
    }

    /// How every two nodes talk, `(a, b, link)` with `a < b`.
    pub fn pairs(&self) -> Vec<(u64, u64, Link)> {
        let mut out = Vec::new();
        for a in &self.nodes {
            for b in self.nodes.iter().filter(|b| b.id > a.id) {
                out.push((a.id, b.id, link(a, b).expect("checked when read")));
            }
        }
        out
    }

    /// How this node talks to each other one.
    pub fn links(&self) -> BTreeMap<u64, Link> {
        let me = self.me();
        self.nodes.iter().filter(|n| n.id != me.id).map(|n| (n.id, link(me, n).expect("checked when read"))).collect()
    }
}

fn link(a: &NodeSettings, b: &NodeSettings) -> Option<Link> {
    match (a.grpc.is_some() && b.grpc.is_some(), a.outbox.is_some() && b.outbox.is_some()) {
        (true, _) => Some(Link::Grpc),
        (false, true) => Some(Link::Chat),
        (false, false) => None,
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct FileCluster {
    /// This node's id; `PIGEONHOLE_NODE` wins.
    node: Option<u64>,
    /// The cluster's id (any number the nodes agree on).
    id: u64,
    /// The env var holding the key: 64 hex digits.
    key_env: Option<String>,
    #[serde(with = "humantime_serde")]
    heartbeat: Option<Duration>,
    #[serde(with = "humantime_serde")]
    election_timeout_min: Option<Duration>,
    #[serde(with = "humantime_serde")]
    election_timeout_max: Option<Duration>,
    #[serde(with = "humantime_serde")]
    tick: Option<Duration>,
    #[serde(with = "humantime_serde")]
    poll: Option<Duration>,
    /// The chat of the outboxes: `"telegram"` (the default).
    chat: Option<String>,
    /// Fake outboxes (builds with the feature `fake`): `"memory"`, or the
    /// local_disk store they keep themselves in; no bots.
    fake: Option<String>,
    /// Env vars with this node's bots' tokens; default `["RAFT_BOT_TOKEN"]`.
    bot_token_envs: Option<Vec<String>>,
    api_id_env: Option<String>,
    api_hash_env: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileNode {
    id: u64,
    grpc: Option<String>,
    /// A channel's id (`-100…`).
    outbox: Option<i64>,
    /// Its bots in the chat (`[cluster] bot_token_envs` on its host), by
    /// id: required for a node on the chat. No bot is two nodes' or a
    /// store's too (they would share one budget); checked by every node,
    /// and against its own tokens by the node itself.
    pub(crate) bot_ids: Option<Vec<u64>>,
    /// Its own log and snapshots (store ids, as `[meta]` names them): one
    /// file serves the whole cluster, each node finds its own here.
    log: Option<String>,
    snapshot: Option<String>,
    /// Its own `[meta]` knobs (`None`: `[meta]`'s): its log may be a disk
    /// or a chat, so how often it snapshots and how much it keeps is its own.
    #[serde(default, deserialize_with = "crate::units::count_or_off_set")]
    pub(crate) snapshot_every_entries: Option<Option<u64>>,
    #[serde(default, deserialize_with = "crate::units::duration_or_off_set")]
    pub(crate) snapshot_every: Option<Option<Duration>>,
    pub(crate) keep_snapshots: Option<usize>,
    #[serde(default, deserialize_with = "crate::units::count_or_off_set")]
    pub(crate) keep_log_entries: Option<Option<u64>>,
    #[serde(default, deserialize_with = "crate::units::duration_or_off_set")]
    pub(crate) lease: Option<Option<Duration>>,
}

impl FileNode {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

/// What the running node takes from the `[[node]]` list before the stores
/// are resolved: its own log and snapshots, and the stores that are other
/// nodes' (checked, never opened: their bots' secrets are their hosts').
#[derive(Default)]
pub(crate) struct NodeStores {
    pub log: Option<String>,
    pub snapshot: Option<String>,
    /// Store id → whose log or snapshots it is (`[[node]] 2 log`).
    pub others: std::collections::HashMap<String, String>,
}

impl FileCluster {
    /// Which `[[node]]` this process is: `as_node` (one process running
    /// several, the site's demo), else `PIGEONHOLE_NODE`, else `[cluster]
    /// node`. `None` without `[[node]]`s: a cluster of one.
    pub(crate) fn this_node(&self, nodes: &[FileNode], as_node: Option<u64>) -> Result<Option<u64>> {
        if nodes.is_empty() {
            return Ok(None);
        }
        if let Some(n) = as_node {
            return Ok(Some(n));
        }
        Ok(Some(match std::env::var("PIGEONHOLE_NODE") {
            Ok(v) => v.parse().with_context(|| format!("PIGEONHOLE_NODE={v:?}"))?,
            Err(_) => self.node.context("[cluster] node (or PIGEONHOLE_NODE): which [[node]] this process is")?,
        }))
    }
}

/// Node `me`'s own log and snapshots, and every other node's stores.
pub(crate) fn node_stores(nodes: &[FileNode], me: Option<u64>) -> Result<NodeStores> {
    let mut out = NodeStores::default();
    let mut whose: std::collections::HashMap<&str, String> = Default::default();
    for n in nodes {
        for (what, id) in [("log", &n.log), ("snapshot", &n.snapshot)] {
            let Some(id) = id.as_deref() else { continue };
            let role = format!("[[node]] {} {what}", n.id);
            if let Some(prev) = whose.insert(id, role.clone()) {
                // A node's log and snapshots may share a directory; two nodes may not.
                if !prev.starts_with(&format!("[[node]] {} ", n.id)) {
                    bail!("[[store]] {id} is both {prev} and {role}: each node keeps its own");
                }
            }
            if Some(n.id) == me {
                *(if what == "log" { &mut out.log } else { &mut out.snapshot }) = Some(id.to_string());
            } else {
                out.others.insert(id.to_string(), role);
            }
        }
    }
    Ok(out)
}

/// No node's bot is a store's: the store's posts and the node's Raft
/// frames would share one budget. Checked on the file as written, every
/// node's and every store's, before anything is resolved.
pub(crate) fn check_bots_apart(nodes: &[FileNode], stores: &[crate::stores::FileStore]) -> Result<()> {
    for s in stores {
        for bot in s.declared_bot_ids() {
            if let Some(n) = nodes.iter().find(|n| n.bot_ids.as_deref().unwrap_or_default().contains(&bot)) {
                bail!(
                    "bot {bot} is in the bot_ids of [[node]] {} and of [[store]] {}: a node's Raft bots must be its own (they would share one budget)",
                    n.id,
                    s.store_id()
                );
            }
        }
    }
    Ok(())
}

/// Timing for the slowest link: heartbeat, election timeout, tick, poll
/// (docs/cluster.md § Timing).
struct Timing(Duration, (Duration, Duration), Duration, Duration);

const fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// gRPC alone, or a Telegram link somewhere in the cluster (it paces
/// every node: a leader's heartbeat must reach that follower in time).
fn timing(nodes: &[NodeSettings]) -> Timing {
    let uses_chat = nodes.iter().any(|a| nodes.iter().any(|b| a.id != b.id && link(a, b) == Some(Link::Chat)));
    match uses_chat {
        false => Timing(ms(250), (ms(1_500), ms(3_000)), ms(20), ms(200)),
        true => Timing(ms(10_000), (ms(45_000), ms(90_000)), ms(4_000), ms(3_000)),
    }
}

fn read_nodes(nodes: Vec<FileNode>) -> Result<Vec<NodeSettings>> {
    let mut ids = BTreeSet::new();
    let mut settings = Vec::new();
    for n in nodes {
        if !ids.insert(n.id) {
            bail!("[[node]] id {} twice", n.id);
        }
        settings.push(NodeSettings {
            id: n.id,
            grpc: n.grpc,
            outbox: n.outbox,
            bot_ids: n.bot_ids.unwrap_or_default(),
        });
    }
    let mut owner = BTreeMap::new();
    for n in &settings {
        for &bot in &n.bot_ids {
            if let Some(other) = owner.insert(bot, n.id) {
                bail!(
                    "bot {bot} is in the bot_ids of [[node]] {other} and {}: each node needs bots of its own (they would share one budget)",
                    n.id
                );
            }
        }
    }
    for a in &settings {
        for b in settings.iter().filter(|b| b.id > a.id) {
            if link(a, b).is_none() {
                bail!("[[node]] {} and {} share no transport: give both `grpc`, or both an `outbox`", a.id, b.id);
            }
        }
    }
    Ok(settings)
}

impl FileCluster {
    /// `None` without `[[node]]`s: a cluster of one.
    /// `fake_base(store)`: what `[cluster] fake` names, resolved.
    /// `node`: [`Self::this_node`]'s.
    pub(crate) fn into_settings(
        self,
        nodes: Vec<FileNode>,
        node: Option<u64>,
        fake_base: impl FnOnce(&str) -> Result<FakeBacking>,
    ) -> Result<Option<ClusterSettings>> {
        let Some(node) = node else { return Ok(None) };
        let nodes = read_nodes(nodes)?;
        let me =
            nodes.iter().find(|n| n.id == node).with_context(|| format!("this node, {node}, is not a [[node]]"))?;
        let chat_link = nodes.iter().any(|n| n.id != node && link(me, n) == Some(Link::Chat));
        let fake = match &self.fake {
            None => None,
            Some(_) if !cfg!(feature = "fake") => {
                bail!("[cluster] fake: fake outboxes are only in builds with the `fake` feature")
            }
            Some(_) if self.bot_token_envs.is_some() || self.api_id_env.is_some() => {
                bail!("[cluster] fake outboxes have no real bots: drop bot_token_envs, api_id_env, api_hash_env")
            }
            Some(under) => Some(fake_base(under).context("[cluster] fake")?),
        };
        let mut nodes = nodes;
        let on_chat =
            |n: &NodeSettings, all: &[NodeSettings]| all.iter().any(|m| m.id != n.id && link(n, m) == Some(Link::Chat));
        if fake.is_some() {
            for n in &mut nodes {
                if n.bot_ids.is_empty() {
                    n.bot_ids = vec![fake_raft_bot(n.id)];
                }
            }
        } else {
            let all = nodes.clone();
            if let Some(n) = all.iter().find(|n| on_chat(n, &all) && n.bot_ids.is_empty()) {
                bail!("[[node]] {}: bot_ids = [...] is required for a node on the chat: its bots' ids", n.id);
            }
        }
        let me = nodes.iter().find(|n| n.id == node).expect("checked above");
        let chat = match (chat_link, &fake) {
            (false, _) => None,
            (true, Some(_)) => Some(ChatSettings::Fake),
            (true, None) => Some(self.chat_settings(me)?),
        };
        let key_env = self.key_env.context("[cluster] key_env: the env var with the cluster's key")?;
        let key = parse_key(&std::env::var(&key_env).with_context(|| format!("{key_env} is not set"))?)
            .with_context(|| key_env.clone())?;
        let Timing(heartbeat, election, tick, poll) = timing(&nodes);
        let election_timeout =
            (self.election_timeout_min.unwrap_or(election.0), self.election_timeout_max.unwrap_or(election.1));
        let heartbeat = self.heartbeat.unwrap_or(heartbeat);
        if !(heartbeat < election_timeout.0 && election_timeout.0 < election_timeout.1) {
            bail!("[cluster]: need heartbeat < election_timeout_min < election_timeout_max");
        }
        Ok(Some(ClusterSettings {
            node,
            id: self.id as u128,
            key,
            nodes,
            chat,
            heartbeat,
            election_timeout,
            tick: self.tick.unwrap_or(tick),
            poll: self.poll.unwrap_or(poll),
            fake,
        }))
    }

    /// This node's bots in the chat of the outboxes: its tokens must be
    /// the bots its `[[node]] bot_ids` names.
    fn chat_settings(&self, me: &NodeSettings) -> Result<ChatSettings> {
        match self.chat.as_deref().unwrap_or("telegram") {
            "telegram" => {
                let envs = self.bot_token_envs.clone().unwrap_or_else(|| vec!["RAFT_BOT_TOKEN".into()]);
                if envs.is_empty() {
                    bail!("[cluster] bot_token_envs is empty");
                }
                let bots = TelegramBots {
                    envs: &envs,
                    api_id_env: self.api_id_env.clone(),
                    api_hash_env: self.api_hash_env.clone(),
                    session: None,
                };
                let logins = bots.logins("[cluster]")?;
                let tokens: Vec<&str> = logins.iter().map(|l| l.bot_token.as_str()).collect();
                let what = format!("[cluster] bot_token_envs of [[node]] {}", me.id);
                check_bot_ids(&what, &envs, &tokens, telegram_bot_id, Some(&me.bot_ids))?;
                Ok(ChatSettings::Telegram { logins })
            }
            other => bail!("[cluster] chat = {other:?}: only \"telegram\" for now"),
        }
    }
}

fn parse_key(hex: &str) -> Result<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("the cluster key is 64 hex digits (32 bytes; `openssl rand -hex 32`)");
    }
    let mut key = [0; 32];
    for (i, k) in key.iter_mut().enumerate() {
        *k = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex digits");
    }
    Ok(key)
}
