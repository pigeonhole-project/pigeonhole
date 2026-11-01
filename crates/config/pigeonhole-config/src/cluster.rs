//! `[cluster]` and `[[node]]`: the metadata tablet as one node of a Raft
//! cluster (docs/raft-over-chat.md § Transports). Without them a process is
//! a cluster of one.
//!
//! Every node runs with the same `[cluster]` and `[[node]]` list (and the
//! same blob stores); each has a `[meta]` log and snapshots of its own, and
//! knows which node it is from `[cluster] node` or `PIGEONHOLE_NODE`. Two
//! nodes talk over gRPC when both have `grpc`, else over the chat when both
//! have an `outbox`.

use crate::stores::{Store, Topology};
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

/// One member of the cluster.
#[derive(Clone, Debug)]
pub struct NodeSettings {
    pub id: u64,
    /// `host:port` it serves Raft on and the others call it at.
    pub grpc: Option<String>,
    /// The channel it posts its Raft frames into (a telegram or discord store).
    pub outbox: Option<Store>,
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
    pub heartbeat: Duration,
    pub election_timeout: (Duration, Duration),
    /// The chat transport's tick and poll.
    pub tick: Duration,
    pub poll: Duration,
}

impl ClusterSettings {
    pub fn me(&self) -> &NodeSettings {
        self.nodes.iter().find(|n| n.id == self.node).expect("checked when read")
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileNode {
    id: u64,
    grpc: Option<String>,
    /// A `[[store]]` id.
    outbox: Option<String>,
}

/// Timing for the slowest link: heartbeat, election timeout, tick, poll
/// (docs/raft-over-chat.md § Timing).
struct Timing(Duration, (Duration, Duration), Duration, Duration);

const fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn timing(nodes: &[NodeSettings]) -> Timing {
    let chats: Vec<&Store> = nodes.iter().filter_map(|n| n.outbox.as_ref()).collect();
    let uses_chat = nodes.iter().any(|a| nodes.iter().any(|b| a.id != b.id && link(a, b) == Some(Link::Chat)));
    match (uses_chat, chats.iter().any(|s| matches!(s, Store::Telegram { .. }))) {
        (false, _) => Timing(ms(250), (ms(1_500), ms(3_000)), ms(20), ms(200)),
        (true, true) => Timing(ms(10_000), (ms(45_000), ms(90_000)), ms(4_000), ms(3_000)),
        (true, false) => Timing(ms(3_000), (ms(15_000), ms(30_000)), ms(1_000), ms(1_000)),
    }
}

impl FileCluster {
    /// `None` without `[[node]]`s: a cluster of one.
    pub(crate) fn into_settings(
        self,
        nodes: Vec<FileNode>,
        topology: &mut Topology,
    ) -> Result<Option<ClusterSettings>> {
        if nodes.is_empty() {
            return Ok(None);
        }
        let mut ids = BTreeSet::new();
        let mut settings = Vec::new();
        for n in nodes {
            if !ids.insert(n.id) {
                bail!("[[node]] id {} twice", n.id);
            }
            let outbox = match n.outbox {
                None => None,
                Some(id) => match topology.meta_store(&id, "[[node]] outbox")? {
                    s @ (Store::Telegram { .. } | Store::Discord { .. } | Store::Fake(_)) => Some(s),
                    _ => bail!("[[node]] {}: outbox {id:?} must be a telegram or discord store", n.id),
                },
            };
            settings.push(NodeSettings { id: n.id, grpc: n.grpc, outbox });
        }
        for a in &settings {
            for b in settings.iter().filter(|b| b.id > a.id) {
                if link(a, b).is_none() {
                    bail!("[[node]] {} and {} share no transport: give both `grpc`, or both an `outbox`", a.id, b.id);
                }
            }
        }
        let node = match std::env::var("PIGEONHOLE_NODE") {
            Ok(v) => v.parse().with_context(|| format!("PIGEONHOLE_NODE={v:?}"))?,
            Err(_) => self.node.context("[cluster] node (or PIGEONHOLE_NODE): which [[node]] this process is")?,
        };
        if !ids.contains(&node) {
            bail!("this node, {node}, is not a [[node]]");
        }
        let key_env = self.key_env.context("[cluster] key_env: the env var with the cluster's key")?;
        let key = parse_key(&std::env::var(&key_env).with_context(|| format!("{key_env} is not set"))?)
            .with_context(|| key_env.clone())?;
        let Timing(heartbeat, election, tick, poll) = timing(&settings);
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
            nodes: settings,
            heartbeat,
            election_timeout,
            tick: self.tick.unwrap_or(tick),
            poll: self.poll.unwrap_or(poll),
        }))
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
