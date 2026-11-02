//! The node's stores as its config names them, and the views of them the
//! UI draws ([`Channels`]): the plain list, a local disk's files and the
//! records in them, several views as one.

use crate::Channels;
use anyhow::{bail, Result};
use async_trait::async_trait;
use pigeonhole_config::stores::FakeChannel;
use pigeonhole_config::{Config, MetaPlace};
use pigeonhole_meta::log::chain::{summarize, BlobSummary};
use pigeonhole_storage_api::inspect::SegmentView;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One store of the config, and what the node uses it for.
#[derive(Clone, Debug)]
pub struct StoreRef {
    pub store: String,
    /// `data` (a chunk store), `log`, `snapshots`, or `outbox` (a node's
    /// Raft frames, in a cluster over the chat).
    pub role: &'static str,
    /// `telegram`, `discord`, `local_disk`, `memory`.
    pub kind: &'static str,
    /// The chat or channel id; a local disk's path.
    pub scope: String,
    /// How many bots post to it.
    pub bots: usize,
    /// A data store's place: its shard of how many, its copy of how many.
    pub place: Option<Place>,
    /// The fake channel it is, if it is one.
    pub fake: Option<FakeChannel>,
    /// A local disk's directory of segments.
    pub dir: Option<PathBuf>,
    /// The cluster node it belongs to (its log, snapshots, outbox); none:
    /// the cluster's (the chunk stores the leader writes).
    pub node: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Place {
    pub shard: usize,
    pub shards: usize,
    pub copy: usize,
    pub copies: usize,
}

impl StoreRef {
    /// What every view says of the store, before what it holds.
    pub fn json(&self) -> Value {
        let place =
            self.place.map(|p| json!({ "shard": p.shard, "shards": p.shards, "copy": p.copy, "copies": p.copies }));
        json!({ "store": self.store, "role": self.role, "kind": self.kind, "scope": self.scope, "bots": self.bots, "place": place, "node": self.node })
    }

    fn meta(role: &'static str, kind: &'static str, scope: String) -> Self {
        let store = if role == "log" { crate::LOG_STORE } else { crate::SNAPSHOT_STORE };
        Self { store: store.into(), role, kind, scope, bots: 1, place: None, fake: None, dir: None, node: None }
    }

    fn fake(f: &FakeChannel, role: &'static str) -> Self {
        Self {
            store: f.id.clone(),
            role,
            kind: f.kind.as_str(),
            scope: f.scope.clone(),
            bots: f.bots,
            place: None,
            fake: Some(f.clone()),
            dir: None,
            node: None,
        }
    }

    fn named(self, store: String) -> Self {
        Self { store, ..self }
    }

    /// A metadata log or snapshots in `path` (`local_disk`): in its `log/`
    /// or `snapshots/`, one directory each.
    fn disk(role: &'static str, path: &Path) -> Self {
        let dir = path.join(role);
        Self { scope: dir.display().to_string(), dir: Some(dir), ..Self::meta(role, "local_disk", String::new()) }
    }
}

/// Every store the node uses, in the config's order: the chunk stores of
/// the placement, then the metadata log and snapshots.
pub fn of(cfg: &Config) -> Vec<StoreRef> {
    let shards = &cfg.placement.shards;
    let place = |id: &str| {
        shards.iter().enumerate().find_map(|(shard, s)| {
            let copy = s.group.iter().position(|m| m == id)?;
            Some(Place { shard, shards: shards.len(), copy, copies: s.group.len() })
        })
    };
    let mut out: Vec<StoreRef> = cfg
        .instances
        .iter()
        .filter_map(|i| {
            // A store outside the placement backs something else (a fake channel's tablet).
            let place = Some(place(&i.info.id)?);
            let bots = i.fake.as_ref().map_or_else(|| i.bot_token_envs.len().max(1), |f| f.bots);
            let local = i.kind.as_str() == "local_disk";
            Some(StoreRef {
                store: i.info.id.clone(),
                role: "data",
                kind: i.kind.as_str(),
                scope: i.scope_id.clone(),
                bots,
                place,
                fake: i.fake.clone(),
                dir: local.then(|| PathBuf::from(&i.scope_id)),
                node: None,
            })
        })
        .collect();
    // This node's own log and snapshots: under its dovecote in a cluster's
    // graph, by names no other node's have (meta-log-2; a fake channel's
    // id is its own already).
    let me = cfg.cluster.as_ref().map(|c| c.node);
    for (role, place) in [("log", cfg.meta.log.place()), ("snapshots", cfg.meta.snapshot.place())] {
        let s = match place {
            MetaPlace::Fake(f) => StoreRef::fake(f, role),
            MetaPlace::LocalDisk(path) => StoreRef::disk(role, path),
            MetaPlace::Chat { kind, scope } => StoreRef::meta(role, kind, scope.to_string()),
            MetaPlace::Memory => StoreRef::meta(role, "memory", String::new()),
        };
        let s = match me {
            Some(node) if s.fake.is_none() => {
                let store = format!("{}-{node}", s.store);
                s.named(store)
            }
            _ => s,
        };
        out.push(StoreRef { node: me, ..s });
    }
    // Every node's outbox: the channel its Raft frames go through (a fake
    // one in the demo, read as the other fakes are).
    for (cluster, node) in cfg.cluster.iter().flat_map(|c| c.nodes.iter().map(move |n| (c, n))) {
        if let Some(chat) = node.outbox {
            let store = crate::outbox_store(node.id);
            let fake = cluster.outbox_channel(node.id);
            let outbox = StoreRef::meta("outbox", "telegram", chat.to_string());
            out.push(StoreRef { node: Some(node.id), fake, ..outbox }.named(store));
        }
    }
    out
}

/// What a post (or a record) of the log's store is to the log: a probe of
/// the chain's order, a generation's claim, or a batch of entries (with its
/// summary: which positions).
pub fn log_post(name: &str, summary: Option<BlobSummary>) -> Value {
    let label = name.split('~').next().unwrap_or(name);
    let gen = |prefix: &str| label.strip_prefix(prefix).and_then(|g| g.parse::<u64>().ok());
    if label.starts_with("chain-probe") {
        json!({ "what": "probe" })
    } else if let Some(g) = gen("log-claim-") {
        json!({ "what": "claim", "gen": g })
    } else if let Some(g) = gen("log-batch-") {
        match summary {
            Some(b) if b.entries > 0 => {
                json!({ "what": "batch", "gen": g, "first": b.first, "last": b.first + b.entries as u64 - 1 })
            }
            Some(b) => json!({ "what": "barrier", "gen": g, "first": b.first }),
            None => json!({ "what": "batch", "gen": g }),
        }
    } else {
        json!({ "what": "other", "label": label })
    }
}

/// The stores listed, holding nothing the node reads (real channels).
pub struct ListedStores(pub Vec<StoreRef>);

#[async_trait]
impl Channels for ListedStores {
    async fn describe(&self, _limit: usize) -> Result<Value> {
        let channels: Vec<Value> = self.0.iter().map(|s| with(s.json(), "messages", json!([]))).collect();
        Ok(json!({ "channels": channels }))
    }

    fn holds(&self, store: &str) -> bool {
        self.0.iter().any(|s| s.store == store)
    }

    async fn delete_message(&self, store: &str, _id: &str) -> Result<()> {
        bail!("{store}: this node does not read its channels")
    }
}

/// Reads a directory of segments: `want(label)` says which records' data to
/// read too (pigeonhole_storage_disk::inspect on the real file system).
pub type Inspect = fn(&Path, &dyn Fn(&str) -> bool) -> Result<Vec<SegmentView>>;

/// The local disks: their segment files and the records in them, the log's
/// batches decoded. `Id`: how the disk names a blob (the chain's links).
pub struct DiskView<Id> {
    stores: Vec<StoreRef>,
    inspect: Inspect,
    id: PhantomData<fn() -> Id>,
}

impl<Id> DiskView<Id> {
    /// The disks among `stores`.
    pub fn new(stores: &[StoreRef], inspect: Inspect) -> Self {
        Self { stores: stores.iter().filter(|s| s.dir.is_some()).cloned().collect(), inspect, id: PhantomData }
    }
}

/// A log batch's record: its data is wanted, to say which positions it holds.
fn is_batch(label: &str) -> bool {
    label.starts_with("log-batch-")
}

impl<Id: Serialize + DeserializeOwned> DiskView<Id> {
    /// Store `s`'s files, the last `limit` records in all.
    fn describe_one(&self, s: &StoreRef, limit: usize) -> Value {
        let dir = s.dir.as_deref().expect("a disk");
        let log = s.role == "log";
        let segments = match (self.inspect)(dir, &|label| log && is_batch(label)) {
            Ok(segments) => segments,
            // Not there yet (nothing written), or unreadable: no files.
            Err(e) => return with(with(s.json(), "files", json!([])), "error", json!(format!("{e:#}"))),
        };
        let total: usize = segments.iter().map(|g| g.records.iter().filter(|r| !r.dead).count()).sum();
        let mut skip = segments.iter().map(|g| g.records.len()).sum::<usize>().saturating_sub(limit);
        let files: Vec<Value> = segments
            .iter()
            .map(|g| {
                let shown = g.records.len().saturating_sub(skip);
                skip -= g.records.len() - shown;
                let records: Vec<Value> = g.records[g.records.len() - shown..]
                    .iter()
                    .map(|r| {
                        let mut v = json!({ "key": r.key, "name": r.label, "size": r.size, "dead": r.dead, "chained": r.chained });
                        if log {
                            let summary = r.data.as_deref().and_then(|d| summarize::<Id>(d).ok());
                            v["log"] = log_post(&r.label, summary);
                        }
                        v
                    })
                    .collect();
                json!({ "name": g.name, "size": g.size, "records": records })
            })
            .collect();
        with(with(s.json(), "files", json!(files)), "total", json!(total))
    }
}

#[async_trait]
impl<Id: Serialize + DeserializeOwned + 'static> Channels for DiskView<Id> {
    async fn describe(&self, limit: usize) -> Result<Value> {
        Ok(json!({ "channels": self.stores.iter().map(|s| self.describe_one(s, limit)).collect::<Vec<_>>() }))
    }

    fn holds(&self, store: &str) -> bool {
        self.stores.iter().any(|s| s.store == store)
    }

    async fn delete_message(&self, store: &str, _id: &str) -> Result<()> {
        bail!("{store}: a local disk's records are the node's own")
    }
}

/// Several views as one, the stores in the config's order: each store is
/// drawn by the first view that holds it.
pub struct Views {
    pub order: Vec<StoreRef>,
    pub views: Vec<Arc<dyn Channels>>,
}

impl Views {
    fn holder(&self, store: &str) -> Result<&Arc<dyn Channels>> {
        self.views.iter().find(|v| v.holds(store)).ok_or_else(|| anyhow::anyhow!("no store {store:?}"))
    }
}

#[async_trait]
impl Channels for Views {
    async fn describe(&self, limit: usize) -> Result<Value> {
        let mut drawn: HashMap<String, Value> = HashMap::new();
        for v in &self.views {
            for c in v.describe(limit).await?["channels"].as_array().into_iter().flatten() {
                if let Some(store) = c["store"].as_str() {
                    drawn.entry(store.to_string()).or_insert_with(|| c.clone());
                }
            }
        }
        Ok(json!({ "channels": self.order.iter().filter_map(|s| drawn.remove(&s.store)).collect::<Vec<_>>() }))
    }

    fn holds(&self, store: &str) -> bool {
        self.views.iter().any(|v| v.holds(store))
    }

    async fn delete_message(&self, store: &str, id: &str) -> Result<()> {
        self.holder(store)?.delete_message(store, id).await
    }

    async fn fault(&self, op: &str, store: &str, down: bool) -> Result<Value> {
        self.holder(store)?.fault(op, store, down).await
    }
}

fn with(mut v: Value, key: &str, value: Value) -> Value {
    v[key] = value;
    v
}
