//! The admin ops over a running node: what the site's UI calls, the same
//! whether the node is the in-browser demo (crates/web, over fake channels)
//! or a server on real hardware (the binary's `/admin/ops`).
//!
//! An op is a JSON request (`{"op": "put", "key": …}`) and, for uploads, a
//! body; the answer is JSON. [`Admin`] holds the node's parts and runs the
//! ops on them; what differs between the two is how its channels are seen
//! ([`Channels`]): fakes the demo can break, or the real ones.

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_chunk_store::{ChunkStore, RepairStats, Repairer, SweepStats, Sweeper};
use pigeonhole_config::DynConfig;
use pigeonhole_gateway_s3::{Index, S3Gateway};
use pigeonhole_meta::{LogStore, MetaStore};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

mod background;
mod cluster;
pub mod log_view;
mod s3;
pub mod stores;

pub use stores::{DiskView, ListedStores, StoreRef, Views};

pub use cluster::{outbox_store, LOG_STORE, SNAPSHOT_STORE};

/// The channels behind the node's stores, as their apps show them.
#[async_trait]
pub trait Channels: Send + Sync {
    /// Every channel the node uses: its messages (the last `limit`), its pins.
    async fn describe(&self, limit: usize) -> Result<Value>;

    /// Whether `store` is one of the stores this view draws.
    fn holds(&self, store: &str) -> bool;

    /// Deletes message `id` (a string: Discord's exceed JS numbers) of
    /// `store`'s channel, as someone in the app would.
    async fn delete_message(&self, store: &str, id: &str) -> Result<()>;

    /// A fault (`outage`, `wipe`): only where the channels are fakes.
    async fn fault(&self, op: &str, store: &str, down: bool) -> Result<Value> {
        let _ = (store, down);
        bail!("{op}: this node's channels are real, and are not broken on purpose")
    }
}

/// One op, as the UI sends it.
#[derive(Deserialize, Debug, Default)]
pub struct Request {
    pub op: String,
    #[serde(default = "demo")]
    pub bucket: String,
    /// The object's key; for the dynamic config, the setting's.
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub range: Option<(u64, u64)>,
    /// `pigeonhole.toml` for `open` / `restart` (the demo's own ops).
    #[serde(default)]
    pub config: Option<String>,
    /// The body's length (the worker reads it from stdin after the line).
    #[serde(default)]
    pub len: usize,
    /// `log`: the first position, and how many; `channels`: how many messages.
    #[serde(default)]
    pub from: Option<u64>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// `dynamic_set`: the setting's new value.
    #[serde(default)]
    pub value: String,
    /// `run_task`: which background loop.
    #[serde(default)]
    pub task: String,
    /// `outage`, `wipe`, `verify`, `delete_message`: the store; `outage`: down or back.
    #[serde(default)]
    pub store: String,
    #[serde(default)]
    pub down: bool,
    /// `delete_message`: the message's id.
    #[serde(default)]
    pub message: String,
    /// The cluster node the op runs on (the demo runs them all; a server is one).
    #[serde(default)]
    pub node: Option<u64>,
}

fn demo() -> String {
    "demo".into()
}

/// A running node, for its admin.
pub struct Admin {
    pub meta: Arc<MetaStore>,
    /// The store the metadata log lives in (read by the inspector).
    pub log: Arc<dyn LogStore>,
    pub store: Arc<ChunkStore>,
    pub s3: S3Gateway,
    pub index: Index,
    /// The background loops' knobs, the file's with the tablet's overrides.
    pub dynamic: Arc<DynConfig>,
    /// Whether objects may be inline (`[inline] max_object_size`): the cooler's policy.
    pub inline_on: bool,
    /// `[meta] snapshot_every_entries`.
    pub snapshot_every: Option<u64>,
    pub channels: Option<Arc<dyn Channels>>,
    /// The config file as the node read it (no secrets: those are in the
    /// environment), shown read-only.
    pub config_text: Option<String>,
    /// The tablet's Raft cluster as the config has it; `None`: of one.
    pub cluster: Option<pigeonhole_config::cluster::ClusterSettings>,
    /// The node's stores as the config names them ([`stores::of`]).
    pub stores: Vec<StoreRef>,
    /// Each service's rate limiters (`telegram`, `discord`): what op
    /// `budgets` shows.
    pub limiters: Vec<(&'static str, Arc<pigeonhole_limits::LimiterRegistry>)>,
}

/// Where an op runs: on any node, only on the leader that serves (it
/// reads the state a follower may lag), or only where writes work too
/// (not read-only). Declared with each op ([`Admin::handle`]); the S3
/// port's gate checks the same ([`MetaStore::serves`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Any,
    Leader,
    Writes,
}

impl Access {
    /// Whether `meta`'s node runs an op of this access now: the error
    /// says why not (`NotLeader`, `CatchingUp`, `NoQuorum`), as the S3
    /// port's 503 does; `op` names it in the log.
    pub fn check(self, meta: &MetaStore, op: &str) -> Result<()> {
        let writes = match self {
            Access::Any => return Ok(()),
            Access::Leader => false,
            Access::Writes => true,
        };
        meta.serves(writes).inspect_err(|e| {
            tracing::info!(op, leader = meta.leader(), why = %format!("{e:#}"), "not serving");
        })
    }
}

/// An op's work, not started.
type Run<'a> = futures::future::BoxFuture<'a, Result<Value>>;

fn ok() -> Value {
    json!({ "ok": true })
}

impl Admin {
    /// Runs `r` (with `body`, an upload's bytes) where its [`Access`] allows.
    pub async fn handle(&self, r: &Request, body: Bytes) -> Result<Value> {
        let (access, run) = match self.s3_op(r, body)? {
            Some(op) => op,
            None => self.node_op(r)?,
        };
        access.check(&self.meta, &r.op)?;
        run.await
    }

    /// The S3 calls of the UI's client panel: the leader's.
    fn s3_op<'a>(&'a self, r: &'a Request, body: Bytes) -> Result<Option<(Access, Run<'a>)>> {
        use Access::{Leader, Writes};
        let (b, k) = (&r.bucket, &r.key);
        Ok(Some(match r.op.as_str() {
            "put" => (Writes, Box::pin(async move { self.put(b, k, body).await.map(|()| ok()) })),
            "get" => (Leader, Box::pin(async move { Ok(json!({ "len": self.get(b, k, r.range).await?.len() })) })),
            "copy" => (Writes, Box::pin(async move { self.copy(b, k, &r.to).await.map(|()| ok()) })),
            "delete" => (Writes, Box::pin(async move { self.delete(b, k).await.map(|()| ok()) })),
            "list" => (Leader, Box::pin(async move { Ok(json!({ "objects": self.list(b).await? })) })),
            "buckets" => (Leader, Box::pin(async move { Ok(json!({ "buckets": self.buckets().await? })) })),
            _ => return Ok(None),
        }))
    }

    /// The node's own ops: what it holds and runs, and its channels.
    fn node_op<'a>(&'a self, r: &'a Request) -> Result<(Access, Run<'a>)> {
        use Access::{Any, Writes};
        Ok(match r.op.as_str() {
            "info" => (Any, Box::pin(self.info())),
            "log" => (Any, Box::pin(self.log_entries(r.from, r.limit.unwrap_or(200)))),
            "background" => (Any, Box::pin(self.background())),
            "budgets" => (Any, Box::pin(async move { Ok(self.budgets()) })),
            "cluster" => (Any, Box::pin(async move { Ok(self.cluster()) })),
            "dynamic" => (Any, Box::pin(async move { Ok(self.dynamic_view()) })),
            "dynamic_set" => (Writes, Box::pin(async move { self.dynamic.set(&r.key, &r.value).await.map(|()| ok()) })),
            "dynamic_unset" => {
                (Writes, Box::pin(async move { Ok(json!({ "unset": self.dynamic.unset(&r.key).await? })) }))
            }
            // A snapshot is every node's own; the other tasks are the leader's work.
            "run_task" => {
                let access = if r.task == "snapshot" { Any } else { Writes };
                (access, Box::pin(async move { self.dynamic.run_now(&r.task).map(|()| ok()) }))
            }
            "verify" => (
                Writes,
                Box::pin(async move {
                    let (checked, gone) = self.verify(&r.store).await?;
                    Ok(json!({ "checked": checked, "gone": gone }))
                }),
            ),
            "repair" => (
                Writes,
                Box::pin(async move {
                    let s = self.repair().await?;
                    Ok(json!({ "repaired": s.repaired, "failed": s.failed, "queued": s.scrub_enqueued }))
                }),
            ),
            "sweep" => (
                Writes,
                Box::pin(async move {
                    let s = self.sweep().await?;
                    Ok(json!({ "deleted": s.keys_deleted, "zero_ref": s.zero_ref_chunks, "failed": s.failed }))
                }),
            ),
            "channels" | "delete_message" | "outage" | "wipe" => (Any, Box::pin(self.channel_op(r))),
            other => bail!("unknown op {other:?}"),
        })
    }

    /// `info`: the entries the boot replayed, the config as read, and the
    /// buckets where this node serves (none known elsewhere).
    async fn info(&self) -> Result<Value> {
        let buckets = match self.meta.serves(false) {
            Ok(()) => Some(self.buckets().await?),
            Err(_) => None,
        };
        Ok(json!({ "replayed": self.replayed(), "config": self.config_text, "buckets": buckets }))
    }

    fn dynamic_view(&self) -> Value {
        json!({
            "keys": self.dynamic.keys(),
            "tasks": self.dynamic.tasks(),
            "changed_at": self.dynamic.changed_at(),
            "nodes": self.dynamic.nodes(),
        })
    }

    /// The channels as their apps show them, and what can be done to them.
    async fn channel_op(&self, r: &Request) -> Result<Value> {
        Ok(match r.op.as_str() {
            "channels" => match &self.channels {
                Some(c) => c.describe(r.limit.unwrap_or(200)).await?,
                None => json!({ "channels": [] }),
            },
            "delete_message" => {
                self.channels()?.delete_message(&r.store, &r.message).await?;
                json!({ "deleted": 1 })
            }
            op => self.channels()?.fault(op, &r.store, r.down).await?,
        })
    }

    /// Every bot's budgets as they stand: per chat, each class's (send,
    /// read, delete) and the bot's own across its chats.
    fn budgets(&self) -> Value {
        let view = |b: pigeonhole_limits::Budget| {
            json!({
                "tokens": (b.tokens * 100.0).round() / 100.0,
                "capacity": b.capacity,
                "per_sec": b.per_sec,
                "cooling_ms": b.cooling.as_millis() as u64,
                "waiting": b.waiting,
            })
        };
        let mut out = Vec::new();
        for (service, registry) in &self.limiters {
            for (bot, chat, limiter) in registry.chats() {
                let classes: serde_json::Map<String, Value> =
                    limiter.budgets().into_iter().map(|(c, b)| (c.0.to_string(), view(b))).collect();
                out.push(json!({ "service": service, "bot": bot, "chat": chat, "classes": classes, "bot_total": view(limiter.bot_budget()) }));
            }
        }
        json!({ "budgets": out })
    }

    fn channels(&self) -> Result<&Arc<dyn Channels>> {
        self.channels.as_ref().ok_or_else(|| anyhow::anyhow!("this node shows no channels"))
    }

    /// Entries the boot replayed from the log after its snapshot.
    pub fn replayed(&self) -> u64 {
        self.meta.applied_position().saturating_sub(self.meta.base_position())
    }

    /// The log's entries as they lie in its store, commands decoded: the last
    /// `limit`, or `limit` from position `from`.
    pub async fn log_entries(&self, from: Option<u64>, limit: usize) -> Result<Value> {
        log_view::read(self.log.as_ref(), from, limit).await
    }

    fn repairer(&self) -> Repairer {
        Repairer::new(self.store.db().clone(), self.store.replicated().clone(), self.dynamic.repair.get())
    }

    /// One repair pass: copy chunks that lost a replica back to full.
    pub async fn repair(&self) -> Result<RepairStats> {
        self.repairer().repair_once().await
    }

    /// Repair jobs waiting.
    pub async fn repair_queue(&self) -> Result<u64> {
        self.store.db().repair_queue_len().await
    }

    /// Probe every copy store `id` should hold; queue a repair for each one
    /// gone. How many it checked, and how many were gone.
    pub async fn verify(&self, id: &str) -> Result<(u64, u64)> {
        self.repairer().verify_instance(id).await
    }

    /// One sweeper pass: delete posts that no chunk points to.
    pub async fn sweep(&self) -> Result<SweepStats> {
        let replicated = self.store.replicated();
        let sweeper = Sweeper::new(
            self.store.db().clone(),
            replicated.members().to_vec(),
            self.dynamic.sweep.get(),
            replicated.pins().clone(),
        );
        sweeper.sweep_once().await
    }
}
