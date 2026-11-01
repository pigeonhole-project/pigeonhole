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
pub mod log_view;
mod s3;

/// The channels behind the node's stores, as their apps show them.
#[async_trait]
pub trait Channels: Send + Sync {
    /// Every channel the node uses: its messages (the last `limit`), its pins.
    async fn describe(&self, limit: usize) -> Result<Value>;

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
}

/// The node's stores as the config names them, without their messages:
/// what a node whose channels are not read shows.
pub struct ListedStores(pub Vec<Value>);

#[async_trait]
impl Channels for ListedStores {
    async fn describe(&self, _limit: usize) -> Result<Value> {
        Ok(json!({ "channels": self.0 }))
    }

    async fn delete_message(&self, store: &str, _id: &str) -> Result<()> {
        bail!("{store}: this node does not read its channels")
    }
}

impl Admin {
    /// Runs `r` (with `body`, an upload's bytes).
    pub async fn handle(&self, r: &Request, body: Bytes) -> Result<Value> {
        Ok(match r.op.as_str() {
            "put" => {
                self.put(&r.bucket, &r.key, body).await?;
                json!({ "ok": true })
            }
            "get" => json!({ "len": self.get(&r.bucket, &r.key, r.range).await?.len() }),
            "copy" => {
                self.copy(&r.bucket, &r.key, &r.to).await?;
                json!({ "ok": true })
            }
            "delete" => {
                self.delete(&r.bucket, &r.key).await?;
                json!({ "ok": true })
            }
            "list" => json!({ "objects": self.list(&r.bucket).await? }),
            "buckets" => json!({ "buckets": self.buckets().await? }),
            "info" => json!({
                "replayed": self.replayed(),
                "config": self.config_text,
                "buckets": self.buckets().await?,
            }),
            "log" => self.log_entries(r.from, r.limit.unwrap_or(200)).await?,
            "background" => self.background().await?,
            "dynamic" => json!({ "keys": self.dynamic.keys(), "tasks": self.dynamic.tasks() }),
            "dynamic_set" => {
                self.dynamic.set(&r.key, &r.value).await?;
                json!({ "ok": true })
            }
            "dynamic_unset" => json!({ "unset": self.dynamic.unset(&r.key).await? }),
            "run_task" => {
                self.dynamic.run_now(&r.task)?;
                json!({ "ok": true })
            }
            "verify" => {
                let (checked, gone) = self.verify(&r.store).await?;
                json!({ "checked": checked, "gone": gone })
            }
            "repair" => {
                let s = self.repair().await?;
                json!({ "repaired": s.repaired, "failed": s.failed, "queued": s.scrub_enqueued })
            }
            "sweep" => {
                let s = self.sweep().await?;
                json!({ "deleted": s.keys_deleted, "zero_ref": s.zero_ref_chunks })
            }
            "channels" => match &self.channels {
                Some(c) => c.describe(r.limit.unwrap_or(200)).await?,
                None => json!({ "channels": [] }),
            },
            "delete_message" => {
                self.channels()?.delete_message(&r.store, &r.message).await?;
                json!({ "deleted": 1 })
            }
            "outage" | "wipe" => self.channels()?.fault(&r.op, &r.store, r.down).await?,
            other => bail!("unknown op {other:?}"),
        })
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
