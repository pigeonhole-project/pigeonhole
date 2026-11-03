//! The dynamic config (docs/dynamic-config.md): the knobs of the background
//! loops, changed while the process runs.
//!
//! The file gives the base; overrides (`repair.interval = "5s"`) live in the
//! tablet, namespace `config`, so they outlive a restart and every node of a
//! log sees the same ones. The effective knobs are always the file with the
//! overrides on top, parsed and checked as the file is: removing an override
//! brings the file's value back, and an override the file would refuse is
//! refused.

use crate::{units, Config, FileConfig};
use anyhow::{bail, Context, Result};
use pigeonhole_chunk_store::{RepairConfig, SweepConfig};
use pigeonhole_gateway_s3::CoolPolicy;
use pigeonhole_meta::{MetaStore, Namespaces, Never, Outcome, StateMachine};
use pigeonhole_types::schedule::{Control, Expiry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The keys an override may set: everything else changes with a restart.
pub const KEYS: &[&str] = &[
    "sweep.interval",
    "sweep.batch_size",
    "repair.interval",
    "repair.batch_size",
    "repair.concurrency",
    "repair.max_cost_wait",
    "repair.scrub",
    "inline.cool_every",
    "inline.cool_after",
    "inline.min_inline",
    "inline.cool_batch",
    "inline.budget",
    "inline.low_watermark",
    "meta.snapshot_every",
    "metrics.every",
    "bytestream.gc_every",
    "bytestream.gc_ttl",
    "bytestream.upload_gc_every",
];

/// The keys each `[[node]]` has of its own: `node.<id>.snapshot_every`
/// (over `meta.snapshot_every`, which holds for the nodes that set none).
pub const NODE_KEYS: &[&str] = &["snapshot_every"];

/// `node.<id>.<name>`: the node and the name.
fn node_key(key: &str) -> Option<(u64, &str)> {
    let (id, name) = key.strip_prefix("node.")?.split_once('.')?;
    Some((id.parse().ok()?, name))
}

/// A background loop the dynamic config drives. A root runs some of them
/// ([`DynConfig::open`]): only those are shown, run on demand, and have
/// their keys set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Task {
    Sweep,
    Repair,
    Cool,
    /// Every node's own snapshot ticker (in the `MetaStore`): always run.
    Snapshot,
    Metrics,
    CasGc,
    UploadGc,
}

impl Task {
    pub const ALL: [Task; 7] =
        [Task::Sweep, Task::Repair, Task::Cool, Task::Snapshot, Task::Metrics, Task::CasGc, Task::UploadGc];

    pub fn name(self) -> &'static str {
        match self {
            Task::Sweep => "sweep",
            Task::Repair => "repair",
            Task::Cool => "cool",
            Task::Snapshot => "snapshot",
            Task::Metrics => "metrics",
            Task::CasGc => "cas_gc",
            Task::UploadGc => "upload_gc",
        }
    }

    pub fn named(name: &str) -> Option<Task> {
        Task::ALL.into_iter().find(|t| t.name() == name)
    }

    /// Whether it is the leader's work (the others skip their passes); a
    /// snapshot is every node's own.
    pub fn leader_only(self) -> bool {
        self != Task::Snapshot
    }

    /// The loop dynamic key `key` drives.
    fn of_key(key: &str) -> Option<Task> {
        if node_key(key).is_some() {
            return Some(Task::Snapshot);
        }
        Some(match key.split_once('.')? {
            ("sweep", _) => Task::Sweep,
            ("repair", _) => Task::Repair,
            ("inline", _) => Task::Cool,
            ("meta", _) => Task::Snapshot,
            ("metrics", _) => Task::Metrics,
            ("bytestream", "upload_gc_every") => Task::UploadGc,
            ("bytestream", _) => Task::CasGc,
            _ => return None,
        })
    }
}

/// What the dynamic keys come to.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicConfig {
    pub sweep: SweepConfig,
    pub repair: RepairConfig,
    pub cool: Cooling,
    /// This node's periodic snapshot: its `[[node]] snapshot_every`, else
    /// `[meta]`'s. Snapshots are every node's own, not the leader's work.
    pub snapshot_every: Option<Duration>,
    /// `[meta] snapshot_every`: of every node that sets none.
    pub snapshot_every_default: Option<Duration>,
    /// Every `[[node]]`'s periodic snapshot, its own or the default.
    pub node_snapshot_every: BTreeMap<u64, Option<Duration>>,
    /// How often the tablet's gauges are set (`[metrics] every`).
    pub metrics_every: Option<Duration>,
    /// The REAPI CAS GC (`[bytestream] gc_every`, `gc_ttl`).
    pub cas_gc: Expiry,
    /// Dropping stale incomplete REAPI uploads (`[bytestream] upload_gc_every`).
    pub upload_gc_every: Option<Duration>,
}

/// The cooler's knobs (`[inline]`).
#[derive(Clone, Debug, PartialEq)]
pub struct Cooling {
    /// Pause between passes; `None`: only when asked.
    pub every: Option<Duration>,
    pub policy: CoolPolicy,
}

impl DynamicConfig {
    fn of_file(file: FileConfig, me: Option<u64>) -> Result<Self> {
        let inline = file.inline.into_settings()?;
        let bytestream = file.bytestream.into_settings();
        let default = file.meta.snapshot_every;
        let nodes: BTreeMap<u64, Option<Duration>> =
            file.node.iter().map(|n| (n.id(), n.snapshot_every.unwrap_or(default))).collect();
        Ok(Self {
            sweep: file.sweep.into_sweep_config(),
            repair: file.repair.into_repair_config(),
            cool: Cooling { every: inline.cool_every, policy: inline.cool },
            snapshot_every: me.and_then(|m| nodes.get(&m).copied()).unwrap_or(default),
            snapshot_every_default: default,
            node_snapshot_every: nodes,
            metrics_every: file.metrics.into_settings().every,
            cas_gc: Expiry { every: bytestream.gc_every, ttl: bytestream.gc_ttl },
            upload_gc_every: bytestream.upload_gc_every,
        })
    }

    /// `key`'s value, as an override would write it.
    pub fn value(&self, key: &str) -> Option<String> {
        if let Some((id, name)) = node_key(key) {
            return (name == "snapshot_every").then(|| self.node_snapshot_every.get(&id).map(|&p| off_or(p)))?;
        }
        let (r, c) = (&self.repair, &self.cool.policy);
        Some(match key {
            "sweep.interval" => off_or(self.sweep.interval),
            "sweep.batch_size" => self.sweep.batch_size.to_string(),
            "repair.interval" => off_or(r.interval),
            "repair.batch_size" => r.batch_size.to_string(),
            "repair.concurrency" => r.concurrency.to_string(),
            "repair.max_cost_wait" => duration(Duration::from_secs_f64(r.max_cost_wait_secs)),
            "repair.scrub" => r.scrub.to_string(),
            "inline.cool_every" => off_or(self.cool.every),
            "inline.cool_after" => off_or(c.cool_after),
            "inline.min_inline" => duration(c.min_inline),
            "inline.cool_batch" => c.batch.to_string(),
            "inline.budget" => c.budget_bytes.to_string(),
            "inline.low_watermark" => c.low_watermark.to_string(),
            "meta.snapshot_every" => off_or(self.snapshot_every_default),
            "metrics.every" => off_or(self.metrics_every),
            "bytestream.gc_every" => off_or(self.cas_gc.every),
            "bytestream.gc_ttl" => off_or(self.cas_gc.ttl),
            "bytestream.upload_gc_every" => off_or(self.upload_gc_every),
            _ => return None,
        })
    }
}

fn duration(d: Duration) -> String {
    units::format_duration(d)
}

fn off_or(d: Option<Duration>) -> String {
    d.map_or_else(|| units::OFF.to_string(), duration)
}

impl Config {
    /// The dynamic knobs of this config, no overrides: the file's (a
    /// config built in code has its own, `Config::for_test`).
    pub fn dynamic(&self) -> DynamicConfig {
        self.dynamic_with(&BTreeMap::new()).expect("the file was read once already")
    }

    /// The dynamic knobs with `overrides` (key → value) on top of the file;
    /// an error if one is not a dynamic key or the file would refuse it.
    pub fn dynamic_with(&self, overrides: &BTreeMap<String, String>) -> Result<DynamicConfig> {
        let mut table = self.file.clone();
        for (key, value) in overrides {
            self.check_key(key)?;
            set_path(&mut table, key, parse_value(value)).with_context(|| format!("{key} = {value}"))?;
        }
        let file: FileConfig = toml::Value::Table(table).try_into().context("the config with the overrides")?;
        DynamicConfig::of_file(file, self.cluster.as_ref().map(|c| c.node))
    }

    /// [`Self::dynamic_with`] of every override it takes, and why it took
    /// none of the others (those the file no longer allows).
    pub fn dynamic_lenient(&self, overrides: &BTreeMap<String, String>) -> (DynamicConfig, BTreeMap<String, String>) {
        let (mut taken, mut refused) = (BTreeMap::new(), BTreeMap::new());
        for (key, value) in overrides {
            taken.insert(key.clone(), value.clone());
            if let Err(e) = self.dynamic_with(&taken) {
                taken.remove(key);
                refused.insert(key.clone(), format!("{e:#}"));
            }
        }
        (self.dynamic_with(&taken).unwrap_or_else(|_| self.dynamic()), refused)
    }

    /// `key`'s value in the file, if it sets one.
    fn file_value(&self, key: &str) -> Option<String> {
        let v = match node_key(key) {
            Some((id, name)) => node_table(&self.file, id)?.get(name)?,
            None => {
                let mut v = self.file.get(key.split('.').next()?)?;
                for part in key.split('.').skip(1) {
                    v = v.as_table()?.get(part)?;
                }
                v
            }
        };
        Some(v.as_str().map_or_else(|| v.to_string(), str::to_string))
    }

    /// Where `key`'s value comes from, `overridden(key)` saying whether an
    /// override of it is taken (see [`KeyView::from`]).
    fn source(&self, key: &str, overridden: impl Fn(&str) -> bool) -> String {
        if overridden(key) {
            return "override".into();
        }
        if self.file_value(key).is_some() {
            return "file".into();
        }
        match node_key(key) {
            Some((_, name)) => format!("inherited: {}", self.source(&format!("meta.{name}"), overridden)),
            None => "default".into(),
        }
    }

    /// The `[[node]]`s' ids.
    fn member_ids(&self) -> Vec<u64> {
        self.cluster.as_ref().map(|c| c.nodes.iter().map(|n| n.id).collect()).unwrap_or_default()
    }

    /// Every dynamic key: [`KEYS`], then [`NODE_KEYS`] of each `[[node]]`
    /// (the keys of every loop, run or not: [`DynConfig::keys`] has those run).
    pub fn dynamic_keys(&self) -> Vec<String> {
        let nodes =
            self.member_ids().into_iter().flat_map(|id| NODE_KEYS.iter().map(move |k| format!("node.{id}.{k}")));
        KEYS.iter().map(|k| k.to_string()).chain(nodes).collect()
    }

    fn check_key(&self, key: &str) -> Result<()> {
        if let Some((id, name)) = node_key(key) {
            if !self.member_ids().contains(&id) {
                bail!("{key}: no [[node]] {id}");
            }
            if !NODE_KEYS.contains(&name) {
                bail!("{key}: a node's dynamic keys are node.<id>.{{{}}}", NODE_KEYS.join(", "));
            }
            return Ok(());
        }
        if !KEYS.contains(&key) {
            bail!("{key} cannot change while running (a restart reads it); dynamic keys: {}", KEYS.join(", "));
        }
        Ok(())
    }
}

/// `[[node]]` `id`'s table in the file.
fn node_table(file: &toml::Table, id: u64) -> Option<&toml::Table> {
    file.get("node")?.as_array()?.iter().filter_map(toml::Value::as_table).find(|t| is_node(t, id))
}

fn is_node(t: &toml::Table, id: u64) -> bool {
    t.get("id").and_then(toml::Value::as_integer) == Some(id as i64)
}

/// An override's value as TOML (`16`, `true`, `"5s"`), or else the text
/// itself as a string (`5s`, `off`).
fn parse_value(text: &str) -> toml::Value {
    format!("v = {text}")
        .parse::<toml::Table>()
        .ok()
        .and_then(|mut t| t.remove("v"))
        .unwrap_or_else(|| toml::Value::String(text.trim().to_string()))
}

fn set_path(table: &mut toml::Table, key: &str, value: toml::Value) -> Result<()> {
    if let Some((id, name)) = node_key(key) {
        let nodes = table.get_mut("node").and_then(toml::Value::as_array_mut).context("no [[node]]s")?;
        let node = nodes.iter_mut().filter_map(toml::Value::as_table_mut).find(|t| is_node(t, id));
        node.with_context(|| format!("no [[node]] {id}"))?.insert(name.to_string(), value);
        return Ok(());
    }
    let (sections, name) = key.rsplit_once('.').unwrap_or(("", key));
    let mut t = table;
    for part in sections.split('.').filter(|p| !p.is_empty()) {
        let entry = t.entry(part).or_insert_with(|| toml::Value::Table(toml::Table::new()));
        t = entry.as_table_mut().with_context(|| format!("{part} is not a table"))?;
    }
    t.insert(name.to_string(), value);
    Ok(())
}

/// The overrides in the tablet: key → value as written.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Overrides {
    values: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OverrideCommand {
    Set { key: String, value: String },
    Unset { key: String },
}

impl StateMachine for Overrides {
    const NAME: &'static str = "config";
    const VERSION: u32 = 1;
    type Command = OverrideCommand;
    type Reply = ();
    type Reject = Never;

    fn apply(&mut self, command: OverrideCommand) -> Result<Outcome<Self>> {
        match command {
            OverrideCommand::Set { key, value } => self.values.insert(key, value),
            OverrideCommand::Unset { key } => self.values.remove(&key),
        };
        Ok(Ok(()))
    }
}

/// One dynamic key as [`DynConfig::keys`] shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyView {
    pub key: String,
    /// What the loops run with.
    pub value: String,
    /// What the file says, if it sets the key.
    pub file: Option<String>,
    /// The override in the tablet, if any.
    #[serde(rename = "override")]
    pub overridden: Option<String>,
    /// Why the override is not taken (the file no longer allows it).
    pub refused: Option<String>,
    /// Where `value` comes from: `override`, `file`, `default`; a node's
    /// key that sets none, `meta.<name>`'s, as `inherited: <its from>`.
    pub from: String,
}

/// Whether the last config change reached a node, as [`DynConfig::nodes`]
/// shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeSync {
    pub id: u64,
    pub role: &'static str,
    /// Position its log holds up to: known on the leader.
    pub matched: Option<u64>,
    /// Its log holds the last change (`changed_at`); `None` where not known
    /// (another node, seen from a follower).
    pub synced: Option<bool>,
}

/// A background loop as [`DynConfig::tasks`] shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskView {
    pub task: &'static str,
    /// Its period, or `off`.
    pub every: String,
    /// When its next pass is due, in ms from now; `None` while off.
    pub next_in_ms: Option<u64>,
}

/// The dynamic config of a running process: the overrides in the tablet,
/// and the controls of the loops they drive.
pub struct DynConfig {
    config: Config,
    meta: Arc<MetaStore>,
    /// The loops the root runs (the snapshot ticker always).
    runs: std::collections::BTreeSet<Task>,
    /// The overrides refused (key → why), as of the last change.
    refused: Mutex<BTreeMap<String, String>>,
    /// One change at a time: each is checked against the ones before it.
    writes: tokio::sync::Mutex<()>,
    /// Position of the last change to the overrides, at most: exact for
    /// one written here, else the position applied when this node saw it.
    changed_at: std::sync::atomic::AtomicU64,
    pub sweep: Control<SweepConfig>,
    pub repair: Control<RepairConfig>,
    pub cool: Control<Cooling>,
    pub metrics: Control<Option<Duration>>,
    pub cas_gc: Control<Expiry>,
    pub upload_gc: Control<Option<Duration>>,
}

impl DynConfig {
    /// Serve the overrides' namespace.
    pub fn register(namespaces: Namespaces) -> Namespaces {
        namespaces.with::<Overrides>()
    }

    /// The knobs of `config` with the overrides `meta` holds; an override
    /// the file no longer allows is left out (and shown as refused). `runs`:
    /// the loops the root runs, with the tablet's snapshot ticker: only
    /// those are shown ([`Self::keys`], [`Self::tasks`]), changed and run.
    pub fn open(config: Config, meta: Arc<MetaStore>, runs: &[Task]) -> Self {
        let overrides = meta.read::<Overrides, _>(|o| o.values.clone());
        let (knobs, refused) = config.dynamic_lenient(&overrides);
        for (key, why) in &refused {
            tracing::warn!(key, why, "a config override is refused; the file's value holds");
        }
        meta.snapshot_schedule().set(knobs.snapshot_every);
        let changed_at = meta.applied_position().into();
        Self {
            config,
            meta,
            runs: runs.iter().copied().chain([Task::Snapshot]).collect(),
            refused: Mutex::new(refused),
            writes: tokio::sync::Mutex::new(()),
            changed_at,
            sweep: Control::new(knobs.sweep),
            repair: Control::new(knobs.repair),
            cool: Control::new(knobs.cool),
            metrics: Control::new(knobs.metrics_every),
            cas_gc: Control::new(knobs.cas_gc),
            upload_gc: Control::new(knobs.upload_gc_every),
        }
    }

    pub fn overrides(&self) -> BTreeMap<String, String> {
        self.meta.read::<Overrides, _>(|o| o.values.clone())
    }

    /// The knobs the loops run with.
    pub fn knobs(&self) -> DynamicConfig {
        let others = self.config.dynamic_lenient(&self.overrides()).0;
        DynamicConfig {
            sweep: self.sweep.get(),
            repair: self.repair.get(),
            cool: self.cool.get(),
            snapshot_every: self.meta.snapshot_schedule().get(),
            snapshot_every_default: others.snapshot_every_default,
            node_snapshot_every: others.node_snapshot_every,
            metrics_every: self.metrics.get(),
            cas_gc: self.cas_gc.get(),
            upload_gc_every: self.upload_gc.get(),
        }
    }

    /// Override `key` with `value` (`"5s"`, `off`, `16`, `true`): checked
    /// as the file would be, then written to the tablet and applied.
    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        self.check_runs(key)?;
        let _one = self.writes.lock().await;
        let refused = self.refused.lock().unwrap().clone();
        let mut taken: BTreeMap<_, _> =
            self.overrides().into_iter().filter(|(k, _)| !refused.contains_key(k)).collect();
        taken.insert(key.to_string(), value.to_string());
        let knobs = self.config.dynamic_with(&taken)?;
        let set = OverrideCommand::Set { key: key.to_string(), value: value.to_string() };
        let (at, Ok(())) = self.meta.propose_at::<Overrides>(set).await.context("write the override")?;
        self.changed(at);
        self.refused.lock().unwrap().remove(key);
        self.apply(knobs);
        tracing::info!(key, value, "config override set");
        Ok(())
    }

    /// Drop the override of `key`: the file's value holds again. Whether
    /// there was one.
    pub async fn unset(&self, key: &str) -> Result<bool> {
        self.config.check_key(key)?;
        self.check_runs(key)?;
        let _one = self.writes.lock().await;
        if !self.overrides().contains_key(key) {
            return Ok(false);
        }
        let unset = OverrideCommand::Unset { key: key.to_string() };
        let (at, Ok(())) = self.meta.propose_at::<Overrides>(unset).await.context("drop the override")?;
        self.changed(at);
        let (knobs, refused) = self.config.dynamic_lenient(&self.overrides());
        *self.refused.lock().unwrap() = refused;
        self.apply(knobs);
        tracing::info!(key, "config override dropped");
        Ok(true)
    }

    /// Keep the knobs in step with the overrides in the tablet as they
    /// change by replication (on a follower, or written by the leader
    /// before this node took over); ends once the tablet stops.
    pub async fn follow(self: Arc<Self>) {
        let mut changes = self.meta.changes();
        let mut seen = self.overrides();
        while changes.changed().await.is_ok() && !self.meta.is_stopped() {
            if self.overrides() == seen {
                continue;
            }
            let _one = self.writes.lock().await;
            seen = self.overrides();
            self.changed(self.meta.applied_position());
            let (knobs, refused) = self.config.dynamic_lenient(&seen);
            *self.refused.lock().unwrap() = refused;
            self.apply(knobs);
            tracing::info!(overrides = seen.len(), "config overrides changed by replication");
        }
    }

    /// Whether `key` drives a loop this node runs (checked as a key first).
    fn check_runs(&self, key: &str) -> Result<()> {
        self.config.check_key(key)?;
        match Task::of_key(key) {
            Some(task) if !self.runs.contains(&task) => {
                bail!("{key}: drives the {} loop, which this node does not run", task.name())
            }
            _ => Ok(()),
        }
    }

    /// The loops this node runs, in [`Task::ALL`]'s order.
    pub fn runs(&self) -> impl Iterator<Item = Task> + '_ {
        self.runs.iter().copied()
    }

    fn changed(&self, at: u64) {
        self.changed_at.fetch_max(at, std::sync::atomic::Ordering::SeqCst);
    }

    /// Position of the last change to the overrides (at most).
    pub fn changed_at(&self) -> u64 {
        self.changed_at.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Every node of the cluster, and whether the last change reached its
    /// log: known for all on the leader, for itself on a follower.
    pub fn nodes(&self) -> Vec<NodeSync> {
        let (view, at) = (self.meta.cluster_view(), self.changed_at());
        view.nodes
            .iter()
            .map(|n| {
                let holds = n.matched.or_else(|| n.applied.filter(|_| n.id == view.me));
                NodeSync { id: n.id, role: n.role, matched: n.matched, synced: holds.map(|p| p >= at) }
            })
            .collect()
    }

    fn apply(&self, knobs: DynamicConfig) {
        self.sweep.set(knobs.sweep);
        self.repair.set(knobs.repair);
        self.cool.set(knobs.cool);
        self.meta.snapshot_schedule().set(knobs.snapshot_every);
        self.metrics.set(knobs.metrics_every);
        self.cas_gc.set(knobs.cas_gc);
        self.upload_gc.set(knobs.upload_gc_every);
    }

    /// Ask `task` (one this node runs) for a pass now, even while it is
    /// off. The leader's work only where it serves and may write
    /// ([`MetaStore::serves`]: `NotLeader`, `NoQuorum`, …): a follower
    /// would skip the pass.
    pub fn run_now(&self, task: &str) -> Result<()> {
        let Some(t) = Task::named(task).filter(|t| self.runs.contains(t)) else {
            let names: Vec<_> = self.runs().map(Task::name).collect();
            bail!("no task {task:?} on this node; tasks: {}", names.join(", "))
        };
        if t.leader_only() {
            self.meta.serves(true)?;
        }
        match t {
            Task::Sweep => self.sweep.run_now(),
            Task::Repair => self.repair.run_now(),
            Task::Cool => self.cool.run_now(),
            Task::Snapshot => self.meta.snapshot_schedule().run_now(),
            Task::Metrics => self.metrics.run_now(),
            Task::CasGc => self.cas_gc.run_now(),
            Task::UploadGc => self.upload_gc.run_now(),
        }
        Ok(())
    }

    /// Every dynamic key: its value, the file's, the override.
    pub fn keys(&self) -> Vec<KeyView> {
        let (knobs, overrides) = (self.knobs(), self.overrides());
        let refused = self.refused.lock().unwrap();
        let taken: BTreeMap<_, _> = overrides.iter().filter(|(k, _)| !refused.contains_key(*k)).collect();
        self.config
            .dynamic_keys()
            .into_iter()
            .filter(|key| Task::of_key(key).is_none_or(|t| self.runs.contains(&t)))
            .map(|key| KeyView {
                value: knobs.value(&key).unwrap_or_default(),
                file: self.config.file_value(&key),
                overridden: overrides.get(&key).cloned(),
                refused: refused.get(&key).cloned(),
                from: self.config.source(&key, |k| taken.contains_key(&k.to_string())),
                key,
            })
            .collect()
    }

    /// Every task this node runs: its period and when its next pass is due.
    pub fn tasks(&self) -> Vec<TaskView> {
        let now = tokio::time::Instant::now();
        let snapshots = self.meta.snapshot_schedule();
        self.runs()
            .map(|task| {
                let (due, every) = match task {
                    Task::Sweep => (self.sweep.due(|c| c.interval), self.sweep.get().interval),
                    Task::Repair => (self.repair.due(|c| c.interval), self.repair.get().interval),
                    Task::Cool => (self.cool.due(|c| c.every), self.cool.get().every),
                    Task::Snapshot => (snapshots.due(|p| *p), snapshots.get()),
                    Task::Metrics => (self.metrics.due(|p| *p), self.metrics.get()),
                    Task::CasGc => (self.cas_gc.due(Expiry::period), self.cas_gc.get().period()),
                    Task::UploadGc => (self.upload_gc.due(|p| *p), self.upload_gc.get()),
                };
                TaskView {
                    task: task.name(),
                    every: off_or(every),
                    next_in_ms: due.map(|at| at.saturating_duration_since(now).as_millis() as u64),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    const S: Duration = Duration::from_secs(1);

    fn config(extra: &str) -> Config {
        let text = format!("{extra}\n[chunk]\nstore = \"mem\"\n[[store]]\nid = \"mem\"\nkind = \"memory\"\n");
        Config::from_toml(&text, Path::new("test.toml")).unwrap()
    }

    fn one(key: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(key.to_string(), value.to_string())])
    }

    #[test]
    fn overrides_go_on_top_of_the_file() {
        let cfg = config("[repair]\ninterval = \"10s\"\nbatch_size = 8\n[inline]\ncool_every = \"off\"\n");
        assert_eq!(cfg.dynamic_with(&BTreeMap::new()).unwrap(), cfg.dynamic(), "the file alone");
        let knobs = cfg.dynamic_with(&one("repair.interval", "off")).unwrap();
        assert_eq!((knobs.repair.interval, knobs.repair.batch_size), (None, 8), "the rest of [repair] stays");
        assert_eq!(cfg.dynamic_with(&one("inline.cool_every", "2m")).unwrap().cool.every, Some(120 * S));
        assert_eq!(cfg.dynamic_with(&one("repair.batch_size", "32")).unwrap().repair.batch_size, 32);
        assert!(!cfg.dynamic_with(&one("repair.scrub", "false")).unwrap().repair.scrub);
        assert_eq!(cfg.dynamic_with(&one("bytestream.gc_ttl", "off")).unwrap().cas_gc.period(), None);
        // The file's own floors hold for an override too.
        assert_eq!(cfg.dynamic_with(&one("sweep.interval", "10ms")).unwrap().sweep.interval, Some(S));
    }

    #[test]
    fn an_override_the_file_would_refuse_is_refused() {
        let cfg = config("");
        for (key, value) in [
            ("sweep.interval", "0s"),
            ("sweep.interval", "60"),
            ("inline.low_watermark", "2.0"),
            ("listen_addr", "\"0.0.0.0:1\""),
            ("chunk.size", "1 MiB"),
            ("repair.no_such_knob", "1"),
        ] {
            assert!(cfg.dynamic_with(&one(key, value)).is_err(), "{key} = {value} accepted");
        }
        let err = format!("{:#}", cfg.dynamic_with(&one("chunk.size", "1 MiB")).unwrap_err());
        assert!(err.contains("cannot change while running"), "{err}");
    }

    #[test]
    fn every_key_shows_its_value() {
        let knobs = config("[sweep]\ninterval = \"90s\"\n").dynamic();
        for key in KEYS {
            assert!(knobs.value(key).is_some(), "{key}");
        }
        assert_eq!(knobs.value("sweep.interval").as_deref(), Some("1m 30s"));
        assert_eq!(knobs.value("bytestream.gc_ttl").as_deref(), Some("7days"));
        // What it shows reads back as the same.
        let cfg = config("");
        for key in KEYS {
            let shown = knobs.value(key).unwrap();
            let again = cfg.dynamic_with(&one(key, &shown)).unwrap();
            assert_eq!(again.value(key).unwrap(), shown, "{key}");
        }
    }

    /// Node `me` of three; node 2 snapshots on a period of its own.
    fn cluster(me: u64) -> Config {
        std::env::set_var("PH_TEST_DYN_CLUSTER_KEY", "00".repeat(32));
        let text = "[cluster]\nid = 9\nkey_env = \"PH_TEST_DYN_CLUSTER_KEY\"\n[meta]\nsnapshot_every = \"10m\"\n\
             [[node]]\nid = 1\ngrpc = \"a:1\"\n[[node]]\nid = 2\ngrpc = \"b:1\"\nsnapshot_every = \"1h\"\n\
             [[node]]\nid = 3\ngrpc = \"c:1\"\n\
             [chunk]\nstore = \"mem\"\n[[store]]\nid = \"mem\"\nkind = \"memory\"\n";
        Config::from_toml_as(text, Path::new("test.toml"), me).unwrap()
    }

    #[test]
    fn each_node_snapshots_on_its_own_period() {
        let m = 60 * S;
        let (n1, n2) = (cluster(1), cluster(2));
        assert_eq!((n1.dynamic().snapshot_every, n2.dynamic().snapshot_every), (Some(10 * m), Some(60 * m)));
        let keys = n1.dynamic_keys();
        assert!(keys
            .ends_with(&["node.1.snapshot_every", "node.2.snapshot_every", "node.3.snapshot_every"].map(String::from)));

        // Off on node 3 only: the override reaches every node, node 3 acts on it.
        let off3 = one("node.3.snapshot_every", "off");
        assert_eq!(cluster(3).dynamic_with(&off3).unwrap().snapshot_every, None);
        let seen = n1.dynamic_with(&off3).unwrap();
        assert_eq!(seen.snapshot_every, Some(10 * m), "node 1 keeps its own");
        assert_eq!(seen.value("node.3.snapshot_every").as_deref(), Some("off"));
        // [meta]'s holds for the nodes that set none; node 2 keeps its own.
        let meta5 = one("meta.snapshot_every", "5m");
        assert_eq!(n1.dynamic_with(&meta5).unwrap().snapshot_every, Some(5 * m));
        assert_eq!(n2.dynamic_with(&meta5).unwrap().snapshot_every, Some(60 * m));
        assert_eq!(n2.file_value("node.2.snapshot_every").as_deref(), Some("1h"));
        assert_eq!(n2.file_value("node.1.snapshot_every"), None);

        for (key, want) in
            [("node.4.snapshot_every", "no [[node]] 4"), ("node.1.keep_snapshots", "node.<id>.{snapshot_every}")]
        {
            let err = format!("{:#}", n1.dynamic_with(&one(key, "1h")).unwrap_err());
            assert!(err.contains(want), "{key}: {err}");
        }
        assert!(config("").dynamic_with(&one("node.1.snapshot_every", "1h")).is_err(), "no cluster, no nodes");
    }

    #[test]
    fn each_value_says_where_it_comes_from() {
        let n1 = cluster(1);
        let none = |_: &str| false;
        assert_eq!(n1.source("node.2.snapshot_every", none), "file");
        assert_eq!(n1.source("node.1.snapshot_every", none), "inherited: file");
        assert_eq!(n1.source("sweep.interval", none), "default");
        let meta = |k: &str| k == "meta.snapshot_every";
        assert_eq!(n1.source("node.1.snapshot_every", meta), "inherited: override");
        assert_eq!(n1.source("node.2.snapshot_every", meta), "file", "a node's own value beats [meta]'s override");
        assert_eq!(n1.source("node.2.snapshot_every", |k: &str| k == "node.2.snapshot_every"), "override");
        assert_eq!(config("").source("meta.snapshot_every", none), "default");
    }

    #[tokio::test]
    async fn overrides_live_in_the_tablet() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config("[sweep]\ninterval = \"90s\"\n");
        {
            let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
            let dynamic = DynConfig::open(cfg.clone(), meta.clone(), &Task::ALL);
            assert_eq!(dynamic.sweep.get().interval, Some(90 * S));
            dynamic.set("sweep.interval", "off").await.unwrap();
            dynamic.set("meta.snapshot_every", "5m").await.unwrap();
            assert_eq!((dynamic.sweep.get().interval, meta.snapshot_schedule().get()), (None, Some(300 * S)));
            assert!(dynamic.set("sweep.interval", "0s").await.is_err());
            assert!(dynamic.set("listen_addr", "x").await.is_err());
            assert_eq!(dynamic.sweep.get().interval, None, "a refused change changes nothing");
            meta.close().await.unwrap();
        }
        let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
        let dynamic = DynConfig::open(cfg, meta.clone(), &Task::ALL);
        assert_eq!(dynamic.sweep.get().interval, None, "the override outlives a restart");
        let view = dynamic.keys().into_iter().find(|k| k.key == "sweep.interval").unwrap();
        assert_eq!(
            (view.value.as_str(), view.file.as_deref(), view.overridden.as_deref()),
            ("off", Some("90s"), Some("off"))
        );
        assert!(dynamic.unset("sweep.interval").await.unwrap());
        assert!(!dynamic.unset("sweep.interval").await.unwrap());
        assert_eq!(dynamic.sweep.get().interval, Some(90 * S), "back to the file's");
        assert_eq!(meta.snapshot_schedule().get(), Some(300 * S));
    }

    #[tokio::test]
    async fn an_override_the_file_no_longer_allows_is_left_out() {
        let dir = tempfile::tempdir().unwrap();
        let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
        // Written by a build that took it (or by hand): this one refuses it.
        let bad = OverrideCommand::Set { key: "repair.batch_size".into(), value: "\"many\"".into() };
        meta.propose::<Overrides>(bad).await.unwrap().unwrap();
        let good = OverrideCommand::Set { key: "repair.concurrency".into(), value: "2".into() };
        meta.propose::<Overrides>(good).await.unwrap().unwrap();
        let dynamic = DynConfig::open(config(""), meta, &Task::ALL);
        assert_eq!((dynamic.repair.get().batch_size, dynamic.repair.get().concurrency), (16, 2));
        let view = dynamic.keys().into_iter().find(|k| k.key == "repair.batch_size").unwrap();
        assert!(view.refused.is_some(), "{view:?}");
        // Setting it anew clears it.
        dynamic.set("repair.batch_size", "4").await.unwrap();
        assert_eq!(dynamic.repair.get().batch_size, 4);
        assert!(dynamic.keys().iter().all(|k| k.refused.is_none()));
    }

    #[tokio::test]
    async fn overrides_by_replication_are_followed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
        let dynamic = Arc::new(DynConfig::open(config(""), meta.clone(), &Task::ALL));
        let follow = tokio::spawn(dynamic.clone().follow());
        // As the leader's entry would reach a follower: not through `set`.
        let set = OverrideCommand::Set { key: "repair.interval".into(), value: "off".into() };
        meta.propose::<Overrides>(set).await.unwrap().unwrap();
        for _ in 0..100 {
            if dynamic.repair.get().interval.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(dynamic.repair.get().interval, None);
        follow.abort();
    }

    #[tokio::test]
    async fn tasks_show_when_they_pass_next() {
        let dir = tempfile::tempdir().unwrap();
        let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
        let dynamic = DynConfig::open(config("[repair]\ninterval = \"off\"\n"), meta, &Task::ALL);
        let tasks = dynamic.tasks();
        assert_eq!(tasks.iter().map(|t| t.task).collect::<Vec<_>>(), Task::ALL.map(Task::name));
        let repair = &tasks[1];
        assert_eq!((repair.every.as_str(), repair.next_in_ms), ("off", None));
        assert!(tasks[0].next_in_ms.unwrap() <= 60_000);
        assert!(dynamic.run_now("repair").is_ok());
        assert!(dynamic.run_now("nap").is_err());
    }

    #[tokio::test]
    async fn only_the_loops_the_root_runs_are_shown_set_and_run() {
        let dir = tempfile::tempdir().unwrap();
        let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
        let dynamic = DynConfig::open(config(""), meta, &[Task::Sweep, Task::Repair, Task::Cool]);
        let tasks: Vec<_> = dynamic.tasks().into_iter().map(|t| t.task).collect();
        assert_eq!(tasks, ["sweep", "repair", "cool", "snapshot"], "the snapshot ticker is the tablet's own");
        let keys: Vec<_> = dynamic.keys().into_iter().map(|k| k.key).collect();
        assert!(keys.contains(&"meta.snapshot_every".to_string()) && keys.contains(&"inline.budget".to_string()));
        assert!(!keys.iter().any(|k| k.starts_with("metrics.") || k.starts_with("bytestream.")), "{keys:?}");
        for task in ["metrics", "cas_gc", "upload_gc", "nap"] {
            let err = format!("{:#}", dynamic.run_now(task).unwrap_err());
            assert!(err.contains("on this node"), "{task}: {err}");
        }
        assert!(dynamic.run_now("snapshot").is_ok());
        let err = format!("{:#}", dynamic.set("metrics.every", "10s").await.unwrap_err());
        assert!(err.contains("does not run"), "{err}");
        assert!(dynamic.unset("bytestream.gc_ttl").await.is_err());
        dynamic.set("sweep.batch_size", "8").await.unwrap();
    }

    #[test]
    fn a_config_built_in_code_has_its_own_knobs() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_test(dir.path());
        let knobs = cfg.dynamic();
        assert_eq!((knobs.sweep, knobs.repair), (cfg.sweep.clone(), cfg.repair.clone()));
        assert_eq!(knobs.cool, Cooling { every: cfg.inline.cool_every, policy: cfg.inline.cool.clone() });
        assert_eq!(knobs.snapshot_every, cfg.meta.snapshot_interval);
        assert_eq!(knobs.metrics_every, cfg.metrics.every);
        assert_eq!(cfg.inline.max_object_size, None, "nothing inline in tests that count chunks");
        let knobs = cfg.dynamic_lenient(&one("repair.batch_size", "3")).0;
        assert_eq!(knobs.repair.batch_size, 3);
    }
}
