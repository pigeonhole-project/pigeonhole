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

/// The background loops a pass can be asked of.
pub const TASKS: &[&str] = &["sweep", "repair", "cool", "snapshot", "metrics", "cas_gc", "upload_gc"];

/// What the dynamic keys come to.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicConfig {
    pub sweep: SweepConfig,
    pub repair: RepairConfig,
    pub cool: Cooling,
    /// The periodic snapshot (`[meta] snapshot_every`).
    pub snapshot_every: Option<Duration>,
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
    fn of_file(file: FileConfig) -> Result<Self> {
        let inline = file.inline.into_settings()?;
        let bytestream = file.bytestream.into_settings();
        Ok(Self {
            sweep: file.sweep.into_sweep_config(),
            repair: file.repair.into_repair_config(),
            cool: Cooling { every: inline.cool_every, policy: inline.cool },
            snapshot_every: file.meta.snapshot_every,
            metrics_every: file.metrics.into_settings().every,
            cas_gc: Expiry { every: bytestream.gc_every, ttl: bytestream.gc_ttl },
            upload_gc_every: bytestream.upload_gc_every,
        })
    }

    /// `key`'s value, as an override would write it.
    pub fn value(&self, key: &str) -> Option<String> {
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
            "meta.snapshot_every" => off_or(self.snapshot_every),
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
    /// The dynamic knobs of this config, no overrides.
    pub fn dynamic(&self) -> DynamicConfig {
        DynamicConfig {
            sweep: self.sweep.clone(),
            repair: self.repair.clone(),
            cool: Cooling { every: self.inline.cool_every, policy: self.inline.cool.clone() },
            snapshot_every: self.meta.snapshot_interval,
            metrics_every: self.metrics.every,
            cas_gc: Expiry { every: self.bytestream.gc_every, ttl: self.bytestream.gc_ttl },
            upload_gc_every: self.bytestream.upload_gc_every,
        }
    }

    /// The dynamic knobs with `overrides` (key → value) on top of the file;
    /// an error if one is not a dynamic key or the file would refuse it.
    pub fn dynamic_with(&self, overrides: &BTreeMap<String, String>) -> Result<DynamicConfig> {
        let mut table = self.file.clone();
        for (key, value) in overrides {
            check_key(key)?;
            set_path(&mut table, key, parse_value(value)).with_context(|| format!("{key} = {value}"))?;
        }
        let file: FileConfig = toml::Value::Table(table).try_into().context("the config with the overrides")?;
        DynamicConfig::of_file(file)
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
        let mut v = self.file.get(key.split('.').next()?)?;
        for part in key.split('.').skip(1) {
            v = v.as_table()?.get(part)?;
        }
        Some(v.as_str().map_or_else(|| v.to_string(), str::to_string))
    }
}

fn check_key(key: &str) -> Result<()> {
    if !KEYS.contains(&key) {
        bail!("{key} cannot change while running (a restart reads it); dynamic keys: {}", KEYS.join(", "));
    }
    Ok(())
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
    pub key: &'static str,
    /// What the loops run with.
    pub value: String,
    /// What the file says, if it sets the key.
    pub file: Option<String>,
    /// The override in the tablet, if any.
    #[serde(rename = "override")]
    pub overridden: Option<String>,
    /// Why the override is not taken (the file no longer allows it).
    pub refused: Option<String>,
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
    /// The overrides refused (key → why), as of the last change.
    refused: Mutex<BTreeMap<String, String>>,
    /// One change at a time: each is checked against the ones before it.
    writes: tokio::sync::Mutex<()>,
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
    /// the file no longer allows is left out (and shown as refused).
    pub fn open(config: Config, meta: Arc<MetaStore>) -> Self {
        let overrides = meta.read::<Overrides, _>(|o| o.values.clone());
        let (knobs, refused) = config.dynamic_lenient(&overrides);
        for (key, why) in &refused {
            tracing::warn!(key, why, "a config override is refused; the file's value holds");
        }
        meta.snapshot_schedule().set(knobs.snapshot_every);
        Self {
            config,
            meta,
            refused: Mutex::new(refused),
            writes: tokio::sync::Mutex::new(()),
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
        DynamicConfig {
            sweep: self.sweep.get(),
            repair: self.repair.get(),
            cool: self.cool.get(),
            snapshot_every: self.meta.snapshot_schedule().get(),
            metrics_every: self.metrics.get(),
            cas_gc: self.cas_gc.get(),
            upload_gc_every: self.upload_gc.get(),
        }
    }

    /// Override `key` with `value` (`"5s"`, `off`, `16`, `true`): checked
    /// as the file would be, then written to the tablet and applied.
    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        let _one = self.writes.lock().await;
        let refused = self.refused.lock().unwrap().clone();
        let mut taken: BTreeMap<_, _> =
            self.overrides().into_iter().filter(|(k, _)| !refused.contains_key(k)).collect();
        taken.insert(key.to_string(), value.to_string());
        let knobs = self.config.dynamic_with(&taken)?;
        let set = OverrideCommand::Set { key: key.to_string(), value: value.to_string() };
        let Ok(()) = self.meta.propose::<Overrides>(set).await.context("write the override")?;
        self.refused.lock().unwrap().remove(key);
        self.apply(knobs);
        tracing::info!(key, value, "config override set");
        Ok(())
    }

    /// Drop the override of `key`: the file's value holds again. Whether
    /// there was one.
    pub async fn unset(&self, key: &str) -> Result<bool> {
        check_key(key)?;
        let _one = self.writes.lock().await;
        if !self.overrides().contains_key(key) {
            return Ok(false);
        }
        let unset = OverrideCommand::Unset { key: key.to_string() };
        let Ok(()) = self.meta.propose::<Overrides>(unset).await.context("drop the override")?;
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
            let (knobs, refused) = self.config.dynamic_lenient(&seen);
            *self.refused.lock().unwrap() = refused;
            self.apply(knobs);
            tracing::info!(overrides = seen.len(), "config overrides changed by replication");
        }
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

    /// Ask `task` (one of [`TASKS`]) for a pass now, even while it is off.
    pub fn run_now(&self, task: &str) -> Result<()> {
        match task {
            "sweep" => self.sweep.run_now(),
            "repair" => self.repair.run_now(),
            "cool" => self.cool.run_now(),
            "snapshot" => self.meta.snapshot_schedule().run_now(),
            "metrics" => self.metrics.run_now(),
            "cas_gc" => self.cas_gc.run_now(),
            "upload_gc" => self.upload_gc.run_now(),
            _ => bail!("no task {task:?}; tasks: {}", TASKS.join(", ")),
        }
        Ok(())
    }

    /// Every dynamic key: its value, the file's, the override.
    pub fn keys(&self) -> Vec<KeyView> {
        let (knobs, overrides) = (self.knobs(), self.overrides());
        let refused = self.refused.lock().unwrap();
        KEYS.iter()
            .map(|&key| KeyView {
                key,
                value: knobs.value(key).unwrap_or_default(),
                file: self.config.file_value(key),
                overridden: overrides.get(key).cloned(),
                refused: refused.get(key).cloned(),
            })
            .collect()
    }

    /// Every task: its period and when its next pass is due.
    pub fn tasks(&self) -> Vec<TaskView> {
        let now = tokio::time::Instant::now();
        let view = |task, due: Option<tokio::time::Instant>, every: Option<Duration>| TaskView {
            task,
            every: off_or(every),
            next_in_ms: due.map(|at| at.saturating_duration_since(now).as_millis() as u64),
        };
        let snapshots = self.meta.snapshot_schedule();
        vec![
            view("sweep", self.sweep.due(|c| c.interval), self.sweep.get().interval),
            view("repair", self.repair.due(|c| c.interval), self.repair.get().interval),
            view("cool", self.cool.due(|c| c.every), self.cool.get().every),
            view("snapshot", snapshots.due(|p| *p), snapshots.get()),
            view("metrics", self.metrics.due(|p| *p), self.metrics.get()),
            view("cas_gc", self.cas_gc.due(Expiry::period), self.cas_gc.get().period()),
            view("upload_gc", self.upload_gc.due(|p| *p), self.upload_gc.get()),
        ]
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

    #[tokio::test]
    async fn overrides_live_in_the_tablet() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config("[sweep]\ninterval = \"90s\"\n");
        {
            let meta = pigeonhole_testkit::tablet(dir.path(), DynConfig::register).await;
            let dynamic = DynConfig::open(cfg.clone(), meta.clone());
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
        let dynamic = DynConfig::open(cfg, meta.clone());
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
        let dynamic = DynConfig::open(config(""), meta);
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
        let dynamic = Arc::new(DynConfig::open(config(""), meta.clone()));
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
        let dynamic = DynConfig::open(config("[repair]\ninterval = \"off\"\n"), meta);
        let tasks = dynamic.tasks();
        assert_eq!(tasks.iter().map(|t| t.task).collect::<Vec<_>>(), TASKS);
        let repair = &tasks[1];
        assert_eq!((repair.every.as_str(), repair.next_in_ms), ("off", None));
        assert!(tasks[0].next_in_ms.unwrap() <= 60_000);
        assert!(dynamic.run_now("repair").is_ok());
        assert!(dynamic.run_now("nap").is_err());
    }
}
