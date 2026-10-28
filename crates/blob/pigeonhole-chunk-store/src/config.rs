use crate::instances::{memory_instance, resolve_instances, FileInstance, InstanceConfig};
use crate::repair::{
    RepairConfig, DEFAULT_MAX_COST_WAIT_SECS, DEFAULT_REPAIR_INTERVAL, REPAIR_BATCH_SIZE,
};
use crate::sweep::{SweepConfig, DEFAULT_SWEEP_INTERVAL, SWEEP_BATCH_SIZE};
use pigeonhole_blob::{CacheConfig, ChatLimiter, ChatLimiterConfig, InstanceKind, InstanceRole};
use pigeonhole_codec::{self as chunker, ChunkCodec};
use pigeonhole_codec::ByteBudget;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Placement group: which instances receive chunk replicas and the write quorum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementConfig {
    /// Instance ids in the write group (read-write members).
    pub group: Vec<String>,
    pub write_quorum: usize,
}

/// Runtime config: non-secrets from TOML, secrets from environment / `.env`.
#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: String,
    /// Metadata tablet (`[meta]`).
    pub meta: MetaSettings,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    /// Max on-wire chunk size in bytes (`< 20 MiB`).
    pub chunk_size: usize,
    pub chunk_codec: ChunkCodec,
    /// Independent block size for `blocks` packing.
    pub block_size: usize,
    /// Process-wide ingest buffer budget (shared across PUTs).
    pub ingest_budget: Option<ByteBudget>,
    pub tg: ChatLimiterConfig,
    pub cache: CacheConfig,
    pub bytestream: BytestreamSettings,
    pub http: HttpSettings,
    /// Resolved backend instances (`[[instances]]`).
    pub instances: Vec<InstanceConfig>,
    /// Replica placement (`[placement]`); defaults to a single read-write instance.
    pub placement: PlacementConfig,
    /// Stage H sweeper (`[sweep]`).
    pub sweep: SweepConfig,
    /// Stage I repair / backfill (`[repair]`).
    pub repair: RepairConfig,
    /// Prometheus scrape endpoint (`[metrics]`); off by default.
    pub metrics: MetricsSettings,
    pub config_path: PathBuf,
}

/// Metadata tablet: group commit knobs and the command log (`[meta]`, `[meta.log]`).
#[derive(Clone, Debug)]
pub struct MetaSettings {
    /// Group commit window; `0` = append as soon as the previous append returns.
    pub batch_max_delay: Duration,
    pub batch_max_entries: usize,
    /// Snapshot (and collect the log behind it) every this many entries; 0 = never.
    pub snapshot_every_entries: u64,
    /// Also snapshot this often when there are new entries; zero = never.
    pub snapshot_interval: Duration,
    /// Also copy every snapshot here (for `pigeonhole restore`).
    pub snapshot_export: Option<ExportSettings>,
    pub log: LogSettings,
}

/// Where snapshot exports go (`[meta.snapshot.export] kind = ...`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExportSettings {
    /// `tablet-<pos>.snap` files in a directory (another disk, a mount).
    Dir { path: PathBuf },
    /// The newest export pinned in a dedicated Telegram chat.
    Telegram { chat_id: String, bot_token: String },
}

/// Where the command log lives (`[meta.log] kind = ...`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogSettings {
    /// Segmented files in a directory on this host.
    Local { path: PathBuf },
    /// A dedicated Telegram channel / supergroup (pinned head chain,
    /// description fence; docs/meta-backends.md).
    Telegram {
        chat_id: String,
        bot_token: String,
        /// A claim waits this long for a deposed writer's last batch.
        straggler_window: Duration,
    },
}

impl MetaSettings {
    /// Defaults with the log under `dir` (tests).
    pub fn in_dir(dir: &Path) -> Self {
        let mut meta = FileMeta::default().into_settings().expect("default meta settings");
        if let LogSettings::Local { path } = &mut meta.log {
            *path = dir.join(&*path);
        }
        meta
    }
}

/// Optional Prometheus metrics exporter (`[metrics]`).
#[derive(Clone, Debug)]
pub struct MetricsSettings {
    /// When false (default), no recorder / `/metrics` route is installed.
    pub enabled: bool,
    /// Optional dedicated listen address. Empty = `GET /metrics` on the main HTTP server.
    pub listen_addr: Option<String>,
}

impl Default for MetricsSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: None,
        }
    }
}

/// Axum/tower HTTP server knobs.
#[derive(Clone, Debug)]
pub struct HttpSettings {
    /// Idle timeout for request/response bodies (seconds). Resets on each frame.
    /// Replaces the former whole-request timeout so large streaming PUT/GET can
    /// run longer than this as long as bytes keep flowing.
    pub request_timeout_secs: u64,
    /// Max time to produce response headers after the request body reaches EOF.
    /// `0` disables. Default matches [`Self::request_timeout_secs`].
    pub headers_timeout_secs: u64,
    /// Max concurrent in-flight HTTP requests.
    pub max_concurrent_requests: usize,
    /// Max HTTP/1 headers per request (hyper).
    pub max_headers: usize,
}

impl Default for HttpSettings {
    fn default() -> Self {
        Self {
            request_timeout_secs: 300,
            headers_timeout_secs: 300,
            max_concurrent_requests: 256,
            max_headers: 100,
        }
    }
}

/// REAPI v2 remote cache gRPC (optional, feature `bytestream` on the binary).
#[derive(Clone, Debug)]
pub struct BytestreamSettings {
    pub enabled: bool,
    pub listen_addr: String,
    pub instance_name: String,
    pub max_batch_total_size_bytes: i64,
    pub gc_ttl_secs: u64,
}

impl Default for BytestreamSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: "127.0.0.1:8980".into(),
            instance_name: "pigeonhole".into(),
            max_batch_total_size_bytes: 4 * 1024 * 1024,
            gc_ttl_secs: 7 * 24 * 3600,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let path = config_path_from_env();
        Self::load_from_path(&path)
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let _ = dotenvy::dotenv();
        let text = fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        let file: FileConfig = toml::from_str(&text)
            .with_context(|| format!("parse TOML {}", path.display()))?;
        Self::from_file(file, path.to_path_buf())
    }

    fn from_file(file: FileConfig, config_path: PathBuf) -> Result<Self> {
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "pigeonhole".into());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "pigeonholesecret".into());
        if access_key.is_empty() || secret_key.is_empty() {
            bail!("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must not be empty");
        }

        let chunk_size = file.chunk.size;
        chunker::validate_chunk_size(chunk_size).context("chunk.size")?;
        let chunk_codec = file.chunk.codec;
        let block_size = file.chunk.resolve_block_size();
        if block_size > chunker::MAX_LOGICAL_CHUNK {
            bail!("chunk.block_size exceeds MAX_LOGICAL_CHUNK");
        }
        let memory_budget = file.ingest.memory_budget;
        if memory_budget < block_size.saturating_mul(2) {
            bail!(
                "ingest.memory_budget ({memory_budget}) must be >= 2 * chunk.block_size ({})",
                block_size.saturating_mul(2)
            );
        }
        let ingest_budget = Some(ByteBudget::new(memory_budget));

        let tg = file.telegram.into_limiter_config();
        if tg.send_rate_per_sec <= 0.0
            || tg.get_file_rate_per_sec <= 0.0
            || tg.delete_rate_per_sec <= 0.0
        {
            bail!("telegram.*_rate_per_sec must be > 0");
        }
        if tg.upload_concurrency == 0 || tg.download_concurrency == 0 {
            bail!("telegram upload/download concurrency must be >= 1");
        }

        if file.instances.is_empty() {
            bail!("pigeonhole.toml: at least one [[instances]] entry is required");
        }
        let instances = resolve_instances(&file.instances)?;
        let meta = file.meta.into_settings()?;
        if let LogSettings::Telegram { chat_id, .. } = &meta.log {
            if let Some(i) = instances
                .iter()
                .find(|i| i.info.kind == InstanceKind::Telegram && i.scope_id == *chat_id)
            {
                bail!(
                    "[meta.log] chat_id {chat_id} is also instance {:?}: the sweeper deletes every \
                     message of a blob chat, so the log needs a chat of its own",
                    i.info.id
                );
            }
        }
        if let Some(ExportSettings::Telegram { chat_id, .. }) = &meta.snapshot_export {
            if let Some(i) = instances
                .iter()
                .find(|i| i.info.kind == InstanceKind::Telegram && i.scope_id == *chat_id)
            {
                bail!(
                    "[meta.snapshot.export] chat_id {chat_id} is also instance {:?}: the sweeper \
                     deletes every message of a blob chat, so exports need a chat of their own",
                    i.info.id
                );
            }
            if matches!(&meta.log, LogSettings::Telegram { chat_id: log, .. } if log == chat_id) {
                bail!("[meta.snapshot.export] chat_id {chat_id} is the log chat: exports need a chat of their own");
            }
        }

        let placement = resolve_placement(file.placement, &instances)?;
        check_block_size_against_instances(block_size, &instances)?;

        Ok(Self {
            listen_addr: file.listen_addr,
            meta,
            access_key,
            secret_key,
            region: file.region,
            chunk_size,
            chunk_codec,
            block_size,
            ingest_budget,
            tg,
            cache: file.cache.into_cache_config()?,
            bytestream: file.bytestream.into_settings(),
            http: file.http.into_settings(),
            instances,
            placement,
            sweep: file.sweep.into_sweep_config(),
            repair: file.repair.into_repair_config(),
            metrics: file.metrics.into_settings(),
            config_path,
        })
    }

    /// Defaults for in-process tests; metadata files go under `dir`.
    pub fn for_test(dir: &Path) -> Self {
        Self {
            listen_addr: "127.0.0.1:0".into(),
            meta: MetaSettings::in_dir(dir),
            access_key: "pigeonhole".into(),
            secret_key: "pigeonholesecret".into(),
            region: "us-east-1".into(),
            chunk_size: chunker::DEFAULT_CHUNK_SIZE,
            chunk_codec: ChunkCodec::Zstd,
            block_size: chunker::DEFAULT_BLOCK_SIZE,
            ingest_budget: Some(ByteBudget::new(chunker::DEFAULT_INGEST_MEMORY_BUDGET)),
            tg: ChatLimiterConfig {
                send_rate_per_sec: 1000.0,
                send_burst: 100.0,
                get_file_rate_per_sec: 1000.0,
                get_file_burst: 100.0,
                delete_rate_per_sec: 1000.0,
                delete_burst: 100.0,
                upload_concurrency: 8,
                download_concurrency: 8,
            },
            cache: CacheConfig {
                enabled: false,
                ..CacheConfig::default()
            },
            bytestream: BytestreamSettings::default(),
            http: HttpSettings::default(),
            instances: vec![memory_instance("mem")],
            placement: PlacementConfig {
                group: vec!["mem".into()],
                write_quorum: 1,
            },
            sweep: SweepConfig::default(),
            repair: RepairConfig::default(),
            metrics: MetricsSettings::default(),
            config_path: PathBuf::from("(test)"),
        }
    }

    pub fn chat_limiter(&self) -> Arc<ChatLimiter> {
        Arc::new(ChatLimiter::new(self.tg.clone()))
    }

    /// Primary write instance (first member of `[placement].group`).
    pub fn primary_instance(&self) -> Result<&InstanceConfig> {
        let id = self
            .placement
            .group
            .first()
            .context("placement.group is empty")?;
        self.instances
            .iter()
            .find(|i| i.info.id == *id)
            .with_context(|| format!("placement primary instance {id:?} not in [[instances]]"))
    }

    /// Scope id (chat/channel) of the primary write instance.
    pub fn primary_scope_id(&self) -> Result<&str> {
        Ok(self.primary_instance()?.scope_id.as_str())
    }
}

fn config_path_from_env() -> PathBuf {
    env::var("PIGEONHOLE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("pigeonhole.toml"))
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    listen_addr: String,
    region: String,
    meta: FileMeta,
    chunk: FileChunk,
    #[serde(default)]
    ingest: FileIngest,
    telegram: FileTelegram,
    #[serde(default)]
    cache: FileCache,
    #[serde(default)]
    bytestream: FileBytestream,
    #[serde(default)]
    http: FileHttp,
    #[serde(default)]
    instances: Vec<FileInstance>,
    #[serde(default)]
    placement: Option<FilePlacement>,
    #[serde(default)]
    sweep: FileSweep,
    #[serde(default)]
    repair: FileRepair,
    #[serde(default)]
    metrics: FileMetrics,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8333".into(),
            region: "us-east-1".into(),
            meta: FileMeta::default(),
            chunk: FileChunk::default(),
            ingest: FileIngest::default(),
            telegram: FileTelegram::default(),
            cache: FileCache::default(),
            bytestream: FileBytestream::default(),
            http: FileHttp::default(),
            instances: Vec::new(),
            placement: None,
            sweep: FileSweep::default(),
            repair: FileRepair::default(),
            metrics: FileMetrics::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMeta {
    batch_max_delay_ms: u64,
    batch_max_entries: usize,
    log: FileLog,
    snapshot: FileMetaSnapshot,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMetaSnapshot {
    every_entries: u64,
    every_secs: u64,
    export: Option<FileExport>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum FileExport {
    Dir {
        path: PathBuf,
    },
    Telegram {
        chat_id: i64,
        #[serde(default = "default_bot_token_env")]
        bot_token_env: String,
    },
}

impl Default for FileMetaSnapshot {
    fn default() -> Self {
        Self { every_entries: 10_000, every_secs: 600, export: None }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum FileLog {
    Local {
        path: PathBuf,
    },
    Telegram {
        chat_id: i64,
        #[serde(default = "default_bot_token_env")]
        bot_token_env: String,
        #[serde(default = "default_straggler_window_secs")]
        straggler_window_secs: u64,
    },
}

fn default_bot_token_env() -> String {
    "BOT_TOKEN".into()
}

fn default_straggler_window_secs() -> u64 {
    90
}

impl Default for FileMeta {
    fn default() -> Self {
        Self {
            batch_max_delay_ms: 0,
            batch_max_entries: 1024,
            log: FileLog::default(),
            snapshot: FileMetaSnapshot::default(),
        }
    }
}

impl Default for FileLog {
    fn default() -> Self {
        FileLog::Local { path: "pigeonhole-log".into() }
    }
}

impl FileMeta {
    fn into_settings(self) -> Result<MetaSettings> {
        let log = match self.log {
            FileLog::Local { path } => LogSettings::Local { path },
            FileLog::Telegram { chat_id, bot_token_env, straggler_window_secs } => {
                let bot_token = env::var(&bot_token_env)
                    .with_context(|| format!("[meta.log] bot_token_env {bot_token_env} is not set"))?;
                LogSettings::Telegram {
                    chat_id: chat_id.to_string(),
                    bot_token,
                    straggler_window: Duration::from_secs(straggler_window_secs),
                }
            }
        };
        let snapshot_export = match self.snapshot.export {
            None => None,
            Some(FileExport::Dir { path }) => Some(ExportSettings::Dir { path }),
            Some(FileExport::Telegram { chat_id, bot_token_env }) => Some(ExportSettings::Telegram {
                chat_id: chat_id.to_string(),
                bot_token: env::var(&bot_token_env).with_context(|| {
                    format!("[meta.snapshot.export] bot_token_env {bot_token_env} is not set")
                })?,
            }),
        };
        Ok(MetaSettings {
            batch_max_delay: Duration::from_millis(self.batch_max_delay_ms),
            batch_max_entries: self.batch_max_entries.max(1),
            snapshot_every_entries: self.snapshot.every_entries,
            snapshot_interval: Duration::from_secs(self.snapshot.every_secs),
            snapshot_export,
            log,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMetrics {
    enabled: bool,
    /// Empty / omitted → scrape on the main listen address at `GET /metrics`.
    listen_addr: Option<String>,
}

impl Default for FileMetrics {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: None,
        }
    }
}

impl FileMetrics {
    fn into_settings(self) -> MetricsSettings {
        let listen_addr = self
            .listen_addr
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        MetricsSettings {
            enabled: self.enabled,
            listen_addr,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileSweep {
    interval_secs: u64,
    batch_size: usize,
}

impl Default for FileSweep {
    fn default() -> Self {
        Self {
            interval_secs: DEFAULT_SWEEP_INTERVAL.as_secs(),
            batch_size: SWEEP_BATCH_SIZE,
        }
    }
}

impl FileSweep {
    fn into_sweep_config(self) -> SweepConfig {
        SweepConfig {
            batch_size: self.batch_size.max(1),
            interval: Duration::from_secs(self.interval_secs.max(1)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileRepair {
    interval_secs: u64,
    batch_size: usize,
    max_cost_wait_secs: f64,
    scrub: bool,
}

impl Default for FileRepair {
    fn default() -> Self {
        Self {
            interval_secs: DEFAULT_REPAIR_INTERVAL.as_secs(),
            batch_size: REPAIR_BATCH_SIZE,
            max_cost_wait_secs: DEFAULT_MAX_COST_WAIT_SECS,
            scrub: true,
        }
    }
}

impl FileRepair {
    fn into_repair_config(self) -> RepairConfig {
        RepairConfig {
            interval: Duration::from_secs(self.interval_secs.max(1)),
            batch_size: self.batch_size.max(1),
            max_cost_wait_secs: self.max_cost_wait_secs.max(0.0),
            scrub: self.scrub,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePlacement {
    group: Vec<String>,
    write_quorum: usize,
}

/// E.4: `block_size` (+ margin) must fit under every instance's max_blob_size,
/// including read-only / retired members that may still serve repair reads.
pub fn check_block_size_against_instances(
    block_size: usize,
    instances: &[InstanceConfig],
) -> Result<()> {
    if instances.is_empty() {
        return Ok(());
    }
    let mut min_max = usize::MAX;
    for inst in instances {
        let max = match inst.info.kind {
            InstanceKind::Telegram | InstanceKind::Memory => 20 * 1024 * 1024 - 1,
            InstanceKind::Discord => {
                pigeonhole_types::BackendLimits::discord(inst.max_blob_size).max_blob_size
            }
        };
        min_max = min_max.min(max);
    }
    if block_size >= min_max {
        bail!(
            "chunk.block_size ({block_size}) must be < min(max_blob_size) of configured instances ({min_max})"
        );
    }
    // Incompressible blocks store raw `block_size`; keep a small header margin.
    let need = block_size.saturating_add(64);
    if need >= min_max {
        bail!(
            "chunk.block_size ({block_size}) with block margin exceeds min(max_blob_size) ({min_max})"
        );
    }
    Ok(())
}

fn resolve_placement(
    raw: Option<FilePlacement>,
    instances: &[InstanceConfig],
) -> Result<PlacementConfig> {
    match raw {
        None => {
            // Without [placement]: single read-write instance group.
            let rw: Vec<&InstanceConfig> = instances
                .iter()
                .filter(|i| i.info.role == InstanceRole::ReadWrite)
                .collect();
            let chosen = match rw.as_slice() {
                [] => bail!("placement: no read-write instance available"),
                [one] => one,
                _ => bail!("placement: several read-write instances; set [placement].group"),
            };
            Ok(PlacementConfig {
                group: vec![chosen.info.id.clone()],
                write_quorum: 1,
            })
        }
        Some(p) => {
            if p.group.is_empty() {
                bail!("placement.group must not be empty");
            }
            if p.write_quorum == 0 || p.write_quorum > p.group.len() {
                bail!(
                    "placement.write_quorum {} out of range for group of {}",
                    p.write_quorum,
                    p.group.len()
                );
            }
            let known: std::collections::HashSet<&str> =
                instances.iter().map(|i| i.info.id.as_str()).collect();
            for id in &p.group {
                if !known.contains(id.as_str()) {
                    bail!("placement.group references unknown instance {id:?}");
                }
            }
            for id in &p.group {
                let inst = instances.iter().find(|i| i.info.id == *id).unwrap();
                if inst.info.role != InstanceRole::ReadWrite {
                    bail!(
                        "placement.group member {id:?} is {:?}; write group requires read-write",
                        inst.info.role
                    );
                }
            }
            Ok(PlacementConfig {
                group: p.group,
                write_quorum: p.write_quorum,
            })
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileHttp {
    request_timeout_secs: u64,
    headers_timeout_secs: Option<u64>,
    max_concurrent_requests: usize,
    max_headers: usize,
}

impl Default for FileHttp {
    fn default() -> Self {
        let d = HttpSettings::default();
        Self {
            request_timeout_secs: d.request_timeout_secs,
            headers_timeout_secs: None,
            max_concurrent_requests: d.max_concurrent_requests,
            max_headers: d.max_headers,
        }
    }
}

impl FileHttp {
    fn into_settings(self) -> HttpSettings {
        let idle = self.request_timeout_secs.max(1);
        HttpSettings {
            request_timeout_secs: idle,
            // Absent key → same as idle; explicit 0 disables headers timeout.
            headers_timeout_secs: self.headers_timeout_secs.unwrap_or(idle),
            max_concurrent_requests: self.max_concurrent_requests.max(1),
            max_headers: self.max_headers.max(16),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileBytestream {
    enabled: bool,
    listen_addr: String,
    instance_name: String,
    max_batch_total_size_bytes: i64,
    gc_ttl_secs: u64,
}

impl Default for FileBytestream {
    fn default() -> Self {
        let d = BytestreamSettings::default();
        Self {
            enabled: d.enabled,
            listen_addr: d.listen_addr,
            instance_name: d.instance_name,
            max_batch_total_size_bytes: d.max_batch_total_size_bytes,
            gc_ttl_secs: d.gc_ttl_secs,
        }
    }
}

impl FileBytestream {
    fn into_settings(self) -> BytestreamSettings {
        BytestreamSettings {
            enabled: self.enabled,
            listen_addr: self.listen_addr,
            instance_name: self.instance_name,
            max_batch_total_size_bytes: self.max_batch_total_size_bytes.max(1024),
            gc_ttl_secs: self.gc_ttl_secs,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileCache {
    enabled: bool,
    memory_bytes: usize,
    disk_path: String,
    disk_bytes: Option<usize>,
    block_memory_bytes: Option<usize>,
    readahead_blocks: Option<usize>,
    write_through: bool,
    max_object_bytes: usize,
    metrics_interval_secs: u64,
}

impl Default for FileCache {
    fn default() -> Self {
        let d = CacheConfig::default();
        Self {
            enabled: d.enabled,
            memory_bytes: d.memory_bytes,
            disk_path: String::new(),
            disk_bytes: None,
            block_memory_bytes: None,
            readahead_blocks: None,
            write_through: d.write_through,
            max_object_bytes: d.max_object_bytes,
            metrics_interval_secs: d.metrics_interval_secs,
        }
    }
}

impl FileCache {
    fn into_cache_config(self) -> Result<CacheConfig> {
        let disk_path = if self.disk_path.trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(self.disk_path))
        };
        if disk_path.is_some() && self.disk_bytes.is_none() {
            bail!("cache.disk_bytes is required when cache.disk_path is set");
        }
        let defaults = CacheConfig::default();
        let block_memory_bytes = self
            .block_memory_bytes
            .unwrap_or(defaults.block_memory_bytes);
        let readahead_blocks = self.readahead_blocks.unwrap_or(defaults.readahead_blocks);
        Ok(CacheConfig {
            enabled: self.enabled,
            memory_bytes: self.memory_bytes.max(1024 * 1024),
            disk_path,
            disk_bytes: self.disk_bytes,
            block_memory_bytes: block_memory_bytes.max(1024 * 1024),
            readahead_blocks,
            write_through: self.write_through,
            max_object_bytes: self.max_object_bytes.max(1024),
            metrics_interval_secs: self.metrics_interval_secs,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileChunk {
    size: usize,
    codec: ChunkCodec,
    block_size: Option<usize>,
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            size: chunker::DEFAULT_CHUNK_SIZE,
            codec: ChunkCodec::Zstd,
            block_size: None,
        }
    }
}

impl FileChunk {
    fn resolve_block_size(&self) -> usize {
        self.block_size
            .map(|b| b.max(1024))
            .unwrap_or(chunker::DEFAULT_BLOCK_SIZE)
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileIngest {
    memory_budget: usize,
}

impl Default for FileIngest {
    fn default() -> Self {
        Self {
            memory_budget: chunker::DEFAULT_INGEST_MEMORY_BUDGET,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileTelegram {
    send_rate_per_sec: Option<f64>,
    send_burst: Option<f64>,
    get_file_rate_per_sec: Option<f64>,
    get_file_burst: Option<f64>,
    delete_rate_per_sec: Option<f64>,
    delete_burst: Option<f64>,
    upload_concurrency: usize,
    download_concurrency: usize,
}

impl Default for FileTelegram {
    fn default() -> Self {
        let d = ChatLimiterConfig::default();
        Self {
            send_rate_per_sec: None,
            send_burst: None,
            get_file_rate_per_sec: None,
            get_file_burst: None,
            delete_rate_per_sec: None,
            delete_burst: None,
            upload_concurrency: d.upload_concurrency,
            download_concurrency: d.download_concurrency,
        }
    }
}

impl FileTelegram {
    fn into_limiter_config(self) -> ChatLimiterConfig {
        let d = ChatLimiterConfig::default();
        ChatLimiterConfig {
            send_rate_per_sec: self.send_rate_per_sec.unwrap_or(d.send_rate_per_sec),
            send_burst: self.send_burst.unwrap_or(d.send_burst),
            get_file_rate_per_sec: self
                .get_file_rate_per_sec
                .unwrap_or(d.get_file_rate_per_sec),
            get_file_burst: self.get_file_burst.unwrap_or(d.get_file_burst),
            delete_rate_per_sec: self
                .delete_rate_per_sec
                .unwrap_or(d.delete_rate_per_sec),
            delete_burst: self.delete_burst.unwrap_or(d.delete_burst),
            upload_concurrency: self.upload_concurrency.max(1),
            download_concurrency: self.download_concurrency.max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn load(toml: &str) -> Result<Config> {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(toml.as_bytes()).unwrap();
        Config::load_from_path(f.path())
    }

    const MEM: &str = r#"
[[instances]]
id = "mem"
kind = "memory"
"#;

    #[test]
    fn parses_example_shape() {
        let cfg = load(&format!(
            r#"
listen_addr = "127.0.0.1:9"
[chunk]
size = 1024
codec = "raw"
[telegram]
send_rate_per_sec = 1.0
send_burst = 2.0
get_file_rate_per_sec = 20.0
upload_concurrency = 4
{MEM}"#
        ))
        .unwrap();
        assert_eq!(cfg.chunk_size, 1024);
        assert_eq!(cfg.chunk_codec, ChunkCodec::Raw);
        assert_eq!(cfg.tg.send_rate_per_sec, 1.0);
        assert_eq!(cfg.tg.get_file_rate_per_sec, 20.0);
        assert_eq!(cfg.tg.upload_concurrency, 4);
        assert_eq!(cfg.placement.group, vec!["mem".to_string()]);
        assert_eq!(cfg.placement.write_quorum, 1);
        assert_eq!(cfg.meta.batch_max_delay, Duration::ZERO);
        assert_eq!(cfg.meta.snapshot_every_entries, 10_000);
        assert_eq!(cfg.meta.snapshot_interval, Duration::from_secs(600));
        assert_eq!(cfg.meta.log, LogSettings::Local { path: "pigeonhole-log".into() });
    }

    #[test]
    fn parses_meta_section() {
        let cfg = load(&format!(
            "[meta]\nbatch_max_delay_ms = 250\n[meta.snapshot]\nevery_entries = 0\nevery_secs = 60\n[meta.snapshot.export]\nkind = \"dir\"\npath = \"/backup\"\n[meta.log]\nkind = \"local\"\npath = \"/var/log-dir\"\n{MEM}"
        ))
        .unwrap();
        assert_eq!(cfg.meta.batch_max_delay, Duration::from_millis(250));
        assert_eq!(cfg.meta.snapshot_every_entries, 0);
        assert_eq!(cfg.meta.snapshot_interval, Duration::from_secs(60));
        assert_eq!(cfg.meta.snapshot_export, Some(ExportSettings::Dir { path: "/backup".into() }));
        assert_eq!(cfg.meta.log, LogSettings::Local { path: "/var/log-dir".into() });
        for bad in ["[meta]\ndb_path = \"m.db\"\n", "[meta.log]\nkind = \"s3\"\n", "[meta.log]\nkind = \"local\"\nchat_id = 1\n"] {
            assert!(load(&format!("{bad}{MEM}")).is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn telegram_log_needs_a_chat_of_its_own() {
        std::env::set_var("PH_TEST_LOG_TOKEN", "1:test");
        let log = "[meta.log]\nkind = \"telegram\"\nchat_id = -100\nbot_token_env = \"PH_TEST_LOG_TOKEN\"\n";
        let cfg = load(&format!("{log}{MEM}")).unwrap();
        assert_eq!(
            cfg.meta.log,
            LogSettings::Telegram {
                chat_id: "-100".into(),
                bot_token: "1:test".into(),
                straggler_window: Duration::from_secs(90),
            }
        );
        let blob_chat = "[[instances]]\nid = \"tg\"\nkind = \"telegram\"\nchat_id = -100\nbot_token_env = \"PH_TEST_LOG_TOKEN\"\n";
        let err = load(&format!("{log}{blob_chat}")).unwrap_err();
        assert!(format!("{err:#}").contains("chat of its own"), "{err:#}");
    }

    #[test]
    fn telegram_export_needs_a_chat_of_its_own() {
        std::env::set_var("PH_TEST_EXPORT_TOKEN", "1:test");
        let export = |chat: i64| {
            format!("[meta.snapshot.export]\nkind = \"telegram\"\nchat_id = {chat}\nbot_token_env = \"PH_TEST_EXPORT_TOKEN\"\n")
        };
        let cfg = load(&format!("{}{MEM}", export(-200))).unwrap();
        assert_eq!(
            cfg.meta.snapshot_export,
            Some(ExportSettings::Telegram { chat_id: "-200".into(), bot_token: "1:test".into() })
        );
        let blob_chat = "[[instances]]\nid = \"tg\"\nkind = \"telegram\"\nchat_id = -200\nbot_token_env = \"PH_TEST_EXPORT_TOKEN\"\n";
        let err = load(&format!("{}{blob_chat}", export(-200))).unwrap_err();
        assert!(format!("{err:#}").contains("chat of their own"), "{err:#}");
        let log = "[meta.log]\nkind = \"telegram\"\nchat_id = -200\nbot_token_env = \"PH_TEST_EXPORT_TOKEN\"\n";
        let err = load(&format!("{}{log}{MEM}", export(-200))).unwrap_err();
        assert!(format!("{err:#}").contains("log chat"), "{err:#}");
    }

    #[test]
    fn example_config_loads() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../pigeonhole.toml.example");
        std::env::set_var("BOT_TOKEN", "123:example");
        let cfg = Config::load_from_path(&path).unwrap();
        assert_eq!(cfg.primary_instance().unwrap().info.id, "tg-main");
    }

    #[test]
    fn requires_instances() {
        let msg = format!("{:#}", load("listen_addr = \"127.0.0.1:9\"\n").unwrap_err());
        assert!(msg.contains("[[instances]]"), "unexpected error: {msg}");
    }

    #[test]
    fn rejects_unknown_keys() {
        for stale in ["database_url = \"sqlite:x.db\"\n", "memory = true\n", "chat_id = \"-1001\"\n", "[telegram]\nrate_per_sec = 1.0\n"] {
            let err = load(&format!("{stale}{MEM}")).unwrap_err();
            assert!(format!("{err:#}").contains("unknown field"), "{stale:?} accepted");
        }
    }

    #[test]
    fn several_read_write_instances_need_placement() {
        let two = r#"
[[instances]]
id = "a"
kind = "memory"
[[instances]]
id = "b"
kind = "memory"
"#;
        let msg = format!("{:#}", load(two).unwrap_err());
        assert!(msg.contains("[placement]"), "unexpected error: {msg}");
        let cfg = load(&format!("{two}[placement]\ngroup = [\"a\", \"b\"]\nwrite_quorum = 2\n")).unwrap();
        assert_eq!(cfg.placement.group, vec!["a", "b"]);
        assert_eq!(cfg.placement.write_quorum, 2);
    }

    #[test]
    fn parses_discord_instance() {
        std::env::set_var("DISCORD_BOT_TOKEN", "discord-test-token");
        let cfg = load(
            r#"
[[instances]]
id = "dc"
kind = "discord"
channel_id = 123456789
max_blob_size = 8388608
"#,
        )
        .unwrap();
        std::env::remove_var("DISCORD_BOT_TOKEN");
        let inst = cfg.primary_instance().unwrap();
        assert_eq!(inst.info.kind, InstanceKind::Discord);
        assert_eq!(inst.scope_id, "123456789");
        assert_eq!(inst.max_blob_size, Some(8388608));
    }

    #[test]
    fn rejects_memory_budget_below_two_blocks() {
        let err = load(&format!(
            "[chunk]\nblock_size = 1048576\n[ingest]\nmemory_budget = 1048576\n{MEM}"
        ))
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("ingest.memory_budget"), "unexpected error: {msg}");
    }
}
