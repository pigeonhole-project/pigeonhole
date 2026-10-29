use crate::instances::{memory_instance, InstanceConfig, TelegramLogin};
use crate::stores::{FileReplicas, FileShards, FileStore, Store, Topology};
use pigeonhole_chunk_store::{
    RepairConfig, SweepConfig, DEFAULT_MAX_COST_WAIT_SECS, DEFAULT_REPAIR_INTERVAL, DEFAULT_SWEEP_INTERVAL,
    REPAIR_BATCH_SIZE, SWEEP_BATCH_SIZE,
};
use pigeonhole_blob::{CacheConfig, InstanceKind};
use pigeonhole_limits::{ChatLimiter, ChatLimiterConfig, LimiterRegistry};
use pigeonhole_codec::{self as chunker, ChunkCodec};
use pigeonhole_codec::ByteBudget;
use pigeonhole_gateway_s3::S3Options;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Blob placement (`[chunk] store`): shards, each a replica group. A chunk
/// goes to one shard and to every member of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementConfig {
    pub shards: Vec<ShardPlacement>,
}

/// One shard: the stores holding a replica of each of its chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardPlacement {
    pub group: Vec<String>,
    pub write_quorum: usize,
}

impl PlacementConfig {
    /// One shard, one store.
    pub fn single(id: &str) -> Self {
        Self { shards: vec![ShardPlacement { group: vec![id.to_string()], write_quorum: 1 }] }
    }
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
    /// `[telegram]`: rate limits of Telegram chats.
    pub tg: ChatLimiterConfig,
    /// One rate limiter per (bot, chat), sharing one per bot.
    pub limiters: Arc<LimiterRegistry>,
    /// `[discord]`: rate limits of Discord channels.
    pub discord: ChatLimiterConfig,
    pub discord_limiters: Arc<LimiterRegistry>,
    pub cache: CacheConfig,
    pub bytestream: BytestreamSettings,
    pub http: HttpSettings,
    /// The blob stores of `[chunk] store`.
    pub instances: Vec<InstanceConfig>,
    /// How chunks are placed on them.
    pub placement: PlacementConfig,
    /// Stage H sweeper (`[sweep]`).
    pub sweep: SweepConfig,
    /// Stage I repair / backfill (`[repair]`).
    pub repair: RepairConfig,
    /// Prometheus scrape endpoint (`[metrics]`); off by default.
    pub metrics: MetricsSettings,
    /// What the process logs (`[log]`).
    pub log: LoggingSettings,
    pub config_path: PathBuf,
}

/// Default `[log] level`: our crates at `info`; openraft's per-step chatter
/// and s3s's own error lines (a refused signature logged as ERROR) are off —
/// the request log says what each request did.
pub const DEFAULT_LOG_LEVEL: &str = "info,openraft=warn,s3s=off";

/// `[log]`: what the process prints, to stderr (docs/observability.md).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggingSettings {
    /// `tracing` filter directives (`info`, `info,pigeonhole_meta=debug`, …);
    /// `RUST_LOG`, when set, wins.
    pub level: String,
    pub format: LogFormat,
    /// A request slower than this to its response headers is logged at `info`.
    pub slow_request: Duration,
}

/// `[log] format`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// For people; colours only on a terminal.
    Text,
    /// One JSON object per line, with the spans an event happened in.
    Json,
}

impl Default for LoggingSettings {
    fn default() -> Self {
        FileLog::default().into_settings()
    }
}

impl LoggingSettings {
    /// `[log]` of the config file (`PIGEONHOLE_CONFIG`, else
    /// `pigeonhole.toml`), read before anything else so that loading the rest
    /// of the config is logged too. A missing or broken file gives the
    /// defaults: `Config::load` reports the error.
    pub fn from_config_file() -> Self {
        let _ = dotenvy::dotenv();
        let log = fs::read_to_string(config_path_from_env())
            .ok()
            .and_then(|text| text.parse::<toml::Table>().ok())
            .and_then(|mut t| t.remove("log"))
            .and_then(|v| v.try_into::<FileLog>().ok());
        log.unwrap_or_default().into_settings()
    }
}

/// Metadata tablet: group commit knobs, the command log and the snapshot
/// store (`[meta]`, `log` / `snapshot` naming a `[[store]]`).
#[derive(Clone, Debug)]
pub struct MetaSettings {
    /// Snapshot (and collect the log behind it) every this many entries; 0 = never.
    pub snapshot_every_entries: u64,
    /// Also snapshot this often when there are new entries; zero = never.
    pub snapshot_interval: Duration,
    /// The newest snapshots kept, with the log from the oldest of them (≥ 1).
    pub keep_snapshots: usize,
    /// A Discord log's lease (docs/discord-lease.md): batches without a
    /// thread each; a new writer waits it out after a crash. Zero = a
    /// thread per batch.
    pub lease: Duration,
    pub log: LogSettings,
    pub snapshot: SnapshotSettings,
}

/// Where snapshots live (`[meta] snapshot`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotSettings {
    /// Files in a directory (may be the log's).
    LocalDisk { path: PathBuf },
    /// Process memory, with a memory log (tests).
    Memory,
    /// A dedicated Telegram channel read over MTProto.
    Telegram { chat_id: String, login: TelegramLogin },
    /// A dedicated Discord channel (a chain over threads; docs/discord-log.md).
    Discord { channel_id: String, bot_token: String, max_blob_size: Option<usize> },
}

/// Where the command log lives (`[meta] log`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogSettings {
    /// Segmented files in a directory on this host.
    LocalDisk { path: PathBuf },
    /// Process memory: gone with the process (tests, throwaway runs).
    Memory,
    /// A dedicated Telegram channel read over MTProto (fenced by the order
    /// of its messages; docs/telegram-mtproto.md).
    Telegram { chat_id: String, login: TelegramLogin },
    /// A dedicated Discord channel, fenced by threads (docs/discord-log.md).
    Discord { channel_id: String, bot_token: String, max_blob_size: Option<usize> },
}

impl LogSettings {
    /// The log lives off this host (a chat), so its snapshots must too.
    fn off_host(&self) -> bool {
        matches!(self, LogSettings::Telegram { .. } | LogSettings::Discord { .. })
    }
}

impl MetaSettings {
    /// Defaults with the log and snapshots under `dir` (tests).
    pub fn in_dir(dir: &Path) -> Self {
        let mut topology = Topology::new(Vec::new(), Vec::new(), Vec::new()).expect("empty topology");
        let mut meta = FileMeta::default().into_settings(&mut topology).expect("default meta settings");
        if let LogSettings::LocalDisk { path } = &mut meta.log {
            *path = dir.join(&*path);
        }
        if let SnapshotSettings::LocalDisk { path } = &mut meta.snapshot {
            *path = dir.join(&*path);
        }
        meta
    }

    /// Whether snapshots can survive losing the log (another directory, a
    /// chat next to a local log): then a lossy restore is possible.
    pub fn snapshots_outlive_log(&self) -> bool {
        match (&self.log, &self.snapshot) {
            (LogSettings::LocalDisk { path: log }, SnapshotSettings::LocalDisk { path }) => log != path,
            (LogSettings::LocalDisk { .. }, SnapshotSettings::Telegram { .. } | SnapshotSettings::Discord { .. }) => {
                true
            }
            (LogSettings::LocalDisk { .. }, SnapshotSettings::Memory) => false,
            (LogSettings::Telegram { .. } | LogSettings::Discord { .. } | LogSettings::Memory, _) => false,
        }
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

        let tg = file.telegram.into_limiter_config("telegram", ChatLimiterConfig::default())?;
        let discord = file.discord.into_limiter_config("discord", ChatLimiterConfig::discord())?;
        if tg.upload_concurrency == 0 || tg.download_concurrency == 0 {
            bail!("telegram upload/download concurrency must be >= 1");
        }

        let mut topology = Topology::new(file.store, file.replicas, file.shards)?;
        let meta = file.meta.into_settings(&mut topology)?;
        let blob_store = file
            .chunk
            .store
            .as_deref()
            .context("[chunk] store is required: the [[store]] or [[replicas]] id chunks go to")?;
        let (instances, placement) = topology.blob_placement(blob_store)?;
        topology.check_all_used()?;
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
            limiters: Arc::new(LimiterRegistry::new(tg.clone())),
            discord_limiters: Arc::new(LimiterRegistry::new(discord.clone())),
            tg,
            discord,
            cache: file.cache.into_cache_config()?,
            bytestream: file.bytestream.into_settings(),
            http: file.http.into_settings(),
            instances,
            placement,
            sweep: file.sweep.into_sweep_config(),
            repair: file.repair.into_repair_config(),
            metrics: file.metrics.into_settings(),
            log: file.log.into_settings(),
            config_path,
        })
    }

    fn test_limits() -> ChatLimiterConfig {
        ChatLimiterConfig {
            bot_rate_per_sec: 1000.0,
            bot_burst: 100.0,
            send_rate_per_sec: 1000.0,
            send_burst: 100.0,
            get_file_rate_per_sec: 1000.0,
            get_file_burst: 100.0,
            delete_rate_per_sec: 1000.0,
            delete_burst: 100.0,
            info_rate_per_sec: 1000.0,
            info_burst: 100.0,
            upload_concurrency: 8,
            download_concurrency: 8,
        }
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
            tg: Self::test_limits(),
            limiters: Arc::new(LimiterRegistry::new(Self::test_limits())),
            discord: Self::test_limits(),
            discord_limiters: Arc::new(LimiterRegistry::new(Self::test_limits())),
            cache: CacheConfig {
                enabled: false,
                ..CacheConfig::default()
            },
            bytestream: BytestreamSettings::default(),
            http: HttpSettings::default(),
            instances: vec![memory_instance("mem")],
            placement: PlacementConfig::single("mem"),
            sweep: SweepConfig::default(),
            repair: RepairConfig::default(),
            metrics: MetricsSettings::default(),
            log: LoggingSettings::default(),
            config_path: PathBuf::from("(test)"),
        }
    }

    /// What the S3 gateway takes from this config.
    pub fn s3_options(&self) -> S3Options {
        S3Options {
            region: self.region.clone(),
            chunk_size: self.chunk_size,
            chunk_codec: self.chunk_codec,
            block_size: self.block_size,
            ingest_budget: self.ingest_budget.clone(),
        }
    }

    /// The limiter of `chat` for the bot holding `bot_token` (shared by every
    /// user of that chat, and bot-wide with the bot's other chats), with the
    /// limits of `kind`'s service.
    pub fn chat_limiter(&self, kind: InstanceKind, bot_token: &str, chat: &str) -> Arc<ChatLimiter> {
        // Keyed by the bot's id, never the secret: a Telegram token is
        // `<id>:<secret>`, a Discord one `<base64 id>.<timestamp>.<hmac>`.
        match kind {
            InstanceKind::Discord => self.discord_limiters.chat(bot_token.split('.').next().unwrap_or_default(), chat),
            _ => self.limiters.chat(bot_token.split(':').next().unwrap_or_default(), chat),
        }
    }

    /// Primary blob store (first member of `[chunk] store`).
    pub fn primary_instance(&self) -> Result<&InstanceConfig> {
        let id = self
            .placement
            .shards
            .first()
            .and_then(|s| s.group.first())
            .context("no blob store")?;
        self.instances
            .iter()
            .find(|i| i.info.id == *id)
            .with_context(|| format!("primary blob store {id:?} not resolved"))
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
    telegram: FileLimits,
    discord: FileLimits,
    #[serde(default)]
    cache: FileCache,
    #[serde(default)]
    bytestream: FileBytestream,
    #[serde(default)]
    http: FileHttp,
    #[serde(default)]
    store: Vec<FileStore>,
    #[serde(default)]
    replicas: Vec<FileReplicas>,
    #[serde(default)]
    shards: Vec<FileShards>,
    #[serde(default)]
    sweep: FileSweep,
    #[serde(default)]
    repair: FileRepair,
    #[serde(default)]
    metrics: FileMetrics,
    #[serde(default)]
    log: FileLog,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLog {
    level: String,
    format: LogFormat,
    slow_request_ms: u64,
}

impl Default for FileLog {
    fn default() -> Self {
        Self { level: DEFAULT_LOG_LEVEL.into(), format: LogFormat::Text, slow_request_ms: 5_000 }
    }
}

impl FileLog {
    fn into_settings(self) -> LoggingSettings {
        LoggingSettings { level: self.level, format: self.format, slow_request: Duration::from_millis(self.slow_request_ms) }
    }
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8333".into(),
            region: "us-east-1".into(),
            meta: FileMeta::default(),
            chunk: FileChunk::default(),
            ingest: FileIngest::default(),
            telegram: FileLimits::default(),
            discord: FileLimits::default(),
            cache: FileCache::default(),
            bytestream: FileBytestream::default(),
            http: FileHttp::default(),
            store: Vec::new(),
            replicas: Vec::new(),
            shards: Vec::new(),
            sweep: FileSweep::default(),
            repair: FileRepair::default(),
            metrics: FileMetrics::default(),
            log: FileLog::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileMeta {
    /// `[[store]]` id of the log (`dir` | `telegram`); unset = a `dir` store
    /// at `pigeonhole-meta`.
    log: Option<String>,
    /// `[[store]]` id of the snapshots; unset = the log's store if it is a `dir`.
    snapshot: Option<String>,
    snapshot_every_entries: u64,
    snapshot_every_secs: u64,
    keep_snapshots: usize,
    /// A Discord log's lease, in seconds; 0 = a thread per batch.
    lease_secs: u64,
}

impl Default for FileMeta {
    fn default() -> Self {
        Self {
            log: None,
            snapshot: None,
            snapshot_every_entries: 10_000,
            snapshot_every_secs: 600,
            keep_snapshots: 2,
            lease_secs: 45,
        }
    }
}

/// Where the metadata goes when `[meta] log` is unset.
const DEFAULT_META_DIR: &str = "pigeonhole-meta";

impl FileMeta {
    fn into_settings(self, topology: &mut Topology) -> Result<MetaSettings> {
        let log_store = match &self.log {
            Some(id) => topology.meta_store(id, "[meta] log")?,
            None => Store::LocalDisk { path: DEFAULT_META_DIR.into() },
        };
        let log = match &log_store {
            Store::LocalDisk { path } => LogSettings::LocalDisk { path: path.clone() },
            Store::Memory => LogSettings::Memory,
            Store::Telegram { chat_id, login, .. } => {
                LogSettings::Telegram { chat_id: chat_id.clone(), login: login.clone() }
            }
            Store::Discord { channel_id, bot_token, max_blob_size, .. } => LogSettings::Discord {
                channel_id: channel_id.clone(),
                bot_token: bot_token.clone(),
                max_blob_size: *max_blob_size,
            },
        };
        let snapshot_store = match &self.snapshot {
            Some(id) => topology.meta_store(id, "[meta] snapshot")?,
            None if matches!(log_store, Store::LocalDisk { .. } | Store::Memory) => log_store.clone(),
            None => bail!("[meta] snapshot is required with a telegram or discord log"),
        };
        let snapshot = match snapshot_store {
            Store::LocalDisk { path } => SnapshotSettings::LocalDisk { path },
            Store::Memory => SnapshotSettings::Memory,
            Store::Telegram { chat_id, login, .. } => SnapshotSettings::Telegram { chat_id, login },
            Store::Discord { channel_id, bot_token, max_blob_size, .. } => {
                SnapshotSettings::Discord { channel_id, bot_token, max_blob_size }
            }
        };
        match (&log, &snapshot) {
            (log, SnapshotSettings::LocalDisk { .. } | SnapshotSettings::Memory) if log.off_host() => bail!(
                "[meta] snapshot: a telegram or discord log needs its snapshots in a channel too (a \
                 log off-host cannot start from snapshots on this host)"
            ),
            (LogSettings::Memory, SnapshotSettings::Memory) => {}
            (LogSettings::Memory, _) | (_, SnapshotSettings::Memory) => bail!(
                "[meta]: a memory log needs memory snapshots and the other way round (one is gone \
                 after a restart, the other would not match it)"
            ),
            _ => {}
        }
        if self.keep_snapshots == 0 {
            bail!("[meta] keep_snapshots: at least 1 (the snapshot the log starts from)");
        }
        Ok(MetaSettings {
            snapshot_every_entries: self.snapshot_every_entries,
            snapshot_interval: Duration::from_secs(self.snapshot_every_secs),
            keep_snapshots: self.keep_snapshots,
            lease: Duration::from_secs(self.lease_secs),
            log,
            snapshot,
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

/// `block_size` (+ margin) must fit under every blob store's max_blob_size.
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
            InstanceKind::Memory => 20 * 1024 * 1024 - 1,
            InstanceKind::LocalDisk => pigeonhole_types::BackendLimits::local_disk().max_blob_size,
            InstanceKind::Telegram => pigeonhole_types::BackendLimits::telegram().max_blob_size,
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
    l1_bytes: usize,
    l2_bytes: usize,
    write_through: bool,
}

impl Default for FileCache {
    fn default() -> Self {
        let d = CacheConfig::default();
        Self { enabled: d.enabled, l1_bytes: d.l1_bytes, l2_bytes: d.l2_bytes, write_through: d.write_through }
    }
}

impl FileCache {
    fn into_cache_config(self) -> Result<CacheConfig> {
        Ok(CacheConfig {
            enabled: self.enabled,
            l1_bytes: self.l1_bytes.max(1024 * 1024),
            l2_bytes: self.l2_bytes,
            write_through: self.write_through,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileChunk {
    /// `[[store]]` or `[[replicas]]` id chunks go to.
    store: Option<String>,
    size: usize,
    codec: ChunkCodec,
    block_size: Option<usize>,
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            store: None,
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

/// `[telegram]` / `[discord]`: what is not set keeps the service's default.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileLimits {
    bot_rate_per_sec: Option<f64>,
    bot_burst: Option<f64>,
    info_rate_per_sec: Option<f64>,
    info_burst: Option<f64>,
    send_rate_per_sec: Option<f64>,
    send_burst: Option<f64>,
    get_file_rate_per_sec: Option<f64>,
    get_file_burst: Option<f64>,
    delete_rate_per_sec: Option<f64>,
    delete_burst: Option<f64>,
    upload_concurrency: Option<usize>,
    download_concurrency: Option<usize>,
}

impl FileLimits {
    fn into_limiter_config(self, section: &str, d: ChatLimiterConfig) -> Result<ChatLimiterConfig> {
        let c = ChatLimiterConfig {
            bot_rate_per_sec: self.bot_rate_per_sec.unwrap_or(d.bot_rate_per_sec),
            bot_burst: self.bot_burst.unwrap_or(d.bot_burst),
            info_rate_per_sec: self.info_rate_per_sec.unwrap_or(d.info_rate_per_sec),
            info_burst: self.info_burst.unwrap_or(d.info_burst),
            send_rate_per_sec: self.send_rate_per_sec.unwrap_or(d.send_rate_per_sec),
            send_burst: self.send_burst.unwrap_or(d.send_burst),
            get_file_rate_per_sec: self.get_file_rate_per_sec.unwrap_or(d.get_file_rate_per_sec),
            get_file_burst: self.get_file_burst.unwrap_or(d.get_file_burst),
            delete_rate_per_sec: self.delete_rate_per_sec.unwrap_or(d.delete_rate_per_sec),
            delete_burst: self.delete_burst.unwrap_or(d.delete_burst),
            upload_concurrency: self.upload_concurrency.unwrap_or(d.upload_concurrency).max(1),
            download_concurrency: self.download_concurrency.unwrap_or(d.download_concurrency).max(1),
        };
        if c.send_rate_per_sec <= 0.0 || c.get_file_rate_per_sec <= 0.0 || c.delete_rate_per_sec <= 0.0 {
            bail!("{section}.*_rate_per_sec must be > 0");
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The tests set and read the environment from many threads, which is
    /// not safe at the same time: one at a time.
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn set_env(key: &str, value: &str) {
        let _one = ENV.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(key, value);
    }

    fn load(toml: &str) -> Result<Config> {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(toml.as_bytes()).unwrap();
        let _one = ENV.lock().unwrap_or_else(|e| e.into_inner());
        Config::load_from_path(f.path())
    }

    /// `toml` plus a memory blob store as `[chunk] store`.
    fn load_mem(toml: &str) -> Result<Config> {
        let toml = if toml.contains("[chunk]\n") {
            toml.replacen("[chunk]\n", "[chunk]\nstore = \"mem\"\n", 1)
        } else {
            format!("{toml}\n[chunk]\nstore = \"mem\"\n")
        };
        load(&format!("{toml}\n[[store]]\nid = \"mem\"\nkind = \"memory\"\n"))
    }

    fn tg_store(id: &str, chat: i64) -> String {
        set_env("PH_TEST_TGM_TOKEN", "42:secret");
        set_env("PH_TEST_TGM_API_ID", "12345");
        set_env("PH_TEST_TGM_API_HASH", "hash");
        tgm_store(id, chat)
    }

    #[test]
    fn discord_limits_are_its_own() {
        let cfg = load_mem("[discord]\nsend_rate_per_sec = 2.0\n[telegram]\nsend_rate_per_sec = 0.5\n").unwrap();
        assert_eq!((cfg.discord.send_rate_per_sec, cfg.tg.send_rate_per_sec), (2.0, 0.5));
        assert_eq!(cfg.discord.send_burst, ChatLimiterConfig::discord().send_burst);
        assert!(load_mem("[discord]\nsend_rate_per_sec = 0.0\n").is_err());
        // Keyed by the bot's id, and each service by its own registry.
        let dc = cfg.chat_limiter(InstanceKind::Discord, "MTIz.ts.hmac-a", "7");
        assert!(Arc::ptr_eq(&dc, &cfg.chat_limiter(InstanceKind::Discord, "MTIz.ts2.hmac-b", "7")));
        assert!(!Arc::ptr_eq(&dc, &cfg.chat_limiter(InstanceKind::Telegram, "MTIz.ts.hmac-a", "7")));
    }

    #[test]
    fn parses_example_shape() {
        let cfg = load_mem(
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
"#,
        )
        .unwrap();
        assert_eq!(cfg.chunk_size, 1024);
        assert_eq!(cfg.chunk_codec, ChunkCodec::Raw);
        assert_eq!(cfg.tg.send_rate_per_sec, 1.0);
        assert_eq!(cfg.tg.get_file_rate_per_sec, 20.0);
        assert_eq!(cfg.tg.upload_concurrency, 4);
        assert_eq!(cfg.discord.send_rate_per_sec, ChatLimiterConfig::discord().send_rate_per_sec, "[discord] keeps its own defaults");
        assert_eq!(cfg.placement, PlacementConfig::single("mem"));
        assert_eq!(cfg.meta.snapshot_every_entries, 10_000);
        assert_eq!(cfg.meta.snapshot_interval, Duration::from_secs(600));
        assert_eq!(cfg.meta.keep_snapshots, 2);
        assert_eq!(cfg.meta.log, LogSettings::LocalDisk { path: "pigeonhole-meta".into() });
        assert_eq!(cfg.meta.snapshot, SnapshotSettings::LocalDisk { path: "pigeonhole-meta".into() });
        assert!(!cfg.meta.snapshots_outlive_log(), "same directory, same disk");
    }

    #[test]
    fn parses_meta_section() {
        let dirs = "[[store]]\nid = \"disk\"\nkind = \"local_disk\"\npath = \"/var/log-dir\"\n\
                    [[store]]\nid = \"backup\"\nkind = \"local_disk\"\npath = \"/backup\"\n";
        let cfg = load_mem(&format!(
            "[meta]\nlog = \"disk\"\nsnapshot = \"backup\"\n\
             snapshot_every_entries = 0\nsnapshot_every_secs = 60\nkeep_snapshots = 3\n{dirs}"
        ))
        .unwrap();
        assert_eq!(cfg.meta.keep_snapshots, 3);
        assert_eq!(cfg.meta.lease, Duration::from_secs(45));
        assert_eq!(cfg.meta.snapshot_every_entries, 0);
        assert_eq!(cfg.meta.snapshot_interval, Duration::from_secs(60));
        assert_eq!(cfg.meta.log, LogSettings::LocalDisk { path: "/var/log-dir".into() });
        assert_eq!(cfg.meta.snapshot, SnapshotSettings::LocalDisk { path: "/backup".into() });
        assert!(cfg.meta.snapshots_outlive_log());

        // One dir for both: the default layout.
        let cfg = load_mem(&format!("[meta]\nlog = \"disk\"\nsnapshot = \"disk\"\n{}", &dirs[..dirs.find("[[store]]\nid = \"backup\"").unwrap()]))
            .unwrap();
        assert!(!cfg.meta.snapshots_outlive_log());

        for bad in [
            "[meta]\ndb_path = \"m.db\"\n",
            "[meta]\nlog = \"nope\"\n",
            "[meta]\nlog = \"mem\"\n",
            "[meta.log]\nkind = \"local_disk\"\n",
            "[[store]]\nid = \"x\"\nkind = \"s3\"\n",
            "[[store]]\nid = \"x\"\nkind = \"local_disk\"\npath = \"/x\"\nchat_id = 1\n",
            "[[store]]\nid = \"x\"\nkind = \"local_disk\"\n",
        ] {
            assert!(load_mem(bad).is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn telegram_log_needs_a_chat_of_its_own() {
        let stores = format!("{}{}", tg_store("tg-log", -100), tg_store("tg-snap", -101));
        let cfg = load_mem(&format!("[meta]\nlog = \"tg-log\"\nsnapshot = \"tg-snap\"\n{stores}")).unwrap();
        assert!(!cfg.meta.snapshots_outlive_log());
        assert!(matches!(&cfg.meta.log, LogSettings::Telegram { chat_id, .. } if chat_id == "-100"));
        assert!(matches!(&cfg.meta.snapshot, SnapshotSettings::Telegram { chat_id, .. } if chat_id == "-101"));

        let err = load(&format!(
            "[meta]\nlog = \"tg-log\"\nsnapshot = \"tg-snap\"\n[chunk]\nstore = \"tg-log\"\n{stores}"
        ))
        .unwrap_err();
        assert!(format!("{err:#}").contains("one purpose"), "{err:#}");
        let err = load_mem(&format!("[meta]\nlog = \"tg-log\"\nsnapshot = \"tg-log\"\n{stores}")).unwrap_err();
        assert!(format!("{err:#}").contains("one purpose"), "{err:#}");
        let err = load_mem(&format!("[meta]\nlog = \"tg-log\"\nsnapshot = \"tg-snap\"\n{stores}{}", tg_store("again", -100)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("same place"), "{err:#}");

        let err = load_mem(&format!("[meta]\nlog = \"tg-log\"\n{}", tg_store("tg-log", -100))).unwrap_err();
        assert!(format!("{err:#}").contains("snapshot is required"), "{err:#}");
        let disk = "[[store]]\nid = \"disk\"\nkind = \"local_disk\"\npath = \"/x\"\n";
        let err = load_mem(&format!("[meta]\nlog = \"tg-log\"\nsnapshot = \"disk\"\n{}{disk}", tg_store("tg-log", -100)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("cannot start from snapshots on this host"), "{err:#}");
    }

    fn tgm_store(id: &str, chat: i64) -> String {
        format!(
            "[[store]]\nid = \"{id}\"\nkind = \"telegram\"\nchat_id = {chat}\nbot_token_env = \"PH_TEST_TGM_TOKEN\"\n\
             api_id_env = \"PH_TEST_TGM_API_ID\"\napi_hash_env = \"PH_TEST_TGM_API_HASH\"\n"
        )
    }

    #[test]
    fn telegram_stores_log_in_one_session_per_bot() {
        set_env("PH_TEST_TGM_TOKEN", "42:secret");
        set_env("PH_TEST_TGM_API_ID", "12345");
        set_env("PH_TEST_TGM_API_HASH", "hash");
        let stores = format!("{}{}{}", tgm_store("log", -1001), tgm_store("snap", -1002), tgm_store("blobs", -1003));
        let cfg = load(&format!("[meta]\nlog = \"log\"\nsnapshot = \"snap\"\n[chunk]\nstore = \"blobs\"\n{stores}")).unwrap();
        let login = TelegramLogin {
            bot_token: "42:secret".into(),
            api_id: 12345,
            api_hash: "hash".into(),
            session: "pigeonhole-tg-42.session".into(),
        };
        assert_eq!(cfg.meta.log, LogSettings::Telegram { chat_id: "-1001".into(), login: login.clone() });
        assert_eq!(cfg.meta.snapshot, SnapshotSettings::Telegram { chat_id: "-1002".into(), login: login.clone() });
        let inst = cfg.primary_instance().unwrap();
        assert_eq!((inst.info.kind, inst.info.fingerprint.as_str()), (InstanceKind::Telegram, "tg:42:-1003"));
        assert_eq!(inst.info.location, "tg:chat:-1003", "the same place as a Bot API store of that chat");
        assert_eq!(inst.telegram, Some(login));
        assert!(!format!("{:?}", inst.telegram).contains("secret"));

        let err = load_mem(&format!("[meta]\nlog = \"log\"\nsnapshot = \"log\"\n{}", tgm_store("log", -1001))).unwrap_err();
        assert!(format!("{err:#}").contains("one purpose"), "{err:#}");
        let err = load_mem("[[store]]\nid = \"x\"\nkind = \"memory\"\nsession = \"s\"\n").unwrap_err();
        assert!(format!("{err:#}").contains("unknown field `session`"), "{err:#}");
    }

    #[test]
    fn every_kind_serves_every_purpose_it_can() {
        // Memory metadata (tests: no fsync), blobs on a local disk.
        let cfg = load(
            "[meta]\nlog = \"ram\"\n[chunk]\nstore = \"disk\"\n\
             [[store]]\nid = \"ram\"\nkind = \"memory\"\n\
             [[store]]\nid = \"disk\"\nkind = \"local_disk\"\npath = \"/blobs\"\n",
        )
        .unwrap();
        assert_eq!((cfg.meta.log.clone(), cfg.meta.snapshot.clone()), (LogSettings::Memory, SnapshotSettings::Memory));
        let inst = cfg.primary_instance().unwrap();
        assert_eq!((inst.info.kind, inst.scope_id.as_str()), (InstanceKind::LocalDisk, "/blobs"));

        // A memory log next to snapshots that survive it (or the other way round).
        let mixed = "[meta]\nlog = \"ram\"\nsnapshot = \"disk\"\n[chunk]\nstore = \"mem\"\n\
                     [[store]]\nid = \"ram\"\nkind = \"memory\"\n\
                     [[store]]\nid = \"disk\"\nkind = \"local_disk\"\npath = \"/m\"\n\
                     [[store]]\nid = \"mem\"\nkind = \"memory\"\n";
        let err = load(mixed).unwrap_err();
        assert!(format!("{err:#}").contains("memory log needs memory snapshots"), "{err:#}");

        // One disk for blobs and metadata: refused.
        let shared = "[meta]\nlog = \"disk\"\n[chunk]\nstore = \"disk\"\n\
                      [[store]]\nid = \"disk\"\nkind = \"local_disk\"\npath = \"/x\"\n";
        let err = load(shared).unwrap_err();
        assert!(format!("{err:#}").contains("serves one purpose"), "{err:#}");

        // Two stores on one directory: refused.
        let twice = "[chunk]\nstore = \"a\"\n[[store]]\nid = \"a\"\nkind = \"local_disk\"\npath = \"/x\"\n\
                     [[store]]\nid = \"b\"\nkind = \"local_disk\"\npath = \"/x\"\n";
        assert!(format!("{:#}", load(twice).unwrap_err()).contains("same place"));

        // A discord log needs its snapshots in a channel too.
        set_env("DISCORD_BOT_TOKEN", "discord-test-token");
        let dc = "[meta]\nlog = \"dc\"\n[[store]]\nid = \"dc\"\nkind = \"discord\"\nchannel_id = 1\n";
        let err = load_mem(dc).unwrap_err();
        assert!(format!("{err:#}").contains("snapshot is required"), "{err:#}");
        let both = format!("[meta]\nlog = \"dc\"\nsnapshot = \"dc-snap\"\n[[store]]\nid = \"dc\"\nkind = \"discord\"\nchannel_id = 1\n\
                            [[store]]\nid = \"dc-snap\"\nkind = \"discord\"\nchannel_id = 2\n");
        let cfg = load_mem(&both).unwrap();
        assert!(matches!(cfg.meta.log, LogSettings::Discord { ref channel_id, .. } if channel_id == "1"));
        assert!(matches!(cfg.meta.snapshot, SnapshotSettings::Discord { ref channel_id, .. } if channel_id == "2"));
    }

    #[test]
    fn a_local_log_with_snapshots_in_a_chat() {
        set_env("PH_TEST_TG_TOKEN", "1:test");
        let cfg = load_mem(&format!("[meta]\nsnapshot = \"tg-snap\"\n{}", tg_store("tg-snap", -200))).unwrap();
        assert_eq!(cfg.meta.log, LogSettings::LocalDisk { path: "pigeonhole-meta".into() });
        assert!(cfg.meta.snapshots_outlive_log());
    }

    #[test]
    fn blobs_on_a_replica_group() {
        let stores = "[[store]]\nid = \"a\"\nkind = \"memory\"\n[[store]]\nid = \"b\"\nkind = \"memory\"\n";
        let group = |q: usize| format!("[[replicas]]\nid = \"ab\"\nmembers = [\"a\", \"b\"]\nwrite_quorum = {q}\n");
        let cfg = load(&format!("[chunk]\nstore = \"ab\"\n{stores}{}", group(2))).unwrap();
        assert_eq!(
            cfg.placement.shards,
            vec![ShardPlacement { group: vec!["a".into(), "b".into()], write_quorum: 2 }]
        );
        assert_eq!(cfg.instances.len(), 2);

        let err = load(&format!("[chunk]\nstore = \"ab\"\n{stores}{}", group(3))).unwrap_err();
        assert!(format!("{err:#}").contains("write_quorum"), "{err:#}");
        let err = load(&format!("[chunk]\nstore = \"a\"\n{stores}")).unwrap_err();
        assert!(format!("{err:#}").contains("[[store]] b is not used"), "{err:#}");
        let nested = "[[replicas]]\nid = \"top\"\nmembers = [\"ab\"]\n";
        let err = load(&format!("[chunk]\nstore = \"top\"\n{stores}{}{nested}", group(1))).unwrap_err();
        assert!(format!("{err:#}").contains("replicas of groups"), "{err:#}");
        let err = load(&format!("[meta]\nlog = \"ab\"\n[chunk]\nstore = \"ab\"\n{stores}{}", group(1))).unwrap_err();
        assert!(format!("{err:#}").contains("not supported here yet"), "{err:#}");
    }

    #[test]
    fn blobs_on_shards() {
        let stores = "[[store]]\nid = \"a1\"\nkind = \"memory\"\n[[store]]\nid = \"a2\"\nkind = \"memory\"\n\
                      [[store]]\nid = \"b\"\nkind = \"memory\"\n\
                      [[replicas]]\nid = \"a\"\nmembers = [\"a1\", \"a2\"]\nwrite_quorum = 2\n";
        let shards = "[[shards]]\nid = \"blobs\"\nmembers = [\"a\", \"b\"]\n";
        let cfg = load(&format!("[chunk]\nstore = \"blobs\"\n{stores}{shards}")).unwrap();
        assert_eq!(
            cfg.placement.shards,
            vec![
                ShardPlacement { group: vec!["a1".into(), "a2".into()], write_quorum: 2 },
                ShardPlacement { group: vec!["b".into()], write_quorum: 1 },
            ]
        );
        assert_eq!(cfg.instances.len(), 3);
        assert_eq!(cfg.primary_instance().unwrap().info.id, "a1");

        let twice = "[[shards]]\nid = \"blobs\"\nmembers = [\"a\", \"a1\", \"b\"]\n";
        let err = load(&format!("[chunk]\nstore = \"blobs\"\n{stores}{twice}")).unwrap_err();
        assert!(format!("{err:#}").contains("in two shards"), "{err:#}");
        let nested = "[[shards]]\nid = \"top\"\nmembers = [\"blobs\"]\n";
        let err = load(&format!("[chunk]\nstore = \"top\"\n{stores}{shards}{nested}")).unwrap_err();
        assert!(format!("{err:#}").contains("shards of shards"), "{err:#}");
        let err = load(&format!("[chunk]\nstore = \"a\"\n{stores}{shards}")).unwrap_err();
        assert!(format!("{err:#}").contains("group blobs is not used"), "{err:#}");
    }

    #[test]
    fn example_config_loads() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../pigeonhole.toml.example");
        set_env("BOT_TOKEN", "123:example");
        set_env("TG_API_ID", "1");
        set_env("TG_API_HASH", "example");
        let cfg = {
            let _one = ENV.lock().unwrap_or_else(|e| e.into_inner());
            Config::load_from_path(&path).unwrap()
        };
        assert_eq!(cfg.primary_instance().unwrap().info.id, "tg-main");
    }

    #[test]
    fn requires_a_blob_store() {
        let msg = format!("{:#}", load("listen_addr = \"127.0.0.1:9\"\n").unwrap_err());
        assert!(msg.contains("[chunk] store"), "unexpected error: {msg}");
    }

    #[test]
    fn rejects_unknown_keys() {
        for stale in ["database_url = \"sqlite:x.db\"\n", "memory = true\n", "chat_id = \"-1001\"\n", "[telegram]\nrate_per_sec = 1.0\n"] {
            let err = load_mem(stale).unwrap_err();
            assert!(format!("{err:#}").contains("unknown field"), "{stale:?} accepted");
        }
        for stale in ["[[instances]]\nid = \"x\"\nkind = \"memory\"\n", "[placement]\ngroup = [\"mem\"]\n"] {
            assert!(load_mem(stale).is_err(), "{stale:?} accepted");
        }
    }

    #[test]
    fn log_settings_default_and_parse() {
        let cfg = load_mem("").unwrap();
        assert_eq!(cfg.log, LoggingSettings::default());
        assert_eq!(cfg.log.level, DEFAULT_LOG_LEVEL);
        let cfg = load_mem("[log]\nlevel = \"debug\"\nformat = \"json\"\nslow_request_ms = 250\n").unwrap();
        assert_eq!(
            cfg.log,
            LoggingSettings { level: "debug".into(), format: LogFormat::Json, slow_request: Duration::from_millis(250) }
        );
        assert!(load_mem("[log]\nformat = \"xml\"\n").is_err());
        assert!(load_mem("[log]\nlevels = \"debug\"\n").is_err(), "unknown keys are refused");
    }

    #[test]
    fn parses_discord_store() {
        set_env("DISCORD_BOT_TOKEN", "discord-test-token");
        let cfg = load(
            r#"
[chunk]
store = "dc"
[[store]]
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
        let err = load_mem("[chunk]\nblock_size = 1048576\n[ingest]\nmemory_budget = 1048576\n").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("ingest.memory_budget"), "unexpected error: {msg}");
    }

    /// Each mistake in `[[store]]` / `[[replicas]]` / `[[shards]]` names itself.
    #[test]
    fn store_mistakes_are_named() {
        let cases = [
            ("[[store]]\nid = \"x\"\nkind = \"memory\"\npath = \"/tmp\"\n", "unknown field `path`"),
            ("[[store]]\nid = \"x\"\nkind = \"local_disk\"\nchat_id = 1\n", "unknown field `chat_id`"),
            ("[[store]]\nid = \"x\"\nkind = \"local_disk\"\n", "missing field `path`"),
            ("[[store]]\nid = \"x\"\nkind = \"discord\"\n", "missing field `channel_id`"),
            ("[[store]]\nid = \"x\"\nkind = \"floppy\"\n", "unknown variant `floppy`"),
            ("[[store]]\nid = \"x\"\n", "missing field `kind`"),
            ("[[store]]\nid = \" \"\nkind = \"memory\"\n", "id must be non-empty"),
            ("[[store]]\nid = \"mem\"\nkind = \"memory\"\n", "duplicate id \"mem\""),
            ("[[replicas]]\nid = \"mem\"\nmembers = [\"mem\"]\n", "duplicate id \"mem\""),
            ("[[replicas]]\nid = \"r\"\nmembers = [\"mem\", \"mem\"]\n", "[[replicas]] r: member \"mem\" listed twice"),
            ("[[replicas]]\nid = \"r\"\nmembers = [\"mem\"]\nwrite_quorum = 2\n", "1 <= write_quorum (2) <= 1"),
            ("[[shards]]\nid = \"s\"\nmembers = []\n", "[[shards]] s: needs members"),
            ("[[shards]]\nid = \"s\"\nmembers = [\"mem\", \"mem\"]\n", "[[shards]] s: member \"mem\" listed twice"),
        ];
        for (toml, want) in cases {
            let err = format!("{:#}", load_mem(toml).unwrap_err());
            assert!(err.contains(want), "{toml:?}: want {want:?} in {err}");
        }
    }
}
