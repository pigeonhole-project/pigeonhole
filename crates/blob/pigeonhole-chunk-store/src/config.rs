use crate::instances::{
    legacy_default_instance, resolve_instances, FileInstance, InstanceConfig,
};
use pigeonhole_blob::{
    CacheConfig, ChatLimiter, ChatLimiterConfig, InstanceKind, InstanceRole,
};
use pigeonhole_codec::{self as chunker, ChunkCodec};
use pigeonhole_codec::ByteBudget;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::warn;

/// Placement group: which instances receive chunk replicas and the write quorum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementConfig {
    /// Instance ids in the write group (read-write members).
    pub group: Vec<String>,
    pub write_quorum: usize,
}

/// Which chat blob backend backs non-memory mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Telegram,
    Discord,
}

/// Runtime config: non-secrets from TOML, secrets from environment / `.env`.
#[derive(Clone, Debug)]
pub struct Config {
    pub backend_kind: BackendKind,
    pub bot_token: String,
    pub chat_id: String,
    pub discord_max_blob_size: Option<usize>,
    pub listen_addr: String,
    pub database_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub snapshot_interval_secs: u64,
    /// Max on-wire chunk size in bytes (`< 20 MiB`).
    pub chunk_size: usize,
    pub chunk_codec: ChunkCodec,
    /// Independent block size for `blocks` packing.
    pub block_size: usize,
    /// Process-wide ingest buffer budget (shared across PUTs).
    pub ingest_budget: Option<ByteBudget>,
    pub tg: ChatLimiterConfig,
    pub memory_store: bool,
    pub cache: CacheConfig,
    pub bytestream: BytestreamSettings,
    pub http: HttpSettings,
    /// Resolved backend instances (`[[instances]]` or legacy single default).
    pub instances: Vec<InstanceConfig>,
    /// Replica placement (`[placement]`); defaults to a single read-write instance.
    pub placement: PlacementConfig,
    pub config_path: PathBuf,
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
            instance_name: "s3gram".into(),
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
        let memory_store = file.memory;
        let backend_kind = if memory_store {
            BackendKind::Telegram
        } else {
            match file.backend.kind.as_deref() {
                Some("discord") => BackendKind::Discord,
                Some("telegram") | Some("") | None => BackendKind::Telegram,
                Some(other) => bail!("s3gram.toml: unknown backend.kind {other:?}"),
            }
        };

        let bot_token = if memory_store {
            env::var("BOT_TOKEN").unwrap_or_else(|_| "unused".into())
        } else if backend_kind == BackendKind::Discord {
            require_secret("DISCORD_BOT_TOKEN")?
        } else {
            require_secret("BOT_TOKEN")?
        };

        let chat_id = if memory_store {
            "-100memory".into()
        } else if backend_kind == BackendKind::Discord {
            file.discord
                .channel_id
                .clone()
                .or_else(|| file.chat_id.clone())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("s3gram.toml: discord.channel_id or chat_id required for discord backend")
                })?
        } else {
            match file.chat_id.filter(|s| !s.is_empty()) {
                Some(id) => id,
                None => bail!("s3gram.toml: chat_id is required when memory = false"),
            }
        };

        let discord_max_blob_size = file.discord.max_blob_size;

        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "s3gram".into());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "s3gramsecret".into());
        if access_key.is_empty() || secret_key.is_empty() {
            bail!("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY must not be empty");
        }

        let database_url = file.database_url.unwrap_or_else(|| {
            if memory_store {
                "sqlite:s3gram-memory.db".into()
            } else {
                "sqlite:s3gram.db".into()
            }
        });

        let chunk_size = file.chunk.size;
        chunker::validate_chunk_size(chunk_size).context("chunk.size")?;
        let chunk_codec = file.chunk.codec;
        let block_size = file.chunk.resolve_block_size()?;
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

        let instances = if !file.instances.is_empty() {
            resolve_instances(&file.instances)?
        } else if memory_store {
            vec![legacy_default_instance(
                InstanceKind::Memory,
                "",
                "local",
                "",
            )?]
        } else {
            let kind = match backend_kind {
                BackendKind::Telegram => InstanceKind::Telegram,
                BackendKind::Discord => InstanceKind::Discord,
            };
            let env_name = match backend_kind {
                BackendKind::Telegram => "BOT_TOKEN",
                BackendKind::Discord => "DISCORD_BOT_TOKEN",
            };
            vec![legacy_default_instance(
                kind,
                &bot_token,
                &chat_id,
                env_name,
            )?]
        };

        let placement = resolve_placement(file.placement, &instances)?;
        check_block_size_against_instances(
            block_size,
            &instances,
            discord_max_blob_size,
        )?;

        Ok(Self {
            backend_kind,
            bot_token,
            chat_id,
            discord_max_blob_size,
            listen_addr: file.listen_addr,
            database_url,
            access_key,
            secret_key,
            region: file.region,
            snapshot_interval_secs: file.snapshot.interval_secs,
            chunk_size,
            chunk_codec,
            block_size,
            ingest_budget,
            tg,
            memory_store,
            cache: file.cache.into_cache_config()?,
            bytestream: file.bytestream.into_settings(),
            http: file.http.into_settings(),
            instances,
            placement,
            config_path,
        })
    }

    pub fn for_test(database_url: &str) -> Self {
        Self {
            backend_kind: BackendKind::Telegram,
            bot_token: "test-token".into(),
            chat_id: "-100test".into(),
            discord_max_blob_size: None,
            listen_addr: "127.0.0.1:0".into(),
            database_url: database_url.into(),
            access_key: "s3gram".into(),
            secret_key: "s3gramsecret".into(),
            region: "us-east-1".into(),
            snapshot_interval_secs: 0,
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
            memory_store: true,
            cache: CacheConfig {
                enabled: false,
                ..CacheConfig::default()
            },
            bytestream: BytestreamSettings::default(),
            http: HttpSettings::default(),
            instances: vec![legacy_default_instance(
                InstanceKind::Memory,
                "",
                "local",
                "",
            )
            .expect("memory instance")],
            placement: PlacementConfig {
                group: vec!["default".into()],
                write_quorum: 1,
            },
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

    /// Scope id (chat/channel) of the primary write instance — replaces runtime `chat_id`.
    pub fn primary_scope_id(&self) -> Result<&str> {
        Ok(self.primary_instance()?.scope_id.as_str())
    }
}

fn config_path_from_env() -> PathBuf {
    env::var("S3GRAM_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("s3gram.toml"))
}

fn require_secret(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("missing required secret env var {key}"))?;
    if v.is_empty() {
        bail!("missing required secret env var {key}");
    }
    Ok(v)
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct FileConfig {
    #[serde(default)]
    backend: FileBackendSection,
    chat_id: Option<String>,
    listen_addr: String,
    database_url: Option<String>,
    region: String,
    memory: bool,
    snapshot: FileSnapshot,
    chunk: FileChunk,
    #[serde(default)]
    ingest: FileIngest,
    telegram: FileTelegram,
    #[serde(default)]
    discord: FileDiscord,
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
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            backend: FileBackendSection::default(),
            chat_id: None,
            listen_addr: "0.0.0.0:8333".into(),
            database_url: None,
            region: "us-east-1".into(),
            memory: false,
            snapshot: FileSnapshot::default(),
            chunk: FileChunk::default(),
            ingest: FileIngest::default(),
            telegram: FileTelegram::default(),
            discord: FileDiscord::default(),
            cache: FileCache::default(),
            bytestream: FileBytestream::default(),
            http: FileHttp::default(),
            instances: Vec::new(),
            placement: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct FilePlacement {
    group: Vec<String>,
    write_quorum: usize,
}

/// E.4: `block_size` (+ margin) must fit under every instance's max_blob_size,
/// including read-only / retired members that may still serve repair reads.
pub fn check_block_size_against_instances(
    block_size: usize,
    instances: &[InstanceConfig],
    discord_max_blob_size: Option<usize>,
) -> Result<()> {
    if instances.is_empty() {
        return Ok(());
    }
    let mut min_max = usize::MAX;
    for inst in instances {
        let max = match inst.info.kind {
            InstanceKind::Telegram | InstanceKind::Memory => 20 * 1024 * 1024 - 1,
            InstanceKind::Discord => {
                pigeonhole_types::BackendLimits::discord(discord_max_blob_size).max_blob_size
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
            "chunk.block_size ({block_size}) with frame margin exceeds min(max_blob_size) ({min_max})"
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
                many => {
                    // Prefer the legacy `default` id when present; else first RW.
                    many.iter()
                        .find(|i| i.info.id == "default")
                        .copied()
                        .unwrap_or(many[0])
                }
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
#[serde(default)]
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
#[serde(default)]
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

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct FileBackendSection {
    kind: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct FileDiscord {
    channel_id: Option<String>,
    max_blob_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct FileCache {
    enabled: bool,
    memory_bytes: usize,
    disk_path: String,
    disk_bytes: Option<usize>,
    block_memory_bytes: Option<usize>,
    /// Deprecated alias for [`Self::block_memory_bytes`].
    frame_memory_bytes: Option<usize>,
    readahead_blocks: Option<usize>,
    /// Deprecated alias for [`Self::readahead_blocks`].
    readahead_frames: Option<usize>,
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
            frame_memory_bytes: None,
            readahead_blocks: None,
            readahead_frames: None,
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
        let block_memory_bytes = match (self.block_memory_bytes, self.frame_memory_bytes) {
            (Some(b), Some(_)) => {
                warn!("cache.frame_memory_bytes ignored; using cache.block_memory_bytes");
                b
            }
            (Some(b), None) => b,
            (None, Some(f)) => {
                warn!("cache.frame_memory_bytes is deprecated; use cache.block_memory_bytes");
                f
            }
            (None, None) => defaults.block_memory_bytes,
        };
        let readahead_blocks = match (self.readahead_blocks, self.readahead_frames) {
            (Some(b), Some(_)) => {
                warn!("cache.readahead_frames ignored; using cache.readahead_blocks");
                b
            }
            (Some(b), None) => b,
            (None, Some(f)) => {
                warn!("cache.readahead_frames is deprecated; use cache.readahead_blocks");
                f
            }
            (None, None) => defaults.readahead_blocks,
        };
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
#[serde(default)]
struct FileSnapshot {
    interval_secs: u64,
}

impl Default for FileSnapshot {
    fn default() -> Self {
        Self {
            interval_secs: 300,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct FileChunk {
    size: usize,
    codec: ChunkCodec,
    block_size: Option<usize>,
    /// Deprecated alias for [`Self::block_size`].
    frame_size: Option<usize>,
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            size: chunker::DEFAULT_CHUNK_SIZE,
            codec: ChunkCodec::Zstd,
            block_size: None,
            frame_size: None,
        }
    }
}

impl FileChunk {
    fn resolve_block_size(&self) -> Result<usize> {
        match (self.block_size, self.frame_size) {
            (Some(b), Some(f)) if b != f => {
                warn!(
                    "chunk.frame_size ({f}) ignored; using chunk.block_size ({b})"
                );
                Ok(b.max(1024))
            }
            (Some(b), _) => Ok(b.max(1024)),
            (None, Some(f)) => {
                warn!("chunk.frame_size is deprecated; use chunk.block_size");
                Ok(f.max(1024))
            }
            (None, None) => Ok(chunker::DEFAULT_BLOCK_SIZE),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
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
#[serde(default)]
struct FileTelegram {
    /// Legacy alias for `send_rate_per_sec`.
    rate_per_sec: Option<f64>,
    rate_burst: Option<f64>,
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
            rate_per_sec: None,
            rate_burst: None,
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
            send_rate_per_sec: self
                .send_rate_per_sec
                .or(self.rate_per_sec)
                .unwrap_or(d.send_rate_per_sec),
            send_burst: self
                .send_burst
                .or(self.rate_burst)
                .unwrap_or(d.send_burst),
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

    #[test]
    fn parses_example_shape() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
chat_id = "-1001"
listen_addr = "127.0.0.1:9"
memory = true
[snapshot]
interval_secs = 0
[chunk]
size = 1024
codec = "raw"
[telegram]
send_rate_per_sec = 1.0
send_burst = 2.0
get_file_rate_per_sec = 20.0
upload_concurrency = 4
"#
        )
        .unwrap();
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert!(cfg.memory_store);
        assert_eq!(cfg.chunk_size, 1024);
        assert_eq!(cfg.chunk_codec, ChunkCodec::Raw);
        assert_eq!(cfg.tg.send_rate_per_sec, 1.0);
        assert_eq!(cfg.tg.get_file_rate_per_sec, 20.0);
        assert_eq!(cfg.tg.upload_concurrency, 4);
        assert_eq!(cfg.placement.group, vec!["default".to_string()]);
        assert_eq!(cfg.placement.write_quorum, 1);
    }

    #[test]
    fn parses_placement_section() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
memory = true
[[instances]]
id = "mem-a"
kind = "memory"
role = "read-write"
[placement]
group = ["mem-a"]
write_quorum = 1
"#
        )
        .unwrap();
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert_eq!(cfg.placement.group, vec!["mem-a".to_string()]);
        assert_eq!(cfg.placement.write_quorum, 1);
    }

    #[test]
    fn resolve_placement_group_and_quorum() {
        use pigeonhole_blob::{InstanceInfo, InstanceKind, InstanceRole};
        let instances = vec![
            InstanceConfig {
                info: InstanceInfo {
                    id: "a".into(),
                    kind: InstanceKind::Memory,
                    fingerprint: "memory:a".into(),
                    location: "memory:a".into(),
                    role: InstanceRole::ReadWrite,
                },
                bot_token_env: String::new(),
                bot_token: String::new(),
                scope_id: "a".into(),
            },
            InstanceConfig {
                info: InstanceInfo {
                    id: "b".into(),
                    kind: InstanceKind::Memory,
                    fingerprint: "memory:b".into(),
                    location: "memory:b".into(),
                    role: InstanceRole::ReadWrite,
                },
                bot_token_env: String::new(),
                bot_token: String::new(),
                scope_id: "b".into(),
            },
        ];
        let p = resolve_placement(
            Some(FilePlacement {
                group: vec!["a".into(), "b".into()],
                write_quorum: 2,
            }),
            &instances,
        )
        .unwrap();
        assert_eq!(p.group, vec!["a", "b"]);
        assert_eq!(p.write_quorum, 2);
    }

    #[test]
    fn parses_discord_backend() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
memory = false
[backend]
kind = "discord"
[discord]
channel_id = "123456789"
max_blob_size = 8388608
"#
        )
        .unwrap();
        std::env::set_var("DISCORD_BOT_TOKEN", "discord-test-token");
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert_eq!(cfg.backend_kind, BackendKind::Discord);
        assert_eq!(cfg.chat_id, "123456789");
        assert_eq!(cfg.discord_max_blob_size, Some(8388608));
        std::env::remove_var("DISCORD_BOT_TOKEN");
    }

    #[test]
    fn legacy_rate_per_sec_maps_to_send() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
memory = true
[telegram]
rate_per_sec = 0.25
rate_burst = 2.0
"#
        )
        .unwrap();
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert_eq!(cfg.tg.send_rate_per_sec, 0.25);
        assert_eq!(cfg.tg.send_burst, 2.0);
    }

    #[test]
    fn rejects_memory_budget_below_two_frames() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
memory = true
[chunk]
block_size = 1048576
[ingest]
memory_budget = 1048576
"#
        )
        .unwrap();
        let err = Config::load_from_path(f.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("ingest.memory_budget"),
            "unexpected error: {msg}"
        );
    }
}
