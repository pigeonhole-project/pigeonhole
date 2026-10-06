use s3gram_blob::{CacheConfig, ChatLimiter, ChatLimiterConfig};
use s3gram_chunk::{self as chunker, ChunkCodec};
use s3gram_chunk::ByteBudget;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    /// Independent frame size for `frames` packing.
    pub frame_size: usize,
    /// Process-wide ingest buffer budget (shared across PUTs).
    pub ingest_budget: Option<ByteBudget>,
    pub tg: ChatLimiterConfig,
    pub memory_store: bool,
    pub cache: CacheConfig,
    pub bytestream: BytestreamSettings,
    pub http: HttpSettings,
    pub config_path: PathBuf,
}

/// Axum/tower HTTP server knobs.
#[derive(Clone, Debug)]
pub struct HttpSettings {
    /// Per-request timeout (seconds).
    pub request_timeout_secs: u64,
    /// Max concurrent in-flight HTTP requests.
    pub max_concurrent_requests: usize,
    /// Max HTTP/1 headers per request (hyper).
    pub max_headers: usize,
}

impl Default for HttpSettings {
    fn default() -> Self {
        Self {
            request_timeout_secs: 300,
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
        let frame_size = file.chunk.frame_size.max(1024);
        if frame_size > chunker::MAX_LOGICAL_CHUNK {
            bail!("chunk.frame_size exceeds MAX_LOGICAL_CHUNK");
        }
        let memory_budget = file.ingest.memory_budget.max(frame_size);
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
            frame_size,
            ingest_budget,
            tg,
            memory_store,
            cache: file.cache.into_cache_config()?,
            bytestream: file.bytestream.into_settings(),
            http: file.http.into_settings(),
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
            frame_size: chunker::DEFAULT_FRAME_SIZE,
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
            config_path: PathBuf::from("(test)"),
        }
    }

    pub fn chat_limiter(&self) -> Arc<ChatLimiter> {
        Arc::new(ChatLimiter::new(self.tg.clone()))
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
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct FileHttp {
    request_timeout_secs: u64,
    max_concurrent_requests: usize,
    max_headers: usize,
}

impl Default for FileHttp {
    fn default() -> Self {
        let d = HttpSettings::default();
        Self {
            request_timeout_secs: d.request_timeout_secs,
            max_concurrent_requests: d.max_concurrent_requests,
            max_headers: d.max_headers,
        }
    }
}

impl FileHttp {
    fn into_settings(self) -> HttpSettings {
        HttpSettings {
            request_timeout_secs: self.request_timeout_secs.max(1),
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
    frame_memory_bytes: usize,
    readahead_frames: usize,
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
            frame_memory_bytes: d.frame_memory_bytes,
            readahead_frames: d.readahead_frames,
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
        Ok(CacheConfig {
            enabled: self.enabled,
            memory_bytes: self.memory_bytes.max(1024 * 1024),
            disk_path,
            disk_bytes: self.disk_bytes,
            frame_memory_bytes: self.frame_memory_bytes.max(1024 * 1024),
            readahead_frames: self.readahead_frames,
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
    frame_size: usize,
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            size: chunker::DEFAULT_CHUNK_SIZE,
            codec: ChunkCodec::Zstd,
            frame_size: chunker::DEFAULT_FRAME_SIZE,
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
max_blob_size = 1048576
"#
        )
        .unwrap();
        std::env::set_var("DISCORD_BOT_TOKEN", "discord-test-token");
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert_eq!(cfg.backend_kind, BackendKind::Discord);
        assert_eq!(cfg.chat_id, "123456789");
        assert_eq!(cfg.discord_max_blob_size, Some(1048576));
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
}
