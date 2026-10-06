use crate::chunker::{self, ChunkCodec};
use crate::rate_limit::ChatLimiter;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Runtime config: non-secrets from TOML, secrets from environment / `.env`.
#[derive(Clone, Debug)]
pub struct Config {
    pub bot_token: String,
    /// Single Telegram chat/channel where all object blobs and snapshots live.
    pub chat_id: String,
    pub listen_addr: String,
    pub database_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    /// Seconds between automatic index snapshots. `0` disables.
    pub snapshot_interval_secs: u64,
    /// Logical object/snapshot part size in bytes. Must be `< 20 MiB`.
    pub chunk_size: usize,
    /// Encoding policy for new object chunks.
    pub chunk_codec: ChunkCodec,
    pub tg_rate_per_sec: f64,
    pub tg_rate_burst: f64,
    pub tg_upload_concurrency: usize,
    /// Use in-memory BlobStore (no Telegram). For local s3-tests / CI.
    pub memory_store: bool,
    /// Path the TOML was loaded from (for logs).
    pub config_path: PathBuf,
}

impl Config {
    /// Load `S3GRAM_CONFIG` or `./s3gram.toml`, then secrets from the environment.
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

        let bot_token = if memory_store {
            env::var("BOT_TOKEN").unwrap_or_else(|_| "unused".into())
        } else {
            require_secret("BOT_TOKEN")?
        };

        let chat_id = match file.chat_id.filter(|s| !s.is_empty()) {
            Some(id) => id,
            None if memory_store => "-100memory".into(),
            None => bail!("s3gram.toml: chat_id is required when memory = false"),
        };

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

        let tg = file.telegram;
        if tg.rate_per_sec <= 0.0 {
            bail!("telegram.rate_per_sec must be > 0");
        }
        if tg.rate_burst < 1.0 {
            bail!("telegram.rate_burst must be >= 1");
        }
        if tg.upload_concurrency == 0 {
            bail!("telegram.upload_concurrency must be >= 1");
        }

        Ok(Self {
            bot_token,
            chat_id,
            listen_addr: file.listen_addr,
            database_url,
            access_key,
            secret_key,
            region: file.region,
            snapshot_interval_secs: file.snapshot.interval_secs,
            chunk_size,
            chunk_codec,
            tg_rate_per_sec: tg.rate_per_sec,
            tg_rate_burst: tg.rate_burst,
            tg_upload_concurrency: tg.upload_concurrency,
            memory_store,
            config_path,
        })
    }

    /// Config for in-process tests (no Telegram / files required).
    pub fn for_test(database_url: &str) -> Self {
        Self {
            bot_token: "test-token".into(),
            chat_id: "-100test".into(),
            listen_addr: "127.0.0.1:0".into(),
            database_url: database_url.into(),
            access_key: "s3gram".into(),
            secret_key: "s3gramsecret".into(),
            region: "us-east-1".into(),
            snapshot_interval_secs: 0,
            chunk_size: chunker::DEFAULT_CHUNK_SIZE,
            chunk_codec: ChunkCodec::Gzip,
            tg_rate_per_sec: 1000.0,
            tg_rate_burst: 100.0,
            tg_upload_concurrency: 8,
            memory_store: true,
            config_path: PathBuf::from("(test)"),
        }
    }

    pub fn chat_limiter(&self) -> ChatLimiter {
        ChatLimiter::new(
            self.tg_rate_per_sec,
            self.tg_rate_burst,
            self.tg_upload_concurrency,
        )
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
    chat_id: Option<String>,
    listen_addr: String,
    database_url: Option<String>,
    region: String,
    memory: bool,
    snapshot: FileSnapshot,
    chunk: FileChunk,
    telegram: FileTelegram,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            chat_id: None,
            listen_addr: "0.0.0.0:8333".into(),
            database_url: None,
            region: "us-east-1".into(),
            memory: false,
            snapshot: FileSnapshot::default(),
            chunk: FileChunk::default(),
            telegram: FileTelegram::default(),
        }
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
}

impl Default for FileChunk {
    fn default() -> Self {
        Self {
            size: chunker::DEFAULT_CHUNK_SIZE,
            codec: ChunkCodec::Gzip,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct FileTelegram {
    rate_per_sec: f64,
    rate_burst: f64,
    upload_concurrency: usize,
}

impl Default for FileTelegram {
    fn default() -> Self {
        Self {
            rate_per_sec: 0.5,
            rate_burst: 3.0,
            upload_concurrency: 2,
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
rate_per_sec = 1.0
rate_burst = 2.0
upload_concurrency = 4
"#
        )
        .unwrap();
        // memory mode does not require BOT_TOKEN
        let cfg = Config::load_from_path(f.path()).unwrap();
        assert!(cfg.memory_store);
        assert_eq!(cfg.chunk_size, 1024);
        assert_eq!(cfg.chunk_codec, ChunkCodec::Raw);
        assert_eq!(cfg.snapshot_interval_secs, 0);
        assert_eq!(cfg.tg_upload_concurrency, 4);
    }
}
