use anyhow::{bail, Context, Result};
use std::env;

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
    /// Use in-memory BlobStore (no Telegram). For local s3-tests / CI.
    pub memory_store: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let memory_store = env::var("S3GRAM_MEMORY").ok().as_deref() == Some("1");
        let (bot_token, chat_id) = if memory_store {
            (
                env::var("BOT_TOKEN").unwrap_or_else(|_| "unused".into()),
                env::var("CHAT_ID").unwrap_or_else(|_| "-100memory".into()),
            )
        } else {
            (require("BOT_TOKEN")?, require("CHAT_ID")?)
        };

        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "s3gram".into());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "s3gramsecret".into());
        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8333".into());
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            if memory_store {
                "sqlite:s3gram-memory.db".into()
            } else {
                "sqlite:s3gram.db".into()
            }
        });
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let snapshot_interval_secs = if memory_store {
            env::var("SNAPSHOT_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        } else {
            env::var("SNAPSHOT_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300)
        };

        Ok(Self {
            bot_token,
            chat_id,
            listen_addr,
            database_url,
            access_key,
            secret_key,
            region,
            snapshot_interval_secs,
            memory_store,
        })
    }

    /// Config for in-process tests (no Telegram / dotenv required).
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
            memory_store: true,
        }
    }
}

fn require(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("missing required env var {key}"))?;
    if v.is_empty() {
        bail!("missing required env var {key}");
    }
    Ok(v)
}
