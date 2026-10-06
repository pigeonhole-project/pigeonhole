use anyhow::{bail, Context, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub bot_token: String,
    pub chat_id: String,
    pub listen_addr: String,
    pub database_url: String,
    pub access_key: String,
    pub secret_key: String,
    #[allow(dead_code)]
    pub region: String,
    /// Seconds between automatic index snapshots. `0` disables.
    pub snapshot_interval_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let bot_token = require("BOT_TOKEN")?;
        let chat_id = require("CHAT_ID")?;
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "s3gram".into());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "s3gramsecret".into());
        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8333".into());
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:s3gram.db".into());
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let snapshot_interval_secs = env::var("SNAPSHOT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        Ok(Self {
            bot_token,
            chat_id,
            listen_addr,
            database_url,
            access_key,
            secret_key,
            region,
            snapshot_interval_secs,
        })
    }
}

fn require(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("missing required env var {key}"))?;
    if v.is_empty() {
        bail!("missing required env var {key}");
    }
    Ok(v)
}
