use anyhow::{bail, Context, Result};
use std::env;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct Config {
    pub bot_token: String,
    /// Telegram chat for service bucket only: registry JSON + index snapshots.
    /// Never used for object data blobs.
    pub service_chat_id: String,
    /// Admin chat for /bucket commands (required).
    pub admin_chat_id: String,
    /// Optional fallback chat for CreateBucket without prior /bucket bind (tests/scripts).
    /// Must not equal service_chat_id.
    pub default_data_chat_id: Option<String>,
    /// Reserved S3 bucket name that maps to `service_chat_id`.
    pub service_bucket: String,
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
        let service_chat_id = match env::var("SERVICE_CHAT_ID") {
            Ok(v) if !v.is_empty() => v,
            _ => match env::var("CHAT_ID") {
                Ok(v) if !v.is_empty() => {
                    warn!("CHAT_ID is deprecated; rename to SERVICE_CHAT_ID");
                    v
                }
                _ => bail!("missing required env var SERVICE_CHAT_ID"),
            },
        };
        let admin_chat_id = require("ADMIN_CHAT_ID")?;
        let default_data_chat_id = env::var("DEFAULT_DATA_CHAT_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(ref data) = default_data_chat_id {
            if data == &service_chat_id {
                bail!("DEFAULT_DATA_CHAT_ID must not equal SERVICE_CHAT_ID");
            }
        }

        let service_bucket = env::var("SERVICE_BUCKET").unwrap_or_else(|_| "s3gram".into());
        let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "s3gram".into());
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "s3gramsecret".into());
        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8333".into());
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:s3gram.db".into());
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let snapshot_interval_secs = env::var("SNAPSHOT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        if service_bucket.len() < 3 || service_bucket.len() > 63 {
            bail!("SERVICE_BUCKET must be a valid bucket name (3-63 chars)");
        }

        Ok(Self {
            bot_token,
            service_chat_id,
            admin_chat_id,
            default_data_chat_id,
            service_bucket,
            listen_addr,
            database_url,
            access_key,
            secret_key,
            region,
            snapshot_interval_secs,
        })
    }

    pub fn is_service_chat(&self, chat_id: &str) -> bool {
        chat_id == self.service_chat_id || chat_id == self.admin_chat_id
    }
}

fn require(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("missing required env var {key}"))?;
    if v.is_empty() {
        bail!("missing required env var {key}");
    }
    Ok(v)
}
