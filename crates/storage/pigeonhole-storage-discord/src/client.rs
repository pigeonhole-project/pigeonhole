//! Discord over HTTP: [`DiscordClient`] (one bot) and [`DiscordChannel`]
//! (one channel, a [`DiscordApi`]).

use crate::api::{CdnStale, DcAttachment, DcMessage, DiscordApi, ThreadStart, PAGE};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::ChatLimiter;
use reqwest::header::{HeaderMap, AUTHORIZATION};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::debug;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
/// Discord snowflake epoch (2015-01-01T00:00:00.000Z), milliseconds.
pub const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// Bulk-delete only accepts messages younger than 14 days (API error 50034).
const BULK_DELETE_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
/// Safety margin so we do not race the 14-day cutoff.
const BULK_DELETE_AGE_SLACK: Duration = Duration::from_secs(60);
/// `flags`: no link previews.
const SUPPRESS_EMBEDS: u64 = 1 << 2;
/// Attempts per request (network errors and 429s).
const ATTEMPTS: u32 = 6;

/// Milliseconds since Unix epoch encoded in a Discord snowflake.
pub fn snowflake_timestamp_ms(id: u64) -> u64 {
    (id >> 22) + DISCORD_EPOCH_MS
}

/// Whether Discord's bulk-delete endpoint will accept this message id by age.
pub fn snowflake_bulk_deletable(id: u64, unix_now_ms: u64) -> bool {
    let age_ms = unix_now_ms.saturating_sub(snowflake_timestamp_ms(id));
    age_ms < BULK_DELETE_MAX_AGE.saturating_sub(BULK_DELETE_AGE_SLACK).as_millis() as u64
}

fn unix_now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Which local budget a request spends.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RateBudget {
    Send,
    Read,
    Delete,
    Download,
}

/// Per-route wait derived from `X-RateLimit-*` response headers (no token capture).
#[derive(Default)]
struct RouteLimitState {
    /// Cool-down deadline per local rate budget (send / read / delete).
    cool_down_until: [Option<Instant>; 3],
}

impl RouteLimitState {
    fn idx(budget: RateBudget) -> Option<usize> {
        match budget {
            RateBudget::Send => Some(0),
            RateBudget::Read => Some(1),
            RateBudget::Delete => Some(2),
            RateBudget::Download => None,
        }
    }

    fn observe(&mut self, budget: RateBudget, headers: &HeaderMap) {
        let Some(i) = Self::idx(budget) else { return };
        let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok());
        let remaining = header("x-ratelimit-remaining").and_then(|s| s.parse::<u64>().ok());
        let reset_after = header("x-ratelimit-reset-after").and_then(|s| s.parse::<f64>().ok());
        match (remaining, reset_after) {
            (Some(0), Some(secs)) if secs > 0.0 => {
                let until = Instant::now() + Duration::from_secs_f64(secs);
                self.cool_down_until[i] = Some(self.cool_down_until[i].map_or(until, |prev| prev.max(until)));
            }
            (Some(r), _) if r > 0 => self.cool_down_until[i] = None,
            _ => {}
        }
    }

    fn wait_secs(&self, budget: RateBudget) -> f64 {
        Self::idx(budget)
            .and_then(|i| self.cool_down_until[i])
            .map_or(0.0, |until| until.saturating_duration_since(Instant::now()).as_secs_f64())
    }
}

/// Seconds a 429 asks to wait.
fn retry_after(headers: &HeaderMap, body: &serde_json::Value) -> f64 {
    body.get("retry_after")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            headers
                .get("retry-after")
                .or_else(|| headers.get("x-ratelimit-reset-after"))
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(3.0)
        .max(0.1)
}

/// One bot's HTTP client, shared by its channels.
#[derive(Clone)]
pub struct DiscordClient {
    http: reqwest::Client,
    bot_token: String,
    api_base: String,
    route_limits: Arc<Mutex<RouteLimitState>>,
}

/// One answer: status and JSON body (`Null` if none).
struct Answer {
    status: u16,
    body: serde_json::Value,
}

impl DiscordClient {
    pub fn new(bot_token: String) -> Result<Self> {
        Self::with_api_base(bot_token, DEFAULT_API_BASE.into())
    }

    pub fn with_api_base(bot_token: String, api_base: String) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bot {bot_token}").parse().context("authorization header")?);
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent("DiscordBot (https://github.com/pigeonhole-project/pigeonhole, 0.1)")
            .default_headers(headers)
            .build()?;
        Ok(Self { http, bot_token, api_base, route_limits: Arc::default() })
    }

    /// Application id prefix of the bot token (`app_id.xxx.yyy`); never logs the secret.
    pub fn app_id(&self) -> String {
        self.bot_token.split('.').next().filter(|s| !s.is_empty()).unwrap_or("unknown").to_string()
    }

    /// Wait implied by the last `X-RateLimit-*` answer for this budget.
    pub fn route_wait_secs(&self, budget: RateBudget) -> f64 {
        self.route_limits.lock().unwrap().wait_secs(budget)
    }

    /// Channel `channel_id` through this client.
    pub fn channel(&self, channel_id: u64, limiter: Arc<ChatLimiter>) -> DiscordChannel {
        DiscordChannel { client: self.clone(), channel_id, limiter }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    /// Send `build()` until it is not a 429 or a network error.
    async fn request(
        &self,
        limiter: &ChatLimiter,
        budget: RateBudget,
        what: &str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Answer> {
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            match budget {
                RateBudget::Send => limiter.acquire_send().await,
                RateBudget::Delete => limiter.acquire_delete().await,
                RateBudget::Read => limiter.acquire_get_file().await,
                RateBudget::Download => {}
            }
            let resp = match build().send().await {
                Ok(r) => r,
                Err(e) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(what, attempt, ?wait, error = %e, "discord http retry");
                    last = Some(anyhow::Error::from(e));
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };
            let status = resp.status().as_u16();
            let headers = resp.headers().clone();
            self.route_limits.lock().unwrap().observe(budget, &headers);
            let text = resp.text().await.unwrap_or_default();
            let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
            if status == 429 {
                let wait = Duration::from_secs_f64(retry_after(&headers, &body));
                debug!(what, attempt, ?wait, "discord 429");
                match budget {
                    RateBudget::Send => limiter.penalize_send(wait),
                    RateBudget::Delete => limiter.penalize_delete(wait),
                    RateBudget::Read => limiter.penalize_get_file(wait),
                    RateBudget::Download => {}
                }
                tokio::time::sleep(wait).await;
                last = Some(anyhow!("{what}: rate limited"));
                continue;
            }
            return Ok(Answer { status, body });
        }
        Err(last.unwrap_or_else(|| anyhow!("{what}: failed")))
    }

    /// Fail fast unless the bot can do what a store needs in `channel_id`
    /// (`chain`: also start and manage threads).
    pub async fn ensure_permissions(&self, channel_id: u64, chain: bool, limiter: &ChatLimiter) -> Result<()> {
        const VIEW: u64 = 1 << 10;
        const SEND: u64 = 1 << 11;
        const MANAGE_MESSAGES: u64 = 1 << 13;
        const ATTACH: u64 = 1 << 15;
        const HISTORY: u64 = 1 << 16;
        const MANAGE_THREADS: u64 = 1 << 34;
        const CREATE_PUBLIC_THREADS: u64 = 1 << 35;
        let mut required = VIEW | SEND | MANAGE_MESSAGES | ATTACH | HISTORY;
        if chain {
            required |= MANAGE_THREADS | CREATE_PUBLIC_THREADS;
        }
        let path = format!("channels/{channel_id}/permissions/@me");
        let a = self.request(limiter, RateBudget::Read, "permissions", || self.http.get(self.url(&path))).await?;
        if a.status != 200 {
            bail!("channel {channel_id}: permission check answered {} (is the bot in this channel?)", a.status);
        }
        let bits: u64 = a.body["permissions"].as_str().unwrap_or("0").parse().context("permissions bitfield")?;
        if bits & required != required {
            bail!("bot lacks permissions in channel {channel_id}: have {bits:#x}, need {required:#x}");
        }
        Ok(())
    }
}

/// One channel over HTTP.
#[derive(Clone)]
pub struct DiscordChannel {
    client: DiscordClient,
    channel_id: u64,
    limiter: Arc<ChatLimiter>,
}

impl DiscordChannel {
    pub fn client(&self) -> &DiscordClient {
        &self.client
    }

    fn path(&self, rest: &str) -> String {
        format!("channels/{}/{rest}", self.channel_id)
    }

    async fn delete_one(&self, id: u64) -> Result<()> {
        let path = self.path(&format!("messages/{id}"));
        let c = &self.client;
        let a = c.request(&self.limiter, RateBudget::Delete, "delete message", || c.http.delete(c.url(&path))).await?;
        match a.status {
            200..=299 | 404 => Ok(()),
            s => bail!("delete message {id}: {s} {}", a.body),
        }
    }
}

#[derive(Deserialize)]
struct RawMessage {
    id: String,
    #[serde(rename = "type", default)]
    kind: u8,
    #[serde(default)]
    content: String,
    #[serde(default)]
    attachments: Vec<RawAttachment>,
    #[serde(default)]
    thread: Option<RawThread>,
}

#[derive(Deserialize)]
struct RawAttachment {
    #[serde(default)]
    filename: String,
    #[serde(default)]
    size: u64,
    url: String,
}

#[derive(Deserialize)]
struct RawThread {
    #[serde(default)]
    name: String,
}

fn message(v: serde_json::Value) -> Result<DcMessage> {
    let m: RawMessage = serde_json::from_value(v).context("message json")?;
    Ok(DcMessage {
        id: m.id.parse().context("message id")?,
        kind: m.kind,
        content: m.content,
        attachment: m
            .attachments
            .into_iter()
            .next()
            .map(|a| DcAttachment { filename: a.filename, size: a.size, url: a.url }),
        thread: m.thread.map(|t| t.name),
    })
}

/// Discord's JSON error code, if any.
fn code(body: &serde_json::Value) -> Option<u64> {
    body.get("code").and_then(|c| c.as_u64())
}

#[async_trait]
impl DiscordApi for DiscordChannel {
    fn channel_id(&self) -> u64 {
        self.channel_id
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        let mut payload = serde_json::json!({
            "content": content,
            "nonce": nonce,
            "enforce_nonce": true,
            "flags": SUPPRESS_EMBEDS,
            "allowed_mentions": { "parse": [] },
        });
        let path = self.path("messages");
        let c = &self.client;
        let a = match file {
            None => {
                c.request(&self.limiter, RateBudget::Send, "send message", || c.http.post(c.url(&path)).json(&payload))
                    .await?
            }
            Some((name, data)) => {
                let _upload = self.limiter.acquire_upload().await;
                payload["attachments"] = serde_json::json!([{ "id": 0, "filename": name }]);
                let payload = payload.to_string();
                c.request(&self.limiter, RateBudget::Send, "send attachment", || {
                    let part = Part::stream_with_length(reqwest::Body::from(data.clone()), data.len() as u64)
                        .file_name(name.to_string())
                        .mime_str("application/octet-stream")
                        .expect("a static mime type");
                    let form = Form::new().text("payload_json", payload.clone()).part("files[0]", part);
                    c.http.post(c.url(&path)).multipart(form)
                })
                .await?
            }
        };
        if !(200..300).contains(&a.status) {
            bail!("send message: {} {}", a.status, a.body);
        }
        message(a.body)
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        let path = self.path(&format!("messages/{id}"));
        let c = &self.client;
        let a = c.request(&self.limiter, RateBudget::Read, "get message", || c.http.get(c.url(&path))).await?;
        match a.status {
            200 => Ok(Some(message(a.body)?)),
            404 => Ok(None),
            s => bail!("get message {id}: {s} {}", a.body),
        }
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        let mut query = format!("messages?limit={}", limit.clamp(1, PAGE));
        if let Some(b) = before {
            query.push_str(&format!("&before={b}"));
        }
        if let Some(a) = after {
            query.push_str(&format!("&after={a}"));
        }
        let path = self.path(&query);
        let c = &self.client;
        let a = c.request(&self.limiter, RateBudget::Read, "list messages", || c.http.get(c.url(&path))).await?;
        if a.status != 200 {
            bail!("list messages: {} {}", a.status, a.body);
        }
        let mut out = match a.body {
            serde_json::Value::Array(items) => items.into_iter().map(message).collect::<Result<Vec<_>>>()?,
            other => bail!("list messages: not a list: {other}"),
        };
        out.retain(|m| before.is_none_or(|b| m.id < b) && after.is_none_or(|a| m.id > a));
        out.sort_by_key(|m| m.id);
        Ok(out)
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        let now = unix_now_ms();
        let (mut young, mut old): (Vec<u64>, Vec<u64>) =
            ids.iter().partition(|&&id| snowflake_bulk_deletable(id, now));
        young.sort_unstable();
        young.dedup();
        old.sort_unstable();
        old.dedup();
        for chunk in young.chunks(PAGE) {
            if let [one] = chunk {
                self.delete_one(*one).await?;
                continue;
            }
            let body = serde_json::json!({ "messages": chunk.iter().map(|id| id.to_string()).collect::<Vec<_>>() });
            let path = self.path("messages/bulk-delete");
            let c = &self.client;
            let a = c
                .request(&self.limiter, RateBudget::Delete, "bulk delete", || c.http.post(c.url(&path)).json(&body))
                .await?;
            if !(200..300).contains(&a.status) {
                debug!(status = a.status, "bulk delete refused; one by one");
                for &id in chunk {
                    self.delete_one(id).await?;
                }
            }
        }
        for id in old {
            self.delete_one(id).await?;
        }
        Ok(())
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        let path = self.path(&format!("messages/{message_id}/threads"));
        let body = serde_json::json!({ "name": name, "auto_archive_duration": 60 });
        let c = &self.client;
        let a = c
            .request(&self.limiter, RateBudget::Send, "start thread", || c.http.post(c.url(&path)).json(&body))
            .await?;
        match (a.status, code(&a.body)) {
            (200..=299, _) => Ok(ThreadStart::Created),
            (_, Some(160004)) => Ok(ThreadStart::Taken),
            (404, _) | (_, Some(10008)) => Ok(ThreadStart::NoMessage),
            (s, _) => bail!("start thread on {message_id}: {s} {}", a.body),
        }
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        let path = format!("channels/{message_id}");
        let c = &self.client;
        let a = c.request(&self.limiter, RateBudget::Read, "get thread", || c.http.get(c.url(&path))).await?;
        match a.status {
            200 => Ok(a.body["name"].as_str().map(str::to_string)),
            404 => Ok(None),
            s => bail!("get thread {message_id}: {s} {}", a.body),
        }
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        let path = format!("channels/{message_id}");
        let body = serde_json::json!({ "archived": true });
        let c = &self.client;
        let a = c
            .request(&self.limiter, RateBudget::Delete, "archive thread", || c.http.patch(c.url(&path)).json(&body))
            .await?;
        match a.status {
            200..=299 => Ok(()),
            s => bail!("archive thread {message_id}: {s} {}", a.body),
        }
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        let path = format!("channels/{message_id}");
        let c = &self.client;
        let a = c.request(&self.limiter, RateBudget::Delete, "delete thread", || c.http.delete(c.url(&path))).await?;
        match a.status {
            200..=299 | 404 => Ok(()),
            s => bail!("delete thread {message_id}: {s} {}", a.body),
        }
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let _permit = self.limiter.acquire_download().await;
        let mut req = self.client.http.get(url);
        if let Some((start, end)) = range {
            req = req.header("Range", format!("bytes={start}-{}", end - 1));
        }
        let resp = req.send().await.context("download attachment")?;
        let status = resp.status().as_u16();
        if matches!(status, 403 | 404) {
            return Err(CdnStale.into());
        }
        if let Some((start, _)) = range {
            if status != 206 {
                bail!("ranged download answered {status}, not 206");
            }
            let cr = resp.headers().get(reqwest::header::CONTENT_RANGE).and_then(|v| v.to_str().ok()).unwrap_or("");
            if !cr.starts_with(&format!("bytes {start}-")) {
                bail!("Content-Range {cr:?} does not start at {start}");
            }
        } else if !(200..300).contains(&status) {
            bail!("download answered {status}");
        }
        resp.bytes().await.context("attachment bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_id_from_token_prefix() {
        let dc = DiscordClient::new("1234567890.abc.def".into()).unwrap();
        assert_eq!(dc.app_id(), "1234567890");
    }

    #[test]
    fn bulk_deletable_by_snowflake_age() {
        let now_ms = DISCORD_EPOCH_MS + 30 * 24 * 60 * 60 * 1000;
        let young_id = (now_ms - 2 * 24 * 60 * 60 * 1000 - DISCORD_EPOCH_MS) << 22;
        let old_id = (now_ms - 20 * 24 * 60 * 60 * 1000 - DISCORD_EPOCH_MS) << 22;
        assert!(snowflake_bulk_deletable(young_id, now_ms));
        assert!(!snowflake_bulk_deletable(old_id, now_ms));
    }

    #[test]
    fn route_limit_observe_remaining_zero() {
        let mut st = RouteLimitState::default();
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
        headers.insert("x-ratelimit-reset-after", "1.5".parse().unwrap());
        st.observe(RateBudget::Delete, &headers);
        assert!(st.wait_secs(RateBudget::Delete) > 1.0);
        assert_eq!(st.wait_secs(RateBudget::Send), 0.0);
    }

    #[test]
    fn messages_parse_with_attachment_and_thread() {
        let m = message(serde_json::json!({
            "id": "123", "type": 0, "content": "x",
            "attachments": [{ "id": "9", "filename": "ph-1.bin", "size": 3, "url": "https://cdn/x" }],
            "thread": { "id": "123", "name": "next 7 log-batch-1" }
        }))
        .unwrap();
        assert_eq!(m.id, 123);
        assert_eq!(m.attachment.unwrap().filename, "ph-1.bin");
        assert_eq!(m.thread.as_deref(), Some("next 7 log-batch-1"));
    }
}
