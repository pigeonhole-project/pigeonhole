use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use pigeonhole_blob::{ChatLimiter, DeleteOutcome, PinnedContent};
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::debug;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
const ATTACHMENT_URL_CACHE_CAP: u64 = 4096;
/// Discord CDN links expire; refresh before typical expiry.
const ATTACHMENT_URL_TTL: Duration = Duration::from_secs(50 * 60);
/// Discord snowflake epoch (2015-01-01T00:00:00.000Z), milliseconds.
pub const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;
/// Bulk-delete only accepts messages younger than 14 days (API error 50034).
const BULK_DELETE_MAX_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);
/// Safety margin so we do not race the 14-day cutoff.
const BULK_DELETE_AGE_SLACK: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
struct CachedAttachment {
    url: String,
    message_id: i64,
}

#[derive(Clone)]
pub struct DiscordClient {
    http: reqwest::Client,
    bot_token: String,
    api_base: String,
    attachment_urls: MokaCache<String, CachedAttachment>,
    route_limits: Arc<Mutex<RouteLimitState>>,
}

#[derive(Debug, Deserialize)]
struct RateLimitBody {
    #[serde(default)]
    retry_after: Option<f64>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    id: Snowflake,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    attachments: Vec<Attachment>,
}

impl Message {
    pub fn message_id(&self) -> u64 {
        self.id.as_u64()
    }

    pub fn message_id_i64(&self) -> i64 {
        self.id.as_i64()
    }

    pub fn content(&self) -> Option<&str> {
        self.content.as_deref()
    }

    pub fn first_attachment_id(&self) -> Option<String> {
        self.attachments.first().map(|a| a.id.to_string())
    }
}

#[derive(Debug, Deserialize)]
struct Attachment {
    id: Snowflake,
    url: String,
    #[serde(default)]
    filename: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Snowflake {
    Num(u64),
    Str(String),
}

impl Snowflake {
    fn as_u64(&self) -> u64 {
        match self {
            Self::Num(n) => *n,
            Self::Str(s) => s.parse().unwrap_or(0),
        }
    }

    fn as_i64(&self) -> i64 {
        self.as_u64() as i64
    }

    fn to_string(&self) -> String {
        match self {
            Self::Num(n) => n.to_string(),
            Self::Str(s) => s.clone(),
        }
    }
}

/// Milliseconds since Unix epoch encoded in a Discord snowflake.
pub fn snowflake_timestamp_ms(id: u64) -> u64 {
    (id >> 22) + DISCORD_EPOCH_MS
}

/// Whether Discord's bulk-delete endpoint will accept this message id by age.
pub fn snowflake_bulk_deletable(id: u64, unix_now_ms: u64) -> bool {
    let created = snowflake_timestamp_ms(id);
    let age_ms = unix_now_ms.saturating_sub(created);
    let max = BULK_DELETE_MAX_AGE
        .saturating_sub(BULK_DELETE_AGE_SLACK)
        .as_millis() as u64;
    age_ms < max
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
        let Some(i) = Self::idx(budget) else {
            return;
        };
        let remaining = headers
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        let reset_after = headers
            .get("x-ratelimit-reset-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<f64>().ok());
        match (remaining, reset_after) {
            (Some(0), Some(secs)) if secs > 0.0 => {
                let until = Instant::now() + Duration::from_secs_f64(secs);
                self.cool_down_until[i] = Some(match self.cool_down_until[i] {
                    Some(prev) => prev.max(until),
                    None => until,
                });
            }
            (Some(r), _) if r > 0 => {
                self.cool_down_until[i] = None;
            }
            _ => {}
        }
    }

    fn wait_secs(&self, budget: RateBudget) -> f64 {
        let Some(i) = Self::idx(budget) else {
            return 0.0;
        };
        match self.cool_down_until[i] {
            Some(until) => {
                let now = Instant::now();
                if until > now {
                    until.saturating_duration_since(now).as_secs_f64()
                } else {
                    0.0
                }
            }
            None => 0.0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct User {
    id: Snowflake,
}

#[derive(Debug, Deserialize)]
struct ChannelPermissions {
    permissions: String,
}

fn auth_header(token: &str) -> Result<HeaderMap> {
    let mut h = HeaderMap::new();
    h.insert(
        AUTHORIZATION,
        format!("Bot {token}")
            .parse()
            .context("authorization header")?,
    );
    Ok(h)
}

fn is_cdn_url_stale(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 403 | 404)
}

fn parse_retry_after(headers: &HeaderMap, body: &RateLimitBody) -> u64 {
    if let Some(v) = body.retry_after {
        return (v.ceil() as u64).max(1);
    }
    headers
        .get("retry-after")
        .or_else(|| headers.get("x-ratelimit-reset-after"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .map(|s| (s.ceil() as u64).max(1))
        .unwrap_or(3)
}

impl DiscordClient {
    pub fn new(bot_token: String) -> Result<Self> {
        Self::with_api_base(bot_token, DEFAULT_API_BASE.into())
    }

    pub fn with_api_base(bot_token: String, api_base: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .default_headers(auth_header(&bot_token)?)
            .build()?;
        Ok(Self {
            http,
            bot_token,
            api_base,
            attachment_urls: MokaCache::builder()
                .max_capacity(ATTACHMENT_URL_CACHE_CAP)
                .time_to_live(ATTACHMENT_URL_TTL)
                .build(),
            route_limits: Arc::new(Mutex::new(RouteLimitState::default())),
        })
    }

    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Application id prefix from the bot token (`app_id.xxx.yyy`); never logs the secret.
    pub fn app_id(&self) -> String {
        self.bot_token
            .split('.')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown")
            .to_string()
    }

    /// Peek wait implied by last `X-RateLimit-*` observation for this budget.
    pub fn route_wait_secs(&self, budget: RateBudget) -> f64 {
        self.route_limits.lock().unwrap().wait_secs(budget)
    }

    fn observe_route_limits(&self, budget: RateBudget, headers: &HeaderMap) {
        self.route_limits
            .lock()
            .unwrap()
            .observe(budget, headers);
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.api_base.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn remember_attachment(&self, attachment_id: &str, message_id: i64, url: &str) {
        self.attachment_urls
            .insert(
                attachment_id.to_string(),
                CachedAttachment {
                    url: url.to_string(),
                    message_id,
                },
            )
            .await;
    }

    pub async fn forget_attachment(&self, attachment_id: &str) {
        self.attachment_urls.invalidate(attachment_id).await;
    }

    async fn cached_attachment(&self, attachment_id: &str) -> Option<CachedAttachment> {
        self.attachment_urls.get(attachment_id).await
    }

    async fn request_with_rate_limit(
        &self,
        limiter: Option<&ChatLimiter>,
        budget: RateBudget,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let mut last_err = None;
        for attempt in 0..5u32 {
            match budget {
                RateBudget::Send => {
                    if let Some(lim) = limiter {
                        lim.acquire_send().await;
                    }
                }
                RateBudget::Delete => {
                    if let Some(lim) = limiter {
                        lim.acquire_delete().await;
                    }
                }
                RateBudget::Read => {
                    if let Some(lim) = limiter {
                        lim.acquire_get_file().await;
                    }
                }
                RateBudget::Download => {}
            }

            let resp = match build().send().await {
                Ok(r) => r,
                Err(e) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "discord http retry");
                    last_err = Some(e.into());
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };

            let status = resp.status();
            let headers = resp.headers().clone();
            self.observe_route_limits(budget, &headers);
            if status.as_u16() == 429 {
                let body: RateLimitBody = resp.json().await.unwrap_or(RateLimitBody {
                    retry_after: None,
                    message: Some("rate limited".into()),
                });
                let secs = parse_retry_after(&headers, &body);
                debug!(attempt, secs, "discord 429");
                if let Some(lim) = limiter {
                    match budget {
                        RateBudget::Send => lim.penalize_send(Duration::from_secs(secs)),
                        RateBudget::Delete => lim.penalize_delete(Duration::from_secs(secs)),
                        RateBudget::Read => lim.penalize_get_file(Duration::from_secs(secs)),
                        RateBudget::Download => {}
                    }
                } else {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                }
                last_err = Some(anyhow!(
                    "discord rate limited: {}",
                    body.message.unwrap_or_else(|| status.to_string())
                ));
                continue;
            }
            return Ok(resp);
        }
        Err(last_err.unwrap_or_else(|| anyhow!("discord request failed")))
    }

    /// POST /channels/{channel_id}/messages (multipart: payload_json + files[0]).
    pub async fn send_attachment(
        &self,
        channel_id: &str,
        data: Bytes,
        filename: &str,
        caption: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<(i64, String, String)> {
        if let Some(lim) = limiter {
            let _upload = lim.acquire_upload().await;
        }
        let payload = serde_json::json!({
            "content": caption,
        });
        let path = format!("channels/{channel_id}/messages");

        let mut last_err = None;
        for attempt in 0..5u32 {
            if let Some(lim) = limiter {
                lim.acquire_send().await;
            }
            let len = data.len() as u64;
            let part = Part::stream_with_length(reqwest::Body::from(data.clone()), len)
                .file_name(filename.to_string())
                .mime_str("application/octet-stream")
                .context("multipart part")?;
            let form = Form::new()
                .text("payload_json", payload.to_string())
                .part("files[0]", part);

            let resp = match self.http.post(self.url(&path)).multipart(form).send().await {
                Ok(r) => r,
                Err(e) => {
                    debug!(attempt, error = %e, "send attachment http retry");
                    last_err = Some(e.into());
                    tokio::time::sleep(Duration::from_millis(200 * 2u64.pow(attempt))).await;
                    continue;
                }
            };

            let status = resp.status();
            let headers = resp.headers().clone();
            self.observe_route_limits(RateBudget::Send, &headers);
            if status.as_u16() == 429 {
                let body: RateLimitBody = resp.json().await.unwrap_or(RateLimitBody {
                    retry_after: None,
                    message: Some("rate limited".into()),
                });
                let secs = parse_retry_after(&headers, &body);
                if let Some(lim) = limiter {
                    lim.penalize_send(Duration::from_secs(secs));
                } else {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                }
                last_err = Some(anyhow!("send attachment 429"));
                continue;
            }
            let msg: Message = resp.json().await.context("send message json")?;
            if !status.is_success() {
                last_err = Some(anyhow!("send message failed: {status}"));
                continue;
            }
            let att = msg
                .attachments
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("missing attachment in message"))?;
            let message_id = msg.id.as_i64();
            let attachment_id = att.id.to_string();
            self.remember_attachment(&attachment_id, message_id, &att.url)
                .await;
            return Ok((message_id, attachment_id, att.url));
        }
        Err(last_err.unwrap_or_else(|| anyhow!("send attachment failed")))
    }

    pub async fn get_message(
        &self,
        channel_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Message> {
        let path = format!("channels/{channel_id}/messages/{message_id}");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Read, || {
                self.http.get(self.url(&path))
            })
            .await?;
        let status = resp.status();
        let msg: Message = resp.json().await.context("get message json")?;
        if !status.is_success() {
            bail!("get message failed: {status}");
        }
        Ok(msg)
    }

    async fn refresh_attachment_url(
        &self,
        channel_id: &str,
        message_id: i64,
        attachment_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<String> {
        let msg = self.get_message(channel_id, message_id, limiter).await?;
        let att = msg
            .attachments
            .iter()
            .find(|a| a.id.to_string() == attachment_id)
            .ok_or_else(|| anyhow!("attachment {attachment_id} not on message {message_id}"))?;
        self.remember_attachment(attachment_id, message_id, &att.url)
            .await;
        Ok(att.url.clone())
    }

    pub async fn download_bytes(
        &self,
        channel_id: &str,
        message_id: i64,
        attachment_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Bytes> {
        let mut force_refresh = false;
        let mut last_err = None;
        for attempt in 0..5u32 {
            let url = if force_refresh {
                self.refresh_attachment_url(channel_id, message_id, attachment_id, limiter)
                    .await
            } else if let Some(cached) = self.cached_attachment(attachment_id).await {
                Ok(cached.url)
            } else {
                self.refresh_attachment_url(channel_id, message_id, attachment_id, limiter)
                    .await
            };

            let url = match url {
                Ok(u) => u,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(200 * 2u64.pow(attempt))).await;
                    continue;
                }
            };

            let _dl = if let Some(lim) = limiter {
                Some(lim.acquire_download().await)
            } else {
                None
            };

            match self.download_cdn_bytes(&url).await {
                Ok(b) => return Ok(b),
                Err(e) => {
                    if e.to_string().contains("403") || e.to_string().contains("404") {
                        self.forget_attachment(attachment_id).await;
                        force_refresh = true;
                        debug!(attempt, error = %e, "discord CDN url stale; refreshing");
                    } else {
                        force_refresh = false;
                        tokio::time::sleep(Duration::from_millis(200 * 2u64.pow(attempt))).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("discord download failed")))
    }

    pub async fn download_bytes_range(
        &self,
        channel_id: &str,
        message_id: i64,
        attachment_id: &str,
        start: u64,
        end: u64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Bytes> {
        if end <= start {
            bail!("empty range {start}..{end}");
        }
        let last = end - 1;
        let mut force_refresh = false;
        let mut last_err = None;
        for attempt in 0..5u32 {
            let url = if force_refresh {
                self.refresh_attachment_url(channel_id, message_id, attachment_id, limiter)
                    .await
            } else if let Some(cached) = self.cached_attachment(attachment_id).await {
                Ok(cached.url)
            } else {
                self.refresh_attachment_url(channel_id, message_id, attachment_id, limiter)
                    .await
            };

            let url = match url {
                Ok(u) => u,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };

            let _dl = if let Some(lim) = limiter {
                Some(lim.acquire_download().await)
            } else {
                None
            };

            match self.download_cdn_range(&url, start, last).await {
                Ok(b) => return Ok(b),
                Err(e) => {
                    if e.to_string().contains("403") || e.to_string().contains("404") {
                        self.forget_attachment(attachment_id).await;
                        force_refresh = true;
                    } else {
                        force_refresh = false;
                        tokio::time::sleep(Duration::from_millis(200 * 2u64.pow(attempt))).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("discord ranged download failed")))
    }

    async fn download_cdn_bytes(&self, url: &str) -> Result<Bytes> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("cdn download http")?;
        let status = resp.status();
        if is_cdn_url_stale(status) {
            bail!("cdn download status: {status}");
        }
        resp.error_for_status()
            .context("cdn download status")?
            .bytes()
            .await
            .context("cdn download bytes")
    }

    async fn download_cdn_range(&self, url: &str, start: u64, last: u64) -> Result<Bytes> {
        let resp = self
            .http
            .get(url)
            .header("Range", format!("bytes={start}-{last}"))
            .send()
            .await
            .context("ranged cdn http")?;
        let status = resp.status();
        if is_cdn_url_stale(status) {
            bail!("ranged cdn status: {status}");
        }
        if status.as_u16() != 206 {
            bail!("ranged download expected 206, got {status}");
        }
        let cr = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let ok_prefix = format!("bytes {start}-");
        if !cr.starts_with(&ok_prefix) {
            bail!("Content-Range {cr:?} does not start at {start}");
        }
        resp.bytes().await.context("ranged cdn bytes")
    }

    pub async fn delete_message(
        &self,
        channel_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<DeleteOutcome> {
        let path = format!("channels/{channel_id}/messages/{message_id}");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Delete, || {
                self.http.delete(self.url(&path))
            })
            .await?;
        let status = resp.status();
        if status.as_u16() == 404 {
            return Ok(DeleteOutcome::Gone);
        }
        if status.as_u16() == 204 || status.is_success() {
            return Ok(DeleteOutcome::Deleted);
        }
        debug!(%status, message_id, "delete message not confirmed");
        Ok(DeleteOutcome::Failed)
    }

    /// `POST /channels/{channel_id}/messages/bulk-delete` (2–100 ids, each < 14 days old).
    ///
    /// Missing message ids are ignored by Discord; we treat 204 as success.
    pub async fn bulk_delete_messages(
        &self,
        channel_id: &str,
        message_ids: &[u64],
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        if message_ids.len() < 2 || message_ids.len() > 100 {
            bail!(
                "bulk-delete requires 2..=100 message ids, got {}",
                message_ids.len()
            );
        }
        let path = format!("channels/{channel_id}/messages/bulk-delete");
        let body = serde_json::json!({
            "messages": message_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
        });
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Delete, || {
                self.http
                    .post(self.url(&path))
                    .header(CONTENT_TYPE, "application/json")
                    .json(&body)
            })
            .await?;
        let status = resp.status();
        if status.as_u16() == 204 || status.is_success() {
            return Ok(());
        }
        let err_body = resp.text().await.unwrap_or_default();
        // Do not echo request bodies / tokens; status + short API message only.
        let snippet: String = err_body.chars().take(200).collect();
        bail!("bulk-delete failed: {status} {snippet}");
    }

    pub async fn send_text(
        &self,
        channel_id: &str,
        text: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<i64> {
        let path = format!("channels/{channel_id}/messages");
        let body = serde_json::json!({ "content": text });
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Send, || {
                self.http
                    .post(self.url(&path))
                    .header(CONTENT_TYPE, "application/json")
                    .json(&body)
            })
            .await?;
        let status = resp.status();
        let msg: Message = resp.json().await.context("send text json")?;
        if !status.is_success() {
            bail!("send text failed: {status}");
        }
        Ok(msg.id.as_i64())
    }

    pub async fn pin_message(
        &self,
        channel_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        let path = format!("channels/{channel_id}/pins/{message_id}");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Send, || {
                self.http.put(self.url(&path))
            })
            .await?;
        if resp.status().is_success() {
            return Ok(());
        }
        bail!("pin message failed: {}", resp.status());
    }

    pub async fn unpin_message(
        &self,
        channel_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        let path = format!("channels/{channel_id}/pins/{message_id}");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Send, || {
                self.http.delete(self.url(&path))
            })
            .await?;
        if resp.status().is_success() || resp.status().as_u16() == 404 {
            return Ok(());
        }
        bail!("unpin message failed: {}", resp.status());
    }

    pub async fn get_pinned_messages(
        &self,
        channel_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Vec<Message>> {
        let path = format!("channels/{channel_id}/pins");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Read, || {
                self.http.get(self.url(&path))
            })
            .await?;
        let status = resp.status();
        let msgs: Vec<Message> = resp.json().await.context("pins json")?;
        if !status.is_success() {
            bail!("get pins failed: {status}");
        }
        Ok(msgs)
    }

    pub async fn get_pinned_content(
        &self,
        channel_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Option<PinnedContent>> {
        let pins = self.get_pinned_messages(channel_id, limiter).await?;
        let Some(msg) = pins.into_iter().next() else {
            return Ok(None);
        };
        if let Some(text) = msg.content.filter(|t| !t.is_empty()) {
            return Ok(Some(PinnedContent::Text {
                message_id: msg.id.as_i64(),
                text,
            }));
        }
        if let Some(att) = msg.attachments.into_iter().next() {
            return Ok(Some(PinnedContent::Document {
                message_id: msg.id.as_i64(),
                document_ref: att.id.to_string(),
            }));
        }
        Ok(None)
    }

    /// List channel messages (`before` / `after` snowflakes, max 100).
    pub async fn list_messages(
        &self,
        channel_id: &str,
        before: Option<i64>,
        after: Option<i64>,
        limit: Option<u8>,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Vec<Message>> {
        let mut path = format!("channels/{channel_id}/messages?");
        if let Some(b) = before {
            path.push_str(&format!("before={b}&"));
        }
        if let Some(a) = after {
            path.push_str(&format!("after={a}&"));
        }
        let lim = limit.unwrap_or(50).min(100);
        path.push_str(&format!("limit={lim}"));
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Read, || {
                self.http.get(self.url(&path))
            })
            .await?;
        let status = resp.status();
        let msgs: Vec<Message> = resp.json().await.context("list messages json")?;
        if !status.is_success() {
            bail!("list messages failed: {status}");
        }
        Ok(msgs)
    }

    async fn get_current_user(&self) -> Result<User> {
        let resp = self
            .http
            .get(self.url("users/@me"))
            .send()
            .await
            .context("users/@me http")?;
        let status = resp.status();
        let user: User = resp.json().await.context("users/@me json")?;
        if !status.is_success() {
            bail!("users/@me failed: {status}");
        }
        Ok(user)
    }

    /// Fail fast unless the bot can view, send, attach, read history, and pin.
    pub async fn ensure_channel_permissions(
        &self,
        channel_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        const VIEW: u64 = 1 << 10;
        const SEND: u64 = 1 << 11;
        const ATTACH: u64 = 1 << 15;
        const HISTORY: u64 = 1 << 16;
        const PIN: u64 = 1 << 24;
        let required = VIEW | SEND | ATTACH | HISTORY | PIN;

        let path = format!("channels/{channel_id}/permissions/@me");
        let resp = self
            .request_with_rate_limit(limiter, RateBudget::Read, || {
                self.http.get(self.url(&path))
            })
            .await?;
        let status = resp.status();
        let perm: ChannelPermissions = resp.json().await.context("permissions json")?;
        if !status.is_success() {
            bail!(
                "channel permissions check failed: {status} (is the bot in this channel?)"
            );
        }
        let bits: u64 = perm.permissions.parse().context("permissions bitfield")?;
        if bits & required != required {
            bail!(
                "bot lacks required channel permissions in {channel_id}; \
                 need view/send/attach/read_history/pin (have {bits:#x}, need {required:#x})"
            );
        }
        let me = self.get_current_user().await.context("users/@me")?;
        tracing::info!(
            channel_id,
            bot_id = me.id.as_i64(),
            permissions = bits,
            "discord channel access ok"
        );
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RateBudget {
    Send,
    Read,
    Delete,
    Download,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn attachment_url_moka_roundtrip() {
        let dc = DiscordClient::new("token".into()).unwrap();
        assert!(dc.cached_attachment("aid").await.is_none());
        dc.remember_attachment("aid", 42, "https://cdn.example/a")
            .await;
        let c = dc.cached_attachment("aid").await.unwrap();
        assert_eq!(c.message_id, 42);
        assert_eq!(c.url, "https://cdn.example/a");
        dc.forget_attachment("aid").await;
        assert!(dc.cached_attachment("aid").await.is_none());
    }

    #[test]
    fn app_id_from_token_prefix() {
        let dc = DiscordClient::new("1234567890.abc.def".into()).unwrap();
        assert_eq!(dc.app_id(), "1234567890");
    }

    #[test]
    fn bulk_deletable_by_snowflake_age() {
        let now_ms = DISCORD_EPOCH_MS + 30 * 24 * 60 * 60 * 1000;
        let young_ts = now_ms - 2 * 24 * 60 * 60 * 1000;
        let old_ts = now_ms - 20 * 24 * 60 * 60 * 1000;
        let young_id = (young_ts - DISCORD_EPOCH_MS) << 22;
        let old_id = (old_ts - DISCORD_EPOCH_MS) << 22;
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
}
