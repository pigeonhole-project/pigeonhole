use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use reqwest::multipart::{Form, Part};
use s3gram_blob::{ChatLimiter, DeleteOutcome, PinnedContent};
use serde::Deserialize;
use std::time::Duration;
use tracing::debug;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
const ATTACHMENT_URL_CACHE_CAP: u64 = 4096;
/// Discord CDN links expire; refresh before typical expiry.
const ATTACHMENT_URL_TTL: Duration = Duration::from_secs(50 * 60);

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
    fn as_i64(&self) -> i64 {
        match self {
            Self::Num(n) => *n as i64,
            Self::Str(s) => s.parse().unwrap_or(0),
        }
    }

    fn to_string(&self) -> String {
        match self {
            Self::Num(n) => n.to_string(),
            Self::Str(s) => s.clone(),
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
        })
    }

    pub fn api_base(&self) -> &str {
        &self.api_base
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
            if status.as_u16() == 429 {
                let headers = resp.headers().clone();
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
            if status.as_u16() == 429 {
                let headers = resp.headers().clone();
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
                file_id: att.id.to_string(),
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

#[derive(Copy, Clone)]
enum RateBudget {
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
}
