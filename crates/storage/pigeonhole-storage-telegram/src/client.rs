use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use moka::future::Cache as MokaCache;
use reqwest::multipart::{Form, Part};
use pigeonhole_blob::{ChatLimiter, DeleteOutcome, PinnedContent};
use serde::Deserialize;
use std::time::Duration;
use tracing::debug;

/// Telegram `file_path` from `getFile` is reusable for ~1h; cache to avoid
/// metering every chunk download against the getFile budget.
const FILE_PATH_CACHE_CAP: u64 = 4096;
/// CDN path TTL — Telegram links live about an hour.
const FILE_PATH_TTL: Duration = Duration::from_secs(50 * 60);

const DEFAULT_API_BASE: &str = "https://api.telegram.org";

/// Max message ids per Bot API `deleteMessages` call.
pub const DELETE_MESSAGES_MAX: usize = 100;

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    bot_token: String,
    /// e.g. `https://api.telegram.org` (tests override with wiremock).
    api_base: String,
    /// Shared across clones (`TelegramBlobStore` / snapshot workers).
    file_paths: MokaCache<String, String>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    #[serde(default)]
    parameters: Option<ResponseParameters>,
}

#[derive(Debug, Deserialize)]
struct ResponseParameters {
    retry_after: Option<i64>,
}

enum SendErr {
    /// Ambiguous: request may have reached Telegram — do not retry.
    Ambiguous(anyhow::Error),
    RetryAfter(u64, anyhow::Error),
    /// Safe to retry: connection never established.
    Connect(anyhow::Error),
    Fatal(anyhow::Error),
}

enum DownloadErr {
    RetryAfter(u64, anyhow::Error),
    Other(anyhow::Error),
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    #[serde(default)]
    pub text: Option<String>,
    pub document: Option<Document>,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub file_id: String,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub pinned_message: Option<Message>,
}

#[derive(Debug, Deserialize)]
struct FilePath {
    file_path: String,
}

fn is_cdn_path_stale(err: &anyhow::Error) -> bool {
    let s = err.to_string();
    s.contains("404") || s.contains("410") || s.contains("403")
}

impl TelegramClient {
    pub fn new(bot_token: String) -> Result<Self> {
        Self::with_api_base(bot_token, DEFAULT_API_BASE.into())
    }

    /// Construct a client pointed at a custom Bot API root (wiremock tests).
    pub fn with_api_base(bot_token: String, api_base: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;
        Ok(Self {
            http,
            bot_token,
            api_base: api_base.trim_end_matches('/').to_string(),
            file_paths: MokaCache::builder()
                .max_capacity(FILE_PATH_CACHE_CAP)
                .time_to_live(FILE_PATH_TTL)
                .build(),
        })
    }

    /// Bot id prefix from `bot_token` (`123456:ABC` → `123456`).
    pub fn bot_id(&self) -> Result<&str> {
        self.bot_token
            .split_once(':')
            .map(|(a, _)| a)
            .filter(|s| !s.is_empty())
            .context("telegram bot token missing bot_id prefix before ':'")
    }

    /// `tg:{bot_id}:{chat_id}` — matches `telegram_fingerprint` in blob-store.
    pub fn fingerprint(&self, chat_id: &str) -> Result<String> {
        Ok(format!("tg:{}:{chat_id}", self.bot_id()?))
    }

    /// Sync cache probe for [`crate::TelegramBlobStore`] `cost()` (no token capture).
    pub fn has_cached_file_path(&self, file_id: &str) -> bool {
        self.file_paths.contains_key(file_id)
    }

    async fn cached_file_path(&self, file_id: &str) -> Option<String> {
        self.file_paths.get(file_id).await
    }

    async fn remember_file_path(&self, file_id: &str, path: &str) {
        self.file_paths
            .insert(file_id.to_string(), path.to_string())
            .await;
    }

    async fn forget_file_path(&self, file_id: &str) {
        self.file_paths.invalidate(file_id).await;
    }

    fn api_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.api_base, self.bot_token, method)
    }

    fn file_url(&self, file_path: &str) -> String {
        format!(
            "{}/file/bot{}/{}",
            self.api_base, self.bot_token, file_path
        )
    }

    /// Map transport errors without embedding the bot token (URL redaction).
    fn http_err(what: &str, e: reqwest::Error) -> anyhow::Error {
        anyhow!("{what}: {}", e.without_url())
    }

    /// Upload a document. Uses the **send** budget when `limiter` is set.
    pub async fn send_document(
        &self,
        chat_id: &str,
        data: Bytes,
        filename: &str,
        caption: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<(String, i64)> {
        // Non-idempotent: only retry connect failures and 429. Timeouts, 5xx, and
        // response parse errors are ambiguous (message may already exist).
        let mut last_err = None;
        for attempt in 0..5u32 {
            if let Some(lim) = limiter {
                lim.acquire_send().await;
            }
            match self
                .send_document_once(chat_id, data.clone(), filename, caption)
                .await
            {
                Ok(v) => return Ok(v),
                Err(SendErr::Ambiguous(e)) => {
                    return Err(e).context(
                        "sendDocument ambiguous failure; not retrying to avoid duplicate uploads",
                    );
                }
                Err(SendErr::RetryAfter(secs, e)) => {
                    debug!(attempt, secs, error = %e, "sendDocument rate-limited");
                    last_err = Some(e);
                    if let Some(lim) = limiter {
                        lim.penalize_send(Duration::from_secs(secs.max(1)));
                    } else {
                        tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
                    }
                }
                Err(SendErr::Connect(e)) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "sendDocument connect retry");
                    last_err = Some(e);
                    tokio::time::sleep(wait).await;
                }
                Err(SendErr::Fatal(e)) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("sendDocument failed")))
    }

    async fn send_document_once(
        &self,
        chat_id: &str,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64), SendErr> {
        let len = data.len() as u64;
        let part = Part::stream_with_length(reqwest::Body::from(data), len)
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")
            .map_err(|e| SendErr::Fatal(e.into()))?;

        let form = Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption.to_string())
            .part("document", part);

        let resp = self
            .http
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await
            .map_err(|e| classify_reqwest(e))?;

        self.parse_send_document_response(resp).await
    }

    async fn parse_send_document_response(
        &self,
        resp: reqwest::Response,
    ) -> Result<(String, i64), SendErr> {
        let status = resp.status();
        if status.as_u16() == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(3);
            let body: ApiResponse<Message> = resp.json().await.unwrap_or(ApiResponse {
                ok: false,
                result: None,
                description: Some("rate limited".into()),
                parameters: None,
            });
            let api_retry = body.parameters.as_ref().and_then(|p| p.retry_after);
            let desc = body.description.unwrap_or_else(|| status.to_string());
            let secs = api_retry.unwrap_or(retry_after as i64).max(1) as u64;
            return Err(SendErr::RetryAfter(secs, anyhow!("sendDocument 429: {desc}")));
        }

        let body: ApiResponse<Message> = resp
            .json()
            .await
            .map_err(|e| SendErr::Ambiguous(e.into()))?;
        if status.is_server_error() {
            // 5xx after accept is ambiguous — message may exist.
            return Err(SendErr::Ambiguous(anyhow!(
                "sendDocument {}: {}",
                status,
                body.description.unwrap_or_default()
            )));
        }
        if !status.is_success() || !body.ok {
            return Err(SendErr::Fatal(anyhow!(
                "sendDocument failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            )));
        }

        let msg = body
            .result
            .ok_or_else(|| SendErr::Fatal(anyhow!("missing result")))?;
        let doc = msg
            .document
            .ok_or_else(|| SendErr::Fatal(anyhow!("missing document")))?;
        Ok((doc.file_id, msg.message_id))
    }

    /// Resolve `file_id` via `getFile` (getFile budget) then download bytes from the
    /// CDN (download semaphore only — not API-metered).
    ///
    /// `file_path` is LRU-cached so repeated reads of the same chunk do not spend
    /// another getFile token. Stale paths are dropped on CDN 4xx and re-resolved.
    pub async fn download_file(
        &self,
        file_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Bytes> {
        let mut last_err = None;
        let mut force_refresh = false;
        for attempt in 0..5u32 {
            let path = match self
                .resolve_file_path_cached(file_id, limiter, force_refresh)
                .await
            {
                Ok(p) => p,
                Err(DownloadErr::RetryAfter(secs, e)) => {
                    debug!(attempt, secs, error = %e, "getFile rate-limited");
                    last_err = Some(e);
                    if let Some(lim) = limiter {
                        lim.penalize_get_file(Duration::from_secs(secs.max(1)));
                    } else {
                        tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
                    }
                    continue;
                }
                Err(DownloadErr::Other(e)) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "getFile retry");
                    last_err = Some(e);
                    tokio::time::sleep(wait).await;
                    continue;
                }
            };

            let _dl = if let Some(lim) = limiter {
                Some(lim.acquire_download().await)
            } else {
                None
            };
            match self.download_cdn_bytes(&path).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let stale = is_cdn_path_stale(&e);
                    if stale {
                        self.forget_file_path(file_id).await;
                        force_refresh = true;
                        debug!(attempt, error = %e, "CDN path stale; refreshing getFile");
                    } else {
                        force_refresh = false;
                        let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                        debug!(attempt, ?wait, error = %e, "file CDN download retry");
                        tokio::time::sleep(wait).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("download failed")))
    }

    /// Best-effort ranged CDN download. Succeeds only on HTTP 206 with a
    /// `Content-Range` that starts at `start`. End is exclusive (Rust-style).
    pub async fn download_file_range(
        &self,
        file_id: &str,
        start: u64,
        end: u64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Bytes> {
        if end <= start {
            bail!("empty range {start}..{end}");
        }
        let last = end - 1;
        let mut last_err = None;
        let mut force_refresh = false;
        for attempt in 0..5u32 {
            let path = match self
                .resolve_file_path_cached(file_id, limiter, force_refresh)
                .await
            {
                Ok(p) => p,
                Err(DownloadErr::RetryAfter(secs, e)) => {
                    last_err = Some(e);
                    if let Some(lim) = limiter {
                        lim.penalize_get_file(Duration::from_secs(secs.max(1)));
                    } else {
                        tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
                    }
                    continue;
                }
                Err(DownloadErr::Other(e)) => {
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
            match self.download_cdn_range(&path, start, last).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let stale = is_cdn_path_stale(&e);
                    if stale {
                        self.forget_file_path(file_id).await;
                        force_refresh = true;
                    } else {
                        force_refresh = false;
                        tokio::time::sleep(Duration::from_millis(200 * 2u64.pow(attempt))).await;
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("ranged download failed")))
    }

    async fn download_cdn_range(&self, file_path: &str, start: u64, last: u64) -> Result<Bytes> {
        let resp = self
            .http
            .get(self.file_url(file_path))
            .header("Range", format!("bytes={start}-{last}"))
            .send()
            .await
            .context("ranged file download http")?;
        let status = resp.status();
        if status.as_u16() != 206 {
            bail!("ranged download expected 206, got {status}");
        }
        let cr = resp
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        // Content-Range: bytes START-END/TOTAL
        let ok_prefix = format!("bytes {start}-");
        if !cr.starts_with(&ok_prefix) {
            bail!("Content-Range {cr:?} does not start at {start}");
        }
        resp.bytes().await.context("ranged file download bytes")
    }

    async fn resolve_file_path_cached(
        &self,
        file_id: &str,
        limiter: Option<&ChatLimiter>,
        force_refresh: bool,
    ) -> Result<String, DownloadErr> {
        if !force_refresh {
            if let Some(path) = self.cached_file_path(file_id).await {
                return Ok(path);
            }
        }
        if let Some(lim) = limiter {
            lim.acquire_get_file().await;
        }
        let path = self.resolve_file_path(file_id).await?;
        self.remember_file_path(file_id, &path).await;
        Ok(path)
    }

    async fn resolve_file_path(&self, file_id: &str) -> Result<String, DownloadErr> {
        let resp = self
            .http
            .post(self.api_url("getFile"))
            .form(&[("file_id", file_id)])
            .send()
            .await
            .map_err(|e| DownloadErr::Other(e.into()))?;

        let status = resp.status();
        if status.as_u16() == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(3);
            let body: ApiResponse<FilePath> = resp.json().await.unwrap_or(ApiResponse {
                ok: false,
                result: None,
                description: Some("rate limited".into()),
                parameters: None,
            });
            let api_retry = body.parameters.as_ref().and_then(|p| p.retry_after);
            let desc = body.description.unwrap_or_else(|| status.to_string());
            let secs = api_retry.unwrap_or(retry_after as i64).max(1) as u64;
            return Err(DownloadErr::RetryAfter(secs, anyhow!("getFile 429: {desc}")));
        }

        let body: ApiResponse<FilePath> = resp
            .json()
            .await
            .map_err(|e| DownloadErr::Other(e.into()))?;
        if !status.is_success() || !body.ok {
            return Err(DownloadErr::Other(anyhow!(
                "getFile failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            )));
        }
        body.result
            .map(|r| r.file_path)
            .ok_or_else(|| DownloadErr::Other(anyhow!("missing file_path")))
    }

    async fn download_cdn_bytes(&self, file_path: &str) -> Result<Bytes> {
        self.http
            .get(self.file_url(file_path))
            .send()
            .await
            .context("file download http")?
            .error_for_status()
            .context("file download status")?
            .bytes()
            .await
            .context("file download bytes")
    }

    /// Delete a chat message. Uses the **delete** budget when `limiter` is set.
    pub async fn delete_message(
        &self,
        chat_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<DeleteOutcome> {
        if let Some(lim) = limiter {
            lim.acquire_delete().await;
        }
        let resp = self
            .http
            .post(self.api_url("deleteMessage"))
            .form(&[
                ("chat_id", chat_id),
                ("message_id", &message_id.to_string()),
            ])
            .send()
            .await
            .map_err(|e| Self::http_err("deleteMessage http", e))?;

        let status = resp.status();
        if status.as_u16() == 429 {
            let body: ApiResponse<bool> = resp.json().await.unwrap_or(ApiResponse {
                ok: false,
                result: None,
                description: Some("rate limited".into()),
                parameters: None,
            });
            let secs = body
                .parameters
                .as_ref()
                .and_then(|p| p.retry_after)
                .unwrap_or(3)
                .max(1) as u64;
            if let Some(lim) = limiter {
                lim.penalize_delete(Duration::from_secs(secs));
            }
            debug!(message_id, secs, "deleteMessage rate-limited");
            return Ok(DeleteOutcome::Failed);
        }

        let body: ApiResponse<bool> = resp.json().await.context("deleteMessage json")?;
        if status.is_success() && body.ok && body.result.unwrap_or(false) {
            return Ok(DeleteOutcome::Deleted);
        }
        let desc = body.description.unwrap_or_default();
        if delete_not_found(&desc) {
            return Ok(DeleteOutcome::Gone);
        }
        debug!(
            chat_id,
            message_id,
            %desc,
            %status,
            "deleteMessage not confirmed"
        );
        Ok(DeleteOutcome::Failed)
    }

    /// Batch-delete via Bot API `deleteMessages` (1–100 ids per call).
    /// Missing messages are skipped by Telegram (= success). Uses one **delete**
    /// token per batch HTTP call.
    pub async fn delete_messages(
        &self,
        chat_id: &str,
        message_ids: &[i64],
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        for chunk in message_ids.chunks(DELETE_MESSAGES_MAX) {
            if chunk.is_empty() {
                continue;
            }
            if let Some(lim) = limiter {
                lim.acquire_delete().await;
            }
            let ids_json = serde_json::to_string(chunk).context("serialize message_ids")?;
            let resp = self
                .http
                .post(self.api_url("deleteMessages"))
                .form(&[("chat_id", chat_id), ("message_ids", ids_json.as_str())])
                .send()
                .await
                .map_err(|e| Self::http_err("deleteMessages http", e))?;

            let status = resp.status();
            if status.as_u16() == 429 {
                let body: ApiResponse<bool> = resp.json().await.unwrap_or(ApiResponse {
                    ok: false,
                    result: None,
                    description: Some("rate limited".into()),
                    parameters: None,
                });
                let secs = body
                    .parameters
                    .as_ref()
                    .and_then(|p| p.retry_after)
                    .unwrap_or(3)
                    .max(1) as u64;
                if let Some(lim) = limiter {
                    lim.penalize_delete(Duration::from_secs(secs));
                }
                bail!("deleteMessages rate-limited (retry_after={secs}s)");
            }

            let body: ApiResponse<bool> = resp.json().await.context("deleteMessages json")?;
            if status.is_success() && body.ok {
                // result may be true; missing messages are skipped per Bot API.
                continue;
            }
            let desc = body.description.unwrap_or_else(|| status.to_string());
            if delete_not_found(&desc) {
                continue;
            }
            bail!("deleteMessages failed: {desc}");
        }
        Ok(())
    }

    pub async fn get_me(&self) -> Result<TgUser> {
        let resp = self
            .http
            .get(self.api_url("getMe"))
            .send()
            .await
            .context("getMe http")?;
        let status = resp.status();
        let body: ApiResponse<TgUser> = resp.json().await.context("getMe json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "getMe failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        body.result.ok_or_else(|| anyhow!("getMe missing result"))
    }

    pub async fn get_chat(&self, chat_id: &str) -> Result<Chat> {
        let resp = self
            .http
            .post(self.api_url("getChat"))
            .form(&[("chat_id", chat_id)])
            .send()
            .await
            .context("getChat http")?;
        let status = resp.status();
        let body: ApiResponse<Chat> = resp.json().await.context("getChat json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "getChat failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        body.result.ok_or_else(|| anyhow!("getChat missing result"))
    }

    /// Latest pinned message content, if any.
    pub async fn get_pinned_content(&self, chat_id: &str) -> Result<Option<PinnedContent>> {
        let chat = self.get_chat(chat_id).await?;
        let Some(msg) = chat.pinned_message else {
            return Ok(None);
        };
        if let Some(text) = msg.text.filter(|t| !t.is_empty()) {
            return Ok(Some(PinnedContent::Text {
                message_id: msg.message_id,
                text,
            }));
        }
        if let Some(doc) = msg.document {
            return Ok(Some(PinnedContent::Document {
                message_id: msg.message_id,
                file_id: doc.file_id,
            }));
        }
        Ok(None)
    }

    /// Send a plain text message; returns `message_id`. Uses the **send** budget.
    pub async fn send_message(
        &self,
        chat_id: &str,
        text: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<i64> {
        if let Some(lim) = limiter {
            lim.acquire_send().await;
        }
        let resp = self
            .http
            .post(self.api_url("sendMessage"))
            .form(&[("chat_id", chat_id), ("text", text)])
            .send()
            .await
            .context("sendMessage http")?;
        let status = resp.status();
        if status.as_u16() == 429 {
            let secs = 3u64;
            if let Some(lim) = limiter {
                lim.penalize_send(Duration::from_secs(secs));
            }
            bail!("sendMessage 429");
        }
        let body: ApiResponse<Message> = resp.json().await.context("sendMessage json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "sendMessage failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        Ok(body
            .result
            .ok_or_else(|| anyhow!("sendMessage missing result"))?
            .message_id)
    }

    pub async fn pin_chat_message(
        &self,
        chat_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        if let Some(lim) = limiter {
            lim.acquire_send().await;
        }
        let resp = self
            .http
            .post(self.api_url("pinChatMessage"))
            .form(&[
                ("chat_id", chat_id),
                ("message_id", &message_id.to_string()),
                ("disable_notification", "true"),
            ])
            .send()
            .await
            .context("pinChatMessage http")?;
        let status = resp.status();
        let body: ApiResponse<bool> = resp.json().await.context("pinChatMessage json")?;
        if status.is_success() && body.ok {
            return Ok(());
        }
        Err(anyhow!(
            "pinChatMessage failed: {}",
            body.description.unwrap_or_else(|| status.to_string())
        ))
    }

    pub async fn unpin_chat_message(
        &self,
        chat_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<()> {
        if let Some(lim) = limiter {
            lim.acquire_send().await;
        }
        let resp = self
            .http
            .post(self.api_url("unpinChatMessage"))
            .form(&[
                ("chat_id", chat_id),
                ("message_id", &message_id.to_string()),
            ])
            .send()
            .await
            .context("unpinChatMessage http")?;
        let status = resp.status();
        let body: ApiResponse<bool> = resp.json().await.context("unpinChatMessage json")?;
        if status.is_success() && body.ok {
            return Ok(());
        }
        let desc = body.description.unwrap_or_else(|| status.to_string());
        let lower = desc.to_ascii_lowercase();
        // Already unpinned / missing — fine for cleanup.
        if lower.contains("not found") || lower.contains("message to unpin not found") {
            return Ok(());
        }
        Err(anyhow!("unpinChatMessage failed: {desc}"))
    }

    pub async fn get_chat_member(&self, chat_id: &str, user_id: i64) -> Result<ChatMember> {
        let resp = self
            .http
            .post(self.api_url("getChatMember"))
            .form(&[
                ("chat_id", chat_id),
                ("user_id", &user_id.to_string()),
            ])
            .send()
            .await
            .context("getChatMember http")?;
        let status = resp.status();
        let body: ApiResponse<ChatMember> = resp.json().await.context("getChatMember json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "getChatMember failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        body.result
            .ok_or_else(|| anyhow!("getChatMember missing result"))
    }

    /// Fail fast unless the bot can administer `chat_id` and pin the bootstrap manifest.
    ///
    /// Channels need `can_edit_messages` (pin is implemented as edit). Groups/supergroups
    /// need `can_pin_messages`. Explicit `false` fails; omitted fields are treated as ok
    /// (Bot API variance).
    pub async fn ensure_chat_admin(&self, chat_id: &str) -> Result<()> {
        let me = self.get_me().await.context("getMe")?;
        let chat = self.get_chat(chat_id).await.context("getChat")?;
        let member = self
            .get_chat_member(chat_id, me.id)
            .await
            .with_context(|| format!("bot is not in chat {chat_id}"))?;
        match member.status.as_str() {
            "creator" => {
                tracing::info!(
                    chat_id,
                    bot_id = me.id,
                    status = %member.status,
                    chat_type = %chat.chat_type,
                    "telegram chat access ok"
                );
                Ok(())
            }
            "administrator" => {
                match chat.chat_type.as_str() {
                    "channel" => {
                        if member.can_edit_messages == Some(false) {
                            bail!(
                                "bot is admin in channel {chat_id} but can_edit_messages=false; \
                                 enable Edit messages (required to pin the snapshot manifest)"
                            );
                        }
                    }
                    "group" | "supergroup" => {
                        if member.can_pin_messages == Some(false) {
                            bail!(
                                "bot is admin in {chat_id} but can_pin_messages=false; \
                                 enable Pin messages for snapshot bootstrap"
                            );
                        }
                    }
                    _ => {
                        if member.can_pin_messages == Some(false)
                            && member.can_edit_messages == Some(false)
                        {
                            bail!(
                                "bot is admin in {chat_id} but lacks pin/edit rights for snapshots"
                            );
                        }
                    }
                }
                tracing::info!(
                    chat_id,
                    bot_id = me.id,
                    status = %member.status,
                    chat_type = %chat.chat_type,
                    can_pin = ?member.can_pin_messages,
                    can_edit = ?member.can_edit_messages,
                    "telegram chat access ok"
                );
                Ok(())
            }
            other => bail!("bot must be administrator in chat {chat_id} (current status: {other})"),
        }
    }
}

fn classify_reqwest(e: reqwest::Error) -> SendErr {
    // Strip URL so bot tokens never appear in error chains.
    let connect = e.is_connect();
    let ambiguous = e.is_timeout() || e.is_request() || e.is_body();
    let msg = e.without_url().to_string();
    if connect {
        SendErr::Connect(anyhow!(msg))
    } else if ambiguous {
        SendErr::Ambiguous(anyhow!(msg))
    } else {
        SendErr::Fatal(anyhow!(msg))
    }
}

fn delete_not_found(desc: &str) -> bool {
    let lower = desc.to_ascii_lowercase();
    lower.contains("message to delete not found")
        || lower.contains("message not found")
        || (lower.contains("message can't be deleted") && lower.contains("not found"))
}

#[derive(Debug, Deserialize)]
pub struct ChatMember {
    pub status: String,
    pub user: TgUser,
    /// Groups/supergroups: pin rights.
    #[serde(default)]
    pub can_pin_messages: Option<bool>,
    /// Channels: editing (and pinning) messages.
    #[serde(default)]
    pub can_edit_messages: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TgUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub username: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn file_path_moka_roundtrip() {
        let tg = TelegramClient::new("token".into()).unwrap();
        assert!(tg.cached_file_path("fid-1").await.is_none());
        tg.remember_file_path("fid-1", "photos/file.bin").await;
        assert_eq!(
            tg.cached_file_path("fid-1").await.as_deref(),
            Some("photos/file.bin")
        );
        // Clones share the cache.
        let tg2 = tg.clone();
        assert_eq!(
            tg2.cached_file_path("fid-1").await.as_deref(),
            Some("photos/file.bin")
        );
        tg2.forget_file_path("fid-1").await;
        assert!(tg.cached_file_path("fid-1").await.is_none());
    }

    #[test]
    fn cdn_stale_detects_http_codes() {
        assert!(is_cdn_path_stale(&anyhow!("file download status: 404 Not Found")));
        assert!(is_cdn_path_stale(&anyhow!("410 Gone")));
        assert!(!is_cdn_path_stale(&anyhow!("file download status: 500")));
    }
}
