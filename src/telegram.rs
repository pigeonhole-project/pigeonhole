use crate::rate_limit::ChatLimiter;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use lru::LruCache;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::debug;

/// Telegram `file_path` from `getFile` is reusable for ~1h; cache to avoid
/// metering every chunk download against the getFile budget.
const FILE_PATH_CACHE_CAP: usize = 4096;

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    bot_token: String,
    /// Shared across clones (`TelegramBlobStore` / snapshot workers).
    file_paths: Arc<Mutex<LruCache<String, String>>>,
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

/// Result of deleteMessage: Gone/Deleted both mean the message is no longer present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    /// Message already absent ("message to delete not found") — treat as success.
    Gone,
    /// Transient or policy failure — retry later.
    Failed,
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

/// Content of the chat's latest pinned message (bootstrap pointer).
#[derive(Debug, Clone)]
pub enum PinnedContent {
    Text { message_id: i64, text: String },
    Document { message_id: i64, file_id: String },
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
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;
        let cap = NonZeroUsize::new(FILE_PATH_CACHE_CAP).unwrap();
        Ok(Self {
            http,
            bot_token,
            file_paths: Arc::new(Mutex::new(LruCache::new(cap))),
        })
    }

    fn cached_file_path(&self, file_id: &str) -> Option<String> {
        self.file_paths
            .lock()
            .unwrap()
            .get(file_id)
            .cloned()
    }

    fn remember_file_path(&self, file_id: &str, path: &str) {
        self.file_paths
            .lock()
            .unwrap()
            .put(file_id.to_string(), path.to_string());
    }

    fn forget_file_path(&self, file_id: &str) {
        self.file_paths.lock().unwrap().pop(file_id);
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{}", self.bot_token, method)
    }

    fn file_url(&self, file_path: &str) -> String {
        format!(
            "https://api.telegram.org/file/bot{}/{}",
            self.bot_token, file_path
        )
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
            .map_err(classify_reqwest)?;

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
                        self.forget_file_path(file_id);
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

    async fn resolve_file_path_cached(
        &self,
        file_id: &str,
        limiter: Option<&ChatLimiter>,
        force_refresh: bool,
    ) -> Result<String, DownloadErr> {
        if !force_refresh {
            if let Some(path) = self.cached_file_path(file_id) {
                return Ok(path);
            }
        }
        if let Some(lim) = limiter {
            lim.acquire_get_file().await;
        }
        let path = self.resolve_file_path(file_id).await?;
        self.remember_file_path(file_id, &path);
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
            .context("deleteMessage http")?;

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
        let lower = desc.to_ascii_lowercase();
        if lower.contains("message to delete not found")
            || lower.contains("message not found")
            || (lower.contains("message can't be deleted") && lower.contains("not found"))
        {
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
    if e.is_connect() {
        SendErr::Connect(e.into())
    } else if e.is_timeout() || e.is_request() || e.is_body() {
        SendErr::Ambiguous(e.into())
    } else {
        SendErr::Fatal(e.into())
    }
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

    #[test]
    fn file_path_lru_roundtrip() {
        let tg = TelegramClient::new("token".into()).unwrap();
        assert!(tg.cached_file_path("fid-1").is_none());
        tg.remember_file_path("fid-1", "photos/file.bin");
        assert_eq!(
            tg.cached_file_path("fid-1").as_deref(),
            Some("photos/file.bin")
        );
        // Clones share the cache.
        let tg2 = tg.clone();
        assert_eq!(
            tg2.cached_file_path("fid-1").as_deref(),
            Some("photos/file.bin")
        );
        tg2.forget_file_path("fid-1");
        assert!(tg.cached_file_path("fid-1").is_none());
    }

    #[test]
    fn cdn_stale_detects_http_codes() {
        assert!(is_cdn_path_stale(&anyhow!("file download status: 404 Not Found")));
        assert!(is_cdn_path_stale(&anyhow!("410 Gone")));
        assert!(!is_cdn_path_stale(&anyhow!("file download status: 500")));
    }
}
