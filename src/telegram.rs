use crate::rate_limit::ChatLimiter;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::time::Duration;
use tracing::debug;

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    bot_token: String,
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

impl TelegramClient {
    pub fn new(bot_token: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;
        Ok(Self { http, bot_token })
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

    /// Upload a document. When `limiter` is set, request budget and 429 cool-down
    /// are shared across the process (see [`ChatLimiter`]); otherwise each call
    /// sleeps `retry_after` independently (legacy / one-off tools).
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
                lim.acquire().await;
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
                        lim.penalize(Duration::from_secs(secs.max(1)));
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

    pub async fn download_file(
        &self,
        file_id: &str,
        limiter: Option<&ChatLimiter>,
    ) -> Result<Bytes> {
        let mut last_err = None;
        for attempt in 0..5u32 {
            if let Some(lim) = limiter {
                lim.acquire().await;
            }
            match self.download_file_once(file_id).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "getFile retry");
                    last_err = Some(e);
                    tokio::time::sleep(wait).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("download failed")))
    }

    async fn download_file_once(&self, file_id: &str) -> Result<Bytes> {
        let resp = self
            .http
            .post(self.api_url("getFile"))
            .form(&[("file_id", file_id)])
            .send()
            .await
            .context("getFile http")?;

        let status = resp.status();
        let body: ApiResponse<FilePath> = resp.json().await.context("getFile json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "getFile failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }

        let path = body.result.context("missing file_path")?.file_path;
        let bytes = self
            .http
            .get(self.file_url(&path))
            .send()
            .await
            .context("file download http")?
            .error_for_status()
            .context("file download status")?
            .bytes()
            .await
            .context("file download bytes")?;
        Ok(bytes)
    }

    /// Delete a chat message. `Gone` (already missing) is success for queue purposes.
    pub async fn delete_message(
        &self,
        chat_id: &str,
        message_id: i64,
        limiter: Option<&ChatLimiter>,
    ) -> Result<DeleteOutcome> {
        if let Some(lim) = limiter {
            lim.acquire().await;
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
                lim.penalize(Duration::from_secs(secs));
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

    /// Send a plain text message; returns `message_id`.
    pub async fn send_message(&self, chat_id: &str, text: &str) -> Result<i64> {
        let resp = self
            .http
            .post(self.api_url("sendMessage"))
            .form(&[("chat_id", chat_id), ("text", text)])
            .send()
            .await
            .context("sendMessage http")?;
        let status = resp.status();
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

    pub async fn pin_chat_message(&self, chat_id: &str, message_id: i64) -> Result<()> {
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

    pub async fn unpin_chat_message(&self, chat_id: &str, message_id: i64) -> Result<()> {
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

    /// Fail fast unless the bot is admin/creator and can pin messages in `chat_id`.
    pub async fn ensure_chat_admin(&self, chat_id: &str) -> Result<()> {
        let me = self.get_me().await.context("getMe")?;
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
                    "telegram chat access ok"
                );
                Ok(())
            }
            "administrator" => {
                // Channels/groups: pin bootstrap needs can_pin_messages.
                // Some chat types omit the field for admins that still can pin; treat
                // explicit false as hard fail, missing as ok (API variance).
                if member.can_pin_messages == Some(false) {
                    anyhow::bail!(
                        "bot is admin in {chat_id} but can_pin_messages=false; \
                         enable pin rights for snapshot bootstrap"
                    );
                }
                tracing::info!(
                    chat_id,
                    bot_id = me.id,
                    status = %member.status,
                    can_pin = ?member.can_pin_messages,
                    "telegram chat access ok"
                );
                Ok(())
            }
            other => anyhow::bail!(
                "bot must be administrator in chat {chat_id} (current status: {other})"
            ),
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
    /// Present for administrators in groups/channels when Telegram reports pin rights.
    #[serde(default)]
    pub can_pin_messages: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct TgUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub username: Option<String>,
}
