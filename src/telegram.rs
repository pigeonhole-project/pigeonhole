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
    Timeout(anyhow::Error),
    RetryAfter(u64, anyhow::Error),
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub document: Option<Document>,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub file_id: String,
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

    pub async fn send_document(
        &self,
        chat_id: &str,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        // Non-idempotent: only retry when we know the request did not commit
        // (connection errors, 429, 5xx). Do not retry ambiguous timeouts.
        let mut last_err = None;
        for attempt in 0..5u32 {
            match self
                .send_document_once(chat_id, data.clone(), filename, caption)
                .await
            {
                Ok(v) => return Ok(v),
                Err(SendErr::Timeout(e)) => {
                    return Err(e).context("sendDocument timed out; not retrying to avoid duplicate uploads");
                }
                Err(SendErr::RetryAfter(secs, e)) => {
                    debug!(attempt, secs, error = %e, "sendDocument rate-limited");
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
                }
                Err(SendErr::Retryable(e)) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "sendDocument retry");
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
        let part = Part::bytes(data.to_vec())
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
            .map_err(|e| {
                if e.is_timeout() {
                    SendErr::Timeout(e.into())
                } else if e.is_connect() || e.is_request() {
                    SendErr::Retryable(e.into())
                } else {
                    SendErr::Fatal(e.into())
                }
            })?;

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
            let desc = body
                .description
                .unwrap_or_else(|| status.to_string());
            let secs = api_retry.unwrap_or(retry_after as i64).max(1) as u64;
            return Err(SendErr::RetryAfter(secs, anyhow!("sendDocument 429: {desc}")));
        }

        let body: ApiResponse<Message> = resp
            .json()
            .await
            .map_err(|e| SendErr::Retryable(e.into()))?;
        if status.is_server_error() {
            return Err(SendErr::Retryable(anyhow!(
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

    pub async fn download_file(&self, file_id: &str) -> Result<Bytes> {
        let mut last_err = None;
        for attempt in 0..5u32 {
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

    /// Returns Ok(true) if Telegram confirmed deletion, Ok(false) if API rejected
    /// (message too old / missing rights) — caller should queue for retry/audit.
    pub async fn delete_message(&self, chat_id: &str, message_id: i64) -> Result<bool> {
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
        let body: ApiResponse<bool> = resp.json().await.context("deleteMessage json")?;
        if status.is_success() && body.ok && body.result.unwrap_or(false) {
            return Ok(true);
        }
        debug!(
            chat_id,
            message_id,
            desc = ?body.description,
            %status,
            "deleteMessage not confirmed"
        );
        Ok(false)
    }

    pub async fn send_message(&self, chat_id: &str, text: &str) -> Result<()> {
        let resp = self
            .http
            .post(self.api_url("sendMessage"))
            .form(&[
                ("chat_id", chat_id),
                ("text", text),
                ("disable_web_page_preview", "true"),
            ])
            .send()
            .await
            .context("sendMessage http")?;
        let status = resp.status();
        let body: ApiResponse<serde_json::Value> =
            resp.json().await.context("sendMessage json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "sendMessage failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        Ok(())
    }

    /// Rename a group/supergroup/channel. Requires bot admin with can_change_info.
    /// Private chats cannot be renamed.
    pub async fn set_chat_title(&self, chat_id: &str, title: &str) -> Result<()> {
        let resp = self
            .http
            .post(self.api_url("setChatTitle"))
            .form(&[("chat_id", chat_id), ("title", title)])
            .send()
            .await
            .context("setChatTitle http")?;
        let status = resp.status();
        let body: ApiResponse<bool> = resp.json().await.context("setChatTitle json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "setChatTitle failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        Ok(())
    }

    pub async fn get_updates(&self, offset: i64, timeout_secs: u64) -> Result<Vec<Update>> {
        let resp = self
            .http
            .get(self.api_url("getUpdates"))
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", timeout_secs.to_string()),
                (
                    "allowed_updates",
                    serde_json::json!(["message", "my_chat_member"]).to_string(),
                ),
            ])
            .timeout(Duration::from_secs(timeout_secs + 10))
            .send()
            .await
            .context("getUpdates http")?;

        let status = resp.status();
        let body: ApiResponse<Vec<Update>> = resp.json().await.context("getUpdates json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "getUpdates failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }
        Ok(body.result.unwrap_or_default())
    }
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<IncomingMessage>,
    pub my_chat_member: Option<ChatMemberUpdated>,
}

#[derive(Debug, Deserialize)]
pub struct IncomingMessage {
    pub message_id: i64,
    pub chat: TgChat,
    pub text: Option<String>,
    pub from: Option<TgUser>,
}

#[derive(Debug, Deserialize)]
pub struct ChatMemberUpdated {
    pub chat: TgChat,
    pub new_chat_member: ChatMember,
}

#[derive(Debug, Deserialize)]
pub struct ChatMember {
    pub status: String,
    pub user: TgUser,
}

#[derive(Debug, Deserialize)]
pub struct TgChat {
    pub id: i64,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub chat_type: String,
}

#[derive(Debug, Deserialize)]
pub struct TgUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub username: Option<String>,
}
