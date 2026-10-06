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
    chat_id: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
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
    pub fn new(bot_token: String, chat_id: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;
        Ok(Self {
            http,
            bot_token,
            chat_id,
        })
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
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        let mut last_err = None;
        for attempt in 0..5u32 {
            match self.send_document_once(data.clone(), filename, caption).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let wait = Duration::from_millis(200 * 2u64.pow(attempt));
                    debug!(attempt, ?wait, error = %e, "sendDocument retry");
                    last_err = Some(e);
                    tokio::time::sleep(wait).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("sendDocument failed")))
    }

    async fn send_document_once(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        let part = Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str("application/octet-stream")?;

        let form = Form::new()
            .text("chat_id", self.chat_id.clone())
            .text("caption", caption.to_string())
            .part("document", part);

        let resp = self
            .http
            .post(self.api_url("sendDocument"))
            .multipart(form)
            .send()
            .await
            .context("sendDocument http")?;

        let status = resp.status();
        let body: ApiResponse<Message> = resp.json().await.context("sendDocument json")?;
        if !status.is_success() || !body.ok {
            return Err(anyhow!(
                "sendDocument failed: {}",
                body.description.unwrap_or_else(|| status.to_string())
            ));
        }

        let msg = body.result.context("missing result")?;
        let doc = msg.document.context("missing document")?;
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

    pub async fn delete_message(&self, message_id: i64) -> Result<()> {
        let resp = self
            .http
            .post(self.api_url("deleteMessage"))
            .form(&[
                ("chat_id", self.chat_id.as_str()),
                ("message_id", &message_id.to_string()),
            ])
            .send()
            .await
            .context("deleteMessage http")?;

        let status = resp.status();
        let body: ApiResponse<bool> = resp.json().await.context("deleteMessage json")?;
        if !status.is_success() || !body.ok {
            // Best-effort: message may already be gone.
            debug!(
                message_id,
                desc = ?body.description,
                "deleteMessage soft-fail"
            );
        }
        Ok(())
    }
}
