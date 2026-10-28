//! Binary-only: the Telegram side of the chat LogStore (`[meta.log] kind = "telegram"`).

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use pigeonhole::rate_limit::ChatLimiter;
use pigeonhole::telegram::TelegramClient;
use pigeonhole_meta::log::chat::{ChatApi, ChatDoc, Registers};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// Upper bound on every Bot API call of the log. Dropping the future aborts
/// the request (and the client's own retries), so a deposed writer's batch
/// lands within this long or not at all: the claim's straggler window must
/// be longer.
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);

pub struct TelegramChat {
    tg: TelegramClient,
    chat_id: String,
    limiter: Arc<ChatLimiter>,
}

impl TelegramChat {
    pub fn new(tg: TelegramClient, chat_id: String, limiter: Arc<ChatLimiter>) -> Self {
        Self { tg, chat_id, limiter }
    }
}

async fn within<T>(what: &str, call: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    tokio::time::timeout(CALL_DEADLINE, call)
        .await
        .map_err(|_| anyhow!("{what}: no answer within {CALL_DEADLINE:?}"))?
}

#[async_trait]
impl ChatApi for TelegramChat {
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<ChatDoc> {
        let (file_id, message_id) = within(
            "send log message",
            self.tg.send_document(&self.chat_id, data.into(), name, "", Some(self.limiter.as_ref())),
        )
        .await?;
        Ok(ChatDoc { message_id, download_ref: file_id })
    }

    async fn pin(&self, message_id: i64) -> anyhow::Result<()> {
        within(
            "pin log message",
            self.tg.pin_chat_message(&self.chat_id, message_id, Some(self.limiter.as_ref())),
        )
        .await
    }

    async fn registers(&self) -> anyhow::Result<Registers> {
        let chat = within("getChat", self.tg.get_chat(&self.chat_id)).await?;
        let (pinned, pinned_other) = match chat.pinned_message {
            Some(m) => match m.document {
                Some(d) => (Some(ChatDoc { message_id: m.message_id, download_ref: d.file_id }), false),
                None => (None, true),
            },
            None => (None, false),
        };
        Ok(Registers { description: chat.description, pinned, pinned_other })
    }

    async fn set_description(&self, text: &str) -> anyhow::Result<bool> {
        within("setChatDescription", self.tg.set_chat_description(&self.chat_id, text)).await
    }

    async fn download(&self, download_ref: &str) -> anyhow::Result<Vec<u8>> {
        let data = within(
            "download log message",
            self.tg.download_file(download_ref, Some(self.limiter.as_ref())),
        )
        .await
        .context("download log message")?;
        Ok(data.to_vec())
    }

    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()> {
        // Collection after a snapshot: no deadline, it is best effort anyway.
        self.tg
            .delete_messages(&self.chat_id, message_ids, Some(self.limiter.as_ref()))
            .await
    }
}
