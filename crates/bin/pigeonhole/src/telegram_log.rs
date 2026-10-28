//! Binary-only: the Telegram side of the chat LogStore (`[meta] log` naming a telegram `[[store]]`).

use async_trait::async_trait;
use pigeonhole_blob::{retrying, ChatLimiter, Retry};
use pigeonhole::telegram::TelegramClient;
use pigeonhole_meta::log::chat::{ChatApi, ChatDoc, Registers};
use std::sync::Arc;
use std::time::Duration;

/// Upper bound on sending one log message, retries included. Dropping the
/// future aborts the request, so a deposed writer's batch lands within this
/// long or not at all: the claim's straggler window must be longer.
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// How long the other calls (pin, getChat, download, delete) keep retrying
/// rate limits and network errors before the log gives up (and the tablet
/// stops). A late pin is harmless: the head is the newest pinned message by
/// sending date, so an older message pinned late does not become it.
pub const RETRY_BUDGET: Duration = Duration::from_secs(300);

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

#[async_trait]
impl ChatApi for TelegramChat {
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<ChatDoc> {
        let data = bytes::Bytes::from(data);
        let (file_id, message_id) = retrying("send log message", Retry::RateLimits, CALL_DEADLINE, || {
            self.tg.send_document(&self.chat_id, data.clone(), name, "", Some(self.limiter.as_ref()))
        })
        .await?;
        Ok(ChatDoc { message_id, download_ref: file_id })
    }

    async fn pin(&self, message_id: i64) -> anyhow::Result<()> {
        retrying("pin log message", Retry::Any, RETRY_BUDGET, || {
            self.tg.pin_chat_message(&self.chat_id, message_id, Some(self.limiter.as_ref()))
        })
        .await
    }

    async fn registers(&self) -> anyhow::Result<Registers> {
        let chat = retrying("getChat", Retry::Any, RETRY_BUDGET, || self.tg.get_chat(&self.chat_id, Some(self.limiter.as_ref()))).await?;
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
        // Not retried on other failures: a repeat of an applied write reads
        // "not modified", which a claim takes for a rival claimant.
        retrying("setChatDescription", Retry::RateLimits, RETRY_BUDGET, || {
            self.tg.set_chat_description(&self.chat_id, text, Some(self.limiter.as_ref()))
        })
        .await
    }

    async fn download(&self, download_ref: &str) -> anyhow::Result<Vec<u8>> {
        let data = retrying("download log message", Retry::Any, RETRY_BUDGET, || {
            self.tg.download_file(download_ref, Some(self.limiter.as_ref()))
        })
        .await?;
        Ok(data.to_vec())
    }

    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()> {
        // Collection after a snapshot: best effort anyway.
        retrying("delete log messages", Retry::Any, RETRY_BUDGET, || {
            self.tg.delete_messages(&self.chat_id, message_ids, Some(self.limiter.as_ref()))
        })
        .await
    }
}
