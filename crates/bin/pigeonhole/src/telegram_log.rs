//! Binary-only: the Telegram side of the chat LogStore (`[meta] log` naming a telegram `[[store]]`).

use anyhow::anyhow;
use pigeonhole::telegram::RateLimited;
use async_trait::async_trait;
use pigeonhole::rate_limit::ChatLimiter;
use pigeonhole::telegram::TelegramClient;
use pigeonhole_meta::log::chat::{ChatApi, ChatDoc, Registers};
use std::future::Future;
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

/// What a failed call may do again.
#[derive(Clone, Copy)]
enum Retry {
    /// Only a rate limit (the call was not applied), within `budget`.
    RateLimits,
    /// Rate limits and any other failure (idempotent calls), within `budget`.
    Any,
}

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

/// Run `call` until it succeeds, retrying as `retry` allows within `budget`
/// (each attempt is bounded by what is left of it).
async fn retrying<T, Fut>(
    what: &str,
    retry: Retry,
    budget: Duration,
    mut call: impl FnMut() -> Fut,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = Duration::from_secs(1);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let err = match tokio::time::timeout(left, call()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) => e,
            Err(_) => return Err(anyhow!("{what}: no answer within {budget:?}")),
        };
        let wait = match (err.downcast_ref::<RateLimited>(), retry) {
            (Some(limited), _) => limited.retry_after + Duration::from_secs(1),
            (None, Retry::Any) => {
                let wait = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                wait
            }
            (None, Retry::RateLimits) => return Err(err.context(what.to_string())),
        };
        if tokio::time::Instant::now() + wait >= deadline {
            return Err(err.context(format!("{what}: still failing after {budget:?}")));
        }
        tracing::warn!(what, wait = ?wait, error = %format!("{err:#}"), "telegram log call failed; retrying");
        tokio::time::sleep(wait).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn limited() -> anyhow::Error {
        RateLimited { method: "pinChatMessage", retry_after: Duration::from_secs(20) }.into()
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limits_are_waited_out_and_other_errors_follow_the_policy() {
        let tries = AtomicU32::new(0);
        let call = || async {
            match tries.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(limited()),
                _ => Ok(7),
            }
        };
        assert_eq!(retrying("pin", Retry::RateLimits, RETRY_BUDGET, call).await.unwrap(), 7);
        assert_eq!(tries.load(Ordering::SeqCst), 3);

        // A send may not outlive its deadline: two 20 s waits do not fit in 30 s.
        let tries = AtomicU32::new(0);
        let call = || async {
            tries.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(limited())
        };
        let err = retrying("send", Retry::RateLimits, CALL_DEADLINE, call).await.unwrap_err();
        assert!(format!("{err:#}").contains("still failing"), "{err:#}");
        assert_eq!(tries.load(Ordering::SeqCst), 2);

        // A network error: retried only where any retry is allowed.
        let tries = AtomicU32::new(0);
        let call = || async {
            if tries.fetch_add(1, Ordering::SeqCst) == 0 { Err(anyhow!("connection reset")) } else { Ok(()) }
        };
        retrying("getChat", Retry::Any, RETRY_BUDGET, call).await.unwrap();
        let call = || async { Err::<(), _>(anyhow!("connection reset")) };
        assert!(retrying("setChatDescription", Retry::RateLimits, RETRY_BUDGET, call).await.is_err());
    }
}
