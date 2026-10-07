use anyhow::Result;
use async_trait::async_trait;
use pigeonhole_types::PinnedContent;

/// Backend-agnostic bootstrap pin (Telegram pin / Discord pin).
///
/// Engine snapshot push/restore talks only through this trait so it never
/// depends on a concrete chat backend.
#[async_trait]
pub trait BootstrapPointer: Send + Sync {
    /// Stable scope id for pending-delete queues (`chat_id` / `channel_id`).
    fn scope_id(&self) -> &str;

    async fn get_pinned(&self) -> Result<Option<PinnedContent>>;

    async fn send_text(&self, text: &str) -> Result<i64>;

    async fn pin_message(&self, message_id: i64) -> Result<()>;

    async fn unpin_message(&self, message_id: i64) -> Result<()>;
}
