use anyhow::Result;
use async_trait::async_trait;

/// Content of a chat/channel bootstrap pin (legacy [`BootstrapPointer`] path).
///
/// Prefer [`crate::TypedBootstrapPointer`] (`Bytes` payloads) for new code.
#[derive(Debug, Clone)]
pub enum PinnedContent {
    Text { message_id: i64, text: String },
    /// `document_ref` is the opaque backend document handle (storage-specific).
    Document { message_id: i64, document_ref: String },
}

/// Backend-agnostic bootstrap pin (Telegram pin / Discord pin).
///
/// Engine snapshot push/restore talks only through this trait so it never
/// depends on a concrete chat backend.
#[async_trait]
pub trait BootstrapPointer: Send + Sync {
    /// Stable scope id for pending-delete queues (instance scope).
    fn scope_id(&self) -> &str;

    async fn get_pinned(&self) -> Result<Option<PinnedContent>>;

    async fn send_text(&self, text: &str) -> Result<i64>;

    async fn pin_message(&self, message_id: i64) -> Result<()>;

    async fn unpin_message(&self, message_id: i64) -> Result<()>;
}
