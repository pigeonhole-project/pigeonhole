//! Generalized blob backend trait (Stage 3).

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use futures::Stream;
use pigeonhole_types::{
    BackendId, BackendLimits, ByteRange, DeleteOutcome, Locator, PutHint,
};
use std::pin::Pin;

/// Stream of blob bytes returned by [`BlobBackend::get`].
pub type BoxByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send>>;

/// Collect a [`BoxByteStream`] into a single [`Bytes`].
pub async fn collect_stream(mut stream: BoxByteStream) -> Result<Bytes> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(out))
}

/// One-shot stream wrapping already-buffered bytes (Memory / full Telegram get).
pub fn bytes_stream(data: Bytes) -> BoxByteStream {
    Box::pin(stream::once(async move { Ok(data) }))
}

/// Apply an optional byte range to a full buffer (local fall-back when backend
/// Range is unsupported or rejected).
pub fn slice_range(data: Bytes, range: Option<ByteRange>) -> Result<Bytes> {
    let Some(r) = range else {
        return Ok(data);
    };
    let start = r.start as usize;
    let end = r.end as usize;
    if start > end || end > data.len() {
        anyhow::bail!(
            "byte range {}..{} outside blob of {} bytes",
            r.start,
            r.end,
            data.len()
        );
    }
    Ok(data.slice(start..end))
}

/// Generalized blob backend. Replaces the Telegram-centric [`super::BlobStore`].
#[async_trait]
pub trait BlobBackend: Send + Sync {
    fn id(&self) -> &BackendId;
    fn limits(&self) -> &BackendLimits;

    async fn put(&self, data: Bytes, hint: PutHint) -> Result<Locator>;

    async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream>;

    async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome>;
}

/// Legacy Telegram-shaped API kept as a thin adapter over [`BlobBackend`].
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)>;

    async fn get(&self, file_id: &str) -> Result<Bytes>;

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome>;

    /// Drop cached entries for `file_id` (L2). Default: no-op.
    async fn invalidate_blob(&self, _file_id: &str) {}
}

/// Helper for explicit [`BlobStore`] adapters over a [`BlobBackend`].
pub async fn store_put(
    backend: &dyn BlobBackend,
    data: Bytes,
    filename: &str,
    caption: &str,
) -> Result<(String, i64)> {
    let loc = backend.put(data, PutHint::new(filename, caption)).await?;
    let message_id = loc
        .message_id()
        .ok_or_else(|| anyhow::anyhow!("locator missing message_id"))?;
    // Discord index keys are `{message_id}:{attachment_id}` (not bare attachment id).
    if let Locator::Discord {
        message_id,
        attachment_id,
        ..
    } = &loc
    {
        return Ok((
            Locator::discord_store_file_id(*message_id, attachment_id),
            *message_id,
        ));
    }
    let file_id = loc
        .file_id()
        .ok_or_else(|| anyhow::anyhow!("locator missing file_id"))?
        .to_string();
    Ok((file_id, message_id))
}

/// Build a [`Locator`] for legacy [`BlobStore::get`] / invalidate from `file_id`.
pub fn locator_for_store_file_id(backend: &dyn BlobBackend, file_id: &str) -> Result<Locator> {
    let id = backend.id().as_str();
    if id.starts_with("memory:") {
        return Ok(Locator::memory(file_id, 0));
    }
    if let Some(channel) = id.strip_prefix("discord:") {
        let (message_id, attachment_id) = Locator::parse_discord_store_file_id(file_id)
            .ok_or_else(|| anyhow::anyhow!("invalid discord store file_id {file_id:?}"))?;
        return Ok(Locator::discord(channel, message_id, attachment_id, ""));
    }
    Ok(Locator::telegram(file_id, 0))
}

pub async fn store_get(backend: &dyn BlobBackend, file_id: &str) -> Result<Bytes> {
    let loc = locator_for_store_file_id(backend, file_id)?;
    collect_stream(backend.get(&loc, None).await?).await
}

pub async fn store_delete_message(
    backend: &dyn BlobBackend,
    message_id: i64,
) -> Result<DeleteOutcome> {
    let id = backend.id().as_str();
    let loc = if id.starts_with("memory:") {
        Locator::memory(String::new(), message_id)
    } else if let Some(channel) = id.strip_prefix("discord:") {
        // Attachment id unused for message delete; keep locator kind correct for invalidate.
        Locator::discord(channel, message_id, "", "")
    } else {
        Locator::telegram(String::new(), message_id)
    };
    backend.delete(&loc).await
}
