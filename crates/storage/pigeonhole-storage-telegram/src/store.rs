use crate::TelegramClient;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, slice_range, store_delete_message, store_get, store_put, BlobBackend, BlobStore,
    BootstrapPointer, BoxByteStream, ChatLimiter, DeleteOutcome, PinnedContent,
};
use pigeonhole_types::{
    BackendId, BackendLimits, ByteRange, Locator, PutHint, RangeSupport,
};
use std::sync::Arc;

/// Production adapter: all blobs go to a single Telegram chat.
pub struct TelegramBlobStore {
    id: BackendId,
    limits: BackendLimits,
    tg: TelegramClient,
    chat_id: String,
    limiter: Arc<ChatLimiter>,
}

/// Alias matching Stage 3 naming.
pub type TelegramBackend = TelegramBlobStore;

impl TelegramBlobStore {
    pub fn new(tg: TelegramClient, chat_id: String, limiter: Arc<ChatLimiter>) -> Self {
        Self {
            id: BackendId::telegram(&chat_id),
            limits: BackendLimits::telegram(),
            tg,
            chat_id,
            limiter,
        }
    }

    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn client(&self) -> &TelegramClient {
        &self.tg
    }

    pub fn limiter(&self) -> &ChatLimiter {
        &self.limiter
    }
}

#[async_trait]
impl BlobBackend for TelegramBlobStore {
    fn id(&self) -> &BackendId {
        &self.id
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    async fn put(&self, data: Bytes, hint: PutHint) -> Result<Locator> {
        if data.is_empty() {
            bail!("refusing empty blob upload (Telegram rejects empty documents)");
        }
        if data.len() > self.limits.max_blob_size {
            bail!(
                "blob size {} exceeds backend max {}",
                data.len(),
                self.limits.max_blob_size
            );
        }
        let _upload = self.limiter.acquire_upload().await;
        let (file_id, message_id) = self
            .tg
            .send_document(
                &self.chat_id,
                data,
                &hint.filename,
                &hint.caption,
                Some(self.limiter.as_ref()),
            )
            .await?;
        Ok(Locator::telegram(file_id, message_id))
    }

    async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let file_id = loc
            .file_id()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("telegram get requires file_id"))?;

        if let Some(ref r) = range {
            if self.limits.supports_range == RangeSupport::BestEffort {
                match self
                    .tg
                    .download_file_range(file_id, r.start, r.end, Some(self.limiter.as_ref()))
                    .await
                {
                    Ok(bytes) => return Ok(bytes_stream(bytes)),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            start = r.start,
                            end = r.end,
                            "telegram ranged get failed; falling back to full download"
                        );
                    }
                }
            }
        }

        let data = self
            .tg
            .download_file(file_id, Some(self.limiter.as_ref()))
            .await?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome> {
        let message_id = loc
            .message_id()
            .ok_or_else(|| anyhow::anyhow!("telegram delete requires message_id"))?;
        self.tg
            .delete_message(&self.chat_id, message_id, Some(self.limiter.as_ref()))
            .await
    }
}

#[async_trait]
impl BlobStore for TelegramBlobStore {
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        store_put(self, data, filename, caption).await
    }

    async fn get(&self, file_id: &str) -> Result<Bytes> {
        store_get(self, file_id).await
    }

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome> {
        store_delete_message(self, message_id).await
    }
}

#[async_trait]
impl BootstrapPointer for TelegramBlobStore {
    fn scope_id(&self) -> &str {
        &self.chat_id
    }

    async fn get_pinned(&self) -> Result<Option<PinnedContent>> {
        self.tg.get_pinned_content(&self.chat_id).await
    }

    async fn send_text(&self, text: &str) -> Result<i64> {
        self.tg
            .send_message(&self.chat_id, text, Some(self.limiter.as_ref()))
            .await
    }

    async fn pin_message(&self, message_id: i64) -> Result<()> {
        self.tg
            .pin_chat_message(&self.chat_id, message_id, Some(self.limiter.as_ref()))
            .await
    }

    async fn unpin_message(&self, message_id: i64) -> Result<()> {
        self.tg
            .unpin_chat_message(&self.chat_id, message_id, Some(self.limiter.as_ref()))
            .await
    }
}

