use crate::TelegramClient;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, slice_range, store_delete_message, store_get, store_put, BlobBackend, BlobStore,
    BootstrapPointer, BoxByteStream, ChatLimiter, CostHint, DeleteOutcome, InstanceInfo,
    InstanceKind, InstanceRole, LimitBudget, OpKind, PinnedContent, Sweepable, TypedBlobBackend,
    TypedBootstrapPointer,
};
use pigeonhole_types::{
    BackendId, BackendLimits, ByteRange, Locator, PutHint, RangeSupport,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Telegram text message size limit (Bot API).
const TG_TEXT_MAX: usize = 4096;

/// Typed identity for the Telegram backend (stage B.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramId {
    pub file_id: String,
    pub message_id: i64,
}

/// Production adapter: all blobs go to a single Telegram chat.
pub struct TelegramBlobStore {
    id: BackendId,
    limits: BackendLimits,
    instance: InstanceInfo,
    tg: TelegramClient,
    chat_id: String,
    limiter: Arc<ChatLimiter>,
}

/// Alias matching Stage 3 naming.
pub type TelegramBackend = TelegramBlobStore;

impl TelegramBlobStore {
    pub fn new(tg: TelegramClient, chat_id: String, limiter: Arc<ChatLimiter>) -> Self {
        Self::with_instance_id(tg, chat_id, limiter, "telegram")
    }

    pub fn with_instance_id(
        tg: TelegramClient,
        chat_id: String,
        limiter: Arc<ChatLimiter>,
        instance_id: impl Into<String>,
    ) -> Self {
        let fingerprint = tg
            .fingerprint(&chat_id)
            .unwrap_or_else(|_| format!("tg:unknown:{chat_id}"));
        let location = format!("tg:chat:{chat_id}");
        Self {
            id: BackendId::telegram(&chat_id),
            limits: BackendLimits::telegram(),
            instance: InstanceInfo {
                id: instance_id.into(),
                kind: InstanceKind::Telegram,
                fingerprint,
                location,
                role: InstanceRole::ReadWrite,
            },
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
impl TypedBlobBackend for TelegramBlobStore {
    type Id = TelegramId;
    type Key = i64;

    fn instance(&self) -> &InstanceInfo {
        &self.instance
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    fn key(id: &Self::Id) -> Self::Key {
        id.message_id
    }

    fn cost(&self, op: OpKind, id: Option<&Self::Id>) -> CostHint {
        let wait_secs = match op {
            OpKind::Put => self.limiter.peek_wait(LimitBudget::Send).as_secs_f64(),
            OpKind::Get => {
                if id.is_some_and(|i| self.tg.has_cached_file_path(&i.file_id)) {
                    0.0
                } else {
                    self.limiter.peek_wait(LimitBudget::GetFile).as_secs_f64()
                }
            }
            OpKind::Delete => self.limiter.peek_wait(LimitBudget::Delete).as_secs_f64(),
            OpKind::List => 0.0,
        };
        CostHint {
            wait_secs,
            latency_ewma_secs: 0.0,
            inflight: 0,
        }
    }

    async fn put(&self, data: Bytes) -> Result<Self::Id> {
        let loc = BlobBackend::put(self, data, PutHint::default()).await?;
        match loc {
            Locator::Telegram {
                file_id,
                message_id,
            } => Ok(TelegramId {
                file_id,
                message_id,
            }),
            other => bail!("telegram put returned unexpected locator {other:?}"),
        }
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let loc = Locator::telegram(&id.file_id, id.message_id);
        BlobBackend::get(self, &loc, range).await
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        // Batched via Bot API `deleteMessages` (≤100 ids/call) in the client.
        self.tg
            .delete_messages(&self.chat_id, keys, Some(self.limiter.as_ref()))
            .await
    }
}

#[async_trait]
impl Sweepable for TelegramBlobStore {
    async fn candidates(
        &self,
        after: Option<Self::Key>,
        upto: Self::Key,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Telegram message_ids are dense integers; synthesize the sweep range.
        let start = match after {
            Some(a) => a.saturating_add(1),
            None => 1,
        };
        if start > upto {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(limit.min(1024));
        let mut k = start;
        while k <= upto && out.len() < limit {
            out.push(k);
            if k == i64::MAX {
                break;
            }
            k += 1;
        }
        Ok(out)
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

#[async_trait]
impl TypedBootstrapPointer for TelegramBlobStore {
    async fn read(&self) -> Result<Option<Bytes>> {
        match self.tg.get_pinned_content(&self.chat_id).await? {
            None => Ok(None),
            Some(PinnedContent::Text { text, .. }) => Ok(Some(Bytes::from(text))),
            Some(PinnedContent::Document { file_id, .. }) => {
                let data = self
                    .tg
                    .download_file(&file_id, Some(self.limiter.as_ref()))
                    .await?;
                Ok(Some(data))
            }
        }
    }

    async fn swap(&self, new: Bytes) -> Result<()> {
        let old_id = match self.tg.get_pinned_content(&self.chat_id).await? {
            Some(PinnedContent::Text { message_id, .. })
            | Some(PinnedContent::Document { message_id, .. }) => Some(message_id),
            None => None,
        };

        let new_id = if new.len() <= TG_TEXT_MAX {
            let text = std::str::from_utf8(&new).map_err(|e| {
                anyhow::anyhow!(
                    "bootstrap pin must be valid UTF-8 when ≤ {TG_TEXT_MAX} bytes: {e}"
                )
            })?;
            self.tg
                .send_message(&self.chat_id, text, Some(self.limiter.as_ref()))
                .await?
        } else {
            let (_fid, mid) = self
                .tg
                .send_document(
                    &self.chat_id,
                    new,
                    "superblock.bin",
                    "",
                    Some(self.limiter.as_ref()),
                )
                .await?;
            mid
        };

        self.tg
            .pin_chat_message(&self.chat_id, new_id, Some(self.limiter.as_ref()))
            .await?;

        if let Some(old) = old_id {
            if old != new_id {
                let _ = self
                    .tg
                    .unpin_chat_message(&self.chat_id, old, Some(self.limiter.as_ref()))
                    .await;
            }
        }
        Ok(())
    }
}
