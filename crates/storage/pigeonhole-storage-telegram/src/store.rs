use crate::TelegramClient;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, slice_range, BoxByteStream, ChatLimiter, CostHint,
    InstanceInfo, InstanceKind, InstanceRole, LimitBudget, OpKind, 
    Sweepable, BlobBackend, 
};
use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Typed identity for the Telegram backend (stage B.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramId {
    pub file_id: String,
    pub message_id: i64,
}

/// Production adapter: all blobs go to a single Telegram chat.
pub struct TelegramBlobStore {
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
        Self::with_instance(
            tg,
            chat_id,
            limiter,
            InstanceInfo {
                id: instance_id.into(),
                kind: InstanceKind::Telegram,
                fingerprint,
                location,
                role: InstanceRole::ReadWrite,
            },
        )
    }

    /// Build with a pre-resolved [`InstanceInfo`] (from `[[store]]`).
    pub fn with_instance(
        tg: TelegramClient,
        chat_id: String,
        limiter: Arc<ChatLimiter>,
        instance: InstanceInfo,
    ) -> Self {
        Self {
            limits: BackendLimits::telegram(),
            instance,
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

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
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
                name,
                "",
                Some(self.limiter.as_ref()),
            )
            .await?;
        Ok(TelegramId {
            file_id,
            message_id,
        })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let file_id = id.file_id.as_str();
        if file_id.is_empty() {
            bail!("telegram get requires file_id");
        }

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



