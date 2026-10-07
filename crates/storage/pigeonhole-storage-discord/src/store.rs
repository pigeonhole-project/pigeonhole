use crate::client::{snowflake_bulk_deletable, Message as DiscordMessage, RateBudget};
use crate::DiscordClient;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    bytes_stream, slice_range, BootstrapPointer, BoxByteStream, ChatLimiter, CostHint,
    DeleteOutcome, InstanceInfo, InstanceKind, InstanceRole, OpKind, OrderedKey, PinnedContent,
    Sweepable, BlobBackend, TypedBootstrapPointer,
};
use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Typed identity for Discord attachments (stage B.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscordId {
    pub message_id: u64,
    pub attachment_id: String,
}

/// `dc:{app_id}:{channel_id}` — matches `pigeonhole_chunk_store::instances::discord_fingerprint`.
pub fn discord_fingerprint(app_id: &str, channel_id: &str) -> String {
    format!("dc:{app_id}:{channel_id}")
}

/// `dc:channel:{channel_id}` — matches `pigeonhole_chunk_store::instances::discord_location`.
pub fn discord_location(channel_id: &str) -> String {
    format!("dc:channel:{channel_id}")
}

const DC_TEXT_MAX: usize = 2000;

/// Production adapter: blobs live as message attachments in one Discord channel.
pub struct DiscordBlobStore {
    limits: BackendLimits,
    instance: InstanceInfo,
    dc: DiscordClient,
    channel_id: String,
    limiter: Arc<ChatLimiter>,
}

pub type DiscordBackend = DiscordBlobStore;

impl DiscordBlobStore {
    pub fn new(
        dc: DiscordClient,
        channel_id: String,
        limiter: Arc<ChatLimiter>,
        max_blob_size: Option<usize>,
    ) -> Self {
        let app_id = dc.app_id();
        Self::with_instance(
            dc,
            channel_id.clone(),
            limiter,
            max_blob_size,
            InstanceInfo {
                id: format!("discord-{channel_id}"),
                kind: InstanceKind::Discord,
                fingerprint: discord_fingerprint(&app_id, &channel_id),
                location: discord_location(&channel_id),
                role: InstanceRole::ReadWrite,
            },
        )
    }

    /// Build with a pre-resolved [`InstanceInfo`] (from `[[instances]]`).
    pub fn with_instance(
        dc: DiscordClient,
        channel_id: String,
        limiter: Arc<ChatLimiter>,
        max_blob_size: Option<usize>,
        instance: InstanceInfo,
    ) -> Self {
        Self {
            limits: BackendLimits::discord(max_blob_size),
            instance,
            dc,
            channel_id,
            limiter,
        }
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub fn client(&self) -> &DiscordClient {
        &self.dc
    }

    pub fn limiter(&self) -> &ChatLimiter {
        &self.limiter
    }

    fn unix_now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn delete_one_message(&self, message_id: u64) -> Result<()> {
        let out = self
            .dc
            .delete_message(
                &self.channel_id,
                message_id as i64,
                Some(self.limiter.as_ref()),
            )
            .await?;
        match out {
            DeleteOutcome::Deleted | DeleteOutcome::Gone => Ok(()),
            DeleteOutcome::Failed => bail!("discord delete failed for message {message_id}"),
        }
    }

    /// Bulk-delete young messages (2–100, < ~14 days); older / singles one-by-one.
    async fn delete_message_keys(&self, keys: &[u64]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let now_ms = Self::unix_now_ms();
        let mut young = Vec::new();
        let mut old = Vec::new();
        for &k in keys {
            if snowflake_bulk_deletable(k, now_ms) {
                young.push(k);
            } else {
                old.push(k);
            }
        }
        // Dedup while preserving order for stable requests.
        young.sort_unstable();
        young.dedup();
        old.sort_unstable();
        old.dedup();

        for chunk in young.chunks(100) {
            match chunk {
                [] => {}
                [one] => self.delete_one_message(*one).await?,
                many => {
                    if let Err(e) = self
                        .dc
                        .bulk_delete_messages(
                            &self.channel_id,
                            many,
                            Some(self.limiter.as_ref()),
                        )
                        .await
                    {
                        tracing::debug!(
                            error = %e,
                            n = many.len(),
                            "bulk-delete failed; falling back to one-by-one"
                        );
                        for &id in many {
                            self.delete_one_message(id).await?;
                        }
                    }
                }
            }
        }
        for id in old {
            self.delete_one_message(id).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BlobBackend for DiscordBlobStore {
    type Id = DiscordId;
    type Key = u64;

    fn instance(&self) -> &InstanceInfo {
        &self.instance
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    fn key(id: &Self::Id) -> Self::Key {
        id.message_id
    }

    fn cost(&self, op: OpKind, _id: Option<&Self::Id>) -> CostHint {
        let budget = match op {
            OpKind::Put => RateBudget::Send,
            OpKind::Get | OpKind::List => RateBudget::Read,
            OpKind::Delete => RateBudget::Delete,
        };
        CostHint {
            wait_secs: self.dc.route_wait_secs(budget),
            latency_ewma_secs: 0.0,
            inflight: 0,
        }
    }

    async fn put(&self, data: Bytes) -> Result<Self::Id> {
        if data.is_empty() {
            bail!("refusing empty blob upload");
        }
        if data.len() > self.limits.max_blob_size {
            bail!(
                "blob size {} exceeds backend max {}",
                data.len(),
                self.limits.max_blob_size
            );
        }
        let (message_id, attachment_id, _url) = self
            .dc
            .send_attachment(
                &self.channel_id,
                data,
                "blob.bin",
                "",
                Some(self.limiter.as_ref()),
            )
            .await?;
        Ok(DiscordId {
            message_id: message_id as u64,
            attachment_id,
        })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let message_id = id.message_id as i64;
        let attachment_id = id.attachment_id.as_str();

        if let Some(ref r) = range {
            if self.limits.supports_range == RangeSupport::BestEffort {
                match self
                    .dc
                    .download_bytes_range(
                        &self.channel_id,
                        message_id,
                        attachment_id,
                        r.start,
                        r.end,
                        Some(self.limiter.as_ref()),
                    )
                    .await
                {
                    Ok(bytes) => return Ok(bytes_stream(bytes)),
                    Err(e) => {
                        tracing::debug!(
                            error = %e,
                            start = r.start,
                            end = r.end,
                            "discord ranged get failed; falling back to full download"
                        );
                    }
                }
            }
        }

        let data = self
            .dc
            .download_bytes(
                &self.channel_id,
                message_id,
                attachment_id,
                Some(self.limiter.as_ref()),
            )
            .await?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        self.delete_message_keys(keys).await
    }
}

#[async_trait]
impl Sweepable for DiscordBlobStore {
    async fn candidates(
        &self,
        after: Option<Self::Key>,
        upto: Self::Key,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut cursor = after.unwrap_or(0);
        // Discord returns newest→oldest; we sort ascending and page with `after`.
        while out.len() < limit {
            let batch_limit = ((limit - out.len()).min(100)) as u8;
            let msgs = self
                .dc
                .list_messages(
                    &self.channel_id,
                    None,
                    Some(cursor as i64),
                    Some(batch_limit.max(1)),
                    Some(self.limiter.as_ref()),
                )
                .await?;
            if msgs.is_empty() {
                break;
            }
            let mut ids: Vec<u64> = msgs
                .iter()
                .map(|m| m.message_id())
                .filter(|&k| k <= upto && after.map(|a| k > a).unwrap_or(true))
                .collect();
            if ids.is_empty() {
                // Page may contain only ids above upto (newest-first); stop.
                let min_in_page = msgs.iter().map(|m| m.message_id()).min().unwrap_or(0);
                if min_in_page > upto {
                    break;
                }
                // Advance past this page anyway.
                cursor = msgs.iter().map(|m| m.message_id()).max().unwrap_or(cursor);
                if msgs.len() < batch_limit as usize {
                    break;
                }
                continue;
            }
            ids.sort_unstable();
            ids.dedup();
            let page_max = *ids.last().unwrap_or(&cursor);
            for k in ids {
                if out.len() >= limit {
                    break;
                }
                if after.map(|a| k <= a).unwrap_or(false) {
                    continue;
                }
                out.push(k);
            }
            if page_max <= cursor {
                break;
            }
            cursor = page_max;
            if msgs.len() < batch_limit as usize {
                break;
            }
        }
        out.sort_unstable();
        out.dedup();
        out.truncate(limit);
        Ok(out)
    }
}


#[async_trait]
impl BootstrapPointer for DiscordBlobStore {
    fn scope_id(&self) -> &str {
        &self.channel_id
    }

    async fn get_pinned(&self) -> Result<Option<PinnedContent>> {
        self.dc
            .get_pinned_content(&self.channel_id, Some(self.limiter.as_ref()))
            .await
    }

    async fn send_text(&self, text: &str) -> Result<i64> {
        self.dc
            .send_text(&self.channel_id, text, Some(self.limiter.as_ref()))
            .await
    }

    async fn pin_message(&self, message_id: i64) -> Result<()> {
        self.dc
            .pin_message(&self.channel_id, message_id, Some(self.limiter.as_ref()))
            .await
    }

    async fn unpin_message(&self, message_id: i64) -> Result<()> {
        self.dc
            .unpin_message(&self.channel_id, message_id, Some(self.limiter.as_ref()))
            .await
    }
}

#[async_trait]
impl TypedBootstrapPointer for DiscordBlobStore {
    async fn read(&self) -> Result<Option<Bytes>> {
        match BootstrapPointer::get_pinned(self).await? {
            None => Ok(None),
            Some(PinnedContent::Text { text, .. }) => Ok(Some(Bytes::from(text))),
            Some(PinnedContent::Document {
                message_id,
                file_id,
            }) => {
                let data = self
                    .dc
                    .download_bytes(
                        &self.channel_id,
                        message_id,
                        &file_id,
                        Some(self.limiter.as_ref()),
                    )
                    .await?;
                Ok(Some(data))
            }
        }
    }

    async fn pin_key(&self) -> Result<Option<Vec<u8>>> {
        match BootstrapPointer::get_pinned(self).await? {
            Some(PinnedContent::Text { message_id, .. })
            | Some(PinnedContent::Document { message_id, .. }) => {
                Ok(Some((message_id as u64).to_bytes()))
            }
            None => Ok(None),
        }
    }

    async fn swap(&self, new: Bytes) -> Result<()> {
        let old = BootstrapPointer::get_pinned(self).await?;
        let new_mid = if new.len() <= DC_TEXT_MAX {
            let text = std::str::from_utf8(&new)
                .map_err(|_| anyhow::anyhow!("bootstrap pin payload is not valid UTF-8"))?;
            BootstrapPointer::send_text(self, text).await?
        } else {
            let (message_id, _attachment_id, _url) = self
                .dc
                .send_attachment(
                    &self.channel_id,
                    new,
                    "superblock.bin",
                    "pigeonhole-superblock",
                    Some(self.limiter.as_ref()),
                )
                .await?;
            message_id
        };
        BootstrapPointer::pin_message(self, new_mid).await?;
        if let Some(prev) = old {
            let old_id = match prev {
                PinnedContent::Text { message_id, .. }
                | PinnedContent::Document { message_id, .. } => message_id,
            };
            if old_id != new_mid {
                let _ = BootstrapPointer::unpin_message(self, old_id).await;
            }
        }
        Ok(())
    }
}

impl DiscordBlobStore {
    /// List channel messages (Discord supports cursor pagination via `before` / `after`).
    pub async fn list_messages(
        &self,
        before: Option<i64>,
        after: Option<i64>,
        limit: Option<u8>,
    ) -> Result<Vec<DiscordMessage>> {
        self.dc
            .list_messages(
                &self.channel_id,
                before,
                after,
                limit,
                Some(self.limiter.as_ref()),
            )
            .await
    }
}
