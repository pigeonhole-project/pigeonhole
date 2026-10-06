use crate::client::Message as DiscordMessage;
use crate::DiscordClient;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use s3gram_blob::{
    bytes_stream, collect_stream, slice_range, BlobBackend, BlobStore, BootstrapPointer,
    BoxByteStream, ChatLimiter, DeleteOutcome, PinnedContent,
};
use s3gram_core::{
    BackendId, BackendLimits, ByteRange, Locator, PutHint, RangeSupport,
};
use std::sync::Arc;

/// Production adapter: blobs live as message attachments in one Discord channel.
pub struct DiscordBlobStore {
    id: BackendId,
    limits: BackendLimits,
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
        Self {
            id: BackendId::discord(&channel_id),
            limits: BackendLimits::discord(max_blob_size),
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

    fn locator_from_store_file_id(&self, file_id: &str) -> Result<Locator> {
        let (message_id, attachment_id) = Locator::parse_discord_store_file_id(file_id)
            .ok_or_else(|| anyhow::anyhow!("invalid discord store file_id {file_id:?}"))?;
        Ok(Locator::discord(
            &self.channel_id,
            message_id,
            attachment_id,
            "",
        ))
    }

    fn resolve_discord_loc<'a>(&self, loc: &'a Locator) -> Result<(&'a str, i64, &'a str)> {
        match loc {
            Locator::Discord {
                channel_id,
                message_id,
                attachment_id,
                ..
            } => Ok((
                channel_id.as_str(),
                *message_id,
                attachment_id.as_str(),
            )),
            _ => bail!("discord get/delete requires Discord locator"),
        }
    }
}

#[async_trait]
impl BlobBackend for DiscordBlobStore {
    fn id(&self) -> &BackendId {
        &self.id
    }

    fn limits(&self) -> &BackendLimits {
        &self.limits
    }

    async fn put(&self, data: Bytes, hint: PutHint) -> Result<Locator> {
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
        let (message_id, attachment_id, url) = self
            .dc
            .send_attachment(
                &self.channel_id,
                data,
                &hint.filename,
                &hint.caption,
                Some(self.limiter.as_ref()),
            )
            .await?;
        Ok(Locator::discord(
            &self.channel_id,
            message_id,
            attachment_id,
            url,
        ))
    }

    async fn get(&self, loc: &Locator, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let (channel_id, message_id, attachment_id) = self.resolve_discord_loc(loc)?;

        if let Some(ref r) = range {
            if self.limits.supports_range == RangeSupport::BestEffort {
                match self
                    .dc
                    .download_bytes_range(
                        channel_id,
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
                channel_id,
                message_id,
                attachment_id,
                Some(self.limiter.as_ref()),
            )
            .await?;
        let sliced = slice_range(data, range)?;
        Ok(bytes_stream(sliced))
    }

    async fn delete(&self, loc: &Locator) -> Result<DeleteOutcome> {
        let (_, message_id, attachment_id) = self.resolve_discord_loc(loc)?;
        let out = self
            .dc
            .delete_message(
                &self.channel_id,
                message_id,
                Some(self.limiter.as_ref()),
            )
            .await?;
        if matches!(out, DeleteOutcome::Deleted | DeleteOutcome::Gone) {
            self.dc.forget_attachment(attachment_id).await;
        }
        Ok(out)
    }
}

#[async_trait]
impl BlobStore for DiscordBlobStore {
    async fn put(
        &self,
        data: Bytes,
        filename: &str,
        caption: &str,
    ) -> Result<(String, i64)> {
        let loc = BlobBackend::put(self, data, PutHint::new(filename, caption)).await?;
        let message_id = loc
            .message_id()
            .ok_or_else(|| anyhow::anyhow!("locator missing message_id"))?;
        let attachment_id = loc
            .file_id()
            .ok_or_else(|| anyhow::anyhow!("locator missing attachment_id"))?;
        Ok((
            Locator::discord_store_file_id(message_id, attachment_id),
            message_id,
        ))
    }

    async fn get(&self, file_id: &str) -> Result<Bytes> {
        let loc = self.locator_from_store_file_id(file_id)?;
        collect_stream(BlobBackend::get(self, &loc, None).await?).await
    }

    async fn delete_message(&self, message_id: i64) -> Result<DeleteOutcome> {
        self.dc
            .delete_message(
                &self.channel_id,
                message_id,
                Some(self.limiter.as_ref()),
            )
            .await
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
