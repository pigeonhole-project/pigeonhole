//! [`BlobBackend`] over one channel.

use crate::channel::{windows, ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, IDS_PER_CALL, WINDOW};
use crate::limits::{DELETE, READ, SEND};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use pigeonhole_limits::chat::Classes;
use pigeonhole_limits::ChatLimiter;
use pigeonhole_storage_api::located::{as_text, marked_owner, owned_by, Located, Locations, OWNER_MARKER};
use pigeonhole_storage_api::{
    bytes_stream, check_blob, slice_range, BlobBackend, BoxByteStream, CostHint, Failure, InstanceInfo, OpKind, Owner,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// A blob is a message; everything else (file reference, DC, size) is
/// fetched with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramId {
    pub message_id: i64,
}

/// Locations of recently seen documents (saves a `getMessages` per read).
const LOCATION_CACHE: usize = 4096;
/// Windows of one blob fetched at a time.
const WINDOWS_IN_FLIGHT: usize = 4;

/// What message `msg` holds, if it is a blob: a document's file, or a text
/// message's bytes ([`pigeonhole_storage_api::text`]).
pub(crate) fn located(msg: &ChannelMessage) -> Option<Located<FileLocation>> {
    match msg {
        ChannelMessage::Document { size, file, .. } => Some(Located::File { size: *size, file: file.clone() }),
        ChannelMessage::Text { text } => Located::of_text(text),
        _ => None,
    }
}

/// The name a blob message was put under (document or text).
pub(crate) fn blob_name(msg: &ChannelMessage) -> Option<&str> {
    match msg {
        ChannelMessage::Document { name, .. } => Some(name),
        ChannelMessage::Text { text } => pigeonhole_storage_api::text::name_of(text),
        _ => None,
    }
}

const CLASSES: Classes = Classes { send: SEND, read: READ, delete: DELETE };

pub struct TelegramBlobStore<C> {
    limits: BackendLimits,
    instance: InstanceInfo,
    pub(crate) reader: Reader<C>,
    pub(crate) limiter: Arc<ChatLimiter>,
    pub(crate) chain: crate::chain::ChainOptions,
    /// The chain's anchor, once known ([`crate::chain`]).
    pub(crate) anchor: tokio::sync::Mutex<crate::chain::Anchor>,
    /// Blobs that fit a text message go as text ([`pigeonhole_storage_api::text`]).
    inline: bool,
}

/// The read side, cloned into a download stream.
pub(crate) struct Reader<C> {
    pub(crate) channel: Arc<C>,
    limiter: Arc<ChatLimiter>,
    pub(crate) locations: Arc<Locations<i64, FileLocation>>,
}

impl<C> Clone for Reader<C> {
    fn clone(&self) -> Self {
        Self { channel: self.channel.clone(), limiter: self.limiter.clone(), locations: self.locations.clone() }
    }
}

/// The send's `random_id` for blob `name` in `channel`: a repeated put of one
/// blob (a retry) posts it once.
pub fn blob_random_id(channel_id: i64, name: &str) -> i64 {
    let digest = Sha256::digest(format!("{channel_id}/{name}").as_bytes());
    i64::from_le_bytes(digest[..8].try_into().unwrap())
}

impl<C: ChannelApi> TelegramBlobStore<C> {
    /// The owner the pinned message marks the channel for, if it is a
    /// marker (a bot sees only the latest pin).
    async fn pinned_owner(&self) -> Result<Option<Owner>> {
        let Some(id) = self.reader.channel.pinned().await? else { return Ok(None) };
        let msgs = self.reader.read(&[id]).await?;
        Ok(marked_owner(msgs.iter().map(blob_name)))
    }

    pub fn new(channel: Arc<C>, limiter: Arc<ChatLimiter>, instance: InstanceInfo) -> Self {
        Self {
            limits: crate::BLOB_LIMITS,
            instance,
            reader: Reader { channel, limiter: limiter.clone(), locations: Arc::new(Locations::new(LOCATION_CACHE)) },
            limiter,
            chain: crate::chain::ChainOptions::default(),
            anchor: tokio::sync::Mutex::default(),
            inline: true,
        }
    }

    /// Whether small blobs go as text messages (default) or always as documents.
    pub fn with_inline(mut self, inline: bool) -> Self {
        self.inline = inline;
        self
    }

    /// Post blob `name` as a text message if it fits, else as a document;
    /// its message id. The location is remembered.
    /// No upload permit is held here: the client takes one around the
    /// upload of a document's parts, not around the send (whose flood waits
    /// may last minutes).
    pub(crate) async fn post_blob(&self, name: &str, data: Bytes, random_id: i64) -> Result<i64> {
        let channel = &self.reader.channel;
        let store = &self.instance.id;
        let bytes = data.len();
        if as_text(self.inline, name, bytes, crate::channel::TEXT_MAX_CHARS) {
            let sent = channel.send_text(&pigeonhole_storage_api::text::encode(name, &data), random_id).await?;
            self.reader.locations.remember(sent.message_id, Located::Inline(data));
            tracing::debug!(%store, message_id = sent.message_id, name, bytes, kind = "text", "posted");
            return Ok(sent.message_id);
        }
        let size = bytes as u64;
        let sent = channel.send_document(name, data, random_id).await?;
        if let Some(file) = sent.file {
            self.reader.locations.remember(sent.message_id, Located::File { size, file });
        }
        tracing::debug!(%store, message_id = sent.message_id, name, bytes, kind = "document", "posted");
        Ok(sent.message_id)
    }

    /// How a chain treats ids that read empty ([`crate::chain::ChainOptions`]).
    pub fn with_chain_options(mut self, opts: crate::chain::ChainOptions) -> Self {
        self.chain = opts;
        self
    }

    pub fn channel(&self) -> &Arc<C> {
        &self.reader.channel
    }
}

impl<C: ChannelApi> Reader<C> {
    /// Read messages `ids` (at most [`IDS_PER_CALL`]), remembering where
    /// their blobs are.
    pub(crate) async fn read(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        let msgs = self.channel.get_messages(ids).await?;
        for (id, m) in ids.iter().zip(&msgs) {
            if let Some(l) = located(m) {
                self.locations.remember(*id, l);
            }
        }
        Ok(msgs)
    }

    /// Where blob `message_id` is: from the cache, or read the message
    /// (`fresh` = always read it, for a new file reference). A miss reads
    /// the whole page below: a log replay walks down message by message.
    async fn locate(&self, message_id: i64, fresh: bool) -> Result<Located<FileLocation>> {
        self.locations
            .locate(message_id, fresh, || async move {
                let lo = if fresh { message_id } else { (message_id - IDS_PER_CALL as i64 + 1).max(1) };
                let ids: Vec<i64> = (lo..=message_id).collect();
                match self.read(&ids).await?.pop() {
                    Some(ChannelMessage::Empty) | None => Ok(None),
                    Some(m) => located(&m).map(Some).with_context(|| format!("message {message_id} is not a blob")),
                }
            })
            .await
    }

    /// Read blob `message_id` again: its location, with a fresh file reference.
    async fn relocate(&self, message_id: i64) -> Result<FileLocation> {
        match self.locate(message_id, true).await? {
            Located::File { file, .. } => Ok(file),
            Located::Inline(_) => bail!("blob {message_id} is no longer a document"),
        }
    }

    /// Bytes `[skip, skip + take)` of the window at `offset`, with a fresh
    /// file reference if the cached one expired.
    async fn window(
        &self,
        message_id: i64,
        file: FileLocation,
        offset: u64,
        skip: usize,
        take: usize,
    ) -> Result<Bytes> {
        let _permit = self.limiter.acquire_download().await;
        let bytes = match self.channel.get_file(&file, offset, WINDOW).await {
            Err(e) if e.is::<FileReferenceExpired>() => {
                let fresh = self.relocate(message_id).await?;
                self.channel.get_file(&fresh, offset, WINDOW).await
            }
            other => other,
        }
        .with_context(|| format!("download blob {message_id} at {offset}"))?;
        if bytes.len() < skip + take {
            bail!("blob {message_id}: window at {offset} has {} bytes, expected {}", bytes.len(), skip + take);
        }
        Ok(bytes.slice(skip..skip + take))
    }
}

#[async_trait]
impl<C: ChannelApi> BlobBackend for TelegramBlobStore<C> {
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
        let cached = id.is_some_and(|i| self.reader.locations.holds(i.message_id));
        pigeonhole_limits::chat::cost(&self.limiter, CLASSES, op, cached)
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        check_blob(name, &data, &self.limits)?;
        let random_id = blob_random_id(self.reader.channel.channel_id(), name);
        let message_id = self.post_blob(name, data, random_id).await?;
        Ok(TelegramId { message_id })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let message_id = id.message_id;
        let (size, file) = match self.reader.locate(message_id, false).await? {
            Located::Inline(data) => {
                tracing::debug!(store = %self.instance.id, message_id = message_id, ?range, kind = "text", "read");
                return Ok(bytes_stream(slice_range(data, range)?));
            }
            Located::File { size, file } => (size, file),
        };
        tracing::debug!(store = %self.instance.id, message_id = message_id, ?range, size, kind = "document", "read");
        let (start, end) = match range {
            Some(r) if r.start > r.end || r.end > size => {
                bail!("byte range {}..{} outside blob of {} bytes", r.start, r.end, size)
            }
            Some(r) => (r.start, r.end),
            None => (0, size),
        };
        // A few windows in flight, yielded in order.
        let reader = self.reader.clone();
        let stream = futures::stream::iter(windows(start, end))
            .map(move |(offset, skip, take)| {
                let (reader, file) = (reader.clone(), file.clone());
                async move { reader.window(message_id, file, offset, skip, take).await }
            })
            .buffered(WINDOWS_IN_FLIGHT);
        Ok(Box::pin(stream))
    }

    /// The pinned owner marker if there is one, else ours, posted and pinned
    /// (read again after: of two deployments racing on a new channel, the
    /// last to pin keeps it, [`marked_owner`]).
    async fn mark_owner(&self, owner: Owner) -> Result<Owner> {
        if let Some(marked) = self.pinned_owner().await? {
            return Ok(marked);
        }
        let name = owner.marker_name();
        let channel_id = self.reader.channel.channel_id();
        let id = self.post_blob(&name, Bytes::from_static(OWNER_MARKER), blob_random_id(channel_id, &name)).await?;
        self.reader.channel.pin(id).await?;
        tracing::info!(store = %self.instance.id, %owner, message_id = id, "marked the channel as this deployment's");
        Ok(self.pinned_owner().await?.unwrap_or(owner))
    }

    /// The message, read from the channel now: a text blob's bytes stay in
    /// our cache after it was deleted.
    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        match self.reader.locate(id.message_id, true).await {
            Ok(_) => Ok(true),
            Err(e) if Failure::of(&e) == Failure::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        for ids in keys.chunks(IDS_PER_CALL) {
            self.reader.channel.delete_messages(ids).await?;
            tracing::debug!(store = %self.instance.id, messages = ?ids, "deleted");
            for id in ids {
                self.reader.locations.forget(*id);
            }
        }
        Ok(())
    }

    /// `owner`'s blobs (documents or texts named by a [`BlobId`](pigeonhole_storage_api::BlobId) of its) with
    /// ids in `(after, upto]`, read from the channel 100 ids at a time;
    /// nothing else in the channel is ever a candidate.
    async fn candidates(
        &self,
        owner: pigeonhole_storage_api::Owner,
        after: Option<i64>,
        upto: i64,
        limit: usize,
    ) -> Result<Vec<i64>> {
        let mut out = Vec::new();
        let mut next = after.map_or(1, |a| a.saturating_add(1)).max(1);
        while next <= upto && out.len() < limit {
            let last = upto.min(next.saturating_add(IDS_PER_CALL as i64 - 1));
            let ids: Vec<i64> = (next..=last).collect();
            let msgs = self.reader.read(&ids).await?;
            for (id, msg) in ids.into_iter().zip(msgs) {
                if owned_by(blob_name(&msg), owner) {
                    out.push(id);
                }
            }
            next = last.saturating_add(1);
        }
        out.truncate(limit);
        Ok(out)
    }
}
