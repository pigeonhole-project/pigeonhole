//! [`BlobBackend`] over one channel.

use crate::api::CONTENT_MAX_CHARS;
use crate::api::{CdnStale, DcMessage, DiscordApi, PAGE};
use crate::limits::{DELETE, READ, SEND};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use pigeonhole_limits::chat::Classes;
use pigeonhole_limits::ChatLimiter;
use pigeonhole_storage_api::located::{as_text, marked_owner, owned_by, Located, Locations, OWNER_MARKER};
use pigeonhole_storage_api::text;
use pigeonhole_storage_api::{
    bytes_stream, check_blob, slice_range, BlobBackend, BoxByteStream, CostHint, Failure, InstanceInfo, OpKind, Owner,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// A blob is a message; its URL (or its bytes, for a text) is fetched with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscordId {
    pub message_id: u64,
}

/// `dc:{app_id}:{channel_id}` — matches `pigeonhole_chunk_store::instances::discord_fingerprint`.
pub fn discord_fingerprint(app_id: &str, channel_id: &str) -> String {
    format!("dc:{app_id}:{channel_id}")
}

/// `dc:channel:{channel_id}` — matches `pigeonhole_chunk_store::instances::discord_location`.
pub fn discord_location(channel_id: &str) -> String {
    format!("dc:channel:{channel_id}")
}

/// Locations of recently seen blobs (saves a fetch per read).
const LOCATION_CACHE: usize = 4096;

/// What message `m` holds, if it is a blob: an attachment's URL, or a text
/// message's bytes ([`pigeonhole_storage_api::text`]).
pub(crate) fn located(m: &DcMessage) -> Option<Located<String>> {
    match &m.attachment {
        Some(a) => Some(Located::File { size: a.size, file: a.url.clone() }),
        None => Located::of_text(&m.content),
    }
}

const CLASSES: Classes = Classes { send: SEND, read: READ, delete: DELETE };

/// The name a blob message was put under. An attachment carries it as the
/// message's content: Discord rewrites file names (it drops `~`, live check).
pub(crate) fn blob_name(m: &DcMessage) -> Option<&str> {
    match &m.attachment {
        Some(_) => Some(m.content.as_str()).filter(|c| !c.is_empty() && !c.contains(char::is_whitespace)),
        None => text::name_of(&m.content),
    }
}

/// A nonce (≤ 25 characters) for blob `name` in `channel`: a repeated put of
/// one blob within a few minutes posts it once. Discord scopes nonces to the
/// bot across channels, so the channel is part of it.
pub fn blob_nonce(channel: u64, name: &str) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (channel, name).hash(&mut h);
    let a = h.finish();
    name.hash(&mut h);
    format!("b{:016x}{:08x}", a, h.finish() as u32)
}

pub struct DiscordBlobStore<A> {
    limits: BackendLimits,
    instance: InstanceInfo,
    pub(crate) api: Arc<A>,
    pub(crate) limiter: Arc<ChatLimiter>,
    locations: Locations<u64, String>,
    /// Names of recently seen blob messages (a chain member's name tells
    /// what it follows, which deleting it needs).
    pub(crate) names: Mutex<LruCache<u64, String>>,
    /// Blobs that fit a message's content go as text.
    inline: bool,
    /// The chain's anchor message, once found ([`crate::chain`]).
    pub(crate) anchor: tokio::sync::Mutex<Option<u64>>,
    /// Links to archive in the background ([`Self::archive_later`]).
    archiver: Mutex<Option<tokio::sync::mpsc::UnboundedSender<u64>>>,
}

impl<A: DiscordApi> DiscordBlobStore<A> {
    pub fn new(api: Arc<A>, limiter: Arc<ChatLimiter>, max_blob_size: Option<usize>, instance: InstanceInfo) -> Self {
        Self {
            limits: crate::blob_limits(max_blob_size),
            instance,
            api,
            limiter,
            locations: Locations::new(LOCATION_CACHE),
            names: Mutex::new(LruCache::new(NonZeroUsize::new(LOCATION_CACHE).unwrap())),
            inline: true,
            anchor: tokio::sync::Mutex::new(None),
            archiver: Mutex::new(None),
        }
    }

    /// Archive the thread of message `id` in the background, one at a time
    /// (started on first use; ends with the store).
    pub(crate) fn archive_later(&self, id: u64) {
        let mut tx = self.archiver.lock().unwrap();
        if tx.as_ref().is_none_or(|t| t.is_closed()) {
            let (sender, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
            let api = self.api.clone();
            tokio::spawn(async move {
                while let Some(id) = rx.recv().await {
                    if let Err(e) = api.archive_thread(id).await {
                        tracing::debug!(thread = id, error = %format!("{e:#}"), "archiving a link failed");
                    }
                }
            });
            *tx = Some(sender);
        }
        let _ = tx.as_ref().expect("just set").send(id);
    }

    /// Whether small blobs go as text messages (default) or always as attachments.
    pub fn with_inline(mut self, inline: bool) -> Self {
        self.inline = inline;
        self
    }

    pub fn api(&self) -> &Arc<A> {
        &self.api
    }

    pub(crate) fn remember(&self, m: &DcMessage) {
        if let Some(l) = located(m) {
            self.locations.remember(m.id, l);
        }
        if let Some(name) = blob_name(m) {
            self.names.lock().unwrap().put(m.id, name.to_string());
        }
    }

    /// The owner the latest pinned marker marks the channel for (Discord
    /// lists pins latest first, [`marked_owner`]).
    async fn pinned_owner(&self) -> Result<Option<Owner>> {
        let pins = self.api.pinned().await?;
        Ok(marked_owner(pins.iter().map(blob_name)))
    }

    /// Post blob `name` as text if it fits, else as an attachment; the message.
    pub(crate) async fn post_blob(&self, name: &str, data: Bytes, nonce: &str) -> Result<DcMessage> {
        let (bytes, as_text) = (data.len(), as_text(self.inline, name, data.len(), CONTENT_MAX_CHARS));
        let m = if as_text {
            self.api.send(&text::encode(name, &data), None, nonce).await?
        } else {
            self.api.send(name, Some((name, data)), nonce).await?
        };
        self.remember(&m);
        let kind = if as_text { "text" } else { "attachment" };
        tracing::debug!(store = %self.instance.id, message_id = m.id, name, bytes, kind, "posted");
        Ok(m)
    }

    /// Bytes `want` (all for `None`) of the attachment at `url`; a CDN that
    /// refuses the range gives the whole blob, sliced here.
    async fn fetch(&self, url: &str, want: Option<(u64, u64)>) -> Result<Bytes> {
        match self.api.download(url, want).await {
            Ok(b) => Ok(b),
            Err(e) if want.is_some() && !e.is::<CdnStale>() => {
                tracing::debug!(error = %format!("{e:#}"), "ranged download failed; whole blob");
                let all = self.api.download(url, None).await?;
                let (start, end) = want.unwrap();
                slice_range(all, Some(start..end))
            }
            Err(e) => Err(e),
        }
    }

    /// Where blob `id` is: from the cache, or fetch the message (`fresh`:
    /// always, for a new URL).
    async fn locate(&self, id: u64, fresh: bool) -> Result<Located<String>> {
        self.locations
            .locate(id, fresh, || async move {
                let Some(m) = self.api.get_message(id).await? else { return Ok(None) };
                if let Some(name) = blob_name(&m) {
                    self.names.lock().unwrap().put(id, name.to_string());
                }
                located(&m).map(Some).with_context(|| format!("message {id} is not a blob"))
            })
            .await
    }
}

#[async_trait]
impl<A: DiscordApi> BlobBackend for DiscordBlobStore<A> {
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

    fn cost(&self, op: OpKind, id: Option<&Self::Id>) -> CostHint {
        let cached = id.is_some_and(|i| self.locations.holds(i.message_id));
        pigeonhole_limits::chat::cost(&self.limiter, CLASSES, op, cached)
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        check_blob(name, &data, &self.limits)?;
        let nonce = blob_nonce(self.api.channel_id(), name);
        let m = self.post_blob(name, data, &nonce).await?;
        Ok(DiscordId { message_id: m.id })
    }

    /// The latest pinned owner marker if there is one, else ours, posted and
    /// pinned (read again after: of two deployments racing on a new channel,
    /// the last to pin keeps it, [`marked_owner`]).
    async fn mark_owner(&self, owner: Owner) -> Result<Owner> {
        if let Some(marked) = self.pinned_owner().await? {
            return Ok(marked);
        }
        let name = owner.marker_name();
        let nonce = blob_nonce(self.api.channel_id(), &name);
        let m = self.post_blob(&name, Bytes::from_static(OWNER_MARKER), &nonce).await?;
        self.api.pin(m.id).await?;
        tracing::info!(store = %self.instance.id, %owner, message_id = m.id, "marked the channel as this deployment's");
        Ok(self.pinned_owner().await?.unwrap_or(owner))
    }

    /// The message, read from the channel now: a text blob's bytes stay in
    /// our cache after it was deleted.
    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        match self.locate(id.message_id, true).await {
            Ok(_) => Ok(true),
            Err(e) if Failure::of(&e) == Failure::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let mid = id.message_id;
        let (url, size) = match self.locate(mid, false).await? {
            Located::Inline(data) => {
                tracing::debug!(store = %self.instance.id, message_id = mid, ?range, kind = "text", "read");
                return Ok(bytes_stream(slice_range(data, range)?));
            }
            Located::File { size, file } => (file, size),
        };
        tracing::debug!(store = %self.instance.id, message_id = mid, ?range, size, kind = "attachment", "read");
        let want = match range {
            Some(r) if r.start > r.end || r.end > size => {
                bail!("byte range {}..{} outside blob of {size} bytes", r.start, r.end)
            }
            Some(r) if r.start == r.end => return Ok(bytes_stream(Bytes::new())),
            Some(r) => Some((r.start, r.end)),
            None => None,
        };
        let bytes = match self.fetch(&url, want).await {
            Err(e) if e.is::<CdnStale>() => {
                let Located::File { file: url, .. } = self.locate(mid, true).await? else {
                    bail!("blob {mid} is no longer an attachment")
                };
                self.fetch(&url, want).await
            }
            other => other,
        }
        .with_context(|| format!("download blob {mid}"))?;
        Ok(bytes_stream(bytes))
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        for ids in keys.chunks(PAGE) {
            self.api.delete_messages(ids).await?;
            tracing::debug!(store = %self.instance.id, messages = ?ids, "deleted");
            for id in ids {
                self.locations.forget(*id);
            }
        }
        // Chain members (a removed snapshot): their links go too.
        self.retire_links(keys).await
    }

    /// `owner`'s blobs (attachments or texts named by a [`BlobId`](pigeonhole_storage_api::BlobId) of its)
    /// with ids in `(after, upto]`, listed 100 at a time; nothing else is a
    /// candidate.
    async fn candidates(
        &self,
        owner: pigeonhole_storage_api::Owner,
        after: Option<u64>,
        upto: u64,
        limit: usize,
    ) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        let mut cursor = after.unwrap_or(0);
        while out.len() < limit {
            let page = self.api.list(None, Some(cursor), PAGE).await?;
            let Some(last) = page.last().map(|m| m.id) else { break };
            for m in &page {
                if m.id > upto {
                    break;
                }
                if owned_by(blob_name(m), owner) {
                    self.remember(m);
                    out.push(m.id);
                }
            }
            if last >= upto || page.len() < PAGE {
                break;
            }
            cursor = last;
        }
        out.truncate(limit);
        Ok(out)
    }
}
