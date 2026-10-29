//! [`BlobBackend`] + [`Sweepable`] over one channel.

use crate::api::{CdnStale, DcMessage, DiscordApi, PAGE};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use crate::api::CONTENT_MAX_CHARS;
use pigeonhole_storage_api::text;
use pigeonhole_storage_api::{
    bytes_stream, slice_range, BlobBackend, BlobId, BoxByteStream, CostHint, InstanceInfo, NotFound, OpKind,
    Sweepable,
};
use pigeonhole_limits::{ChatLimiter, LimitBudget};
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

/// Where a blob's bytes are: an attachment, or the bytes themselves (a text
/// message, [`pigeonhole_storage_api::text`]).
#[derive(Clone)]
pub(crate) enum Located {
    Cdn { url: String, size: u64 },
    Inline(Bytes),
}

impl Located {
    pub(crate) fn of(m: &DcMessage) -> Option<Self> {
        match &m.attachment {
            Some(a) => Some(Located::Cdn { url: a.url.clone(), size: a.size }),
            None => text::decode(&m.content).map(|(_, data)| Located::Inline(data)),
        }
    }
}

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
    locations: Mutex<LruCache<u64, Located>>,
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
            limits: BackendLimits::discord(max_blob_size),
            instance,
            api,
            limiter,
            locations: Mutex::new(LruCache::new(NonZeroUsize::new(LOCATION_CACHE).unwrap())),
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
        if let Some(l) = Located::of(m) {
            self.locations.lock().unwrap().put(m.id, l);
        }
        if let Some(name) = blob_name(m) {
            self.names.lock().unwrap().put(m.id, name.to_string());
        }
    }

    /// Post blob `name` as text if it fits, else as an attachment; the message.
    pub(crate) async fn post_blob(&self, name: &str, data: Bytes, nonce: &str) -> Result<DcMessage> {
        let (bytes, as_text) = (data.len(), self.inline && text::fits(name, data.len(), CONTENT_MAX_CHARS));
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
    async fn locate(&self, id: u64, fresh: bool) -> Result<Located> {
        if !fresh {
            if let Some(l) = self.locations.lock().unwrap().get(&id) {
                return Ok(l.clone());
            }
        }
        let Some(m) = self.api.get_message(id).await? else { return Err(NotFound(format!("blob message {id}")).into()) };
        let l = Located::of(&m).with_context(|| format!("message {id} is not a blob"))?;
        self.locations.lock().unwrap().put(id, l.clone());
        Ok(l)
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
        let wait = match op {
            OpKind::Put => self.limiter.peek_wait(LimitBudget::Send),
            OpKind::Get if id.is_some_and(|i| self.locations.lock().unwrap().contains(&i.message_id)) => {
                std::time::Duration::ZERO
            }
            OpKind::Get | OpKind::List => self.limiter.peek_wait(LimitBudget::GetFile),
            OpKind::Delete => self.limiter.peek_wait(LimitBudget::Delete),
        };
        CostHint { wait_secs: wait.as_secs_f64(), latency_ewma_secs: 0.0, inflight: 0 }
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        if data.is_empty() {
            bail!("refusing empty blob upload");
        }
        if data.len() > self.limits.max_blob_size {
            bail!("blob size {} exceeds backend max {}", data.len(), self.limits.max_blob_size);
        }
        let nonce = blob_nonce(self.api.channel_id(), name);
        let m = self.post_blob(name, data, &nonce).await?;
        Ok(DiscordId { message_id: m.id })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let mid = id.message_id;
        let (url, size) = match self.locate(mid, false).await? {
            Located::Inline(data) => {
                tracing::debug!(store = %self.instance.id, message_id = mid, ?range, kind = "text", "read");
                return Ok(bytes_stream(slice_range(data, range)?));
            }
            Located::Cdn { url, size } => (url, size),
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
                let Located::Cdn { url, .. } = self.locate(mid, true).await? else {
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
            let mut cache = self.locations.lock().unwrap();
            for id in ids {
                cache.pop(id);
            }
        }
        // Chain members (a removed snapshot): their links go too.
        self.retire_links(keys).await
    }
}

#[async_trait]
impl<A: DiscordApi> Sweepable for DiscordBlobStore<A> {
    /// Our blobs (attachments or texts named by a [`BlobId`]) with ids in
    /// `(after, upto]`, listed 100 at a time; nothing else is a candidate.
    async fn candidates(&self, after: Option<u64>, upto: u64, limit: usize) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        let mut cursor = after.unwrap_or(0);
        while out.len() < limit {
            let page = self.api.list(None, Some(cursor), PAGE).await?;
            let Some(last) = page.last().map(|m| m.id) else { break };
            for m in &page {
                if m.id > upto {
                    break;
                }
                if blob_name(m).and_then(BlobId::from_name).is_some() {
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
