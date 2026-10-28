//! [`BlobBackend`] + [`Sweepable`] over one channel.

use crate::channel::{windows, ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, IDS_PER_CALL, WINDOW};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use lru::LruCache;
use pigeonhole_blob::{
    bytes_stream, slice_range, BlobBackend, BlobId, BoxByteStream, ChatLimiter, CostHint, InstanceInfo, LimitBudget, OpKind, Sweepable,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

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

/// Where a blob's bytes are: a document's file, or the bytes themselves (a
/// text message, [`crate::inline`]).
#[derive(Clone)]
pub(crate) enum Located {
    Doc { size: u64, file: FileLocation },
    Inline(Bytes),
}

impl Located {
    /// What message `msg` holds, if it is a blob.
    pub(crate) fn of(msg: &ChannelMessage) -> Option<Self> {
        match msg {
            ChannelMessage::Document { size, file, .. } => Some(Located::Doc { size: *size, file: file.clone() }),
            ChannelMessage::Text { text } => crate::inline::decode(text).map(|(_, data)| Located::Inline(data)),
            _ => None,
        }
    }
}

/// The name a blob message was put under (document or text).
pub(crate) fn blob_name(msg: &ChannelMessage) -> Option<&str> {
    match msg {
        ChannelMessage::Document { name, .. } => Some(name),
        ChannelMessage::Text { text } => crate::inline::name_of(text),
        _ => None,
    }
}

type Locations = Arc<Mutex<LruCache<i64, Located>>>;

pub struct TelegramBlobStore<C> {
    limits: BackendLimits,
    instance: InstanceInfo,
    pub(crate) reader: Reader<C>,
    pub(crate) limiter: Arc<ChatLimiter>,
    pub(crate) chain: crate::chain::ChainOptions,
    /// Blobs that fit a text message go as text ([`crate::inline`]).
    inline: bool,
}

/// The read side, cloned into a download stream.
pub(crate) struct Reader<C> {
    pub(crate) channel: Arc<C>,
    limiter: Arc<ChatLimiter>,
    locations: Locations,
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
    pub fn new(channel: Arc<C>, limiter: Arc<ChatLimiter>, instance: InstanceInfo) -> Self {
        let locations = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(LOCATION_CACHE).unwrap())));
        Self {
            limits: BackendLimits::telegram(),
            instance,
            reader: Reader { channel, limiter: limiter.clone(), locations },
            limiter,
            chain: crate::chain::ChainOptions::default(),
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
    pub(crate) async fn post_blob(&self, name: &str, data: Bytes, random_id: i64) -> Result<i64> {
        let _upload = self.limiter.acquire_upload().await;
        let channel = &self.reader.channel;
        if self.inline && crate::inline::fits(name, data.len()) {
            let sent = channel.send_text(&crate::inline::encode(name, &data), random_id).await?;
            self.reader.remember(sent.message_id, Located::Inline(data));
            return Ok(sent.message_id);
        }
        let size = data.len() as u64;
        let sent = channel.send_document(name, data, random_id).await?;
        if let Some(file) = sent.file {
            self.reader.remember(sent.message_id, Located::Doc { size, file });
        }
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
    pub(crate) fn remember(&self, message_id: i64, located: Located) {
        self.locations.lock().unwrap().put(message_id, located);
    }

    /// Read messages `ids` (at most [`IDS_PER_CALL`]), remembering where
    /// their documents are.
    pub(crate) async fn read(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        let msgs = self.channel.get_messages(ids).await?;
        for (id, m) in ids.iter().zip(&msgs) {
            if let Some(located) = Located::of(m) {
                self.remember(*id, located);
            }
        }
        Ok(msgs)
    }

    /// Where blob `message_id` is: from the cache, or read the message
    /// (`fresh` = always read it, for a new file reference). A miss reads
    /// the whole page below: a log replay walks down message by message.
    async fn locate(&self, message_id: i64, fresh: bool) -> Result<Located> {
        if !fresh {
            if let Some(l) = self.locations.lock().unwrap().get(&message_id) {
                return Ok(l.clone());
            }
        }
        let lo = if fresh { message_id } else { (message_id - IDS_PER_CALL as i64 + 1).max(1) };
        let ids: Vec<i64> = (lo..=message_id).collect();
        let msg = self.read(&ids).await?.pop();
        match msg {
            Some(ChannelMessage::Empty) | None => bail!("blob message {message_id} is gone"),
            Some(m) => Located::of(&m).with_context(|| format!("message {message_id} is not a blob")),
        }
    }

    /// Bytes `[skip, skip + take)` of the window at `offset`, with a fresh
    /// file reference if the cached one expired.
    async fn window(&self, message_id: i64, file: FileLocation, offset: u64, skip: usize, take: usize) -> Result<Bytes> {
        let _permit = self.limiter.acquire_download().await;
        let bytes = match self.channel.get_file(&file, offset, WINDOW).await {
            Err(e) if e.is::<FileReferenceExpired>() => {
                let Located::Doc { file: fresh, .. } = self.locate(message_id, true).await? else {
                    bail!("blob {message_id} is no longer a document")
                };
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
        let wait = match op {
            OpKind::Put => self.limiter.peek_wait(LimitBudget::Send),
            OpKind::Get if id.is_some_and(|i| self.reader.locations.lock().unwrap().contains(&i.message_id)) => {
                std::time::Duration::ZERO
            }
            OpKind::Get | OpKind::List => self.limiter.peek_wait(LimitBudget::Info),
            OpKind::Delete => self.limiter.peek_wait(LimitBudget::Delete),
        };
        CostHint { wait_secs: wait.as_secs_f64(), latency_ewma_secs: 0.0, inflight: 0 }
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        if data.is_empty() {
            bail!("refusing empty blob upload (Telegram rejects empty documents)");
        }
        if data.len() > self.limits.max_blob_size {
            bail!("blob size {} exceeds backend max {}", data.len(), self.limits.max_blob_size);
        }
        let random_id = blob_random_id(self.reader.channel.channel_id(), name);
        let message_id = self.post_blob(name, data, random_id).await?;
        Ok(TelegramId { message_id })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let message_id = id.message_id;
        let (size, file) = match self.reader.locate(message_id, false).await? {
            Located::Inline(data) => return Ok(bytes_stream(slice_range(data, range)?)),
            Located::Doc { size, file } => (size, file),
        };
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

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        for ids in keys.chunks(IDS_PER_CALL) {
            self.reader.channel.delete_messages(ids).await?;
            let mut cache = self.reader.locations.lock().unwrap();
            for id in ids {
                cache.pop(id);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<C: ChannelApi> Sweepable for TelegramBlobStore<C> {
    /// Our blobs (documents or texts named by a [`BlobId`]) with ids in `(after,
    /// upto]`, read from the channel 100 ids at a time; nothing else in the
    /// channel is ever a candidate.
    async fn candidates(&self, after: Option<i64>, upto: i64, limit: usize) -> Result<Vec<i64>> {
        let mut out = Vec::new();
        let mut next = after.map_or(1, |a| a.saturating_add(1)).max(1);
        while next <= upto && out.len() < limit {
            let last = upto.min(next.saturating_add(IDS_PER_CALL as i64 - 1));
            let ids: Vec<i64> = (next..=last).collect();
            let msgs = self.reader.read(&ids).await?;
            for (id, msg) in ids.into_iter().zip(msgs) {
                if let Some(name) = blob_name(&msg) {
                    if BlobId::from_name(name).is_some() {
                        out.push(id);
                    }
                }
            }
            next = last.saturating_add(1);
        }
        out.truncate(limit);
        Ok(out)
    }
}
