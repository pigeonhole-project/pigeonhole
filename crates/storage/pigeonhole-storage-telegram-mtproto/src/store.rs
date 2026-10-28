//! [`BlobBackend`] + [`Sweepable`] over one channel.

use crate::channel::{windows, ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, IDS_PER_CALL, WINDOW};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use lru::LruCache;
use pigeonhole_blob::{
    BlobBackend, BlobId, BoxByteStream, ChatLimiter, CostHint, InstanceInfo, LimitBudget, OpKind, Sweepable,
};
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// A blob is a message; everything else (file reference, DC, size) is
/// fetched with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelegramMtprotoId {
    pub message_id: i64,
}

/// Locations of recently seen documents (saves a `getMessages` per read).
const LOCATION_CACHE: usize = 4096;
/// Windows of one blob fetched at a time.
const WINDOWS_IN_FLIGHT: usize = 4;

#[derive(Clone)]
struct Located {
    size: u64,
    file: FileLocation,
}

type Locations = Arc<Mutex<LruCache<i64, Located>>>;

pub struct MtprotoBlobStore<C> {
    limits: BackendLimits,
    instance: InstanceInfo,
    reader: Reader<C>,
    limiter: Arc<ChatLimiter>,
}

/// The read side, cloned into a download stream.
struct Reader<C> {
    channel: Arc<C>,
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

impl<C: ChannelApi> MtprotoBlobStore<C> {
    pub fn new(channel: Arc<C>, limiter: Arc<ChatLimiter>, instance: InstanceInfo) -> Self {
        let locations = Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(LOCATION_CACHE).unwrap())));
        Self {
            limits: BackendLimits::telegram_mtproto(),
            instance,
            reader: Reader { channel, limiter: limiter.clone(), locations },
            limiter,
        }
    }

    pub fn channel(&self) -> &Arc<C> {
        &self.reader.channel
    }
}

impl<C: ChannelApi> Reader<C> {
    fn remember(&self, message_id: i64, located: Located) {
        self.locations.lock().unwrap().put(message_id, located);
    }

    /// Where blob `message_id` is: from the cache, or read the message
    /// (`fresh` = always read it, for a new file reference).
    async fn locate(&self, message_id: i64, fresh: bool) -> Result<Located> {
        if !fresh {
            if let Some(l) = self.locations.lock().unwrap().get(&message_id) {
                return Ok(l.clone());
            }
        }
        let msg = self.channel.get_messages(&[message_id]).await?.into_iter().next();
        match msg {
            Some(ChannelMessage::Document { size, file, .. }) => {
                let located = Located { size, file };
                self.remember(message_id, located.clone());
                Ok(located)
            }
            Some(ChannelMessage::Empty) | None => bail!("blob message {message_id} is gone"),
            Some(ChannelMessage::Other) => bail!("message {message_id} is not a document"),
        }
    }

    /// Bytes `[skip, skip + take)` of the window at `offset`, with a fresh
    /// file reference if the cached one expired.
    async fn window(&self, message_id: i64, file: FileLocation, offset: u64, skip: usize, take: usize) -> Result<Bytes> {
        let _permit = self.limiter.acquire_download().await;
        let bytes = match self.channel.get_file(&file, offset, WINDOW).await {
            Err(e) if e.is::<FileReferenceExpired>() => {
                let fresh = self.locate(message_id, true).await?;
                self.channel.get_file(&fresh.file, offset, WINDOW).await
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
impl<C: ChannelApi> BlobBackend for MtprotoBlobStore<C> {
    type Id = TelegramMtprotoId;
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
        let size = data.len() as u64;
        let _upload = self.limiter.acquire_upload().await;
        let random_id = blob_random_id(self.reader.channel.channel_id(), name);
        let sent = self.reader.channel.send_document(name, data, random_id).await?;
        if let Some(file) = sent.file {
            self.reader.remember(sent.message_id, Located { size, file });
        }
        Ok(TelegramMtprotoId { message_id: sent.message_id })
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        let message_id = id.message_id;
        let located = self.reader.locate(message_id, false).await?;
        let (start, end) = match range {
            Some(r) if r.start > r.end || r.end > located.size => {
                bail!("byte range {}..{} outside blob of {} bytes", r.start, r.end, located.size)
            }
            Some(r) => (r.start, r.end),
            None => (0, located.size),
        };
        // A few windows in flight, yielded in order.
        let reader = self.reader.clone();
        let stream = futures::stream::iter(windows(start, end))
            .map(move |(offset, skip, take)| {
                let (reader, file) = (reader.clone(), located.file.clone());
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
impl<C: ChannelApi> Sweepable for MtprotoBlobStore<C> {
    /// Our blobs (documents named by a [`BlobId`]) with ids in `(after,
    /// upto]`, read from the channel 100 ids at a time; nothing else in the
    /// channel is ever a candidate.
    async fn candidates(&self, after: Option<i64>, upto: i64, limit: usize) -> Result<Vec<i64>> {
        let mut out = Vec::new();
        let mut next = after.map_or(1, |a| a.saturating_add(1)).max(1);
        while next <= upto && out.len() < limit {
            let last = upto.min(next.saturating_add(IDS_PER_CALL as i64 - 1));
            let ids: Vec<i64> = (next..=last).collect();
            let msgs = self.reader.channel.get_messages(&ids).await?;
            for (id, msg) in ids.into_iter().zip(msgs) {
                if let ChannelMessage::Document { name, size, file } = msg {
                    if BlobId::from_name(&name).is_some() {
                        self.reader.remember(id, Located { size, file });
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
