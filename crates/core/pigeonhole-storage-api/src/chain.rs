//! [`Chain`]: what a store needs to hold a log (docs/chain-log.md).
//!
//! One chain per store: blobs linked to a predecessor, where **the first
//! successor wins**. Of every [`Chain::put_after`] naming the same `prev`,
//! exactly one gets [`After::Won`], and every later reader agrees which. How
//! a store decides it is its own business: the order of ids in a channel, a
//! create-once operation, a conditional write, a lock.

use crate::BlobBackend;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

/// A chain member as a reader sees it without downloading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Linked<Id> {
    pub id: Id,
    /// What the writer passed to [`Chain::put_after`].
    pub label: String,
}

/// Outcome of [`Chain::put_after`]; final once returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum After<Id> {
    /// The blob is `prev`'s successor.
    Won(Id),
    /// Another blob became `prev`'s successor; ours is garbage (the store
    /// may keep it until [`Chain::prune`]).
    Lost(Linked<Id>),
}

/// A store whose blobs can form a chain: the log's capability.
#[async_trait]
pub trait Chain: BlobBackend {
    /// Put `data` as the successor of `prev` (`None`: the first blob of the
    /// chain). `label` is short, readable without the data (a file name):
    /// readers decide by it what a member is.
    ///
    /// `Err`: the outcome is unknown, the blob may still land and win.
    async fn put_after(&self, prev: Option<&Self::Id>, label: &str, data: Bytes) -> Result<After<Self::Id>>;

    /// The last member of the chain; `None` if nothing was ever put.
    async fn tail(&self) -> Result<Option<Linked<Self::Id>>>;

    /// The successor of `id`, if any yet.
    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>>;

    /// Delete every blob the store holds below `below` (by key): members and
    /// losers alike. The chain's tail is never below it. Best effort.
    async fn prune(&self, below: &Self::Id) -> Result<()>;

    /// Put a blob built from the current tail after it: `make` gets the
    /// tail's id and data (`None` for an empty chain) and returns the blob,
    /// or `None` to put nothing (then the result is `None`). Returns the new
    /// blob's id once it won.
    ///
    /// The default reads the tail and puts after it, again while another
    /// blob wins first, so it may call `make` more than once: `make` must be
    /// pure. A store that can decide it atomically (under a lock) should, or
    /// a busy writer wins every gap between reading the tail and the put.
    async fn put_after_tail(&self, label: &str, make: &MakeAfter<'_, Self::Id>) -> Result<Option<Self::Id>> {
        let mut tail = self.tail().await?.map(|t| t.id);
        // Bounded: writers appending meanwhile settle soon; a store whose
        // chain never does is a bug, and must not post copies forever.
        for _ in 0..PUT_AFTER_TAIL_ATTEMPTS {
            let data = match &tail {
                Some(id) => Some(crate::collect_stream(self.get(id, None).await?).await?),
                None => None,
            };
            let Some(blob) = make(tail.as_ref().zip(data.as_deref()))? else { return Ok(None) };
            match self.put_after(tail.as_ref(), label, blob).await? {
                After::Won(id) => return Ok(Some(id)),
                After::Lost(winner) => tail = Some(winner.id),
            }
        }
        anyhow::bail!("{label}: lost the link {PUT_AFTER_TAIL_ATTEMPTS} times in a row; the chain does not settle")
    }

    /// Put a blob that needs no order but "the newest of its kind"
    /// ([`Self::newest_loose`]): snapshots. The default links it into the
    /// chain (a store that cannot list finds the newest blob only by
    /// following links); a store that can list should put it plainly, since
    /// a link may cost a rate-limited call (a Discord thread).
    async fn put_loose(&self, label: &str, data: Bytes) -> Result<Self::Id> {
        for _ in 0..PUT_AFTER_TAIL_ATTEMPTS {
            let tail = self.tail().await?;
            match self.put_after(tail.as_ref().map(|t| &t.id), label, data.clone()).await? {
                After::Won(id) => return Ok(id),
                After::Lost(_) => continue,
            }
        }
        anyhow::bail!("{label}: lost the link {PUT_AFTER_TAIL_ATTEMPTS} times in a row; the chain does not settle")
    }

    /// The newest blob [`Self::put_loose`] put with a label starting with
    /// `prefix`. The default: the chain's tail, if its label matches.
    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<Self::Id>>> {
        Ok(self.tail().await?.filter(|t| t.label.starts_with(prefix)))
    }

    /// When the store stamped blob `id`, in ms of the store's own clock, if
    /// its ids carry that (a Discord snowflake). A store that answers holds
    /// a log by a lease (docs/discord-lease.md): the stamps of its blobs,
    /// not a reader's clock, tell whether a batch fell inside it.
    fn stamp_ms(&self, _id: &Self::Id) -> Option<u64> {
        None
    }

    /// Put a blob that names `prev` as a chain member does, without linking
    /// it: nothing decides a winner, and [`Self::tail`] does not see it. It
    /// is listed ([`Self::list_after`]) and pruned like a member. For a lease
    /// log's batches.
    async fn put_unlinked(&self, _prev: &Self::Id, label: &str, _data: Bytes) -> Result<Self::Id> {
        anyhow::bail!("{label}: this store cannot put a blob outside its chain")
    }

    /// Blobs newer than `after` (all for `None`) whose label starts with
    /// `prefix`, oldest first: members, unlinked and loose blobs alike.
    async fn list_after(&self, _after: Option<&Self::Id>, prefix: &str) -> Result<Vec<Linked<Self::Id>>> {
        anyhow::bail!("{prefix}: this store cannot list its blobs")
    }
}

/// Tries of the default [`Chain::put_after_tail`] before it gives up.
pub const PUT_AFTER_TAIL_ATTEMPTS: usize = 64;

/// What [`Chain::put_after_tail`] builds a blob with.
pub type MakeAfter<'a, Id> = dyn Fn(Option<(&Id, &[u8])>) -> Result<Option<Bytes>> + Send + Sync + 'a;

#[async_trait]
impl<C: Chain> Chain for std::sync::Arc<C> {
    async fn put_after(&self, prev: Option<&Self::Id>, label: &str, data: Bytes) -> Result<After<Self::Id>> {
        (**self).put_after(prev, label, data).await
    }
    async fn tail(&self) -> Result<Option<Linked<Self::Id>>> {
        (**self).tail().await
    }
    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>> {
        (**self).successor(id).await
    }
    async fn prune(&self, below: &Self::Id) -> Result<()> {
        (**self).prune(below).await
    }
    async fn put_loose(&self, label: &str, data: Bytes) -> Result<Self::Id> {
        (**self).put_loose(label, data).await
    }
    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<Self::Id>>> {
        (**self).newest_loose(prefix).await
    }
    async fn put_after_tail(&self, label: &str, make: &MakeAfter<'_, Self::Id>) -> Result<Option<Self::Id>> {
        (**self).put_after_tail(label, make).await
    }
    fn stamp_ms(&self, id: &Self::Id) -> Option<u64> {
        (**self).stamp_ms(id)
    }
    async fn put_unlinked(&self, prev: &Self::Id, label: &str, data: Bytes) -> Result<Self::Id> {
        (**self).put_unlinked(prev, label, data).await
    }
    async fn list_after(&self, after: Option<&Self::Id>, prefix: &str) -> Result<Vec<Linked<Self::Id>>> {
        (**self).list_after(after, prefix).await
    }
}
