//! [`Chain`]: what a store needs to hold a log (docs/chain-log.md).
//!
//! One chain per store: blobs linked to a predecessor, where **the first
//! successor wins**. Of every [`Chain::put_after`] naming the same `prev`,
//! exactly one gets [`After::Won`], and every later reader agrees which. How
//! a store decides it is its own business: the order of ids in a channel, a
//! create-once operation, a conditional write, a lock.
//!
//! [`Chain`] and [`LeaseChain`] have no provided methods: a store chooses
//! each behaviour (the shared ways are free functions here, named for what
//! they do), and a decorator must forward every method, or it does not
//! compile. A provided method a decorator forgot would quietly swap a
//! store's own way for the default.

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
    /// blob's id once it won. `hint`: the tail as the caller last saw it, if
    /// it did. `make` may be called more than once: it must be pure.
    ///
    /// A store that can decide it atomically (under a lock) should, or a
    /// busy writer wins every gap between reading the tail and the put;
    /// [`put_after_tail_by_links`] is the way by [`Self::put_after`].
    async fn put_after_tail(
        &self,
        hint: Option<&Self::Id>,
        label: &str,
        make: &MakeAfter<'_, Self::Id>,
    ) -> Result<Option<Self::Id>>;

    /// Put a blob that needs no order but "the newest of its kind"
    /// ([`Self::newest_loose`]): snapshots. `after` is the newest such blob
    /// the caller knows of (the one it put last), if any.
    ///
    /// A store that lists puts it plainly (a link may cost a scarce,
    /// rate-limited call); one that cannot list finds the newest blob only
    /// by following links: [`put_loose_linked`]. A [`LeaseChain`]'s chain
    /// holds lease records only, so its loose blobs must not be linked.
    async fn put_loose(&self, after: Option<&Self::Id>, label: &str, data: Bytes) -> Result<Self::Id>;

    /// The newest blob [`Self::put_loose`] put with a label starting with
    /// `prefix` ([`newest_loose_at_tail`] where loose blobs are linked).
    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<Self::Id>>>;
}

/// [`Chain::put_after_tail`] by [`Chain::put_after`]: after `hint`, or after
/// the tail it reads (on a channel, a post to learn the top), again while
/// another blob wins first. A hint that lost is not followed but read
/// again: the store may answer a put after a long-gone blob with the
/// chain's evidence, not with a winner.
pub async fn put_after_tail_by_links<C: Chain + ?Sized>(
    chain: &C,
    hint: Option<&C::Id>,
    label: &str,
    make: &MakeAfter<'_, C::Id>,
) -> Result<Option<C::Id>> {
    let mut hinted = hint.is_some();
    let mut tail = match hint {
        Some(h) => Some(h.clone()),
        None => chain.tail().await?.map(|t| t.id),
    };
    // Bounded: writers appending meanwhile settle soon; a store whose
    // chain never does is a bug, and must not post copies forever.
    for _ in 0..PUT_AFTER_TAIL_ATTEMPTS {
        let data = match &tail {
            Some(id) => Some(crate::collect_stream(chain.get(id, None).await?).await?),
            None => None,
        };
        let Some(blob) = make(tail.as_ref().zip(data.as_deref()))? else { return Ok(None) };
        match chain.put_after(tail.as_ref(), label, blob).await? {
            After::Won(id) => return Ok(Some(id)),
            After::Lost(_) if std::mem::take(&mut hinted) => tail = chain.tail().await?.map(|t| t.id),
            After::Lost(winner) => tail = Some(winner.id),
        }
    }
    anyhow::bail!("{label}: lost the link {PUT_AFTER_TAIL_ATTEMPTS} times in a row; the chain does not settle")
}

/// [`Chain::put_loose`] for a store that finds the newest blob by its
/// links: the blob becomes a chain member. After `after` when given, which
/// needs no look for the tail (on a channel, a post to learn its top);
/// after the tail when not, or when another blob already follows `after`.
pub async fn put_loose_linked<C: Chain + ?Sized>(
    chain: &C,
    after: Option<&C::Id>,
    label: &str,
    data: Bytes,
) -> Result<C::Id> {
    if let Some(prev) = after {
        // Lost: ours is garbage, like any loser's.
        if let After::Won(id) = chain.put_after(Some(prev), label, data.clone()).await? {
            return Ok(id);
        }
    }
    for _ in 0..PUT_AFTER_TAIL_ATTEMPTS {
        let tail = chain.tail().await?;
        match chain.put_after(tail.as_ref().map(|t| &t.id), label, data.clone()).await? {
            After::Won(id) => return Ok(id),
            After::Lost(_) => continue,
        }
    }
    anyhow::bail!("{label}: lost the link {PUT_AFTER_TAIL_ATTEMPTS} times in a row; the chain does not settle")
}

/// [`Chain::newest_loose`] where [`put_loose_linked`] put them: the chain's
/// tail, if its label matches.
pub async fn newest_loose_at_tail<C: Chain + ?Sized>(chain: &C, prefix: &str) -> Result<Option<Linked<C::Id>>> {
    Ok(chain.tail().await?.filter(|t| t.label.starts_with(prefix)))
}

/// A [`Chain`] that can hold a log by a lease (docs/discord-lease.md): its
/// ids carry the store's time, and it puts and lists blobs outside the
/// chain. Links there are scarce, so only the lease records are linked.
///
/// Its chain holds lease records only: a claim reads the tail as one (it
/// decodes it). So its [`Chain::put_loose`] must put a plain blob, not a
/// chain member ([`put_loose_linked`] links one): snapshots in the log's
/// store would otherwise become the tail and break the next claim. It can,
/// since it lists ([`Self::list_after`]).
#[async_trait]
pub trait LeaseChain: Chain {
    /// When the store stamped blob `id`, in ms of the store's own clock:
    /// the stamps of its blobs, not a reader's clock, tell whether a batch
    /// fell inside a lease.
    fn stamp_ms(&self, id: &Self::Id) -> u64;

    /// The store's clock now, in the ms of [`Self::stamp_ms`]: a claimant
    /// waits by it until the old lease ran out. Puts nothing in the chain
    /// and leaves nothing a reader of the log would see.
    async fn now_ms(&self) -> Result<u64>;

    /// Put a blob that names `prev` as a chain member does, without linking
    /// it: nothing decides a winner, and [`Chain::tail`] does not see it. It
    /// is listed ([`Self::list_after`]) and pruned like a member.
    async fn put_unlinked(&self, prev: &Self::Id, label: &str, data: Bytes) -> Result<Self::Id>;

    /// Blobs newer than `after` (all for `None`) whose label starts with
    /// `prefix`, oldest first: members, unlinked and loose blobs alike.
    async fn list_after(&self, after: Option<&Self::Id>, prefix: &str) -> Result<Vec<Linked<Self::Id>>>;
}

/// Tries of [`put_after_tail_by_links`] and [`put_loose_linked`] before
/// they give up.
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
    async fn put_loose(&self, after: Option<&Self::Id>, label: &str, data: Bytes) -> Result<Self::Id> {
        (**self).put_loose(after, label, data).await
    }
    async fn newest_loose(&self, prefix: &str) -> Result<Option<Linked<Self::Id>>> {
        (**self).newest_loose(prefix).await
    }
    async fn put_after_tail(
        &self,
        hint: Option<&Self::Id>,
        label: &str,
        make: &MakeAfter<'_, Self::Id>,
    ) -> Result<Option<Self::Id>> {
        (**self).put_after_tail(hint, label, make).await
    }
}

#[async_trait]
impl<C: LeaseChain> LeaseChain for std::sync::Arc<C> {
    fn stamp_ms(&self, id: &Self::Id) -> u64 {
        (**self).stamp_ms(id)
    }
    async fn now_ms(&self) -> Result<u64> {
        (**self).now_ms().await
    }
    async fn put_unlinked(&self, prev: &Self::Id, label: &str, data: Bytes) -> Result<Self::Id> {
        (**self).put_unlinked(prev, label, data).await
    }
    async fn list_after(&self, after: Option<&Self::Id>, prefix: &str) -> Result<Vec<Linked<Self::Id>>> {
        (**self).list_after(after, prefix).await
    }
}
