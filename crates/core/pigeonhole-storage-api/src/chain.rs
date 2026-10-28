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
}

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
}
