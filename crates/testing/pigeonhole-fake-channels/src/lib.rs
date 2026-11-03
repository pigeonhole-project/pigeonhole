//! Fake Telegram and Discord channels kept by a metadata tablet.
//!
//! A channel's state ([`TelegramChannelState`], [`DiscordChannelState`]) is a
//! namespace of its own [`MetaStore`]: every change is a command in the
//! tablet's log, snapshots are taken as for pigeonhole's own metadata, and a
//! restart replays them. Documents are blobs in a store next to the log. On a
//! `local_disk` store the channel outlives the process; on memory it does not.
//!
//! The tablet's own work runs in a `fake_channel` span, so a subscriber can
//! tell it from the node's (the site hides it).
//!
//! [`World`] holds a process's fake channels and builds the node's stores on
//! them ([`Fakes`]: as the node's assembly asks); [`FakeView`] shows them to
//! the admin's UI.

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{erase, BlobLocator, DynBlobBackend};
use pigeonhole_meta::{
    BlobSnapshotStore, ChainLogStore, LogStore, MetaOptions, MetaStore, Namespaces, Never, Outcome, SnapshotStore,
    StateMachine,
};
use pigeonhole_storage_api::fake::{FakeHost, FakeState};
use pigeonhole_storage_api::{collect_stream, BlobId};
use pigeonhole_storage_discord::fake::{state::DiscordChannelState, FakeDiscord};
use pigeonhole_storage_disk::DiskBlobStore;
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_storage_telegram::fake::{state::TelegramChannelState, FakeChannel};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use tracing::Instrument;

/// A fake channel's state as a tablet namespace.
#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Tablet<S>(pub S);

impl<S: FakeState> StateMachine for Tablet<S> {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;
    type Command = S::Command;
    type Reply = S::Reply;
    type Reject = Never;

    fn apply(&mut self, command: S::Command) -> Result<Outcome<Self>> {
        Ok(Ok(self.0.apply(command)))
    }
}

/// Where a channel's tablet keeps its log, snapshots and documents.
pub struct Stores {
    pub log: Arc<dyn LogStore>,
    pub snapshots: Arc<dyn SnapshotStore>,
    pub documents: Arc<dyn DynBlobBackend>,
}

impl Stores {
    /// In memory: gone with the process.
    pub fn memory() -> Self {
        Self {
            log: Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new()))),
            snapshots: Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new()))),
            documents: Arc::new(erase(Arc::new(MemoryBlobStore::new()))),
        }
    }

    /// Under `dir`: `log/`, `snapshots/`, `documents/`, each a local_disk store.
    pub fn in_dir(dir: &Path) -> Result<Self> {
        let open = |sub: &str| DiskBlobStore::open(dir.join(sub)).with_context(|| format!("{}/{sub}", dir.display()));
        Ok(Self {
            log: Arc::new(ChainLogStore::new(Arc::new(open("log")?))),
            snapshots: Arc::new(BlobSnapshotStore::new(Arc::new(open("snapshots")?))),
            documents: Arc::new(erase(Arc::new(open("documents")?))),
        })
    }
}

/// A [`FakeHost`] on a tablet.
pub struct TabletHost<S> {
    meta: Arc<MetaStore>,
    documents: Arc<dyn DynBlobBackend>,
    span: tracing::Span,
    _state: PhantomData<fn() -> S>,
}

impl<S: FakeState> TabletHost<S> {
    /// Boots the channel's tablet over `stores`: its state as last written.
    pub async fn open(stores: Stores, label: &str) -> Result<Self> {
        let span = tracing::debug_span!("fake_channel", channel = %label);
        let opts = MetaOptions { snapshot_every: Some(200), ..MetaOptions::default() };
        let meta = MetaStore::open(stores.log, stores.snapshots, Namespaces::new().with::<Tablet<S>>(), opts)
            .instrument(span.clone())
            .await
            .with_context(|| format!("fake channel {label}: boot its tablet"))?;
        Ok(Self { meta, documents: stores.documents, span, _state: PhantomData })
    }

    /// Stops the tablet, as a process exit would.
    pub async fn close(&self) -> Result<()> {
        self.meta.close().instrument(self.span.clone()).await
    }
}

#[async_trait]
impl<S: FakeState> FakeHost<S> for TabletHost<S> {
    async fn apply(&self, command: S::Command) -> Result<S::Reply> {
        let outcome = self.meta.propose::<Tablet<S>>(command).instrument(self.span.clone()).await?;
        Ok(match outcome {
            Ok(reply) => reply,
            Err(never) => match never {},
        })
    }

    fn read<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        self.meta.read::<Tablet<S>, _>(|t| f(&t.0))
    }

    async fn put_blob(&self, data: Bytes) -> Result<Vec<u8>> {
        let locator = self.documents.put(data, &BlobId::random().name()).instrument(self.span.clone()).await?;
        Ok(postcard::to_stdvec(&locator)?)
    }

    async fn get_blob(&self, blob: &[u8], range: Option<(u64, u64)>) -> Result<Bytes> {
        let locator: BlobLocator = postcard::from_bytes(blob).context("a document's blob handle")?;
        let stream = self.documents.get(&locator, range.map(|(s, e)| s..e)).instrument(self.span.clone()).await?;
        collect_stream(stream).await
    }

    async fn delete_blobs(&self, blobs: &[Vec<u8>]) -> Result<()> {
        let keys = blobs.iter().map(|b| Ok(postcard::from_bytes::<BlobLocator>(b)?.key)).collect::<Result<Vec<_>>>()?;
        self.documents.delete(&keys).instrument(self.span.clone()).await
    }
}

/// A fake Telegram channel on a tablet.
pub type TelegramChannel = FakeChannel<TabletHost<TelegramChannelState>>;
/// A fake Discord channel on a tablet.
pub type DiscordChannel = FakeDiscord<TabletHost<DiscordChannelState>>;

pub async fn telegram(chat: i64, stores: Stores) -> Result<TelegramChannel> {
    Ok(FakeChannel::on(chat, TabletHost::open(stores, &format!("telegram:{chat}")).await?))
}

pub async fn discord(channel: u64, stores: Stores) -> Result<DiscordChannel> {
    Ok(FakeDiscord::on(channel, TabletHost::open(stores, &format!("discord:{channel}")).await?))
}

mod opener;
pub mod view;
pub mod world;

pub use opener::Fakes;
pub use view::FakeView;
pub use world::World;

#[cfg(test)]
mod tests;
