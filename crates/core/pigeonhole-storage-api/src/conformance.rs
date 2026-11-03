//! Whether a decorator forwards every method (feature `conformance`).
//!
//! A decorator (`Arc<B>`, a store with faults injected, the blob layer's
//! erasure and metrics) wraps a store and must call the store's own method
//! for each of its own: the traits have no provided methods, so it has to
//! write each one, and these checks see that each one reaches the store.
//! [`Recording`] is a store that notes every call; [`check_blob_backend`],
//! [`check_chain`] and [`check_lease_chain`] call each method through the
//! decorator and expect exactly that method, once, at the store. A method
//! added to a trait goes in [`Recording`] (the compiler asks) and in the
//! check of its trait.

use crate::{
    bytes_stream, After, BlobBackend, BoxByteStream, Chain, CostHint, InstanceInfo, InstanceRole, LeaseChain, Linked,
    MakeAfter, OpKind, Owner,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{BackendLimits, ByteRange, RangeSupport};
use std::sync::Mutex;

/// A store that answers every call with a fixed value and notes its name.
pub struct Recording {
    info: InstanceInfo,
    limits: BackendLimits,
    calls: Mutex<Vec<&'static str>>,
}

impl Default for Recording {
    fn default() -> Self {
        Self {
            info: InstanceInfo {
                id: "recording".into(),
                kind: "recording",
                fingerprint: "recording".into(),
                location: "recording".into(),
                role: InstanceRole::ReadWrite,
            },
            limits: BackendLimits { max_blob_size: MAX_BLOB_SIZE, supports_range: RangeSupport::BestEffort },
            calls: Mutex::default(),
        }
    }
}

impl Recording {
    /// The calls since the last take, in order.
    pub fn take_calls(&self) -> Vec<&'static str> {
        std::mem::take(&mut self.calls.lock().unwrap())
    }

    fn note(&self, method: &'static str) {
        self.calls.lock().unwrap().push(method);
    }

    /// What [`BlobBackend::key`] makes of an id: not the id itself, so that a
    /// decorator's own mapping shows.
    pub const fn key_of(id: u64) -> u64 {
        id + KEY_OFFSET
    }

    fn linked(id: u64) -> Linked<u64> {
        Linked { id, label: "recorded".into() }
    }
}

const KEY_OFFSET: u64 = 1000;
/// The ids and values [`Recording`] answers with.
pub const PUT_ID: u64 = 11;
pub const OWNER: Owner = Owner(0xfeed);
pub const NOW_MS: u64 = 4242;
pub const DATA: &[u8] = b"recorded";
pub const MAX_BLOB_SIZE: usize = 12345;

#[async_trait]
impl BlobBackend for Recording {
    type Id = u64;
    type Key = u64;

    // Not noted: decorators read them for their own needs (a metrics
    // label); the values tell whose they are.
    fn instance(&self) -> &InstanceInfo {
        &self.info
    }
    fn limits(&self) -> &BackendLimits {
        &self.limits
    }
    fn key(id: &u64) -> u64 {
        Self::key_of(*id)
    }
    fn cost(&self, _op: OpKind, _id: Option<&u64>) -> CostHint {
        self.note("cost");
        CostHint { wait_secs: 7.0 }
    }
    async fn put(&self, _data: Bytes, _name: &str) -> Result<u64> {
        self.note("put");
        Ok(PUT_ID)
    }
    async fn get(&self, _id: &u64, _range: Option<ByteRange>) -> Result<BoxByteStream> {
        self.note("get");
        Ok(bytes_stream(Bytes::from_static(DATA)))
    }
    async fn delete(&self, _keys: &[u64]) -> Result<()> {
        self.note("delete");
        Ok(())
    }
    async fn candidates(&self, _owner: Owner, _after: Option<u64>, _upto: u64, _limit: usize) -> Result<Vec<u64>> {
        self.note("candidates");
        Ok(vec![Self::key_of(PUT_ID)])
    }
    async fn mark_owner(&self, _owner: Owner) -> Result<Owner> {
        self.note("mark_owner");
        Ok(OWNER)
    }
    async fn exists(&self, _id: &u64) -> Result<bool> {
        self.note("exists");
        Ok(false)
    }
}

#[async_trait]
impl Chain for Recording {
    async fn put_after(&self, _prev: Option<&u64>, _label: &str, _data: Bytes) -> Result<After<u64>> {
        self.note("put_after");
        Ok(After::Won(PUT_ID))
    }
    async fn tail(&self) -> Result<Option<Linked<u64>>> {
        self.note("tail");
        Ok(Some(Self::linked(PUT_ID)))
    }
    async fn successor(&self, _id: &u64) -> Result<Option<Linked<u64>>> {
        self.note("successor");
        Ok(Some(Self::linked(PUT_ID)))
    }
    async fn prune(&self, _below: &u64) -> Result<()> {
        self.note("prune");
        Ok(())
    }
    async fn put_after_tail(
        &self,
        _hint: Option<&u64>,
        _label: &str,
        _make: &MakeAfter<'_, u64>,
    ) -> Result<Option<u64>> {
        self.note("put_after_tail");
        Ok(Some(PUT_ID))
    }
    async fn put_loose(&self, _after: Option<&u64>, _label: &str, _data: Bytes) -> Result<u64> {
        self.note("put_loose");
        Ok(PUT_ID)
    }
    async fn newest_loose(&self, _prefix: &str) -> Result<Option<Linked<u64>>> {
        self.note("newest_loose");
        Ok(Some(Self::linked(PUT_ID)))
    }
}

#[async_trait]
impl LeaseChain for Recording {
    fn stamp_ms(&self, _id: &u64) -> u64 {
        self.note("stamp_ms");
        NOW_MS
    }
    async fn now_ms(&self) -> Result<u64> {
        self.note("now_ms");
        Ok(NOW_MS)
    }
    async fn put_unlinked(&self, _prev: &u64, _label: &str, _data: Bytes) -> Result<u64> {
        self.note("put_unlinked");
        Ok(PUT_ID)
    }
    async fn list_after(&self, _after: Option<&u64>, _prefix: &str) -> Result<Vec<Linked<u64>>> {
        self.note("list_after");
        Ok(vec![Self::linked(PUT_ID)])
    }
}

/// Expects `method`, once, to be what reached `store` since the last take.
#[track_caller]
pub fn reached(store: &Recording, method: &str) {
    assert_eq!(store.take_calls(), vec![method], "{method} through the decorator");
}

/// Every [`BlobBackend`] method of `d`, a decorator over `store`, reaches it.
pub async fn check_blob_backend<D: BlobBackend<Id = u64, Key = u64>>(store: &Recording, d: &D) -> Result<()> {
    store.take_calls();
    assert_eq!(D::key(&3), Recording::key_of(3), "key");
    assert_eq!(d.instance().id, "recording", "instance");
    assert_eq!(d.limits().max_blob_size, MAX_BLOB_SIZE, "limits");
    assert_eq!(d.cost(OpKind::Get, Some(&3)).wait_secs, 7.0);
    reached(store, "cost");
    assert_eq!(d.put(Bytes::from_static(b"x"), "x.bin").await?, PUT_ID);
    reached(store, "put");
    assert_eq!(crate::collect_stream(d.get(&PUT_ID, Some(0..1)).await?).await?.as_ref(), DATA);
    reached(store, "get");
    d.delete(&[Recording::key_of(PUT_ID)]).await?;
    reached(store, "delete");
    assert_eq!(d.candidates(Owner(0), None, u64::MAX, 10).await?, vec![Recording::key_of(PUT_ID)]);
    reached(store, "candidates");
    assert_eq!(d.mark_owner(Owner(1)).await?, OWNER);
    reached(store, "mark_owner");
    assert!(!d.exists(&PUT_ID).await?, "exists answers what the store does");
    reached(store, "exists");
    Ok(())
}

/// Every [`Chain`] method of `d`, a decorator over `store`, reaches it.
pub async fn check_chain<D: Chain<Id = u64, Key = u64>>(store: &Recording, d: &D) -> Result<()> {
    store.take_calls();
    assert_eq!(d.put_after(Some(&1), "a", Bytes::from_static(b"x")).await?, After::Won(PUT_ID));
    reached(store, "put_after");
    assert_eq!(d.tail().await?.map(|t| t.id), Some(PUT_ID));
    reached(store, "tail");
    assert_eq!(d.successor(&1).await?.map(|t| t.id), Some(PUT_ID));
    reached(store, "successor");
    d.prune(&1).await?;
    reached(store, "prune");
    let make: &MakeAfter<'_, u64> = &|_| Ok(Some(Bytes::from_static(b"x")));
    assert_eq!(d.put_after_tail(Some(&1), "a", make).await?, Some(PUT_ID));
    reached(store, "put_after_tail");
    assert_eq!(d.put_loose(Some(&1), "a", Bytes::from_static(b"x")).await?, PUT_ID);
    reached(store, "put_loose");
    assert_eq!(d.newest_loose("a").await?.map(|t| t.id), Some(PUT_ID));
    reached(store, "newest_loose");
    Ok(())
}

/// Every [`LeaseChain`] method of `d`, a decorator over `store`, reaches it.
pub async fn check_lease_chain<D: LeaseChain<Id = u64, Key = u64>>(store: &Recording, d: &D) -> Result<()> {
    store.take_calls();
    assert_eq!(d.stamp_ms(&1), NOW_MS);
    reached(store, "stamp_ms");
    assert_eq!(d.now_ms().await?, NOW_MS);
    reached(store, "now_ms");
    assert_eq!(d.put_unlinked(&1, "a", Bytes::from_static(b"x")).await?, PUT_ID);
    reached(store, "put_unlinked");
    assert_eq!(d.list_after(None, "a").await?.len(), 1);
    reached(store, "list_after");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn an_arc_forwards_every_method() {
        let store = Arc::new(Recording::default());
        check_blob_backend(&store, &store.clone()).await.unwrap();
        check_chain(&store, &store.clone()).await.unwrap();
        check_lease_chain(&store, &store.clone()).await.unwrap();
    }
}
