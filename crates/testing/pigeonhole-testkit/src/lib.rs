//! Conformance suites for pigeonhole storage backends.

use anyhow::Result;
use bytes::Bytes;
use pigeonhole_blob::{collect_stream, BlobBackend, BlobId, Owner};

/// Shared suite for [`BlobBackend`].
///
/// Covers: put/get/range/delete, repeat delete, empty rejected, near-`max_blob_size`
/// blob, monotonic keys, candidates covering written keys.
pub async fn run_typed_conformance<B>(backend: &B) -> Result<()>
where
    B: BlobBackend,
{
    // empty rejected
    assert!(backend.put(Bytes::new(), &BlobId::random().name()).await.is_err(), "empty blob must be rejected");

    let id = backend.put(Bytes::from_static(b"hello-typed-conformance"), &BlobId::random().name()).await?;
    let got = collect_stream(backend.get(&id, None).await?).await?;
    assert_eq!(got.as_ref(), b"hello-typed-conformance");

    let mid = collect_stream(backend.get(&id, Some(6..18)).await?).await?;
    assert_eq!(mid.as_ref(), b"typed-confor");

    // Near max (capped for test runtime; oversized still uses true max).
    let max = backend.limits().max_blob_size.min(64 * 1024);
    let big = Bytes::from(vec![0xABu8; max]);
    let big_id = backend.put(big.clone(), &BlobId::random().name()).await?;
    let big_got = collect_stream(backend.get(&big_id, None).await?).await?;
    assert_eq!(big_got, big);

    let over = Bytes::from(vec![0u8; backend.limits().max_blob_size + 1]);
    assert!(backend.put(over, &BlobId::random().name()).await.is_err());

    // Monotonic keys across successive puts; one blob of another owner.
    let mut ids = vec![id.clone(), big_id.clone()];
    for i in 0..4u8 {
        ids.push(backend.put(Bytes::from(vec![i; 32]), &BlobId::random().name()).await?);
    }
    let other = Owner(7);
    let theirs = backend.put(Bytes::from_static(b"another deployment's"), &BlobId::random_of(other).name()).await?;
    ids.push(backend.put(Bytes::from_static(b"ours again"), &BlobId::random().name()).await?);
    for w in ids.windows(2) {
        assert!(B::key(&w[0]) < B::key(&w[1]), "keys must grow: {:?} then {:?}", B::key(&w[0]), B::key(&w[1]));
    }

    let max_key = B::key(ids.last().unwrap());
    let written: Vec<_> = ids.iter().map(B::key).collect();
    let theirs_key = B::key(&theirs);
    let only_theirs = backend.candidates(other, None, max_key, 100).await?;
    assert_eq!(only_theirs, vec![theirs_key], "another owner's candidates are its blobs only");
    // Page candidates so backends with dense synthetic ranges (Telegram) still
    // reach high message ids within a reasonable limit per call.
    let mut found = Vec::new();
    let mut after = None;
    for _ in 0..256 {
        let batch = backend.candidates(Owner(0), after, max_key, 100).await?;
        if batch.is_empty() {
            break;
        }
        after = Some(*batch.last().unwrap());
        found.extend(batch);
        if written.iter().all(|k| found.contains(k)) {
            break;
        }
    }
    for k in &written {
        assert!(found.contains(k), "candidates missing key {k:?}; got {} keys up to {:?}", found.len(), found.last());
    }
    assert!(!found.contains(&theirs_key), "another owner's blob is never a candidate");

    // Delete + repeat delete (not-found = ok).
    let del_key = B::key(&id);
    backend.delete(&[del_key]).await?;
    assert!(backend.get(&id, None).await.is_err());
    backend.delete(&[del_key]).await?;

    // Cleanup remaining blobs (best-effort).
    let mut rest: Vec<_> = ids.iter().skip(1).map(B::key).collect();
    rest.push(theirs_key);
    let _ = backend.delete(&rest).await;
    Ok(())
}

/// A tablet over a disk log in `dir/log` with snapshots in `dir/snapshots`
/// (as a `local_disk` `[meta]` store), namespaces added by `register`;
/// reopening the same `dir` boots from what is there.
pub async fn tablet(
    dir: impl AsRef<std::path::Path>,
    register: impl FnOnce(pigeonhole_meta::Namespaces) -> pigeonhole_meta::Namespaces,
) -> std::sync::Arc<pigeonhole_meta::MetaStore> {
    let (log, snapshots) = disk_meta(dir);
    pigeonhole_meta::MetaStore::open(log, snapshots, register(pigeonhole_meta::Namespaces::new()), Default::default())
        .await
        .expect("boot a test tablet")
}

/// The log and snapshot stores of a `local_disk` `[meta]` store at `dir`.
pub fn disk_meta(
    dir: impl AsRef<std::path::Path>,
) -> (std::sync::Arc<dyn pigeonhole_meta::LogStore>, std::sync::Arc<dyn pigeonhole_meta::SnapshotStore>) {
    use pigeonhole_storage_disk::DiskBlobStore;
    use std::sync::Arc;
    let dir = dir.as_ref();
    let log = Arc::new(DiskBlobStore::open(dir.join("log")).expect("open the log directory"));
    let snapshots = Arc::new(DiskBlobStore::open(dir.join("snapshots")).expect("open the snapshot directory"));
    (Arc::new(pigeonhole_meta::ChainLogStore::new(log)), Arc::new(pigeonhole_meta::BlobSnapshotStore::new(snapshots)))
}

/// A log whose appends can be held back: a proposal stays in flight (in
/// the batcher, its entry not yet written) for as long as a test likes.
pub struct GatedLog {
    inner: std::sync::Arc<dyn pigeonhole_meta::LogStore>,
    open: tokio::sync::watch::Sender<bool>,
    parked: tokio::sync::Notify,
}

impl GatedLog {
    pub fn new(inner: std::sync::Arc<dyn pigeonhole_meta::LogStore>) -> Self {
        Self { inner, open: tokio::sync::watch::Sender::new(true), parked: tokio::sync::Notify::new() }
    }

    /// Appends from now on wait until [`Self::open`].
    pub fn close(&self) {
        self.open.send_replace(false);
    }

    /// Let the appends waiting, and those to come, through.
    pub fn open(&self) {
        self.open.send_replace(true);
    }

    /// Wait until an append waits at the closed gate.
    pub async fn parked(&self) {
        self.parked.notified().await;
    }
}

#[async_trait::async_trait]
impl pigeonhole_meta::LogStore for GatedLog {
    async fn claim(&self, gen: u64) -> pigeonhole_meta::LogResult<pigeonhole_meta::Head> {
        self.inner.claim(gen).await
    }

    async fn append(
        &self,
        gen: u64,
        batch: &[pigeonhole_meta::Entry],
    ) -> pigeonhole_meta::LogResult<pigeonhole_meta::Position> {
        let mut open = self.open.subscribe();
        if !*open.borrow() {
            self.parked.notify_one();
            let _ = open.wait_for(|&o| o).await;
        }
        self.inner.append(gen, batch).await
    }

    async fn truncate(&self, gen: u64, from: pigeonhole_meta::Position) -> pigeonhole_meta::LogResult<()> {
        self.inner.truncate(gen, from).await
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> pigeonhole_meta::LogResult<()> {
        self.inner.save_vote(gen, vote).await
    }

    async fn confirm(&self, gen: u64) -> pigeonhole_meta::LogResult<()> {
        self.inner.confirm(gen).await
    }

    async fn discover(&self) -> pigeonhole_meta::LogResult<Option<pigeonhole_meta::Discovered>> {
        self.inner.discover().await
    }

    async fn read(
        &self,
        from: pigeonhole_meta::Position,
        head: pigeonhole_meta::Head,
    ) -> pigeonhole_meta::LogResult<Vec<(pigeonhole_meta::Position, pigeonhole_meta::Entry)>> {
        self.inner.read(from, head).await
    }

    async fn rebase(
        &self,
        gen: u64,
        base: pigeonhole_meta::Base,
        keep: usize,
        unusable: &[pigeonhole_meta::Base],
    ) -> pigeonhole_meta::LogResult<Vec<pigeonhole_meta::Base>> {
        self.inner.rebase(gen, base, keep, unusable).await
    }

    async fn settle(&self) {
        self.inner.settle().await;
    }

    async fn release(&self, gen: u64) -> pigeonhole_meta::LogResult<()> {
        self.inner.release(gen).await
    }
}

/// [`tablet`] over a [`GatedLog`] (open), and the gate.
pub async fn gated_tablet(
    dir: impl AsRef<std::path::Path>,
    register: impl FnOnce(pigeonhole_meta::Namespaces) -> pigeonhole_meta::Namespaces,
) -> (std::sync::Arc<pigeonhole_meta::MetaStore>, std::sync::Arc<GatedLog>) {
    let (log, snapshots) = disk_meta(dir);
    let gate = std::sync::Arc::new(GatedLog::new(log));
    let meta = pigeonhole_meta::MetaStore::open(
        gate.clone(),
        snapshots,
        register(pigeonhole_meta::Namespaces::new()),
        Default::default(),
    )
    .await
    .expect("boot a test tablet");
    (meta, gate)
}
