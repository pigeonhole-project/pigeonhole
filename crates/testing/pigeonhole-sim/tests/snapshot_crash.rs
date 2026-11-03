//! Scenario 4 of docs/simulation.md: the process dies while a snapshot is
//! being taken. A snapshot is stored, the log is rebased on it, then the
//! snapshots of the bases the rebase dropped are removed; the crash comes
//! between the store and the rebase, between the rebase and the removal, or
//! during the removal. The next boot must find the snapshot its log names
//! and hold every acknowledged command; a third boot must agree with the
//! second. A crash leaves at most the one snapshot it was handling behind.

use pigeonhole_meta::{
    Base, BlobSnapshotStore, ChainLogStore, Discovered, Entry, Head, LogResult, LogStore, MetaOptions, MetaStore,
    Namespaces, Position, SnapshotRef, SnapshotStore, StoredSnapshot,
};
use pigeonhole_sim::{random_ms, random_u64, Journal, Record};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Where the process dies.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Crash {
    /// The snapshot is stored; the log is not rebased on it.
    BeforeRebase,
    /// The log is rebased; the dropped bases' snapshots are still there.
    AfterRebase,
    /// While removing a dropped base's snapshot.
    InRemoval,
}

/// Stores that stop the process at `crash`: from then on the call never
/// returns (the process is gone), and `hit` says so.
struct Dying {
    crash: Crash,
    armed: AtomicBool,
    hit: tokio::sync::Notify,
}

impl Dying {
    /// Die here if `at` is the crash point (once).
    async fn maybe_die(&self, at: Crash) {
        if self.crash == at && self.armed.swap(false, Ordering::SeqCst) {
            self.hit.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

struct DyingLog {
    inner: ChainLogStore<MemoryBlobStore>,
    dying: Arc<Dying>,
}

#[async_trait::async_trait]
impl LogStore for DyingLog {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        self.inner.claim(gen).await
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        self.inner.append(gen, batch).await
    }

    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()> {
        self.inner.truncate(gen, from).await
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        self.inner.save_vote(gen, vote).await
    }

    async fn confirm(&self, gen: u64) -> LogResult<()> {
        self.inner.confirm(gen).await
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        self.inner.discover().await
    }

    async fn read(&self, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
        self.inner.read(from, head).await
    }

    async fn rebase(&self, gen: u64, base: Base, keep: usize, unusable: &[Base]) -> LogResult<Vec<Base>> {
        self.dying.maybe_die(Crash::BeforeRebase).await;
        let dropped = self.inner.rebase(gen, base, keep, unusable).await?;
        if !dropped.is_empty() {
            self.dying.maybe_die(Crash::AfterRebase).await;
        }
        Ok(dropped)
    }

    async fn settle(&self) {
        self.inner.settle().await;
    }

    async fn release(&self, gen: u64) -> LogResult<()> {
        self.inner.release(gen).await
    }
}

struct DyingSnaps {
    inner: BlobSnapshotStore<MemoryBlobStore>,
    dying: Arc<Dying>,
}

#[async_trait::async_trait]
impl SnapshotStore for DyingSnaps {
    async fn put(&self, snapshot: &StoredSnapshot, after: Option<&SnapshotRef>) -> anyhow::Result<SnapshotRef> {
        self.inner.put(snapshot, after).await
    }

    async fn get(&self, r: &SnapshotRef) -> anyhow::Result<StoredSnapshot> {
        self.inner.get(r).await
    }

    async fn newest(&self) -> anyhow::Result<Option<StoredSnapshot>> {
        self.inner.newest().await
    }

    async fn remove(&self, r: &SnapshotRef) -> anyhow::Result<()> {
        self.dying.maybe_die(Crash::InRemoval).await;
        self.inner.remove(r).await
    }
}

const KEEP: usize = 1;

fn opts() -> MetaOptions {
    MetaOptions { snapshot_every: Some(5), keep_snapshots: KEEP, ..MetaOptions::default() }
}

async fn tablet(log: Arc<dyn LogStore>, snaps: Arc<dyn SnapshotStore>) -> Arc<MetaStore> {
    MetaStore::open(log, snaps, Namespaces::new().with::<Journal>(), opts()).await.expect("boots")
}

fn held(t: &MetaStore) -> Vec<u64> {
    t.read::<Journal, _>(|j| j.entries.keys().copied().collect())
}

/// Snapshots in `snaps` from before the oldest base the log keeps.
async fn stale_snapshots(log: &Arc<MemoryBlobStore>, snaps: &Arc<MemoryBlobStore>) -> Vec<String> {
    let d = ChainLogStore::new(log.clone()).discover().await.unwrap().unwrap();
    let watermark = d.older.last().or(d.base.as_ref()).map_or(0, |b| b.at);
    snaps
        .message_keys()
        .into_iter()
        .filter_map(|k| snaps.name_of(k))
        .filter(|name| name.ends_with(".snap"))
        .filter(|name| name.trim_end_matches(".snap").rsplit('-').next().unwrap().parse::<u64>().unwrap() < watermark)
        .collect()
}

async fn crash_during_a_snapshot(crash: Crash) {
    let (log, snaps) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
    let dying = Arc::new(Dying { crash, armed: AtomicBool::new(false), hit: tokio::sync::Notify::new() });
    let a = tablet(
        Arc::new(DyingLog { inner: ChainLogStore::new(log.clone()), dying: dying.clone() }),
        Arc::new(DyingSnaps { inner: BlobSnapshotStore::new(snaps.clone()), dying: dying.clone() }),
    )
    .await;

    // A few snapshots first, so the one that dies has a base to drop.
    let acked = Arc::new(Mutex::new(Vec::new()));
    let writer = {
        let (a, acked) = (a.clone(), acked.clone());
        tokio::spawn(async move {
            for id in 0..u64::MAX {
                tokio::time::sleep(random_ms(3)).await;
                if a.propose::<Journal>(Record { id, writer: "a".into() }).await.is_ok() {
                    acked.lock().unwrap().push(id);
                }
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(50 + random_u64() % 100)).await;
    dying.armed.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(30), dying.hit.notified()).await.expect("the crash point is reached");
    // A moment for whatever else was on its way, then the process is gone.
    tokio::time::sleep(random_ms(5)).await;
    writer.abort();
    drop(a);

    let plain = || -> (Arc<dyn LogStore>, Arc<dyn SnapshotStore>) {
        (Arc::new(ChainLogStore::new(log.clone())), Arc::new(BlobSnapshotStore::new(snaps.clone())))
    };
    let (l, s) = plain();
    let b = tablet(l, s).await;
    let acked = acked.lock().unwrap().clone();
    let held_b = held(&b);
    let lost: Vec<&u64> = acked.iter().filter(|id| !held_b.contains(id)).collect();
    assert!(lost.is_empty(), "{crash:?}: lost {lost:?}");
    // B goes on, snapshotting a few times over.
    for id in 1_000_000..1_000_000 + 30 {
        b.propose::<Journal>(Record { id, writer: "b".into() }).await.unwrap().unwrap();
    }
    pigeonhole_testkit::eventually("B snapshots past the crash", Duration::from_secs(10), || {
        b.base_position() + 5 >= b.applied_position()
    })
    .await;
    // Old snapshots are removed by a task of their own: give B's the time.
    // A crash leaves at most the one snapshot it was handling.
    let (log_r, snaps_r) = (&log, &snaps);
    let stale = || async move { stale_snapshots(log_r, snaps_r).await.len() <= 1 };
    pigeonhole_testkit::eventually_async("at most one snapshot left behind", Duration::from_secs(10), stale).await;
    let held_b = held(&b);
    drop(b);

    let (l, s) = plain();
    let c = tablet(l, s).await;
    assert_eq!(held(&c), held_b, "{crash:?}: a third boot disagrees");
}

#[tokio::test]
async fn a_crash_between_storing_a_snapshot_and_rebasing_on_it() {
    crash_during_a_snapshot(Crash::BeforeRebase).await;
}

#[tokio::test]
async fn a_crash_between_the_rebase_and_removing_the_old_snapshot() {
    crash_during_a_snapshot(Crash::AfterRebase).await;
}

#[tokio::test]
async fn a_crash_while_removing_the_old_snapshot() {
    crash_during_a_snapshot(Crash::InRemoval).await;
}
