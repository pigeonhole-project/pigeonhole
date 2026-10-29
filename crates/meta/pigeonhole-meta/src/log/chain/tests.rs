use super::*;
use crate::log::conformance;
use futures::TryStreamExt;
use pigeonhole_storage_memory::MemoryBlobStore;

fn store(mem: &Arc<MemoryBlobStore>) -> Arc<ChainLogStore<MemoryBlobStore>> {
    Arc::new(ChainLogStore::new(mem.clone()))
}

fn entry(i: u64) -> Entry {
    Entry { term: 1, payload: i.to_le_bytes().to_vec() }
}

async fn history(log: &dyn LogStore) -> Vec<u64> {
    let d = log.discover().await.unwrap().unwrap();
    log.read(0, d.head)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|(_, e)| u64::from_le_bytes(e.payload.try_into().unwrap()))
        .collect()
}

#[tokio::test]
async fn passes_the_logstore_conformance_suite() {
    conformance::run_all(|| {
        let mem = Arc::new(MemoryBlobStore::new());
        move || {
            let mem = mem.clone();
            async move { Arc::new(ChainLogStore::new(mem)) as Arc<dyn LogStore> }
        }
    })
    .await;
}

/// A deposed writer's put names a blob that already has a successor: it
/// loses, learns the newer generation, and its blob is never read.
#[tokio::test]
async fn a_deposed_writer_loses_its_next_link() {
    let mem = Arc::new(MemoryBlobStore::new());
    let (a, b) = (store(&mem), store(&mem));
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    b.claim(2).await.unwrap();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
    assert!(matches!(a.append(1, &[entry(3)]).await, Err(LogError::Fenced { current: 2 })), "it stays fenced");
    b.append(2, &[entry(4)]).await.unwrap();
    assert_eq!(history(store(&mem).as_ref()).await, vec![1, 4]);
}

/// Two claimants of the same generation: the first link wins.
#[tokio::test]
async fn racing_claims_leave_one_writer() {
    let mem = Arc::new(MemoryBlobStore::new());
    let first = store(&mem);
    first.claim(1).await.unwrap();
    first.append(1, &[entry(1)]).await.unwrap();

    let (b, c) = (store(&mem), store(&mem));
    let (rb, rc) = tokio::join!(b.claim(2), c.claim(2));
    let won = [&rb, &rc].iter().filter(|r| r.is_ok()).count();
    assert_eq!(won, 1, "{rb:?} {rc:?}");
    assert!([&rb, &rc].iter().any(|r| matches!(r, Err(LogError::Fenced { current: 2 }))));
    let winner = if rb.is_ok() { b } else { c };
    winner.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(history(store(&mem).as_ref()).await, vec![1, 2]);
}

/// A claim that loses to an older generation's append claims after it.
#[tokio::test]
async fn a_claim_follows_an_older_generations_late_append() {
    let mem = Arc::new(MemoryBlobStore::new());
    let a = store(&mem);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // The claimant read the tail before the append below landed.
    let tail = mem.tail().await.unwrap().unwrap();
    a.append(1, &[entry(2)]).await.unwrap();
    let After::Lost(winner) = mem.put_after(Some(&tail.id), "log-claim-2", Bytes::from_static(b"x")).await.unwrap() else {
        panic!("the append took the place first")
    };
    assert_eq!(winner.label, "log-batch-1");
    // The log store retries from the new tail on its own.
    let b = store(&mem);
    assert_eq!(b.claim(2).await.unwrap().position, 2);
    assert_eq!(history(b.as_ref()).await, vec![1, 2]);
}

/// A rebase prunes what no reader needs, the loser blobs included.
#[tokio::test]
async fn a_rebase_prunes_below_the_oldest_needed_blob() {
    let mem = Arc::new(MemoryBlobStore::new());
    let a = store(&mem);
    a.claim(1).await.unwrap();
    for i in 1..=4 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    let before = mem.message_keys().len();
    let base = Base { at: 3, term: 1, leader: 0, snapshot: crate::snapshot::SnapshotRef("s3".into()) };
    a.rebase(1, base, 1).await.unwrap();
    a.settle().await; // the collection runs in the background
    assert!(mem.message_keys().len() < before, "{:?}", mem.message_keys());
    a.append(1, &[entry(5)]).await.unwrap();
    let again = store(&mem);
    let d = again.discover().await.unwrap().unwrap();
    let got = again.read(3, d.head).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5]);
}

/// A rebase returns once the base is written; the collection behind it
/// does not hold the writer (on a chat it deletes for minutes).
#[tokio::test]
async fn a_rebase_does_not_wait_for_its_collection() {
    let mem = Arc::new(MemoryBlobStore::new());
    let a = store(&mem);
    a.claim(1).await.unwrap();
    for i in 1..=4 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    let gate = mem.hold_deletes();
    let base = Base { at: 3, term: 1, leader: 0, snapshot: crate::snapshot::SnapshotRef("s3".into()) };
    tokio::time::timeout(std::time::Duration::from_secs(5), a.rebase(1, base, 1)).await.expect("rebase waited for its prune").unwrap();
    a.append(1, &[entry(5)]).await.unwrap();
    drop(gate);
    a.settle().await;
    let d = store(&mem).discover().await.unwrap().unwrap();
    assert_eq!(d.head.position, 5);
}

/// The registers travel with every blob: the tail alone answers discover.
#[tokio::test]
async fn discover_reads_only_the_tail() {
    let mem = Arc::new(MemoryBlobStore::new());
    let a = store(&mem);
    let head = a.claim(1).await.unwrap();
    a.save_vote(1, b"v").await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let d = store(&mem).discover().await.unwrap().unwrap();
    assert_eq!((d.head, d.gen, d.vote.as_deref()), (Head { position: 1, ..head }, 1, Some(&b"v"[..])));
}

mod disk {
    use super::*;
    use pigeonhole_storage_disk::DiskBlobStore;
    use std::time::{Duration, Instant};

    fn disk(dir: &std::path::Path) -> Arc<DiskBlobStore> {
        Arc::new(DiskBlobStore::open(dir).unwrap())
    }

    #[tokio::test]
    async fn passes_the_logstore_conformance_suite_on_disk() {
        conformance::run_all(|| {
            let dir = Arc::new(tempfile::tempdir().unwrap());
            move || {
                let dir = dir.clone();
                async move { Arc::new(ChainLogStore::new(disk(dir.path()))) as Arc<dyn LogStore> }
            }
        })
        .await;
    }

    #[tokio::test]
    async fn a_deposed_writer_in_another_handle_loses_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (ChainLogStore::new(disk(dir.path())), ChainLogStore::new(disk(dir.path())));
        a.claim(1).await.unwrap();
        a.append(1, &[entry(1)]).await.unwrap();
        b.claim(2).await.unwrap();
        assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
        b.append(2, &[entry(3)]).await.unwrap();
        assert_eq!(history(&ChainLogStore::new(disk(dir.path()))).await, vec![1, 3]);
    }

    fn pct(mut v: Vec<Duration>, p: f64) -> Duration {
        v.sort();
        v[((v.len() as f64 - 1.0) * p).round() as usize]
    }

    /// The disk log: append latency (one entry, fsync),
    /// open + claim, replay of 1M entries, rebase.
    /// `cargo test --release -p pigeonhole-meta bench_disk_logs -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "a benchmark"]
    async fn bench_disk_logs() {
        async fn run(name: &str, open: &dyn Fn(&std::path::Path) -> Arc<dyn LogStore>) {
            let dir = tempfile::tempdir().unwrap();
            let log = open(dir.path());
            log.claim(1).await.unwrap();
            let mut lat = Vec::new();
            for i in 0..1000u64 {
                let t = Instant::now();
                log.append(1, &[entry(i)]).await.unwrap();
                lat.push(t.elapsed());
            }
            let (p50, p99) = (pct(lat.clone(), 0.5), pct(lat, 0.99));

            // 1M entries of 100 bytes in batches of 1000.
            let batch: Vec<Entry> = (0..1000u64).map(|_| Entry { term: 1, payload: vec![7; 100] }).collect();
            let t = Instant::now();
            for _ in 0..1000 {
                log.append(1, &batch).await.unwrap();
            }
            let fill = t.elapsed();

            let t = Instant::now();
            let again = open(dir.path());
            let head = again.claim(2).await.unwrap();
            let claim = t.elapsed();
            let t = Instant::now();
            let n = again.read(0, head).try_fold(0u64, |n, _| async move { Ok(n + 1) }).await.unwrap();
            let replay = t.elapsed();
            assert_eq!(n, head.position);

            let t = Instant::now();
            let base = crate::log::Base {
                at: head.position - 10,
                term: 1,
                leader: 0,
                snapshot: crate::snapshot::SnapshotRef("s".into()),
            };
            again.rebase(2, base, 1).await.unwrap();
            let rebase = t.elapsed();
            println!(
                "{name:>10}: append p50 {p50:?} p99 {p99:?}; 1M entries written in {fill:?}; open + claim {claim:?}; \
                 replay {n} entries {replay:?}; rebase {rebase:?}"
            );
        }
        run("disk chain", &|d| Arc::new(ChainLogStore::new(Arc::new(DiskBlobStore::open(d).unwrap())))).await;
    }
}

/// `confirm` racing our own append: the append is linked but not yet our
/// tip. That successor is ours, not a fence; a newer claim after it is.
#[tokio::test]
async fn confirm_looks_past_our_own_append_in_flight() {
    let mem = Arc::new(MemoryBlobStore::new());
    let a = store(&mem);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // What an append in flight leaves: a batch of generation 1 after the tip.
    let tip = mem.tail().await.unwrap().unwrap();
    let After::Won(ours) = mem.put_after(Some(&tip.id), "log-batch-1", Bytes::from_static(b"in flight")).await.unwrap() else {
        panic!("the tip has no successor yet")
    };
    a.confirm(1).await.unwrap();
    // A newer claim after it fences.
    mem.put_after(Some(&ours), "log-claim-2", Bytes::from_static(b"claim")).await.unwrap();
    assert!(matches!(a.confirm(1).await, Err(LogError::Fenced { current: 2 })));
}
