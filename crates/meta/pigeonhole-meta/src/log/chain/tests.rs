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
    a.rebase(1, base).await.unwrap();
    assert!(mem.message_keys().len() < before, "{:?}", mem.message_keys());
    a.append(1, &[entry(5)]).await.unwrap();
    let again = store(&mem);
    let d = again.discover().await.unwrap().unwrap();
    let got = again.read(3, d.head).try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5]);
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
