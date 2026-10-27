//! Contract tests every [`LogStore`] must pass (meta-backends.md § LogStore
//! contract). `open` returns a new handle onto the **same** log each call, as
//! a new process (or tablet incarnation) would get.

use super::{Entry, Head, LogError, LogStore, Position, Snapshot};
use futures::TryStreamExt;
use std::future::Future;
use std::sync::Arc;

fn entry(i: u64) -> Entry {
    Entry { namespace: "t".into(), version: 1, command: i.to_le_bytes().to_vec() }
}

async fn read_all(log: &dyn LogStore, from: Position, head: Head) -> Result<Vec<(Position, Entry)>, LogError> {
    log.read(from, head).try_collect().await
}

/// Run the whole suite; each case gets fresh logs from `fresh()`, a factory of
/// `open` functions (one per empty log).
pub async fn run_all<New, Open, Fut>(fresh: New)
where
    New: Fn() -> Open,
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    append_read_and_reopen(fresh()).await;
    newer_claim_fences_older_generation(fresh()).await;
    snapshot_covers_and_collects(fresh()).await;
    reading_past_the_head_is_lost(fresh()).await;
}

pub async fn append_read_and_reopen<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    assert_eq!(log.discover().await.unwrap(), None, "a fresh log was never claimed");
    let head = log.claim(1).await.unwrap();
    assert_eq!(head.position, 0);
    assert_eq!(log.append(1, &[]).await.unwrap(), 0, "the zero entry takes no position");
    assert_eq!(log.append(1, &[entry(1), entry(2)]).await.unwrap(), 2);
    assert_eq!(log.append(1, &[entry(3)]).await.unwrap(), 3);

    let again = open().await;
    let d = again.discover().await.unwrap().expect("claimed");
    assert_eq!((d.gen, d.head), (1, Head { log_id: head.log_id, position: 3 }));
    let got = read_all(again.as_ref(), 1, d.head).await.unwrap();
    assert_eq!(got, vec![(2, entry(2)), (3, entry(3))]);

    let head2 = again.claim(2).await.unwrap();
    assert_eq!((head2.log_id, head2.position), (head.log_id, 3), "a claim keeps the log");
    assert_eq!(again.append(2, &[entry(4)]).await.unwrap(), 4);
    let all = read_all(again.as_ref(), 0, Head { position: 4, ..head2 }).await.unwrap();
    assert_eq!(all.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
}

pub async fn newer_claim_fences_older_generation<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let (a, b) = (open().await, open().await);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();

    assert!(matches!(b.claim(1).await, Err(LogError::Fenced { current: 1 })));
    assert_eq!(b.claim(2).await.unwrap().position, 1, "acknowledged entries are ≤ the head");
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
    assert!(matches!(
        a.install_snapshot(1, 1, Snapshot { data: vec![] }).await,
        Err(LogError::Fenced { current: 2 })
    ));
    let head = b.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(head, 2);
    let d = b.discover().await.unwrap().unwrap();
    assert_eq!(read_all(b.as_ref(), 0, d.head).await.unwrap(), vec![(1, entry(1)), (2, entry(2))]);
}

/// The tablet snapshots at its head; entries after the snapshot stay readable.
pub async fn snapshot_covers_and_collects<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    log.claim(1).await.unwrap();
    for i in 1..=4 {
        log.append(1, &[entry(i)]).await.unwrap();
    }
    log.install_snapshot(1, 4, Snapshot { data: b"s4".to_vec() }).await.unwrap();
    log.append(1, &[entry(5)]).await.unwrap();
    log.append(1, &[entry(6)]).await.unwrap();

    let again = open().await;
    let d = again.discover().await.unwrap().unwrap();
    assert_eq!(d.snapshot, Some((4, Snapshot { data: b"s4".to_vec() })));
    assert_eq!(d.head.position, 6);
    assert_eq!(read_all(again.as_ref(), 4, d.head).await.unwrap(), vec![(5, entry(5)), (6, entry(6))]);
    assert!(
        matches!(read_all(again.as_ref(), 2, d.head).await, Err(LogError::Lost(_))),
        "entries behind the snapshot are collected"
    );
    assert_eq!(again.claim(2).await.unwrap().position, 6);
}

pub async fn reading_past_the_head_is_lost<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    let head = log.claim(1).await.unwrap();
    log.append(1, &[entry(1)]).await.unwrap();
    let beyond = Head { position: 3, ..head };
    assert!(matches!(read_all(log.as_ref(), 0, beyond).await, Err(LogError::Lost(_))));
    assert_eq!(read_all(log.as_ref(), 1, Head { position: 1, ..head }).await.unwrap(), vec![]);
}
