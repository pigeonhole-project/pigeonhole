//! Contract tests every [`LogStore`] must pass (meta-backends.md § LogStore
//! contract). `open` returns a new handle onto the **same** log each call, as
//! a new process (or tablet incarnation) would get.

use super::{Base, Entry, Head, LogError, LogStore, Position};
use crate::snapshot::SnapshotRef;
use futures::TryStreamExt;
use std::future::Future;
use std::sync::Arc;

fn entry(i: u64) -> Entry {
    termed(1, i)
}

fn termed(term: u64, i: u64) -> Entry {
    Entry { term, payload: i.to_le_bytes().to_vec() }
}

fn base(at: Position) -> Base {
    Base { at, term: 1, leader: 0, snapshot: SnapshotRef(format!("s{at}")) }
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
    rebase_covers_and_collects(fresh()).await;
    reading_past_the_head_is_lost(fresh()).await;
    truncate_replaces_a_divergent_tail(fresh()).await;
    the_vote_survives_and_is_fenced(fresh()).await;
    a_rebase_past_the_head_empties_the_log(fresh()).await;
}

/// A follower installing a snapshot from elsewhere: the log jumps to it.
pub async fn a_rebase_past_the_head_empties_the_log<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    let head = log.claim(1).await.unwrap();
    log.append(1, &[entry(1), entry(2)]).await.unwrap();
    log.rebase(1, Base { at: 10, term: 4, leader: 0, snapshot: SnapshotRef("s10".into()) }).await.unwrap();
    assert_eq!(log.append(1, &[termed(5, 11)]).await.unwrap(), 11);

    let again = open().await;
    let d = again.discover().await.unwrap().unwrap();
    assert_eq!((d.base.map(|b| b.at), d.head.position, d.head_term), (Some(10), 11, 5));
    assert_eq!(read_all(again.as_ref(), 10, Head { position: 11, ..head }).await.unwrap(), vec![(11, termed(5, 11))]);
    assert!(matches!(read_all(again.as_ref(), 2, Head { position: 11, ..head }).await, Err(LogError::Lost(_))));
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
        a.rebase(1, base(1)).await,
        Err(LogError::Fenced { current: 2 })
    ));
    let head = b.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(head, 2);
    let d = b.discover().await.unwrap().unwrap();
    assert_eq!(read_all(b.as_ref(), 0, d.head).await.unwrap(), vec![(1, entry(1)), (2, entry(2))]);
}

/// The tablet rebases on a stored snapshot; entries after the base stay readable.
pub async fn rebase_covers_and_collects<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    log.claim(1).await.unwrap();
    for i in 1..=4 {
        log.append(1, &[entry(i)]).await.unwrap();
    }
    log.rebase(1, base(2)).await.unwrap();
    log.rebase(1, base(4)).await.unwrap();
    assert!(log.rebase(1, base(3)).await.is_err(), "a base never moves back");
    log.append(1, &[entry(5)]).await.unwrap();
    log.append(1, &[entry(6)]).await.unwrap();

    log.settle().await; // the collection may run in the background
    let again = open().await;
    let d = again.discover().await.unwrap().unwrap();
    assert_eq!(d.base, Some(base(4)));
    assert_eq!(d.head.position, 6);
    assert_eq!(read_all(again.as_ref(), 4, d.head).await.unwrap(), vec![(5, entry(5)), (6, entry(6))]);
    assert!(
        matches!(read_all(again.as_ref(), 2, d.head).await, Err(LogError::Lost(_))),
        "entries behind the base are collected"
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

/// A follower drops entries the leader does not have and takes the leader's.
pub async fn truncate_replaces_a_divergent_tail<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let log = open().await;
    log.claim(1).await.unwrap();
    for i in 1..=5 {
        log.append(1, &[entry(i)]).await.unwrap();
    }
    log.truncate(1, 4).await.unwrap();
    assert_eq!(log.append(1, &[termed(2, 40), termed(2, 50)]).await.unwrap(), 5);
    log.truncate(1, 6).await.unwrap(); // at head + 1: nothing to drop
    assert!(log.truncate(1, 7).await.is_err(), "past head + 1");

    let again = open().await;
    let d = again.discover().await.unwrap().unwrap();
    assert_eq!((d.head.position, d.head_term), (5, 2));
    let all = read_all(again.as_ref(), 0, d.head).await.unwrap();
    let want = vec![(1, entry(1)), (2, entry(2)), (3, entry(3)), (4, termed(2, 40)), (5, termed(2, 50))];
    assert_eq!(all, want);

    // A truncate right after a rebase may not reach behind it.
    let head = again.claim(2).await.unwrap();
    again.rebase(2, base(3)).await.unwrap();
    assert!(again.truncate(2, 3).await.is_err(), "behind the base");
    again.truncate(2, 4).await.unwrap();
    let d = again.discover().await.unwrap().unwrap();
    assert_eq!((d.head.position, d.head_term), (3, 1), "the head term is the base's");
    assert_eq!(again.append(2, &[termed(3, 4)]).await.unwrap(), 4);
    assert_eq!(read_all(again.as_ref(), 3, Head { position: 4, ..head }).await.unwrap(), vec![(4, termed(3, 4))]);
}

/// The vote is durable and only the claimed generation may change it.
pub async fn the_vote_survives_and_is_fenced<Open, Fut>(open: Open)
where
    Open: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn LogStore>>,
{
    let (a, b) = (open().await, open().await);
    a.claim(1).await.unwrap();
    assert_eq!(a.discover().await.unwrap().unwrap().vote, None);
    a.save_vote(1, b"term 1 for n1").await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    assert_eq!(open().await.discover().await.unwrap().unwrap().vote.as_deref(), Some(&b"term 1 for n1"[..]));

    a.confirm(1).await.unwrap();
    b.claim(2).await.unwrap();
    assert!(matches!(a.confirm(1).await, Err(LogError::Fenced { current: 2 })), "a lease check sees the fence");
    b.confirm(2).await.unwrap();
    assert!(matches!(a.save_vote(1, b"stale").await, Err(LogError::Fenced { current: 2 })));
    assert!(matches!(a.truncate(1, 1).await, Err(LogError::Fenced { current: 2 })));
    b.save_vote(2, b"term 2 for n2").await.unwrap();
    assert_eq!(open().await.discover().await.unwrap().unwrap().vote.as_deref(), Some(&b"term 2 for n2"[..]));
}
