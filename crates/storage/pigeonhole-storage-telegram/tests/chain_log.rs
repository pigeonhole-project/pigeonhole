//! The generic chain log and snapshots over a channel (the Telegram
//! [`Chain`]: first successor by id order), on a fake channel.

use futures::TryStreamExt;
use pigeonhole_blob::{ChatLimiter, ChatLimiterConfig, InstanceInfo, InstanceKind, InstanceRole};
use pigeonhole_meta::log::conformance;
use pigeonhole_meta::snapshot::conformance as snapshot_conformance;
use pigeonhole_meta::{Base, BlobSnapshotStore, ChainLogStore, Entry, LogError, LogStore, SnapshotRef, SnapshotStore};
use pigeonhole_storage_telegram::fake::FakeChannel;
use pigeonhole_storage_telegram::{ChainOptions, TelegramBlobStore};
use std::sync::Arc;
use std::time::Duration;

type Store = TelegramBlobStore<FakeChannel>;

fn channel_store(channel: &Arc<FakeChannel>) -> Arc<Store> {
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig {
        bot_rate_per_sec: 1000.0,
        send_rate_per_sec: 1000.0,
        info_rate_per_sec: 1000.0,
        delete_rate_per_sec: 1000.0,
        ..ChatLimiterConfig::default()
    }));
    let info = InstanceInfo {
        id: "log".into(),
        kind: InstanceKind::Telegram,
        fingerprint: "tg:1:77".into(),
        location: "tg:chat:-10077".into(),
        role: InstanceRole::ReadWrite,
    };
    let opts = ChainOptions { empty_retries: 2, empty_retry_delay: Duration::from_millis(1) };
    Arc::new(TelegramBlobStore::new(channel.clone(), limiter, info).with_chain_options(opts))
}

fn store(channel: &Arc<FakeChannel>) -> Arc<ChainLogStore<Store>> {
    Arc::new(ChainLogStore::new(channel_store(channel)))
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

fn base(at: u64) -> Base {
    Base { at, term: 1, leader: 0, snapshot: SnapshotRef(format!("s{at}")) }
}

#[tokio::test]
async fn passes_the_logstore_conformance_suite() {
    conformance::run_all(|| {
        let channel = Arc::new(FakeChannel::new(77));
        move || {
            let channel = channel.clone();
            async move { store(&channel) as Arc<dyn LogStore> }
        }
    })
    .await;
}

#[tokio::test]
async fn passes_the_snapshot_conformance_suite() {
    let channel = Arc::new(FakeChannel::new(78));
    snapshot_conformance::run_all(|| {
        let channel = channel.clone();
        async move { Arc::new(BlobSnapshotStore::new(channel_store(&channel))) as Arc<dyn SnapshotStore> }
    })
    .await;
}

#[tokio::test]
async fn appends_in_a_dedicated_channel_read_nothing() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    let reads = channel.calls().get_messages;
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    assert_eq!(channel.calls().get_messages, reads, "each batch lands right above the last: one send per append");
}

#[tokio::test]
async fn a_gap_is_read_and_foreign_messages_are_skipped() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    channel.post_text();
    channel.post_document("photo.jpg", b"x");
    let reads = channel.calls().get_messages;
    a.append(1, &[entry(2)]).await.unwrap();
    assert_eq!(channel.calls().get_messages, reads + 1, "one read of the two ids in between");
    assert_eq!(history(store(&channel).as_ref()).await, vec![1, 2]);
}

#[tokio::test]
async fn a_deposed_writers_batch_loses_for_everyone() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 1);

    // A's next batch names A's tip, which B's claim already follows.
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    assert!(matches!(a.append(1, &[entry(98)]).await, Err(LogError::Fenced { current: 2 })), "stays fenced");

    // The straggler is the top message, yet no reader takes it.
    assert_eq!(history(store(&channel).as_ref()).await, vec![1]);
    b.append(2, &[entry(2)]).await.unwrap();
    let c = store(&channel);
    assert_eq!(c.claim(3).await.unwrap().position, 2);
    assert_eq!(history(c.as_ref()).await, vec![1, 2]);
}

#[tokio::test]
async fn an_unconfirmed_batch_becomes_the_head() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // The batch lands, but A never learns it (and dies).
    channel.lose_next_answer();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Other(_))));
    assert!(a.append(1, &[entry(3)]).await.is_err(), "the handle cannot go on");

    // It won its link: the next claimant takes it as part of the history
    // (never acknowledged, as Raft allows).
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 2);
    b.append(2, &[entry(3)]).await.unwrap();
    assert_eq!(history(b.as_ref()).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn racing_claims_leave_one_writer() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();

    let (b, c) = (store(&channel), store(&channel));
    let (rb, rc) = tokio::join!(b.claim(2), c.claim(2));
    assert_eq!([&rb, &rc].iter().filter(|r| r.is_ok()).count(), 1, "{rb:?} {rc:?}");
    let winner = if rb.is_ok() { b } else { c };
    assert!(matches!(store(&channel).claim(2).await, Err(LogError::Fenced { current: 2 })));
    winner.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(history(store(&channel).as_ref()).await, vec![1, 2]);
}

#[tokio::test]
async fn a_claim_visible_late_still_fences_and_a_deleted_id_is_skipped() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    // B's claim is invisible for the first read of A's range.
    channel.hide_next("log-claim-2", 1);
    store(&channel).claim(2).await.unwrap();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));

    // A message deleted inside a checked range does not stop the log.
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    let gone = channel.post_text();
    channel.remove(gone);
    a.append(1, &[entry(1)]).await.unwrap();
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 1);
}

#[tokio::test]
async fn a_changed_log_blob_is_detected() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    a.append(1, &[entry(2)]).await.unwrap();
    let first_batch = channel.names().into_iter().find(|(_, n)| n.starts_with("log-batch-1")).unwrap().0;
    channel.edit(first_batch);
    // A new process: nothing cached.
    let fresh = store(&channel);
    let head = fresh.discover().await.unwrap().unwrap().head;
    let read: Result<Vec<_>, _> = fresh.read(0, head).try_collect().await;
    assert!(matches!(read, Err(LogError::Lost(_))), "{read:?}");
}

#[tokio::test]
async fn a_rebase_prunes_our_messages_and_nothing_else() {
    let channel = Arc::new(FakeChannel::new(77));
    let foreign = channel.post_text();
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    a.rebase(1, base(3)).await.unwrap();
    let names = channel.names();
    assert!(names.iter().any(|(id, _)| *id == foreign), "{names:?}");
    // Batches 4 and 5 and the rebase batch stay, and the batch holding 3's
    // successor's link; the claim, its probe and batches 1–2 go.
    let ours = names.iter().filter(|(_, n)| n.starts_with("log-")).count();
    assert!(ours <= 4, "{names:?}");

    a.append(1, &[entry(6)]).await.unwrap();
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 6);
    let head = b.discover().await.unwrap().unwrap().head;
    let got: Vec<_> = b.read(3, head).try_collect().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5, 6]);
}

/// Small log blobs are text messages: a replay in a new process reads pages
/// of messages and downloads nothing.
#[tokio::test]
async fn a_replay_of_small_batches_downloads_nothing() {
    let channel = Arc::new(FakeChannel::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=30 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    assert_eq!(channel.calls().send_document, 0, "{:?}", channel.calls());
    let before = channel.calls();
    assert_eq!(history(store(&channel).as_ref()).await, (1..=30).collect::<Vec<_>>());
    let after = channel.calls();
    assert_eq!(after.get_file, before.get_file, "no downloads");
    assert!(after.get_messages - before.get_messages <= 4, "{before:?} → {after:?}");
}

#[tokio::test]
async fn a_fresh_walk_reads_each_page_once() {
    let channel = Arc::new(FakeChannel::new(77));
    for _ in 0..330 {
        channel.post_text();
    }
    assert_eq!(store(&channel).discover().await.unwrap(), None);
    assert!(channel.calls().get_messages <= 4, "{:?}", channel.calls());
}
