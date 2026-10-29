//! The generic chain log and snapshots over a Discord channel (the Discord
//! [`Chain`]: threads as compare-and-set), on a fake channel.

use futures::TryStreamExt;
use pigeonhole_blob::{After, Chain, ChatLimiter, ChatLimiterConfig, InstanceInfo, InstanceKind, InstanceRole};
use pigeonhole_meta::log::conformance;
use pigeonhole_meta::snapshot::conformance as snapshot_conformance;
use pigeonhole_meta::{Base, BlobSnapshotStore, ChainLogStore, Entry, LogError, LogStore, SnapshotRef, SnapshotStore};
use pigeonhole_storage_discord::fake::FakeDiscord;
use pigeonhole_storage_discord::DiscordBlobStore;
use std::sync::Arc;

type Store = DiscordBlobStore<FakeDiscord>;

fn channel_store(channel: &Arc<FakeDiscord>) -> Arc<Store> {
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig {
        bot_rate_per_sec: 1000.0,
        send_rate_per_sec: 1000.0,
        info_rate_per_sec: 1000.0,
        get_file_rate_per_sec: 1000.0,
        delete_rate_per_sec: 1000.0,
        ..ChatLimiterConfig::default()
    }));
    let info = InstanceInfo {
        id: "log".into(),
        kind: InstanceKind::Discord,
        fingerprint: "dc:1:77".into(),
        location: "dc:channel:77".into(),
        role: InstanceRole::ReadWrite,
    };
    Arc::new(DiscordBlobStore::new(channel.clone(), limiter, None, info))
}

fn store(channel: &Arc<FakeDiscord>) -> Arc<ChainLogStore<Store>> {
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

#[tokio::test]
async fn passes_the_logstore_conformance_suite() {
    conformance::run_all(|| {
        let channel = Arc::new(FakeDiscord::new(77));
        move || {
            let channel = channel.clone();
            async move { store(&channel) as Arc<dyn LogStore> }
        }
    })
    .await;
}

#[tokio::test]
async fn passes_the_snapshot_conformance_suite() {
    let channel = Arc::new(FakeDiscord::new(78));
    snapshot_conformance::run_all(|| {
        let channel = channel.clone();
        async move { Arc::new(BlobSnapshotStore::new(channel_store(&channel))) as Arc<dyn SnapshotStore> }
    })
    .await;
}

#[tokio::test]
async fn a_deposed_writers_batch_loses_its_link() {
    let channel = Arc::new(FakeDiscord::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 1);
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    assert_eq!(history(store(&channel).as_ref()).await, vec![1]);
    b.append(2, &[entry(2)]).await.unwrap();
    let c = store(&channel);
    assert_eq!(c.claim(3).await.unwrap().position, 2);
    assert_eq!(history(c.as_ref()).await, vec![1, 2]);
}

/// Unlike in a Telegram channel, a batch whose writer died before linking it
/// is not in the log: it never got its link.
#[tokio::test]
async fn an_unlinked_batch_is_not_in_the_log() {
    let channel = Arc::new(FakeDiscord::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    channel.lose_next_answer();
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Other(_))));
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 1);
    b.append(2, &[entry(3)]).await.unwrap();
    assert_eq!(history(b.as_ref()).await, vec![1, 3]);
}

#[tokio::test]
async fn racing_claims_leave_one_writer() {
    let channel = Arc::new(FakeDiscord::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let (b, c) = (store(&channel), store(&channel));
    let (rb, rc) = tokio::join!(b.claim(2), c.claim(2));
    assert_eq!([&rb, &rc].iter().filter(|r| r.is_ok()).count(), 1, "{rb:?} {rc:?}");
    let winner = if rb.is_ok() { b } else { c };
    winner.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(history(store(&channel).as_ref()).await, vec![1, 2]);
}

/// Two fresh stores both post an anchor: the oldest one serves both.
#[tokio::test]
async fn racing_first_claims_share_one_anchor() {
    let channel = Arc::new(FakeDiscord::new(77));
    let (x, y) = (channel_store(&channel), channel_store(&channel));
    let (rx, ry) = tokio::join!(
        x.put_after(None, "log-claim-1", bytes::Bytes::from_static(b"x")),
        y.put_after(None, "log-claim-1", bytes::Bytes::from_static(b"y")),
    );
    let won = [rx.unwrap(), ry.unwrap()].iter().filter(|r| matches!(r, After::Won(_))).count();
    assert_eq!(won, 1);
    assert_eq!(channel.thread_ids().len(), 1, "one link from one anchor");
}

#[tokio::test]
async fn a_changed_log_blob_is_detected() {
    let channel = Arc::new(FakeDiscord::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    a.append(1, &[entry(2)]).await.unwrap();
    let ids = channel.message_ids();
    channel.edit(ids[2]); // anchor, claim, first batch
    let fresh = store(&channel);
    let head = fresh.discover().await.unwrap().unwrap().head;
    let read: Result<Vec<_>, _> = fresh.read(0, head).try_collect().await;
    assert!(matches!(read, Err(LogError::Lost(_))), "{read:?}");
}

#[tokio::test]
async fn a_rebase_prunes_messages_and_their_links() {
    let channel = Arc::new(FakeDiscord::new(77));
    let foreign = channel.post_text("hello");
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    let messages = channel.message_ids().len();
    a.rebase(1, Base { at: 3, term: 1, leader: 0, snapshot: SnapshotRef("s3".into()) }, 1).await.unwrap();
    a.settle().await; // the collection runs in the background
    assert!(channel.message_ids().contains(&foreign));
    assert!(channel.message_ids().len() < messages, "pruned");
    let alive = channel.message_ids();
    let dangling: Vec<u64> = channel.thread_ids().into_iter().filter(|t| !alive.contains(t)).collect();
    assert!(dangling.len() <= 1, "only the link into the first survivor outlives its message: {dangling:?}");
    a.append(1, &[entry(6)]).await.unwrap();
    let b = store(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 6);
    let head = b.discover().await.unwrap().unwrap().head;
    let got: Vec<_> = b.read(3, head).try_collect().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![4, 5, 6]);
}

/// Small log blobs are text: a replay in a new process lists and fetches
/// messages, and downloads nothing.
#[tokio::test]
async fn a_replay_of_small_batches_downloads_nothing() {
    let channel = Arc::new(FakeDiscord::new(77));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=30 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    assert_eq!(history(store(&channel).as_ref()).await, (1..=30).collect::<Vec<_>>());
    assert_eq!(channel.calls().download, 0);
}

/// Removed snapshots take their links with them: one dangling link at most.
#[tokio::test]
async fn removed_snapshots_leave_no_links_behind() {
    let channel = Arc::new(FakeDiscord::new(79));
    let store = BlobSnapshotStore::new(channel_store(&channel));
    let mut refs = Vec::new();
    for at in 1..=6u64 {
        // Every other one too big for a text message: an attachment.
        let len = if at % 2 == 0 { 9_000 } else { 10 };
        let snap = pigeonhole_meta::StoredSnapshot { log_id: pigeonhole_meta::LogId(1), at, data: vec![at as u8; len] };
        refs.push(store.put(&snap).await.unwrap());
        if refs.len() > 1 {
            let old = refs.remove(0);
            store.remove(&old).await.unwrap();
        }
    }
    assert_eq!(store.newest().await.unwrap().map(|s| s.at), Some(6));
    let alive = channel.message_ids();
    let dangling: Vec<u64> = channel.thread_ids().into_iter().filter(|t| !alive.contains(t)).collect();
    assert!(dangling.len() <= 1, "{dangling:?}");
}

/// An append is a message and a thread; archiving the link runs after it.
#[tokio::test]
async fn links_are_archived_off_the_appends_path() {
    let channel = Arc::new(FakeDiscord::new(80));
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    let links = channel.calls().start_thread;
    for _ in 0..100 {
        if channel.calls().archive_thread >= links {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("archived {} of {links} links", channel.calls().archive_thread);
}

/// A snapshot is a plain message: no thread (the pacer would hold it), and
/// the newest is found by listing the channel.
#[tokio::test]
async fn snapshots_are_plain_messages_found_by_listing() {
    let channel = Arc::new(FakeDiscord::new(79));
    let snaps = BlobSnapshotStore::new(channel_store(&channel));
    let snap = |log: u128, at| pigeonhole_meta::StoredSnapshot {
        log_id: pigeonhole_meta::LogId(log),
        at,
        data: vec![at as u8; 10],
    };
    snaps.put(&snap(1, 10)).await.unwrap();
    let second = snaps.put(&snap(1, 20)).await.unwrap();
    assert!(channel.thread_ids().is_empty(), "snapshots start no threads: {:?}", channel.thread_ids());
    let fresh = BlobSnapshotStore::new(channel_store(&channel));
    assert_eq!(fresh.newest().await.unwrap(), Some(snap(1, 20)));
    fresh.remove(&second).await.unwrap();
    assert_eq!(fresh.newest().await.unwrap(), Some(snap(1, 10)));
}

// A lease log (docs/discord-lease.md): only lease records take a thread.

const LEASE: std::time::Duration = std::time::Duration::from_millis(300);

fn leased(channel: &Arc<FakeDiscord>) -> Arc<ChainLogStore<Store>> {
    let lease = pigeonhole_meta::Lease { duration: LEASE, margin: std::time::Duration::from_millis(50) };
    Arc::new(ChainLogStore::new(channel_store(channel)).with_lease(lease))
}

#[tokio::test]
async fn a_lease_log_passes_the_logstore_conformance_suite() {
    conformance::run_all(|| {
        let channel = Arc::new(FakeDiscord::new(81));
        move || {
            let channel = channel.clone();
            async move { leased(&channel) as Arc<dyn LogStore> }
        }
    })
    .await;
}

#[tokio::test]
async fn a_lease_log_appends_without_threads() {
    let channel = Arc::new(FakeDiscord::new(82));
    let a = leased(&channel);
    a.claim(1).await.unwrap();
    let threads = channel.calls().start_thread;
    for i in 1..=20 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    assert_eq!(channel.calls().start_thread, threads, "batches are unlinked");
    assert_eq!(history(leased(&channel).as_ref()).await, (1..=20).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_lease_writer_renews_once_a_third_is_gone() {
    let channel = Arc::new(FakeDiscord::new(83));
    let a = leased(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let threads = channel.calls().start_thread;
    tokio::time::sleep(LEASE / 2).await;
    a.append(1, &[entry(2)]).await.unwrap();
    // The renewal runs off the append's path.
    for _ in 0..100 {
        if channel.calls().start_thread > threads {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(channel.calls().start_thread, threads + 1, "one renewal");
    // Long idle: the lease ran out, but nobody claimed; the renewal holds again.
    tokio::time::sleep(LEASE * 2).await;
    a.append(1, &[entry(3)]).await.unwrap();
    a.confirm(1).await.unwrap();
    assert_eq!(history(leased(&channel).as_ref()).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_claimant_waits_out_the_lease_and_fences_the_holder() {
    let channel = Arc::new(FakeDiscord::new(84));
    let a = leased(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    let t0 = std::time::Instant::now();
    let b = leased(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 1);
    assert!(t0.elapsed() >= LEASE / 2, "waited out a's lease: {:?}", t0.elapsed());
    assert!(matches!(a.append(1, &[entry(99)]).await, Err(LogError::Fenced { current: 2 })));
    b.append(2, &[entry(2)]).await.unwrap();
    assert_eq!(history(leased(&channel).as_ref()).await, vec![1, 2]);
}

/// The store answered after the lease's deadline: the writer does not keep
/// the batch and stops; the next writer goes on from what it finds.
#[tokio::test]
async fn a_batch_answered_after_the_deadline_is_not_acknowledged() {
    let channel = Arc::new(FakeDiscord::new(85));
    let a = leased(&channel);
    a.claim(1).await.unwrap();
    a.append(1, &[entry(1)]).await.unwrap();
    channel.delay_next_answer(LEASE);
    assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Other(_))));
    assert!(a.append(1, &[entry(3)]).await.is_err(), "the handle stopped");
    let b = leased(&channel);
    let head = b.claim(2).await.unwrap();
    // Landed inside the lease, so it is in the log (as an unacknowledged
    // write may be); nothing after it.
    assert_eq!(head.position, 2);
    b.append(2, &[entry(4)]).await.unwrap();
    assert_eq!(history(leased(&channel).as_ref()).await, vec![1, 2, 4]);
}

#[tokio::test]
async fn a_lease_log_rebase_keeps_the_newest_lease_record() {
    let channel = Arc::new(FakeDiscord::new(86));
    let a = leased(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=10 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    a.rebase(1, Base { at: 8, term: 1, leader: 0, snapshot: SnapshotRef("s8".into()) }, 1).await.unwrap();
    a.settle().await;
    a.append(1, &[entry(11)]).await.unwrap();
    let b = leased(&channel);
    assert_eq!(b.claim(2).await.unwrap().position, 11);
    let head = b.discover().await.unwrap().unwrap().head;
    let got: Vec<_> = b.read(8, head).try_collect().await.unwrap();
    assert_eq!(got.iter().map(|(p, _)| *p).collect::<Vec<_>>(), vec![9, 10, 11]);
}
