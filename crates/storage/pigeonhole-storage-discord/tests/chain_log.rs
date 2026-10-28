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
async fn a_rebase_prunes_messages_and_keeps_their_links() {
    let channel = Arc::new(FakeDiscord::new(77));
    let foreign = channel.post_text("hello");
    let a = store(&channel);
    a.claim(1).await.unwrap();
    for i in 1..=5 {
        a.append(1, &[entry(i)]).await.unwrap();
    }
    let (messages, threads) = (channel.message_ids().len(), channel.thread_ids().len());
    a.rebase(1, Base { at: 3, term: 1, leader: 0, snapshot: SnapshotRef("s3".into()) }).await.unwrap();
    assert!(channel.message_ids().contains(&foreign));
    assert!(channel.message_ids().len() < messages, "pruned");
    assert!(channel.thread_ids().len() > threads, "links are tombstones: never deleted");
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
