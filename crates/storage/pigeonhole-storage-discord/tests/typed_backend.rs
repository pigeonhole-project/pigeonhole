//! The Discord blob backend on a fake channel.

use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, BlobBackend, BlobId, ChatLimiter, ChatLimiterConfig, InstanceInfo, InstanceKind, InstanceRole,
    Sweepable,
};
use pigeonhole_storage_discord::fake::FakeDiscord;
use pigeonhole_storage_discord::{DiscordBlobStore, DiscordId};
use std::sync::Arc;

type Store = DiscordBlobStore<FakeDiscord>;

fn limiter() -> Arc<ChatLimiter> {
    Arc::new(ChatLimiter::new(ChatLimiterConfig {
        bot_rate_per_sec: 1000.0,
        send_rate_per_sec: 1000.0,
        info_rate_per_sec: 1000.0,
        get_file_rate_per_sec: 1000.0,
        delete_rate_per_sec: 1000.0,
        ..ChatLimiterConfig::default()
    }))
}

/// A store (as a new process would open it) on `channel`.
fn store_on(channel: &Arc<FakeDiscord>) -> Store {
    let info = InstanceInfo {
        id: "dc".into(),
        kind: InstanceKind::Discord,
        fingerprint: "dc:1:77".into(),
        location: "dc:channel:77".into(),
        role: InstanceRole::ReadWrite,
    };
    DiscordBlobStore::new(channel.clone(), limiter(), None, info)
}

fn store() -> (Arc<FakeDiscord>, Store) {
    let channel = Arc::new(FakeDiscord::new(77));
    let store = store_on(&channel);
    (channel, store)
}

fn pattern(len: usize) -> Bytes {
    (0..len).map(|i| (i * 31 % 251) as u8).collect::<Vec<_>>().into()
}

#[tokio::test]
async fn passes_the_typed_conformance_suite() {
    let (_, texts) = store();
    pigeonhole_testkit::run_typed_conformance(&texts).await.unwrap();
    let (_, docs) = store();
    pigeonhole_testkit::run_typed_conformance(&docs.with_inline(false)).await.unwrap();
}

#[tokio::test]
async fn the_limit_is_20_mib() {
    let (_, store) = store();
    assert_eq!(store.limits().max_blob_size, 20 * 1024 * 1024);
    let big = Bytes::from(vec![7u8; 20 * 1024 * 1024 + 1]);
    assert!(store.put(big, &BlobId::random().name()).await.is_err());
}

/// Small blobs go as text: a reader of another process gets the bytes with
/// the listing; large ones are attachments.
#[tokio::test]
async fn small_blobs_are_text_messages() {
    let (channel, first) = store();
    let small = pattern(3_000);
    let id = first.put(small.clone(), &BlobId::random().name()).await.unwrap();
    let big = first.put(pattern(5_000), &BlobId::random().name()).await.unwrap();

    let other = store_on(&channel);
    let found = other.candidates(None, big.message_id, 10).await.unwrap();
    assert_eq!(found, vec![id.message_id, big.message_id]);
    let before = channel.calls();
    assert_eq!(collect_stream(other.get(&id, None).await.unwrap()).await.unwrap(), small);
    assert_eq!(collect_stream(other.get(&id, Some(10..20)).await.unwrap()).await.unwrap(), small.slice(10..20));
    let after = channel.calls();
    assert_eq!((after.get_message, after.download), (before.get_message, before.download), "the listing brought the bytes");
    assert_eq!(collect_stream(other.get(&big, Some(100..200)).await.unwrap()).await.unwrap(), pattern(5_000).slice(100..200));
}

#[tokio::test]
async fn an_expired_url_is_refreshed() {
    let (channel, store) = store();
    let id = store.put(pattern(10_000), &BlobId::random().name()).await.unwrap();
    channel.expire_urls();
    assert_eq!(collect_stream(store.get(&id, None).await.unwrap()).await.unwrap(), pattern(10_000));
    assert_eq!(channel.calls().get_message, 1, "one fetch for a fresh URL");
}

#[tokio::test]
async fn a_repeated_put_of_one_blob_posts_it_once() {
    let (channel, store) = store();
    let name = BlobId::random().name();
    let a = store.put(pattern(10), &name).await.unwrap();
    let b = store.put(pattern(10), &name).await.unwrap();
    assert_eq!(a, b);
    assert_eq!(channel.message_ids().len(), 1);
}

#[tokio::test]
async fn sweeps_only_our_blobs() {
    let (channel, store) = store();
    let ours = store.put(pattern(10), &BlobId::random().name()).await.unwrap().message_id;
    channel.post_text("hello");
    for _ in 0..150 {
        channel.post_text("noise");
    }
    let late = store.put(pattern(9_000), &BlobId::random().name()).await.unwrap().message_id;
    assert_eq!(store.candidates(None, late + 5, 100).await.unwrap(), vec![ours, late]);
    assert_eq!(store.candidates(Some(ours), late, 1).await.unwrap(), vec![late]);
    assert!(store.candidates(Some(late), late + 5, 100).await.unwrap().is_empty());
    store.delete(&[ours, late]).await.unwrap();
    let gone = store.get(&DiscordId { message_id: ours }, None).await;
    assert_eq!(pigeonhole_blob::Failure::of(&gone.err().unwrap()), pigeonhole_blob::Failure::NotFound);
}
