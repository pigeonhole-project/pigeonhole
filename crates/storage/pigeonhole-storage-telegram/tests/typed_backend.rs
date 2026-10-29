//! The MTProto blob backend on a fake channel.

use bytes::Bytes;
use pigeonhole_storage_api::{
    collect_stream, BlobBackend, BlobId, InstanceInfo, InstanceRole,
};
use pigeonhole_limits::{ChatLimiter, ChatLimiterConfig};
use pigeonhole_storage_telegram::fake::FakeChannel;
use pigeonhole_storage_telegram::{TelegramBlobStore, TelegramId, WINDOW};
use std::sync::Arc;

fn store() -> (Arc<FakeChannel>, TelegramBlobStore<FakeChannel>) {
    store_on(Arc::new(FakeChannel::new(77)))
}

/// A store (as a new process would open it) on `channel`.
fn store_on(channel: Arc<FakeChannel>) -> (Arc<FakeChannel>, TelegramBlobStore<FakeChannel>) {
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig {
        bot_rate_per_sec: 1000.0,
        send_rate_per_sec: 1000.0,
        info_rate_per_sec: 1000.0,
        delete_rate_per_sec: 1000.0,
        ..ChatLimiterConfig::default()
    }));
    let info = InstanceInfo {
        id: "tgm".into(),
        kind: pigeonhole_storage_telegram::KIND,
        fingerprint: "tg:1:77".into(),
        location: "tg:chat:-10077".into(),
        role: InstanceRole::ReadWrite,
    };
    (channel.clone(), TelegramBlobStore::new(channel, limiter, info))
}

fn pattern(len: usize) -> Bytes {
    (0..len).map(|i| (i * 31 % 251) as u8).collect::<Vec<_>>().into()
}

#[tokio::test]
async fn passes_the_typed_conformance_suite() {
    let (_, store) = store();
    pigeonhole_testkit::run_typed_conformance(&store).await.unwrap();
}

#[tokio::test]
async fn reads_any_range_of_a_multi_window_blob() {
    let (channel, store) = store();
    let data = pattern(3 * WINDOW as usize + 1234);
    let id = store.put(data.clone(), &BlobId::random().name()).await.unwrap();
    assert_eq!(collect_stream(store.get(&id, None).await.unwrap()).await.unwrap(), data);
    for (a, b) in [(0, 1), (WINDOW - 1, WINDOW + 1), (5, 2 * WINDOW + 7), (data.len() as u64 - 3, data.len() as u64)] {
        let got = collect_stream(store.get(&id, Some(a..b)).await.unwrap()).await.unwrap();
        assert_eq!(got, data.slice(a as usize..b as usize), "{a}..{b}");
    }
    assert!(store.get(&id, Some(0..data.len() as u64 + 1)).await.is_err());
    // The location came with the send: no getMessages for any of it.
    assert_eq!(channel.calls().get_messages, 0);
}

#[tokio::test]
async fn an_expired_file_reference_is_refreshed() {
    let (channel, store) = store();
    // Too big for a text message: a document.
    let id = store.put(pattern(10_000), &BlobId::random().name()).await.unwrap();
    channel.expire_file_references();
    let got = collect_stream(store.get(&id, None).await.unwrap()).await.unwrap();
    assert_eq!(got, pattern(10_000));
    assert_eq!(channel.calls().get_messages, 1);
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
async fn sweeps_only_our_blobs_and_reads_ranges_of_100_ids() {
    let (channel, store) = store();
    let ours = store.put(pattern(10), &BlobId::random().name()).await.unwrap().message_id;
    channel.post_text();
    channel.post_document("photo.jpg", b"not ours");
    for _ in 0..150 {
        channel.post_text();
    }
    let late = store.put(pattern(10), &BlobId::random().name()).await.unwrap().message_id;
    let top = late + 5;

    let got = store.candidates(None, top, 100).await.unwrap();
    assert_eq!(got, vec![ours, late]);
    assert_eq!(channel.calls().get_messages, 2, "{late} ids in pages of 100");
    assert_eq!(store.candidates(Some(ours), top, 1).await.unwrap(), vec![late]);
    assert!(store.candidates(Some(late), top, 100).await.unwrap().is_empty());

    // The listing also tells where the blobs are: reading them needs no lookup.
    let before = channel.calls().get_messages;
    collect_stream(store.get(&TelegramId { message_id: late }, None).await.unwrap()).await.unwrap();
    assert_eq!(channel.calls().get_messages, before);
}

#[tokio::test]
async fn deletes_in_batches_of_100() {
    let (channel, store) = store();
    let mut keys = Vec::new();
    for _ in 0..150 {
        keys.push(store.put(pattern(3), &BlobId::random().name()).await.unwrap().message_id);
    }
    store.delete(&keys).await.unwrap();
    assert!(channel.message_ids().is_empty());
    assert_eq!(channel.calls().delete_messages, 2);
    let gone = store.get(&TelegramId { message_id: keys[0] }, None).await;
    assert_eq!(pigeonhole_storage_api::Failure::of(&gone.err().unwrap()), pigeonhole_storage_api::Failure::NotFound);
}

/// Blobs that fit a text message go as text: no upload, and a reader of
/// another process gets the bytes with the page that lists them.
#[tokio::test]
async fn small_blobs_are_text_messages() {
    let (channel, first) = store();
    let small = pattern(7_000);
    let id = first.put(small.clone(), &BlobId::random().name()).await.unwrap();
    let big = first.put(pattern(9_000), &BlobId::random().name()).await.unwrap();
    assert_eq!((channel.calls().send_text, channel.calls().send_document), (1, 1));

    let (_, other) = store_on(channel.clone());
    assert_eq!(other.candidates(None, big.message_id, 10).await.unwrap(), vec![id.message_id, big.message_id]);
    let reads = channel.calls();
    assert_eq!(collect_stream(other.get(&id, None).await.unwrap()).await.unwrap(), small);
    assert_eq!(collect_stream(other.get(&id, Some(10..20)).await.unwrap()).await.unwrap(), small.slice(10..20));
    assert_eq!(channel.calls(), reads, "the listing brought the bytes");

    let (_, fresh) = store_on(channel.clone());
    assert_eq!(collect_stream(fresh.get(&id, None).await.unwrap()).await.unwrap(), small);
    assert_eq!(channel.calls().get_file, reads.get_file, "a text blob is never downloaded");

    let (channel, docs) = store();
    let docs = docs.with_inline(false);
    docs.put(pattern(10), &BlobId::random().name()).await.unwrap();
    assert_eq!((channel.calls().send_text, channel.calls().send_document), (0, 1));
}
