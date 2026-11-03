//! The MTProto blob backend on a fake channel.

use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, Rate};
use pigeonhole_storage_api::{collect_stream, BlobBackend, BlobId, InstanceInfo, InstanceRole};
use pigeonhole_storage_telegram::fake::FakeChannel;
use pigeonhole_storage_telegram::{TelegramBlobStore, TelegramId, WINDOW};
use std::sync::Arc;

fn store() -> (Arc<FakeChannel>, TelegramBlobStore<FakeChannel>) {
    store_on(Arc::new(FakeChannel::new(77)))
}

/// A store (as a new process would open it) on `channel`.
fn store_on(channel: Arc<FakeChannel>) -> (Arc<FakeChannel>, TelegramBlobStore<FakeChannel>) {
    let limiter = Arc::new(ChatLimiter::new(pigeonhole_storage_telegram::limits().with_all(Rate::new(1000.0, 100.0))));
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

    let got = store.candidates(pigeonhole_storage_api::Owner(0), None, top, 100).await.unwrap();
    assert_eq!(got, vec![ours, late]);
    assert_eq!(channel.calls().get_messages, 2, "{late} ids in pages of 100");
    assert_eq!(store.candidates(pigeonhole_storage_api::Owner(0), Some(ours), top, 1).await.unwrap(), vec![late]);
    assert!(store.candidates(pigeonhole_storage_api::Owner(0), Some(late), top, 100).await.unwrap().is_empty());

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
    assert_eq!(
        other.candidates(pigeonhole_storage_api::Owner(0), None, big.message_id, 10).await.unwrap(),
        vec![id.message_id, big.message_id]
    );
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

/// The first deployment marks the channel with a pinned message; another
/// one learns the channel is taken; the first one again posts nothing.
#[tokio::test]
async fn a_pinned_marker_tells_whose_channel_it_is() {
    use pigeonhole_storage_api::Owner;
    let (channel, a) = store();
    let (ours, theirs) = (Owner(0xa), Owner(0xb));
    assert_eq!(a.mark_owner(ours).await.unwrap(), ours);
    let marker = channel.names().into_iter().find(|(_, n)| n.starts_with(&ours.marker_name())).unwrap().0;
    assert_eq!(channel.pinned_now(), Some(marker));

    let (_, b) = store_on(channel.clone());
    assert_eq!(b.mark_owner(theirs).await.unwrap(), ours, "the channel is taken");
    let posts = channel.message_ids().len();
    assert_eq!(store_on(channel.clone()).1.mark_owner(ours).await.unwrap(), ours);
    assert_eq!(channel.message_ids().len(), posts, "a marked channel takes no new post");

    // Someone pins another message: the next start marks the channel again.
    let other = channel.post_text();
    channel.pin_now(other);
    assert_eq!(store_on(channel.clone()).1.mark_owner(ours).await.unwrap(), ours);
    assert_ne!(channel.pinned_now(), Some(other));
}

/// A put whose answer was lost is repeated with the same `random_id`: one
/// message, and the repeat's answer carries the document as Telegram's
/// does, so the read after it needs no `getMessages`.
#[tokio::test]
async fn a_put_repeated_after_a_lost_answer_posts_once_and_knows_its_document() {
    let (channel, store) = store();
    let store = store.with_inline(false);
    let (data, name) = (pattern(5000), BlobId::random().name());
    channel.lose_next_answer();
    assert!(store.put(data.clone(), &name).await.is_err(), "the answer was lost");
    let id = store.put(data.clone(), &name).await.unwrap();
    assert_eq!(channel.message_ids(), vec![id.message_id], "posted once");
    let reads = channel.calls().get_messages;
    assert_eq!(collect_stream(store.get(&id, None).await.unwrap()).await.unwrap(), data);
    assert_eq!(channel.calls().get_messages, reads, "located by the repeat's answer");
}

/// Deletes and pins applied whose answers were lost: a delete repeats
/// harmlessly, and the owner marker is found pinned when it is asked again.
#[tokio::test]
async fn deletes_and_pins_whose_answers_were_lost() {
    use pigeonhole_storage_telegram::fake::Mutation;
    let (channel, store) = store();
    let id = store.put(pattern(10), &BlobId::random().name()).await.unwrap();
    channel.lose_next_answer_of(Mutation::Delete);
    assert!(store.delete(&[id.message_id]).await.is_err());
    assert!(!store.exists(&id).await.unwrap(), "deleted all the same");
    store.delete(&[id.message_id]).await.unwrap();

    let owner = pigeonhole_storage_api::Owner(7);
    channel.lose_next_answer_of(Mutation::Pin);
    assert!(store.mark_owner(owner).await.is_err());
    assert_eq!(store.mark_owner(owner).await.unwrap(), owner);
    let markers = channel.names().iter().filter(|(_, n)| n.starts_with("owner-")).count();
    assert_eq!(markers, 1, "{:?}", channel.names());
}
