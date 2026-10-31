use super::*;
use pigeonhole_limits::{ChatLimiter, Rate};
use pigeonhole_storage_api::{BlobBackend, InstanceInfo, InstanceRole};
use pigeonhole_storage_discord::DiscordBlobStore;
use pigeonhole_storage_telegram::TelegramBlobStore;

fn info(kind: &'static str) -> InstanceInfo {
    InstanceInfo {
        id: kind.into(),
        kind,
        fingerprint: format!("fake:{kind}:1"),
        location: format!("fake:{kind}:1"),
        role: InstanceRole::ReadWrite,
    }
}

fn tg_store(channel: Arc<TelegramChannel>) -> TelegramBlobStore<Arc<TelegramChannel>> {
    let limiter = Arc::new(ChatLimiter::new(pigeonhole_storage_telegram::limits().with_all(Rate::new(1000.0, 100.0))));
    TelegramBlobStore::new(Arc::new(channel), limiter, info(pigeonhole_storage_telegram::KIND))
}

fn dc_store(channel: Arc<DiscordChannel>) -> DiscordBlobStore<Arc<DiscordChannel>> {
    let limiter = Arc::new(ChatLimiter::new(pigeonhole_storage_discord::limits().with_all(Rate::new(1000.0, 100.0))));
    DiscordBlobStore::new(Arc::new(channel), limiter, None, info(pigeonhole_storage_discord::KIND))
}

fn pattern(len: usize) -> Bytes {
    (0..len).map(|i| (i * 31 % 251) as u8).collect::<Vec<_>>().into()
}

#[tokio::test]
async fn a_telegram_store_on_a_tablet_channel_passes_the_conformance_suite() {
    let channel = Arc::new(telegram(-100, Stores::memory()).await.unwrap());
    pigeonhole_testkit::run_typed_conformance(&tg_store(channel)).await.unwrap();
}

#[tokio::test]
async fn a_discord_store_on_a_tablet_channel_passes_the_conformance_suite() {
    let channel = Arc::new(discord(77, Stores::memory()).await.unwrap());
    pigeonhole_testkit::run_typed_conformance(&dc_store(channel)).await.unwrap();
}

#[tokio::test]
async fn a_telegram_channel_on_disk_outlives_its_process() {
    let dir = tempfile::tempdir().unwrap();
    let (doc, text) = (pattern(300_000), pattern(500));
    let (doc_id, text_id, ids) = {
        let channel = Arc::new(telegram(-100, Stores::in_dir(dir.path()).unwrap()).await.unwrap());
        let store = tg_store(channel.clone());
        let doc_id = store.put(doc.clone(), &BlobId::random().name()).await.unwrap();
        let text_id = store.put(text.clone(), &BlobId::random().name()).await.unwrap();
        let ids = channel.host().read(TelegramChannelState::ids);
        channel.host().close().await.unwrap();
        (doc_id, text_id, ids)
    };
    let channel = Arc::new(telegram(-100, Stores::in_dir(dir.path()).unwrap()).await.unwrap());
    assert_eq!(channel.host().read(TelegramChannelState::ids), ids, "the same messages, the same ids");
    let store = tg_store(channel);
    assert_eq!(collect_stream(store.get(&doc_id, None).await.unwrap()).await.unwrap(), doc);
    assert_eq!(collect_stream(store.get(&text_id, None).await.unwrap()).await.unwrap(), text);
    // Ids keep growing after the restart.
    let next = store.put(pattern(10), &BlobId::random().name()).await.unwrap();
    assert!(next.message_id > *ids.last().unwrap());
}

#[tokio::test]
async fn a_discord_channel_on_disk_outlives_its_process() {
    let dir = tempfile::tempdir().unwrap();
    let doc = pattern(300_000);
    let (doc_id, ids) = {
        let channel = Arc::new(discord(77, Stores::in_dir(dir.path()).unwrap()).await.unwrap());
        let store = dc_store(channel.clone());
        let doc_id = store.put(doc.clone(), &BlobId::random().name()).await.unwrap();
        let ids = channel.host().read(DiscordChannelState::ids);
        channel.host().close().await.unwrap();
        (doc_id, ids)
    };
    let channel = Arc::new(discord(77, Stores::in_dir(dir.path()).unwrap()).await.unwrap());
    assert_eq!(channel.host().read(DiscordChannelState::ids), ids);
    assert_eq!(collect_stream(dc_store(channel).get(&doc_id, None).await.unwrap()).await.unwrap(), doc);
}

#[tokio::test]
async fn a_deleted_document_leaves_no_blob() {
    let dir = tempfile::tempdir().unwrap();
    let channel = Arc::new(telegram(-100, Stores::in_dir(dir.path()).unwrap()).await.unwrap());
    let store = tg_store(channel.clone());
    let id = store.put(pattern(100_000), &BlobId::random().name()).await.unwrap();
    let docs = || std::fs::read_dir(dir.path().join("documents")).unwrap().count();
    let with = docs();
    store.delete(&[id.message_id]).await.unwrap();
    assert!(channel.host().read(TelegramChannelState::ids).is_empty());
    assert!(docs() <= with, "documents/ does not grow after a delete");
}
