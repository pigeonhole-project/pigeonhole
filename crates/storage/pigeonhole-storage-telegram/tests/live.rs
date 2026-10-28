//! Against the channel `TG_BLOBS_CHAT_ID`: `cargo test -p
//! pigeonhole-storage-telegram --test live -- --ignored --nocapture`
//! with it, `TG_BOT_TOKEN`, `TG_API_ID` and `TG_API_HASH` in the workspace `.env`.
//! Deletes only what it posted.

use bytes::Bytes;
use pigeonhole_blob::{
    collect_stream, BlobBackend, BlobId, ChatLimiter, ChatLimiterConfig, InstanceInfo, InstanceKind, InstanceRole,
};
use pigeonhole_storage_telegram::{ChannelApi, Lane, TelegramBlobStore, MtprotoConfig, TelegramMtproto};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

#[tokio::test]
#[ignore = "needs a Telegram bot, api id and a test channel"]
async fn blob_store_against_a_live_channel() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let chat_id: i64 = env("TG_BLOBS_CHAT_ID").parse().unwrap();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let t = Instant::now();
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    println!("connected as bot {} in {:?}", tg.bot_id(), t.elapsed());
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig::default()));
    let channel = Arc::new(tg.channel(chat_id, Lane::Blobs, limiter.clone()).await.unwrap());
    let info = InstanceInfo {
        id: "live".into(),
        kind: InstanceKind::Telegram,
        fingerprint: format!("tg:{}:{}", tg.bot_id(), channel.channel_id()),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    let store = TelegramBlobStore::new(channel.clone(), limiter, info);

    // A 12 MiB blob: a "big" upload, windows across 1 MiB boundaries.
    let data: Bytes = (0..12 * 1024 * 1024 + 4321).map(|i: usize| (i * 7 % 253) as u8).collect::<Vec<_>>().into();
    let t = Instant::now();
    let name = BlobId::random().name();
    let id = store.put(data.clone(), &name).await.unwrap();
    println!("put {} B as message {} in {:?}", data.len(), id.message_id, t.elapsed());
    assert_eq!(store.put(data.clone(), &name).await.unwrap(), id, "a repeat posts nothing new");
    let t = Instant::now();
    let got = collect_stream(store.get(&id, None).await.unwrap()).await.unwrap();
    println!("get in {:?}", t.elapsed());
    assert!(got == data);
    let range = 1_048_000..3_200_000u64;
    let part = collect_stream(store.get(&id, Some(range.clone())).await.unwrap()).await.unwrap();
    assert!(part == data.slice(range.start as usize..range.end as usize));

    pigeonhole_testkit::run_typed_conformance(&store).await.unwrap();
    store.delete(&[id.message_id]).await.unwrap();
}

/// Print what a channel holds: `TG_LIST_CHAT_ID=-100… TG_LIST_TO=<id>
/// cargo test -p pigeonhole-storage-telegram --test live list -- --ignored --nocapture`
#[tokio::test]
#[ignore = "needs a Telegram bot, api id and a channel"]
async fn list() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_LIST_CHAT_ID").parse().unwrap();
    let to: i64 = env("TG_LIST_TO").parse().unwrap();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let channel = tg.channel(chat_id, Lane::Meta, Arc::new(ChatLimiter::new(ChatLimiterConfig::default()))).await.unwrap();
    let mut lo = 1;
    while lo <= to {
        let ids: Vec<i64> = (lo..=to.min(lo + 99)).collect();
        for (id, m) in ids.iter().zip(channel.get_messages(&ids).await.unwrap()) {
            match m {
                pigeonhole_storage_telegram::ChannelMessage::Empty => {}
                pigeonhole_storage_telegram::ChannelMessage::Other => println!("{id} (not a document)"),
                pigeonhole_storage_telegram::ChannelMessage::Document { name, size, .. } => {
                    println!("{id} {name} {size}")
                }
            }
        }
        lo += 100;
    }
}

/// A message is readable by id as soon as its send returns, also from another
/// session (the log treats an empty id below a returned one as "not visible
/// yet"). Posts to `TG_LOG_CHAT_ID` and deletes what it posted.
#[tokio::test]
#[ignore = "needs a Telegram bot, api id and a test channel"]
async fn a_sent_message_is_visible_at_once() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_LOG_CHAT_ID").parse().unwrap();
    let login = |session: &str| MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache").join(session),
    };
    let limiter = || Arc::new(ChatLimiter::new(ChatLimiterConfig::default()));
    let writer = TelegramMtproto::connect(&login("tgm-live.session")).await.unwrap();
    let reader = TelegramMtproto::connect(&login("tgm-live2.session")).await.unwrap();
    let w = writer.channel(chat_id, Lane::Meta, limiter()).await.unwrap();
    let r = reader.channel(chat_id, Lane::Meta, limiter()).await.unwrap();
    let mut posted = Vec::new();
    for k in 0..8 {
        let id = w.send_document("visibility-probe.bin", Bytes::from_static(b"x"), k + uuid_like()).await.unwrap().message_id;
        posted.push(id);
        let seen = r.get_messages(&[id]).await.unwrap();
        assert!(
            matches!(seen[0], pigeonhole_storage_telegram::ChannelMessage::Document { .. }),
            "message {id} not visible to another session right after its send: {seen:?}"
        );
    }
    println!("8 messages visible at once to another session: {posted:?}");
    w.delete_messages(&posted).await.unwrap();
}

fn uuid_like() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64
}
