//! Against the channel `TG_BLOBS_CHAT_ID`: `cargo test -p
//! pigeonhole-storage-telegram --test live -- --ignored --nocapture`
//! with it, `TG_BOT_TOKEN`, `TG_API_ID` and `TG_API_HASH` in the workspace `.env`.
//! Deletes only what it posted.

use bytes::Bytes;
use pigeonhole_storage_api::{collect_stream, BlobBackend, BlobId, InstanceInfo, InstanceKind, InstanceRole};
use pigeonhole_limits::{ChatLimiter, ChatLimiterConfig};
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
                pigeonhole_storage_telegram::ChannelMessage::Text { text } => {
                    println!("{id} text {:?}", text.chars().take(40).collect::<String>())
                }
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

/// Delete the documents and text blobs of a channel whose names start with one of
/// `TG_CLEAN_PREFIXES` (comma-separated) in ids `1..=TG_LIST_TO`: test
/// leftovers. `TG_LIST_CHAT_ID=-100… TG_LIST_TO=<id> TG_CLEAN_PREFIXES=log-
/// cargo test -p pigeonhole-storage-telegram --test live clean -- --ignored --nocapture`
#[tokio::test]
#[ignore = "deletes messages in a real channel"]
async fn clean() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_LIST_CHAT_ID").parse().unwrap();
    let to: i64 = env("TG_LIST_TO").parse().unwrap();
    let prefixes: Vec<String> = env("TG_CLEAN_PREFIXES").split(',').map(str::to_string).collect();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let channel = tg.channel(chat_id, Lane::Meta, Arc::new(ChatLimiter::new(ChatLimiterConfig::default()))).await.unwrap();
    let mut lo = 1;
    let mut deleted = 0;
    while lo <= to {
        let ids: Vec<i64> = (lo..=to.min(lo + 99)).collect();
        let ours: Vec<i64> = ids
            .iter()
            .zip(channel.get_messages(&ids).await.unwrap())
            .filter(|(_, m)| {
                let name = match m {
                    pigeonhole_storage_telegram::ChannelMessage::Document { name, .. } => Some(name.as_str()),
                    pigeonhole_storage_telegram::ChannelMessage::Text { text } => {
                        pigeonhole_storage_api::text::name_of(text)
                    }
                    _ => None,
                };
                name.is_some_and(|n| prefixes.iter().any(|p| n.starts_with(p.as_str())))
            })
            .map(|(id, _)| *id)
            .collect();
        if !ours.is_empty() {
            channel.delete_messages(&ours).await.unwrap();
            deleted += ours.len();
        }
        lo += 100;
    }
    println!("deleted {deleted} messages");
}

/// Small blobs as text messages (15 bits a character): the largest that
/// fits, every byte value, read back by a store that cached nothing. Posts
/// to `TG_BLOBS_CHAT_ID` and deletes what it posted.
#[tokio::test]
#[ignore = "needs a Telegram bot, api id and a test channel"]
async fn inline_blobs_against_a_live_channel() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_BLOBS_CHAT_ID").parse().unwrap();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig::default()));
    let channel = Arc::new(tg.channel(chat_id, Lane::Blobs, limiter.clone()).await.unwrap());
    let info = InstanceInfo {
        id: "live".into(),
        kind: InstanceKind::Telegram,
        fingerprint: format!("tg:{}:{}", tg.bot_id(), channel.channel_id()),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    let store = TelegramBlobStore::new(channel.clone(), limiter.clone(), info.clone());
    let name = BlobId::random().name();
    let mut max = 0;
    while pigeonhole_storage_api::text::fits(&name, max + 1, pigeonhole_storage_api::text::TELEGRAM_MAX_CHARS) {
        max += 1;
    }
    let mut posted = Vec::new();
    for len in [1usize, 256, max] {
        let data: Bytes = (0..len).map(|i| (i * 167 + i / 256) as u8).collect::<Vec<_>>().into();
        let t = Instant::now();
        let id = store.put(data.clone(), &BlobId::random().name()).await.unwrap();
        posted.push(id.message_id);
        let fresh = TelegramBlobStore::new(channel.clone(), limiter.clone(), info.clone());
        let got = collect_stream(fresh.get(&id, None).await.unwrap()).await.unwrap();
        println!("{len} B as a text message: put + fresh get in {:?}", t.elapsed());
        assert!(got == data, "{len} B came back different");
    }
    store.delete(&posted).await.unwrap();
}

/// Chain members too big for a text message go as documents named
/// `<label>~<prev>.bin`: the name must come back as sent (a chat that
/// rewrites file names would hide the chain). Posts to `TG_BLOBS_CHAT_ID`
/// and deletes what it posted.
#[tokio::test]
#[ignore = "needs a Telegram bot, api id and a test channel"]
async fn chain_names_survive_as_documents() {
    use pigeonhole_storage_api::{After, Chain};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let chat_id: i64 = env("TG_BLOBS_CHAT_ID").parse().unwrap();
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let limiter = Arc::new(ChatLimiter::new(ChatLimiterConfig::default()));
    let channel = Arc::new(tg.channel(chat_id, Lane::Blobs, limiter.clone()).await.unwrap());
    let info = InstanceInfo {
        id: "live".into(),
        kind: InstanceKind::Telegram,
        fingerprint: format!("tg:{}:{}", tg.bot_id(), channel.channel_id()),
        location: format!("tg:chat:{chat_id}"),
        role: InstanceRole::ReadWrite,
    };
    let store = TelegramBlobStore::new(channel.clone(), limiter.clone(), info.clone());
    let big = Bytes::from(vec![7u8; 9_000]);
    let After::Won(id) = store.put_after(None, "live-doc-1", big).await.unwrap() else { panic!("a fresh chain") };
    let names = channel.get_messages(&[id.message_id]).await.unwrap();
    println!("document name as stored: {:?}", names.first().map(|m| match m {
        pigeonhole_storage_telegram::ChannelMessage::Document { name, .. } => name.clone(),
        other => format!("{other:?}"),
    }));
    let fresh = TelegramBlobStore::new(channel.clone(), limiter, info);
    let tail = fresh.tail().await.unwrap();
    // Delete everything this test posted (the member and the probes).
    let top = fresh.tail().await.ok();
    let hi = top.flatten().map_or(id.message_id, |t| t.id.message_id).max(id.message_id) + 3;
    let ids: Vec<i64> = (id.message_id..=hi).collect();
    let ours: Vec<i64> = ids
        .iter()
        .zip(channel.get_messages(&ids).await.unwrap())
        .filter(|(_, m)| match m {
            pigeonhole_storage_telegram::ChannelMessage::Document { name, .. } => name.starts_with("live-doc-") || name == "chain-probe.bin",
            pigeonhole_storage_telegram::ChannelMessage::Text { text } => pigeonhole_storage_api::text::name_of(text) == Some("chain-probe.bin"),
            _ => false,
        })
        .map(|(id, _)| *id)
        .collect();
    channel.delete_messages(&ours).await.unwrap();
    assert_eq!(tail.map(|t| t.id), Some(id), "the chain finds its document member");
}

/// Around a live run in the test channels (`scripts/bench.sh`):
/// `TG_BENCH=mark` posts a marker in each of TG_{BLOBS,LOG,SNAP}_CHAT_ID and
/// records its id; `TG_BENCH=clean` posts a second marker and deletes every
/// message between the two, both included (never message 208 of the log
/// channel, which may be the user's).
/// `TG_BENCH=mark cargo test -p pigeonhole-storage-telegram --test live bench_window -- --ignored --nocapture`
#[tokio::test]
#[ignore = "posts and deletes messages in the real test channels"]
async fn bench_window() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let marks = root.join(".cache/bench/tg-marks.txt");
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env("TG_BOT_TOKEN"),
        session: root.join(".cache/tgm-live.session"),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let vars = ["TG_BLOBS_CHAT_ID", "TG_LOG_CHAT_ID", "TG_SNAP_CHAT_ID"];
    let mode = env("TG_BENCH");
    let mut lines = Vec::new();
    let recorded = std::fs::read_to_string(&marks).unwrap_or_default();
    for var in vars {
        let chat_id: i64 = env(var).parse().unwrap();
        let channel = tg.channel(chat_id, Lane::Meta, Arc::new(ChatLimiter::new(ChatLimiterConfig::default()))).await.unwrap();
        let marker = channel.send_text(&format!("pigeonhole bench marker ({mode})"), uuid_like()).await.unwrap().message_id;
        match mode.as_str() {
            "mark" => {
                println!("{var}: from {marker}");
                lines.push(format!("{chat_id} {marker}"));
            }
            "clean" => {
                let from: i64 = recorded
                    .lines()
                    .find_map(|l| l.strip_prefix(&format!("{chat_id} "))?.trim().parse().ok())
                    .unwrap_or_else(|| panic!("no mark for {var} in {}", marks.display()));
                let mut deleted = 0;
                let mut lo = from;
                while lo <= marker {
                    let ids: Vec<i64> = (lo..=marker.min(lo + 99)).collect();
                    let present: Vec<i64> = ids
                        .iter()
                        .zip(channel.get_messages(&ids).await.unwrap())
                        .filter(|(id, m)| !matches!(m, pigeonhole_storage_telegram::ChannelMessage::Empty) && **id != 208)
                        .map(|(id, _)| *id)
                        .collect();
                    if !present.is_empty() {
                        channel.delete_messages(&present).await.unwrap();
                        deleted += present.len();
                    }
                    lo += 100;
                }
                println!("{var}: deleted {deleted} messages in {from}..={marker}");
            }
            other => panic!("TG_BENCH={other}: mark | clean"),
        }
    }
    if mode == "mark" {
        std::fs::create_dir_all(marks.parent().unwrap()).unwrap();
        std::fs::write(&marks, lines.join("\n") + "\n").unwrap();
    }
}
