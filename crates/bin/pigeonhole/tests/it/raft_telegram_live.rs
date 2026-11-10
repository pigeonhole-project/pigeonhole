//! `TelegramMailbox` on a real channel, `TG_RAFT_OUTBOX_1`: bot
//! `TG_BOT_TOKEN` posts frames into it as its node would, bot
//! `TG_BOT_TOKEN_2` reads them as a peer, from Telegram's pushes and then
//! from the chat; then the outbox is pruned (its anchor pinned). Both bots
//! must be admins there; `TG_API_ID`, `TG_API_HASH` and the rest in the
//! workspace `.env`. Deletes what it posted.
//!
//! `cargo test -p pigeonhole --test it raft_telegram_live:: -- --ignored --nocapture`

use bytes::Bytes;
use openraft_chat::Mailbox;
use pigeonhole_limits::ChatLimiter;
use pigeonhole_raft_telegram::TelegramMailbox;
use pigeonhole_storage_telegram::{ChannelApi, ChannelMessage, TEXT_MAX_CHARS};
use pigeonhole_telegram_mtproto::{new_channel_posts, Lane, MtprotoChannel, MtprotoConfig, TelegramMtproto};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

fn login(root: &Path, token: &str, session: &str) -> MtprotoConfig {
    MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env(token),
        session: root.join(format!(".cache/{session}")),
    }
}

/// Reads frames after `after` until `want` of them came or `within` passed.
async fn read_until(
    reader: &TelegramMailbox<MtprotoChannel>,
    after: u64,
    want: usize,
    within: Duration,
) -> Vec<(u64, Bytes)> {
    let (deadline, mut got) = (Instant::now() + within, Vec::new());
    while got.len() < want && Instant::now() < deadline {
        let from = got.last().map_or(after, |(id, _)| *id);
        reader.wait(from, Duration::from_secs(1)).await;
        got.extend(reader.read_after(from, 100).await.unwrap());
    }
    got
}

#[tokio::test]
#[ignore = "needs two Telegram bots, admins of the channel TG_RAFT_OUTBOX_1"]
async fn a_peer_reads_a_live_outbox_from_pushes_and_from_the_chat() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let _ = tracing_subscriber::fmt().with_env_filter("info,pigeonhole_raft_telegram=debug").try_init();
    let chat_id: i64 = env("TG_RAFT_OUTBOX_1").parse().unwrap();
    let limiter = || Arc::new(ChatLimiter::new(pigeonhole_storage_telegram::limits()));

    let owner = TelegramMtproto::connect(&login(&root, "TG_BOT_TOKEN", "raft-live-1.session")).await.unwrap();
    let (peer, mut updates) =
        TelegramMtproto::connect_with_updates(&login(&root, "TG_BOT_TOKEN_2", "raft-live-2.session")).await.unwrap();
    let own = Arc::new(owner.channel(chat_id, Lane::Meta, limiter()).await.unwrap());
    let theirs = Arc::new(peer.channel(chat_id, Lane::Meta, limiter()).await.unwrap());
    println!("bot {} owns outbox {chat_id}, bot {} reads it", owner.bot_id(), peer.bot_id());

    let outbox = TelegramMailbox::new(own.clone()).named("outbox-1");
    // A peer that reads the chat only after a minute without a push: what
    // it reads before that came from pushes.
    let reader = Arc::new(TelegramMailbox::new(theirs.clone()).polling(Duration::from_secs(60)));
    let pushed = Arc::new(Mutex::new(Vec::<(i64, ChannelMessage)>::new()));
    let channel_id = theirs.channel_id();
    tokio::spawn({
        let (reader, pushed) = (reader.clone(), pushed.clone());
        async move {
            while let Some(update) = updates.recv().await {
                for (channel, id, message) in new_channel_posts(&update) {
                    if channel == channel_id {
                        pushed.lock().unwrap().push((id, message.clone()));
                        reader.pushed(id, Some(message));
                    }
                }
            }
        }
    });

    // Both find the top by looking ids up.
    let t = Instant::now();
    let top = outbox.head().await.unwrap();
    let pinned = own.pinned().await.unwrap();
    println!("head {top} (pinned {pinned:?}) in {:?}", t.elapsed());
    assert_eq!(reader.head().await.unwrap(), top, "the peer finds the same top");
    // Taken as read: the peer starts from the top, as a node does at serve.
    let _ = reader.read_after(top, 100).await.unwrap();

    // A text frame and a document frame.
    let small = Bytes::from_static(b"heartbeat: a frame that fits in a text");
    let big: Bytes = (0..3 * TEXT_MAX_CHARS + 17).map(|i| (i * 31 % 251) as u8).collect::<Vec<_>>().into();
    let mut posted = Vec::new();
    for frame in [&small, &big] {
        let t = Instant::now();
        let id = outbox.post(frame.clone()).await.unwrap();
        println!("posted {} B as {id} in {:?}", frame.len(), t.elapsed());
        posted.push(id);
    }

    let t = Instant::now();
    let got = read_until(&reader, top, 2, Duration::from_secs(30)).await;
    println!("peer read {:?} in {:?}", got.iter().map(|(id, b)| (id, b.len())).collect::<Vec<_>>(), t.elapsed());
    let pushes: Vec<i64> = pushed.lock().unwrap().iter().map(|(id, _)| *id).collect();
    println!("pushed: {pushes:?}");
    assert_eq!(got, vec![(posted[0], small.clone()), (posted[1], big.clone())], "the peer reads both frames");
    for id in &posted {
        assert!(pushes.contains(&(*id as i64)), "frame {id} came as a push");
    }

    // A fresh peer (a restart): nothing pushed, it reads the chat.
    let fresh = TelegramMailbox::new(theirs.clone());
    assert_eq!(fresh.head().await.unwrap(), posted[1], "a fresh peer finds the newest frame");
    let t = Instant::now();
    let again = read_until(&fresh, top, 2, Duration::from_secs(30)).await;
    println!("fresh peer read the chat in {:?}", t.elapsed());
    assert_eq!(again, got, "the chat holds what was pushed");

    // Pruning: three more frames, keep from the second of them.
    for n in 0..3u8 {
        posted.push(outbox.post(Bytes::from(vec![n; 8])).await.unwrap());
    }
    let keep_from = posted[3];
    let t = Instant::now();
    outbox.prune(keep_from).await.unwrap();
    let pinned_now = own.pinned().await.unwrap();
    println!("pruned below {keep_from} in {:?}, pinned {pinned_now:?}", t.elapsed());
    assert_eq!(pinned_now, Some(keep_from as i64), "the anchor is pinned");
    let below: Vec<i64> = (posted[0] as i64..keep_from as i64).collect();
    let gone = own.get_messages(&below).await.unwrap();
    assert!(gone.iter().all(|m| *m == ChannelMessage::Empty), "below the anchor all is gone: {gone:?}");

    // The pin's service message, if the channel posts one, and how a peer sees it.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let last = *posted.last().unwrap() as i64;
    let after_pin: Vec<i64> = (last + 1..last + 4).collect();
    let service = own.get_messages(&after_pin).await.unwrap();
    println!("after the last frame: {:?}", after_pin.iter().zip(&service).collect::<Vec<_>>());
    let late: Vec<(i64, ChannelMessage)> =
        pushed.lock().unwrap().iter().filter(|(id, _)| *id > last).cloned().collect();
    println!("pushed after the last frame: {late:?}");

    // A fresh peer after the prune: the lookup starts at the anchor.
    let fresh = TelegramMailbox::new(theirs.clone());
    let head = fresh.head().await.unwrap();
    println!("fresh head after the prune: {head}");
    assert!(head >= last as u64, "the top is found past the pruned ids");
    let kept = read_until(&fresh, 0, 2, Duration::from_secs(30)).await;
    let kept_ids: Vec<u64> = kept.iter().map(|(id, _)| *id).collect();
    println!("fresh peer reads from the bottom: {kept_ids:?}");
    assert_eq!(kept_ids, posted[3..].to_vec(), "only the anchor and above are left");

    // Clean up: our frames and whatever service messages came after them.
    let extra: Vec<i64> =
        after_pin.iter().zip(&service).filter(|(_, m)| **m != ChannelMessage::Empty).map(|(id, _)| *id).collect();
    let ours: Vec<i64> = posted[3..].iter().map(|id| *id as i64).chain(extra).collect();
    own.delete_messages(&ours).await.unwrap();
    println!("deleted {ours:?}; pinned after cleanup: {:?}", own.pinned().await.unwrap());
}
