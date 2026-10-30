//! Spike for docs/shared-limiter.md: are a channel's send limits per bot or
//! per channel, and can one bot read and delete what another posted?
//!
//! `cargo test -p pigeonhole-storage-telegram --test spike_bots -- --ignored
//! --nocapture` with `TG_BOT_TOKEN`, `TG_BOT_TOKEN_2` (both admins of the
//! channel), `TG_API_ID`, `TG_API_HASH` and `TG_BLOBS_CHAT_ID` in `.env`.
//! `SPIKE_SECS` sets each phase's length (default 180). Deletes only what it
//! posted.

use bytes::Bytes;
use pigeonhole_limits::{ChatLimiter, ChatLimiterConfig, Rate};
use pigeonhole_storage_telegram::{ChannelApi, ChannelMessage, Lane, MtprotoChannel, MtprotoConfig, TelegramMtproto};
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

/// Our own limits far above the server's: what paces the sends is Telegram.
fn loose() -> ChatLimiterConfig {
    ChatLimiterConfig { upload_concurrency: 8, download_concurrency: 8, ..pigeonhole_storage_telegram::limits().with_all(Rate::new(1000.0, 1000.0)) }
}

fn random_id() -> i64 {
    static N: AtomicI64 = AtomicI64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
    nanos ^ (N.fetch_add(1, Ordering::Relaxed) << 48)
}

async fn bot(root: &Path, token_var: &str, n: usize, chat_id: i64) -> (i64, MtprotoChannel) {
    let cfg = MtprotoConfig {
        api_id: env("TG_API_ID").parse().unwrap(),
        api_hash: env("TG_API_HASH"),
        bot_token: env(token_var),
        session: root.join(format!(".cache/spike-tg-{n}.session")),
    };
    let tg = TelegramMtproto::connect(&cfg).await.unwrap();
    let limiter = Arc::new(ChatLimiter::new(loose()));
    let ch = tg.channel(chat_id, Lane::Meta, limiter).await.unwrap();
    (tg.bot_id(), ch)
}

/// What one bot did in a phase.
#[derive(Default, Debug)]
struct Run {
    sent: Vec<i64>,
    /// Sends that took long enough to have waited out a flood.
    slow: usize,
    slowest: Duration,
}

/// Send text messages one after another for `secs`.
async fn hammer(ch: &MtprotoChannel, label: &str, secs: u64) -> Run {
    let mut run = Run::default();
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        let t = Instant::now();
        let sent = ch.send_text(&format!("spike {label} {}", run.sent.len()), random_id()).await.unwrap();
        let took = t.elapsed();
        if took > Duration::from_millis(1500) {
            run.slow += 1;
            println!("{label}: send {} took {took:?}", run.sent.len());
        }
        run.slowest = run.slowest.max(took);
        run.sent.push(sent.message_id);
    }
    run
}

fn report(phase: &str, label: &str, run: &Run, secs: u64) {
    println!(
        "{phase} {label}: {} sends in {secs} s = {:.1}/min, {} slow (max {:?})",
        run.sent.len(),
        run.sent.len() as f64 * 60.0 / secs as f64,
        run.slow,
        run.slowest
    );
}

async fn delete_all(ch: &MtprotoChannel, ids: &[i64]) {
    for part in ids.chunks(100) {
        ch.delete_messages(part).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "needs two Telegram bots, admins of a test channel"]
async fn two_bots_in_one_channel() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    let _ = tracing_subscriber::fmt().with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into())).try_init();
    let chat_id: i64 = env("TG_BLOBS_CHAT_ID").parse().unwrap();
    let secs: u64 = std::env::var("SPIKE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(180);
    let (id_a, a) = bot(&root, "TG_BOT_TOKEN", 1, chat_id).await;
    let (id_b, b) = bot(&root, "TG_BOT_TOKEN_2", 2, chat_id).await;
    println!("bots {id_a} and {id_b} in channel {}", a.channel_id());

    // 1. Across bots: B reads and downloads what A posted, and deletes it.
    let data = Bytes::from((0..300_000u32).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let doc = a.send_document("spike-cross.bin", data.clone(), random_id()).await.unwrap();
    let text = a.send_text("spike cross text", random_id()).await.unwrap();
    let seen = b.get_messages(&[doc.message_id, text.message_id]).await.unwrap();
    match &seen[0] {
        ChannelMessage::Document { name, size, file } => {
            println!("B sees A's document {name} ({size} B)");
            let got = b.get_file(file, 0, 256 * 1024).await;
            match got {
                Ok(bytes) => println!("B downloads A's document: {} B, matches = {}", bytes.len(), bytes[..] == data[..bytes.len()]),
                Err(e) => println!("B cannot download A's document: {e:#}"),
            }
        }
        other => println!("B sees A's document as {other:?}"),
    }
    println!("B sees A's text as {:?}", seen[1]);
    b.delete_messages(&[doc.message_id, text.message_id]).await.unwrap();
    let after = a.get_messages(&[doc.message_id, text.message_id]).await.unwrap();
    println!("after B deleted them A sees {after:?}");

    // 2. One bot alone, then both at once, each as fast as the server lets it.
    let alone = hammer(&a, "A", secs).await;
    report("alone", "A", &alone, secs);
    delete_all(&a, &alone.sent).await;
    tokio::time::sleep(Duration::from_secs(60)).await;

    let (ra, rb) = tokio::join!(hammer(&a, "A", secs), hammer(&b, "B", secs));
    report("together", "A", &ra, secs);
    report("together", "B", &rb, secs);
    println!(
        "together: {:.1}/min for the channel",
        (ra.sent.len() + rb.sent.len()) as f64 * 60.0 / secs as f64
    );
    delete_all(&a, &ra.sent).await;
    delete_all(&b, &rb.sent).await;
}
