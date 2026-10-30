//! A pool of the two test bots in `DISCORD_BLOBS_CHANNEL_ID`, with the
//! default limits (docs/shared-limiter.md): `cargo test -p
//! pigeonhole-storage-discord --test spike_pool -- --ignored --nocapture`
//! with `DISCORD_BOT_TOKEN`, `DISCORD_BOT_TOKEN_2` in `.env`. Deletes what
//! it posted.

use pigeonhole_storage_discord::{DiscordApi, DiscordClient, PooledDiscord};
use std::path::Path;
use std::time::Instant;

fn env(k: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    dotenvy::from_path(root.join(".env")).ok();
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

#[tokio::test]
#[ignore = "needs two Discord bots in a test channel"]
async fn a_pool_of_two_bots_sends_twice_as_fast() {
    let channel: u64 = env("DISCORD_BLOBS_CHANNEL_ID").parse().unwrap();
    let sends: usize = std::env::var("SPIKE_SENDS").ok().and_then(|s| s.parse().ok()).unwrap_or(40);
    let reg = pigeonhole_limits::LimiterRegistry::new(pigeonhole_storage_discord::limits());
    let members = ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKEN_2"]
        .into_iter()
        .map(|var| {
            let dc = DiscordClient::new(env(var)).unwrap();
            let limiter = reg.chat(&dc.app_id(), &channel.to_string());
            dc.channel(channel, limiter)
        })
        .collect();
    let pooled = PooledDiscord::of(members);
    let t0 = Instant::now();
    let mut ids = Vec::new();
    for i in 0..sends {
        ids.push(
            pooled.send(&format!("pooled {i}"), None, &format!("p{i}-{}", t0.elapsed().as_nanos())).await.unwrap().id,
        );
    }
    let secs = t0.elapsed().as_secs_f64();
    println!("pool of 2: {sends} sends in {secs:.1} s = {:.1}/min", sends as f64 * 60.0 / secs);
    for part in ids.chunks(100) {
        pooled.delete_messages(part).await.unwrap();
    }
}
