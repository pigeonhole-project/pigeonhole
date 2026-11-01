//! A fake channel is paced like the service, per bot: more bots, more sends.

use bytes::Bytes;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};
use std::time::{Duration, Instant};

/// The demo on memory, 4 sends a second per bot and chat, `bots` bots on
/// every channel; how long `n` small puts take.
async fn puts_with(bots: usize, n: usize) -> Duration {
    let text = DEMO_CONFIG
        .replace("fake = \"channel-data\"", &format!("fake = \"channel-data\"\nbots = {bots}"))
        .replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"")
        .replace("rate_per_sec = 20.0\nburst = 20.0", "rate_per_sec = 4.0\nburst = 1.0");
    let cfg = parse_config(&text).unwrap();
    let world = World::default();
    let node = Node::open(&world, &cfg).await.unwrap();
    node.create_bucket("demo").await.unwrap();
    let t0 = Instant::now();
    for i in 0..n {
        node.put("demo", &format!("k{i}"), Bytes::from(vec![i as u8; 2000])).await.unwrap();
    }
    let took = t0.elapsed();
    node.close().await.unwrap();
    world.close().await.unwrap();
    took
}

#[tokio::test]
async fn three_bots_post_faster_than_one() {
    let one = puts_with(1, 8).await;
    let three = puts_with(3, 8).await;
    // One bot: a send every 250 ms on each channel, 8 puts ≈ 1.75 s.
    assert!(one >= Duration::from_millis(1500), "one bot is paced: {one:?}");
    assert!(three * 2 < one, "three bots share the work: {three:?} vs {one:?}");
}
