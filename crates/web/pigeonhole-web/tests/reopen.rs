//! A node closed cleanly reopens on its fake channels at once: a Discord
//! log's lease was given back (a crash would leave it to run out).

use pigeonhole_web::{parse_config, Node, World};
use std::time::{Duration, Instant};

const CFG: &str = include_str!("discord-log.toml");

async fn open_once(text: &str) -> Duration {
    let world = World::default();
    let t0 = Instant::now();
    let node = Node::open(&world, &parse_config(text).unwrap()).await.unwrap();
    let took = t0.elapsed();
    node.close().await.unwrap();
    world.close().await.unwrap();
    took
}

#[tokio::test]
async fn a_discord_log_closed_cleanly_reopens_within_the_lease_margin() {
    let dir = tempfile::tempdir().unwrap();
    let text = CFG.replace("DIR", dir.path().to_str().unwrap());
    for _ in 0..3 {
        let took = open_once(&text).await;
        // The lease is 45 s; a clean close leaves only its 1 s margin.
        assert!(took < Duration::from_secs(3), "{took:?}");
    }
}
