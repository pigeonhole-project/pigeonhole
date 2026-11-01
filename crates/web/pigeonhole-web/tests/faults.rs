//! The demo's faults: a channel that loses its messages, a channel that is
//! down; reads go to the other copy, and repair puts the copies back.

use bytes::Bytes;
use pigeonhole_config::stores::FakeChannel;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};
use std::time::Duration;

/// The demo on memory channels, `quorum` of its two copies.
fn config(quorum: usize) -> pigeonhole_config::Config {
    let text = DEMO_CONFIG
        .replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"")
        .replace("write_quorum = 2", &format!("write_quorum = {quorum}"));
    parse_config(&text).unwrap()
}

/// Bytes that do not compress: a document, never a text message.
fn noise(len: usize) -> Bytes {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn object(i: u8) -> Bytes {
    Bytes::from((0..5000u32).map(|n| (n as u8) ^ i).collect::<Vec<u8>>())
}

/// What a data channel holds besides blobs: the pinned owner marker, and on
/// Telegram the service message the pin posts.
const TG_MARKER: u64 = 2;
const DC_MARKER: u64 = 1;

/// Store `store` of `node`'s fake channels.
fn fake(node: &Node, store: &str) -> FakeChannel {
    node.stores.iter().find(|s| s.store == store).and_then(|s| s.fake.clone()).unwrap()
}

/// Channel `store` as the admin's UI sees it, with its last `limit` messages.
async fn describe(node: &Node, store: &str, limit: usize) -> serde_json::Value {
    let all = node.admin.channels.as_ref().unwrap().describe(limit).await.unwrap();
    all["channels"].as_array().unwrap().iter().find(|c| c["store"] == store).unwrap().clone()
}

async fn count(_world: &World, node: &Node, store: &str) -> u64 {
    describe(node, store, 0).await["total"].as_u64().unwrap()
}

#[tokio::test]
async fn a_wiped_channel_is_read_around_found_by_verify_and_repaired() {
    let world = std::sync::Arc::new(World::default());
    let node = Node::open(&world, &config(2)).await.unwrap();
    node.create_bucket("demo").await.unwrap();
    for i in 0..3 {
        node.put("demo", &format!("k{i}"), object(i)).await.unwrap();
    }
    let tg = fake(&node, "telegram");
    assert_eq!(world.fakes.wipe(&tg).await.unwrap() as u64, 3 + TG_MARKER, "a chunk a message, and the marker");
    assert_eq!(count(&world, &node, "telegram").await, 0);
    for i in 0..3 {
        assert_eq!(node.get("demo", &format!("k{i}"), None).await.unwrap(), object(i), "read from discord");
    }
    assert_eq!(node.verify("telegram").await.unwrap(), (3, 3));
    assert_eq!(node.verify("discord").await.unwrap(), (3, 0));
    assert_eq!(node.repair().await.unwrap().repaired, 3);
    assert_eq!(count(&world, &node, "telegram").await, 3, "the copies are back");
    assert_eq!(node.verify("telegram").await.unwrap(), (3, 0));
    node.close().await.unwrap();
    world.close().await.unwrap();
}

#[tokio::test]
async fn a_channel_down_fails_a_write_of_two_and_takes_one_of_two() {
    for quorum in [2, 1] {
        let world = std::sync::Arc::new(World::default());
        let node = Node::open(&world, &config(quorum)).await.unwrap();
        node.create_bucket("demo").await.unwrap();
        let dc = fake(&node, "discord");
        world.fakes.set_down(&dc, true);
        let put = node.put("demo", "k", object(1)).await;
        assert_eq!(put.is_ok(), quorum == 1, "quorum {quorum}: {put:?}");
        world.fakes.set_down(&dc, false);
        assert_eq!(count(&world, &node, "discord").await, DC_MARKER);
        let repaired = node.repair().await.unwrap().repaired;
        // The write that counted with one copy gets its second once discord is back.
        let blobs = if quorum == 1 { 1 } else { 0 };
        assert_eq!((repaired, count(&world, &node, "discord").await), (blobs, blobs + DC_MARKER));
        node.close().await.unwrap();
        world.close().await.unwrap();
    }
}

/// The demo on memory, a background repair pass every second (the least).
async fn open_with_repair() -> (std::sync::Arc<World>, Node) {
    let text = DEMO_CONFIG
        .replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"")
        .replace("[repair]\ninterval = \"10s\"", "[repair]\ninterval = \"1s\"");
    let world = std::sync::Arc::new(World::default());
    let cfg = parse_config(&text).unwrap();
    let node = Node::open(&world, &cfg).await.unwrap();
    node.create_bucket("demo").await.unwrap();
    (world, node)
}

/// A read that has to go to `wiped`'s copy of a document (the other copy is
/// down) finds it gone and queues its repair; the background pass does it.
#[tokio::test]
async fn a_read_that_finds_a_document_gone_queues_it_and_the_background_repairs_it() {
    for wiped in ["telegram", "discord"] {
        let (world, node) = open_with_repair().await;
        let body = noise(2_000_000);
        node.put("demo", "k", body.clone()).await.unwrap();
        let lost = fake(&node, wiped);
        let other =
            node.stores.iter().find(|s| s.role == "data" && s.store != wiped).and_then(|s| s.fake.clone()).unwrap();
        world.fakes.wipe(&lost).await.unwrap();
        world.fakes.set_down(&other, true);
        assert!(node.get("demo", "k", None).await.is_err(), "{wiped}: no copy to read");
        world.fakes.set_down(&other, false);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(node.repair_queue().await.unwrap(), 1, "{wiped}: the read queued the lost copy");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(node.repair_queue().await.unwrap(), 0, "{wiped}: a background pass repaired it");
        assert_eq!(count(&world, &node, wiped).await, 1, "the marker went with the wipe");
        world.fakes.set_down(&other, true);
        assert_eq!(node.get("demo", "k", None).await.unwrap(), body, "{wiped}: read from the repaired copy");
        world.fakes.set_down(&other, false);
        node.close().await.unwrap();
        world.close().await.unwrap();
    }
}

/// A text blob is answered from the store's cache: its loss shows only to
/// verify (or once the cache forgets it).
#[tokio::test]
async fn a_deleted_text_blob_is_still_read_from_the_cache_until_verify() {
    let (world, node) = open_with_repair().await;
    node.put("demo", "k", object(1)).await.unwrap();
    let tg = fake(&node, "telegram");
    let id = describe(&node, "telegram", 1).await["messages"][0]["id"].to_string();
    world.fakes.delete_message(&tg, &id).await.unwrap();
    let dc = fake(&node, "discord");
    world.fakes.set_down(&dc, true);
    assert_eq!(node.get("demo", "k", None).await.unwrap(), object(1), "from telegram's cache");
    world.fakes.set_down(&dc, false);
    assert_eq!(node.repair_queue().await.unwrap(), 0);
    assert_eq!(node.verify("telegram").await.unwrap(), (1, 1));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(count(&world, &node, "telegram").await, 1 + TG_MARKER, "repaired in the background");
    node.close().await.unwrap();
    world.close().await.unwrap();
}
