//! The demo's faults: a channel that loses its messages, a channel that is
//! down; reads go to the other copy, and repair puts the copies back.

use bytes::Bytes;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};

/// The demo on memory channels, `quorum` of its two copies.
fn config(quorum: usize) -> pigeonhole_config::Config {
    let text = DEMO_CONFIG
        .replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"")
        .replace("write_quorum = 2", &format!("write_quorum = {quorum}"));
    parse_config(&text).unwrap()
}

fn object(i: u8) -> Bytes {
    Bytes::from((0..5000u32).map(|n| (n as u8) ^ i).collect::<Vec<u8>>())
}

async fn count(world: &World, node: &Node, store: &str) -> u64 {
    let c = node.channels.iter().find(|c| c.store == store).unwrap();
    world.describe(c, 0).await.unwrap()["total"].as_u64().unwrap()
}

#[tokio::test]
async fn a_wiped_channel_is_read_around_found_by_verify_and_repaired() {
    let world = World::default();
    let node = Node::open(&world, &config(2)).await.unwrap();
    node.create_bucket("demo").await.unwrap();
    for i in 0..3 {
        node.put("demo", &format!("k{i}"), object(i)).await.unwrap();
    }
    let tg = node.channels.iter().find(|c| c.store == "telegram").unwrap().clone();
    assert_eq!(world.wipe(&tg).await.unwrap(), 3, "a chunk a message");
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
        let world = World::default();
        let node = Node::open(&world, &config(quorum)).await.unwrap();
        node.create_bucket("demo").await.unwrap();
        let dc = node.channels.iter().find(|c| c.store == "discord").unwrap().clone();
        world.set_down(&dc, true);
        let put = node.put("demo", "k", object(1)).await;
        assert_eq!(put.is_ok(), quorum == 1, "quorum {quorum}: {put:?}");
        world.set_down(&dc, false);
        assert_eq!(count(&world, &node, "discord").await, 0);
        let repaired = node.repair().await.unwrap().repaired;
        // The write that counted with one copy gets its second once discord is back.
        assert_eq!((repaired, count(&world, &node, "discord").await), if quorum == 1 { (1, 1) } else { (0, 0) });
        node.close().await.unwrap();
        world.close().await.unwrap();
    }
}
