//! The admin's view of a local disk: a node whose log and snapshots are on
//! one (as a cluster's third node may have them) shows its segment files,
//! the records in them, and what each of the log's records is to the log.

use bytes::Bytes;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn a_log_on_a_local_disk_shows_its_segments_and_batches() {
    let dir = tempfile::tempdir().unwrap();
    let text = DEMO_CONFIG
        .replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"")
        .replace("log = \"tg-log\"\nsnapshot = \"tg-snap\"", "log = \"node-disk\"\nsnapshot = \"node-disk\"")
        .replace(
            "[[store]]\nid = \"tg-log\"\nkind = \"telegram\"\nchat_id = -1001000000002\nfake = \"channel-data\"\n",
            "",
        )
        .replace(
            "[[store]]\nid = \"tg-snap\"\nkind = \"telegram\"\nchat_id = -1001000000003\nfake = \"channel-data\"\n",
            "",
        )
        + &format!(
            "\n[[store]]\nid = \"node-disk\"\nkind = \"local_disk\"\npath = {:?}\n",
            dir.path().display().to_string()
        );
    let world = Arc::new(World::default());
    let node = Node::open(&world, &parse_config(&text).unwrap()).await.unwrap();
    node.create_bucket("demo").await.unwrap();
    for i in 0..3u8 {
        node.put("demo", &format!("k{i}"), Bytes::from(vec![i; 5000])).await.unwrap();
    }

    let all = node.admin.channels.as_ref().unwrap().describe(200).await.unwrap();
    let stores: Vec<&Value> = all["channels"].as_array().unwrap().iter().collect();
    let roles: Vec<(&str, &str)> =
        stores.iter().map(|c| (c["role"].as_str().unwrap(), c["kind"].as_str().unwrap())).collect();
    assert_eq!(
        roles,
        [("data", "telegram"), ("data", "discord"), ("log", "local_disk"), ("snapshots", "local_disk")],
        "the config's order: chunk stores, then the log and snapshots"
    );
    assert!(stores[0]["messages"].as_array().is_some_and(|m| !m.is_empty()), "a fake channel shows its posts");

    let log = stores[2];
    let files = log["files"].as_array().unwrap();
    assert!(files[0]["name"].as_str().unwrap().starts_with("seg-"), "{log}");
    let records: Vec<&Value> = files.iter().flat_map(|f| f["records"].as_array().unwrap()).collect();
    assert_eq!(log["total"], records.iter().filter(|r| r["dead"] == false).count());
    let batches: Vec<&Value> = records.iter().map(|r| &r["log"]).filter(|l| l["what"] == "batch").collect();
    assert!(!batches.is_empty(), "the log's batches: {records:?}");
    let positions: Vec<u64> = batches.iter().filter_map(|b| b["last"].as_u64()).collect();
    assert!(positions.windows(2).all(|w| w[0] < w[1]), "decoded, in order: {positions:?}");
    assert!(*positions.last().unwrap() >= 4, "the bucket and three puts are in: {positions:?}");

    // A view only shows: nothing to delete on a disk.
    let gone = node.admin.channels.as_ref().unwrap().delete_message("meta-log", "1").await;
    assert!(gone.unwrap_err().to_string().contains("node's own"));
    node.close().await.unwrap();
    world.close().await.unwrap();
}
