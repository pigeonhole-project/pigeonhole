//! The admin ops (pigeonhole-admin) as the UI sends them: the same on the
//! demo's node as on a server's `/admin/ops`.

use bytes::Bytes;
use pigeonhole_admin::Request;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};
use serde_json::Value;
use std::sync::Arc;

async fn op(node: &Node, json: &str, body: &[u8]) -> Value {
    let r: Request = serde_json::from_str(json).unwrap();
    node.admin.handle(&r, Bytes::copy_from_slice(body)).await.unwrap()
}

#[tokio::test]
async fn the_ui_ops_run_on_a_node() {
    let text = DEMO_CONFIG.replace("kind = \"local_disk\"\npath = \"/data/channels\"", "kind = \"memory\"");
    let world = Arc::new(World::default());
    let node = Node::open(&world, &parse_config(&text).unwrap()).await.unwrap();
    node.create_bucket("photos").await.unwrap();

    assert_eq!(op(&node, r#"{"op":"put","bucket":"photos","key":"a.txt"}"#, &[7u8; 5000]).await["ok"], true);
    assert_eq!(op(&node, r#"{"op":"buckets"}"#, b"").await["buckets"], serde_json::json!(["photos"]));
    let info = op(&node, r#"{"op":"info"}"#, b"").await;
    assert_eq!(info["buckets"], serde_json::json!(["photos"]));
    assert_eq!(
        op(&node, r#"{"op":"list","bucket":"photos"}"#, b"").await["objects"],
        serde_json::json!([["a.txt", 5000]])
    );
    assert_eq!(op(&node, r#"{"op":"get","bucket":"photos","key":"a.txt","range":[0,99]}"#, b"").await["len"], 100);

    let bg = op(&node, r#"{"op":"background"}"#, b"").await;
    let tasks: Vec<&str> = bg["tasks"].as_array().unwrap().iter().map(|t| t["task"].as_str().unwrap()).collect();
    assert_eq!(tasks, ["sweep", "repair", "cool", "snapshot"]);
    op(&node, r#"{"op":"dynamic_set","key":"repair.interval","value":"off"}"#, b"").await;
    let repair = &op(&node, r#"{"op":"background"}"#, b"").await["tasks"][1];
    assert_eq!((&repair["every"], &repair["next_in_ms"]), (&Value::from("off"), &Value::Null));

    // The demo's channels can be broken; a node with real ones refuses.
    assert_eq!(op(&node, r#"{"op":"outage","store":"discord","down":true}"#, b"").await["down"], true);
    let channels = op(&node, r#"{"op":"channels","limit":5}"#, b"").await;
    let discord = channels["channels"].as_array().unwrap().iter().find(|c| c["store"] == "discord").unwrap();
    assert_eq!(discord["down"], true);
    let bad: Request = serde_json::from_str(r#"{"op":"nap"}"#).unwrap();
    assert!(node.admin.handle(&bad, Bytes::new()).await.is_err());

    node.close().await.unwrap();
    world.close().await.unwrap();
}
