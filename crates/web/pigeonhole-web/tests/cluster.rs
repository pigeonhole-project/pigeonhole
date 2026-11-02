//! Three nodes of one cluster in this process, as the site's demo runs
//! them: nodes 1 and 2 linked directly (gRPC's place), node 3 over the
//! chat (fake Telegram outboxes). One leader writes; every node applies;
//! the leader cut off, another takes over.

use bytes::Bytes;
use pigeonhole_admin::Request;
use pigeonhole_web::cluster::Nodes;
use pigeonhole_web::{parse_config, World, DEMO_CONFIG};
use std::sync::Arc;
use std::time::Duration;

const KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

/// Node `node`'s pigeonhole.toml: the demo's chunk channels (shared), its
/// own log and snapshots channels, the cluster.
fn config(node: u64) -> String {
    let data = DEMO_CONFIG
        .replace("fake = \"channel-data\"", "fake = \"memory\"")
        .replace("[[store]]\nid = \"channel-data\"\nkind = \"local_disk\"\npath = \"/data/channels\"\n", "")
        .replace("id = \"tg-log\"", &format!("id = \"tg-log-{node}\""))
        .replace("-1001000000002", &format!("-100100000002{node}"))
        .replace("id = \"tg-snapshots\"", &format!("id = \"tg-snapshots-{node}\""))
        .replace("-1001000000003", &format!("-100100000003{node}"))
        .replace(
            "log = \"tg-log\"\nsnapshot = \"tg-snapshots\"",
            &format!("log = \"tg-log-{node}\"\nsnapshot = \"tg-snapshots-{node}\""),
        );
    format!(
        "{data}\n[cluster]\nid = 7\nnode = {node}\nkey_env = \"PH_DEMO_CLUSTER_KEY\"\nfake = \"memory\"\n\
         heartbeat = \"1s\"\nelection_timeout_min = \"4s\"\nelection_timeout_max = \"8s\"\ntick = \"100ms\"\npoll = \"200ms\"\n\
         [[node]]\nid = 1\ngrpc = \"node-1\"\noutbox = -1001000000101\n\
         [[node]]\nid = 2\ngrpc = \"node-2\"\noutbox = -1001000000102\n\
         [[node]]\nid = 3\noutbox = -1001000000103\n"
    )
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    for _ in 0..600 {
        if ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never: {what}");
}

/// An op as the page sends it, on node `node`.
async fn op(nodes: &Nodes, node: Option<u64>, json: &str) -> anyhow::Result<serde_json::Value> {
    let r: Request = serde_json::from_str(json).unwrap();
    nodes.node(node)?.admin.handle(&Request { node, ..r }, Bytes::from(vec![5u8; 3000])).await
}

#[tokio::test]
async fn three_nodes_in_one_process_elect_write_and_fail_over() {
    std::env::set_var("PH_DEMO_CLUSTER_KEY", KEY);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("TEST_LOG").unwrap_or_default())
        .with_test_writer()
        .try_init();
    let world = Arc::new(World::default());
    let configs: Vec<_> = (1..=3).map(|n| parse_config(&config(n)).unwrap()).collect();
    let nodes = Nodes::open(&world, &configs).await.unwrap();
    eventually("a leader", || nodes.leader().is_some()).await;
    let first = nodes.leader().unwrap();

    // The page's ops go to the leader unless they name a node.
    op(&nodes, None, r#"{"op":"put","bucket":"demo","key":"a.bin"}"#).await.unwrap_err();
    nodes.node(None).unwrap().create_bucket("demo").await.unwrap();
    op(&nodes, None, r#"{"op":"put","bucket":"demo","key":"a.bin"}"#).await.unwrap();
    for (id, node) in &nodes.nodes {
        let objects = loop {
            match node.list("demo").await {
                Ok(o) if !o.is_empty() => break o,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        assert_eq!(objects.len(), 1, "node {id} applied the put");
    }
    // A client knocking on a follower is turned away, told who leads.
    let follower = *nodes.nodes.keys().find(|&&id| id != first).unwrap();
    let refused = op(&nodes, Some(follower), r#"{"op":"put","bucket":"demo","key":"b.bin"}"#).await.unwrap_err();
    assert_eq!(refused.to_string(), format!("not the leader; node {first} is"));
    // The cluster as each node sees it: the leader, and its own log by its id.
    let view = op(&nodes, Some(follower), r#"{"op":"cluster"}"#).await.unwrap();
    assert_eq!(view["leader"], first);
    let mine = view["nodes"].as_array().unwrap().iter().find(|n| n["id"] == follower).unwrap();
    assert_eq!(mine["log"], format!("tg-log-{follower}"), "{view}");
    // The chat node's frames are posts in the fake outboxes.
    let channels = op(&nodes, None, r#"{"op":"channels","limit":5}"#).await.unwrap();
    let outbox = channels["channels"].as_array().unwrap().iter().find(|c| c["role"] == "outbox").unwrap();
    assert!(outbox["total"].as_u64().unwrap() > 0, "{outbox}");

    // The leader cut off: the other two elect one of themselves and go on.
    nodes.isolate(&world, first, true).unwrap();
    eventually("a new leader", || nodes.leader().is_some_and(|l| l != first)).await;
    let second = nodes.leader().unwrap();
    op(&nodes, Some(second), r#"{"op":"put","bucket":"demo","key":"c.bin"}"#).await.unwrap();
    assert_eq!(nodes.nodes[&second].list("demo").await.unwrap().len(), 2);
    nodes.close().await.unwrap();
    world.close().await.unwrap();
}

#[tokio::test]
async fn a_frame_posted_to_a_fake_outbox_is_read_back() {
    use openraft_chat::Mailbox;
    std::env::set_var("PH_DEMO_CLUSTER_KEY", KEY);
    let world = World::default();
    let cfg = parse_config(&config(1)).unwrap();
    let outbox = cfg.cluster.as_ref().unwrap().outbox_channel(3).unwrap();
    let writer = pigeonhole_raft_telegram::TelegramMailbox::new(Arc::new(
        world.fakes.telegram_bot(&cfg, &outbox).await.unwrap(),
    ));
    let reader = pigeonhole_raft_telegram::TelegramMailbox::new(Arc::new(
        world.fakes.telegram_bot(&cfg, &outbox).await.unwrap(),
    ));
    let at = writer.post(Bytes::from_static(b"a frame")).await.unwrap();
    assert_eq!(reader.read_after(0, 10).await.unwrap(), vec![(at, Bytes::from_static(b"a frame"))]);
}
