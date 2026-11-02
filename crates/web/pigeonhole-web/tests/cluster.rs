//! Three nodes of one cluster in this process, as the site's demo runs
//! them: nodes 1 and 2 linked directly (gRPC's place), node 3 over the
//! chat (fake Telegram outboxes). One leader writes; every node applies;
//! the leader cut off, another takes over.

use bytes::Bytes;
use pigeonhole_admin::Request;
use pigeonhole_web::cluster::Nodes;
use pigeonhole_web::{parse_config_as, World, DEMO_CONFIG};
use std::sync::Arc;
use std::time::Duration;

const KEY: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";

/// The cluster's pigeonhole.toml, one for every node: the demo's chunk
/// channels (shared), each node's own log and snapshots channels named in
/// its [[node]], the links.
fn config() -> String {
    let data = DEMO_CONFIG
        .replace("fake = \"channel-data\"", "fake = \"memory\"")
        .replace("[[store]]\nid = \"channel-data\"\nkind = \"local_disk\"\npath = \"/data/channels\"\n", "");
    // The demo's log and snapshots channels become node 1's; nodes 2 and 3 get their own.
    let mut text = data.replace("log = \"tg-log\"\nsnapshot = \"tg-snap\"\n", "");
    for node in 2..=3 {
        for (what, at) in [("log", 2), ("snap", 3)] {
            text.push_str(&format!("\n[[store]]\nid = \"tg-{what}-{node}\"\nkind = \"telegram\"\nchat_id = -100100000{at}0{node}\nfake = \"memory\"\n"));
        }
    }
    text.push_str(
        "\n[cluster]\nid = 7\nkey_env = \"PH_DEMO_CLUSTER_KEY\"\nfake = \"memory\"\n\
         heartbeat = \"1s\"\nelection_timeout_min = \"4s\"\nelection_timeout_max = \"8s\"\ntick = \"100ms\"\npoll = \"200ms\"\n\
         [[node]]\nid = 1\ngrpc = \"node-1\"\noutbox = -1001000000101\nlog = \"tg-log\"\nsnapshot = \"tg-snap\"\n\
         [[node]]\nid = 2\ngrpc = \"node-2\"\noutbox = -1001000000102\nlog = \"tg-log-2\"\nsnapshot = \"tg-snap-2\"\n\
         [[node]]\nid = 3\noutbox = -1001000000103\nlog = \"tg-log-3\"\nsnapshot = \"tg-snap-3\"\n",
    );
    text
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
    let nodes = Nodes::open(&world, &config()).await.unwrap();
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
    let own = if follower == 1 { "tg-log".to_string() } else { format!("tg-log-{follower}") };
    assert_eq!(mine["log"], own, "{view}");
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
    let cfg = parse_config_as(&config(), 1).unwrap();
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
