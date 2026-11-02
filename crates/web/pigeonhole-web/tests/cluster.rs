//! Three nodes of one cluster in this process, as the site's demo runs
//! them: nodes 1 and 2 linked directly (gRPC's place), node 3 over the
//! chat (fake Telegram outboxes). One leader writes; every node applies;
//! the leader cut off, another takes over.

use bytes::Bytes;
use pigeonhole_web::cluster::Net;
use pigeonhole_web::{parse_config, Node, World, DEMO_CONFIG};
use std::collections::BTreeMap;
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

fn leader(nodes: &BTreeMap<u64, Node>) -> Option<u64> {
    nodes.iter().find(|(_, n)| n.admin.meta.is_leader()).map(|(id, _)| *id)
}

#[tokio::test]
async fn three_nodes_in_one_process_elect_write_and_fail_over() {
    std::env::set_var("PH_DEMO_CLUSTER_KEY", KEY);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("TEST_LOG").unwrap_or_default())
        .with_test_writer()
        .try_init();
    let world = Arc::new(World::default());
    let net = Net::default();
    let configs: Vec<_> = (1..=3).map(|n| parse_config(&config(n)).unwrap()).collect();
    // All at once: a tablet of a cluster waits for a leader.
    let opened = futures::future::join_all(configs.iter().map(|cfg| Node::open_in(&world, cfg, &net))).await;
    let mut nodes: BTreeMap<u64, Node> = (1..=3).zip(opened.into_iter().map(Result::unwrap)).collect();
    eventually("a leader", || leader(&nodes).is_some()).await;
    let first = leader(&nodes).unwrap();

    nodes[&first].create_bucket("demo").await.unwrap();
    nodes[&first].put("demo", "a.bin", Bytes::from(vec![7u8; 9000])).await.unwrap();
    for (id, node) in &nodes {
        let objects = loop {
            match node.list("demo").await {
                Ok(o) if !o.is_empty() => break o,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        assert_eq!(objects.len(), 1, "node {id} applied the put");
    }
    // The chat node's frames are posts in the fake outboxes.
    let outboxes = nodes[&first].admin.channels.as_ref().unwrap().describe(10).await.unwrap();
    assert!(outboxes.to_string().contains("outbox"), "{outboxes}");

    // The leader cut off: the other two elect one of themselves and go on.
    net.isolate(&world, configs[0].cluster.as_ref().unwrap(), first, true);
    let gone = nodes.remove(&first).unwrap();
    eventually("a new leader", || leader(&nodes).is_some_and(|l| l != first)).await;
    let second = leader(&nodes).unwrap();
    nodes[&second].put("demo", "b.bin", Bytes::from(vec![8u8; 9000])).await.unwrap();
    assert_eq!(nodes[&second].list("demo").await.unwrap().len(), 2);
    gone.close().await.unwrap();
    for n in nodes.values() {
        n.close().await.unwrap();
    }
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
