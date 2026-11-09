//! Three tablets as one Raft cluster over a fake chat that delays, loses
//! and doubles posts, while a node at a time is cut off and healed. Every
//! command a leader acknowledged survives, and the nodes end up equal.

use openraft_chat::{ChatNetwork, FakeChat, Mailbox, Settings};
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, Cluster, MetaOptions, MetaStore, Namespaces};
use pigeonhole_sim::{random_ms, random_u64, Journal, Record};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

const MEMBERS: [u64; 3] = [1, 2, 3];

async fn node(chat: FakeChat, id: u64) -> Arc<MetaStore> {
    let peers: HashMap<u64, Arc<dyn Mailbox>> =
        MEMBERS.iter().filter(|&&m| m != id).map(|&m| (m, chat.mailbox(id, m))).collect();
    let settings = Settings {
        cluster: 7,
        key: [5; 32],
        tick: Duration::from_millis(20),
        poll: Duration::from_millis(200),
        keep: None,
    };
    let net = ChatNetwork::<TypeConfig>::new(id, settings, chat.mailbox(id, id), peers);
    let serving = net.clone();
    let cluster = Cluster {
        node: id,
        members: MEMBERS.into(),
        network: net,
        serve: Some(Box::new(move |raft| Box::pin(async move { serving.serve(Arc::new(raft)).await }))),
    };
    let opts = MetaOptions {
        heartbeat: Duration::from_millis(300),
        election_timeout: (Duration::from_millis(2_000), Duration::from_millis(4_000)),
        snapshot_every: Some(16),
        keep_snapshots: 1,
        ..MetaOptions::default()
    };
    let log = Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let snapshots = Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    MetaStore::open_in(log, snapshots, Namespaces::new().with::<Journal>(), opts, cluster).await.unwrap()
}

fn ids(node: &MetaStore) -> BTreeSet<u64> {
    node.read::<Journal, _>(|j| j.entries.keys().copied().collect())
}

#[tokio::test]
#[cfg_attr(not(madsim), ignore = "a minute of real time on tokio; `make sim` runs it in virtual time")]
async fn acknowledged_commands_survive_a_lossy_slow_chat_and_partitions() {
    let chat = FakeChat::new();
    chat.set_delay(Some(Duration::from_millis(150)));
    chat.set_loss(Some(7));
    chat.set_duplication(Some(5));
    let started = futures::future::join_all(MEMBERS.map(|id| node(chat.clone(), id))).await;
    let nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();

    // A client that writes to whichever node leads, retrying elsewhere.
    let writer = {
        let nodes = nodes.clone();
        tokio::spawn(async move {
            let mut acked = BTreeSet::new();
            for id in 0..60u64 {
                for _ in 0..200 {
                    let leader = nodes.values().find(|n| n.is_leader());
                    if let Some(n) = leader {
                        if n.propose::<Journal>(Record { id, writer: "client".into() }).await.is_ok() {
                            acked.insert(id);
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                tokio::time::sleep(random_ms(50)).await;
            }
            acked
        })
    };
    // A node at a time cut off for a while, then healed.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(1_000 + random_u64() % 3_000)).await;
        let cut = MEMBERS[(random_u64() % 3) as usize];
        chat.isolate(cut, true);
        tokio::time::sleep(Duration::from_millis(2_000 + random_u64() % 4_000)).await;
        chat.isolate(cut, false);
    }
    let acked = writer.await.unwrap();
    assert!(acked.len() >= 50, "too few writes got through: {}", acked.len());

    // Healed and quiet: everyone converges on a set holding every ack.
    chat.set_loss(None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let sets: Vec<_> = nodes.values().map(|n| ids(n)).collect();
        if sets.iter().all(|s| s == &sets[0]) && acked.is_subset(&sets[0]) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no convergence: acked {acked:?}, nodes {sets:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // One leader at rest; one just elected serves once its term's first
    // entry applies and its layers took over (`is_leader`), so wait for that.
    let serving = || nodes.values().filter(|n| n.is_leader()).count();
    while serving() == 0 {
        assert!(tokio::time::Instant::now() < deadline, "no leader serves at rest");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let elected = nodes.values().filter(|n| n.leader() == Some(n.node())).count();
    assert_eq!((serving(), elected), (1, 1), "one leader at rest");
}
