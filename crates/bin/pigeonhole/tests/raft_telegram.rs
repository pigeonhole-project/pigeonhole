//! Two nodes on gRPC and one that only has a Telegram outbox: Raft frames
//! over the real `TelegramMailbox` on fake channels (no push: readers poll).

use openraft_chat::{ChatNetwork, Mailbox, Settings};
use openraft_grpc::{GrpcNetwork, Identity, Mixed};
use pigeonhole::raft_mailbox::TelegramMailbox;
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{
    BlobSnapshotStore, ChainLogStore, Cluster, MetaOptions, MetaStore, Namespaces, Never, Outcome, Serve, StateMachine,
};
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_storage_telegram::fake::FakeChannel;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

const MEMBERS: [u64; 3] = [1, 2, 3];
const GRPC: [u64; 2] = [1, 2];

#[derive(Default, Serialize, Deserialize)]
struct Sum(u64);

impl StateMachine for Sum {
    const NAME: &'static str = "sum";
    const VERSION: u32 = 1;
    type Command = u64;
    type Reply = ();
    type Reject = Never;
    fn apply(&mut self, n: u64) -> anyhow::Result<Outcome<Self>> {
        self.0 += n;
        Ok(Ok(()))
    }
}

fn sum(n: &MetaStore) -> u64 {
    n.read::<Sum, _>(|s| s.0)
}

async fn node(
    id: u64,
    outboxes: &HashMap<u64, Arc<FakeChannel>>,
    addrs: &HashMap<u64, String>,
    listener: Option<tokio::net::TcpListener>,
) -> Arc<MetaStore> {
    let via_chat: HashSet<u64> = match GRPC.contains(&id) {
        true => MEMBERS.into_iter().filter(|m| !GRPC.contains(m)).collect(),
        false => MEMBERS.into_iter().filter(|m| *m != id).collect(),
    };
    let mailbox = |of: u64| -> Arc<dyn Mailbox> { Arc::new(TelegramMailbox::new(outboxes[&of].clone())) };
    let peers = via_chat.iter().map(|&m| (m, mailbox(m))).collect();
    let settings =
        Settings { cluster: 5, key: [9; 32], tick: Duration::from_millis(20), poll: Duration::from_millis(100) };
    let chat = ChatNetwork::<TypeConfig>::new(id, settings, mailbox(id), peers);
    let grpc_peers = addrs.iter().filter(|(&m, _)| m != id && !via_chat.contains(&m)).map(|(&m, a)| (m, a.clone()));
    let identity = Identity { me: id, cluster: 5, key: [9; 32] };
    let grpc = GrpcNetwork::<TypeConfig>::new(identity.clone(), grpc_peers.collect());
    let serving = chat.clone();
    let serve: Serve = Box::new(move |raft| {
        Box::pin(async move {
            let raft = Arc::new(raft);
            if let Some(l) = listener {
                tokio::spawn(openraft_grpc::serve::<TypeConfig>(l, identity, raft.clone()));
            }
            serving.serve(raft).await;
        })
    });
    let opts = MetaOptions {
        heartbeat: Duration::from_millis(200),
        election_timeout: (Duration::from_millis(1_500), Duration::from_millis(3_000)),
        ..MetaOptions::default()
    };
    let cluster = Cluster {
        node: id,
        members: MEMBERS.into(),
        network: Mixed { a: grpc, b: chat, via_b: via_chat },
        serve: Some(serve),
    };
    let log = Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let snapshots = Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    MetaStore::open_in(log, snapshots, Namespaces::new().with::<Sum>(), opts, cluster).await.unwrap()
}

async fn eventually(what: &str, ok: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !ok() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_node_on_telegram_outboxes_joins_two_on_grpc() {
    let outboxes: HashMap<u64, Arc<FakeChannel>> =
        MEMBERS.iter().map(|&id| (id, Arc::new(FakeChannel::new(1000 + id as i64)))).collect();
    let mut listeners = HashMap::new();
    let mut addrs = HashMap::new();
    for id in GRPC {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(id, l.local_addr().unwrap().to_string());
        listeners.insert(id, l);
    }
    let started = futures::future::join_all(MEMBERS.map(|id| node(id, &outboxes, &addrs, listeners.remove(&id)))).await;
    let mut nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();

    let leader = || nodes.values().find(|n| n.is_leader()).cloned();
    eventually("a leader", || leader().is_some()).await;
    for _ in 0..3 {
        leader().unwrap().propose::<Sum>(1).await.unwrap().unwrap();
    }
    eventually("the Telegram node applied the commits", || sum(&nodes[&3]) == 3).await;
    assert!(outboxes[&3].message_ids().len() > 1, "node 3 answered through its outbox");
    assert!(outboxes[&1].message_ids().len() + outboxes[&2].message_ids().len() > 0, "the pair wrote to node 3");

    // The leader dies: the other two (one of them on Telegram only) go on.
    let old = leader().unwrap().node();
    drop(nodes.remove(&old));
    eventually("a new leader", || nodes.values().any(|n| n.is_leader())).await;
    let new = nodes.values().find(|n| n.is_leader()).unwrap().clone();
    new.propose::<Sum>(10).await.unwrap().unwrap();
    eventually("both survivors applied it", || nodes.values().all(|n| sum(n) == 13)).await;
}
