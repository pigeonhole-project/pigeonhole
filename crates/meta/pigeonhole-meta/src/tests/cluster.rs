//! Three tablets as one Raft cluster over openraft-chat on a fake chat
//! (docs/raft-over-chat.md): election, commits applied everywhere, writes
//! to a follower, a killed leader, a partitioned leader that heals.

use super::*;
use crate::raft::TypeConfig;
use openraft_chat::{ChatNetwork, FakeChat, Mailbox, Settings};
use std::collections::HashMap;

const MEMBERS: [u64; 3] = [1, 2, 3];

fn opts() -> MetaOptions {
    MetaOptions {
        heartbeat: Duration::from_millis(100),
        election_timeout: (Duration::from_millis(800), Duration::from_millis(1600)),
        ..MetaOptions::default()
    }
}

fn settings() -> Settings {
    Settings { cluster: 7, key: [3; 32], tick: Duration::from_millis(20), poll: Duration::from_millis(200) }
}

/// Node `id` of a cluster of `MEMBERS`, its log and snapshots in memory.
async fn node(chat: FakeChat, id: u64) -> Arc<MetaStore> {
    let peers: HashMap<u64, Arc<dyn Mailbox>> =
        MEMBERS.iter().filter(|&&m| m != id).map(|&m| (m, chat.mailbox(id, m))).collect();
    let net = ChatNetwork::<TypeConfig>::new(id, settings(), chat.mailbox(id, id), peers);
    let log = Arc::new(crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let snapshots = Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    let serving = net.clone();
    let cluster = Cluster {
        node: id,
        members: MEMBERS.into(),
        network: net,
        serve: Some(Box::new(move |raft| Box::pin(async move { serving.serve(Arc::new(raft)).await }))),
    };
    MetaStore::open_in(log, snapshots, counter(), opts(), cluster).await.unwrap()
}

/// The three nodes, started together (each waits for a leader).
async fn three(chat: &FakeChat) -> HashMap<u64, Arc<MetaStore>> {
    let started = futures::future::join_all(MEMBERS.map(|id| node(chat.clone(), id))).await;
    MEMBERS.into_iter().zip(started).collect()
}

async fn eventually(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !ok() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The leader all of `nodes` agree on.
async fn agreed_leader(nodes: &HashMap<u64, Arc<MetaStore>>) -> u64 {
    let mut leader = None;
    eventually("one leader all agree on", || {
        let seen: Vec<_> = nodes.values().map(|n| n.leader()).collect();
        leader = seen[0];
        leader.is_some() && seen.iter().all(|l| *l == leader) && nodes.contains_key(&leader.unwrap())
    })
    .await;
    leader.unwrap()
}

#[tokio::test]
async fn three_nodes_elect_a_leader_and_apply_its_commits_everywhere() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    for i in 0..5 {
        nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    eventually("every node applied the 5 commits", || nodes.values().all(|n| value(n) == 5)).await;
}

#[tokio::test]
async fn a_write_to_a_follower_names_the_leader_and_the_follower_goes_on() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    let follower = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    let err = nodes[&follower].propose::<Counter>(op(1, 1)).await.unwrap_err();
    assert_eq!(err.downcast_ref::<NotLeader>(), Some(&NotLeader { leader: Some(leader) }), "{err:#}");
    nodes[&leader].propose::<Counter>(op(2, 1)).await.unwrap().unwrap();
    eventually("the follower applied the leader's commit", || value(&nodes[&follower]) == 1).await;
}

#[tokio::test]
async fn a_killed_leader_is_replaced_and_writes_go_on() {
    let chat = FakeChat::new();
    let mut nodes = three(&chat).await;
    let old = agreed_leader(&nodes).await;
    nodes[&old].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    chat.isolate(old, true);
    drop(nodes.remove(&old));
    let new = agreed_leader(&nodes).await;
    assert_ne!(new, old);
    nodes[&new].propose::<Counter>(op(2, 1)).await.unwrap().unwrap();
    eventually("both survivors applied both commits", || nodes.values().all(|n| value(n) == 2)).await;
}

#[tokio::test]
async fn a_partitioned_leader_steps_down_and_catches_up_when_healed() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let old = agreed_leader(&nodes).await;
    nodes[&old].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();

    chat.isolate(old, true);
    let rest: HashMap<_, _> = nodes.iter().filter(|(&id, _)| id != old).map(|(&id, n)| (id, n.clone())).collect();
    let new = agreed_leader(&rest).await;
    assert_ne!(new, old);
    for i in 2..=4 {
        nodes[&new].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }

    chat.isolate(old, false);
    eventually("the old leader follows the new one", || nodes[&old].leader() == Some(new)).await;
    eventually("and applied what it missed", || value(&nodes[&old]) == 4).await;
}
