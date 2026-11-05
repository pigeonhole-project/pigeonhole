//! Three tablets as one Raft cluster over openraft-chat on a fake chat
//! (docs/cluster.md): election, commits applied everywhere, writes
//! to a follower, a killed leader, a partitioned leader that heals, and the
//! Raft gauges.

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
    Settings { cluster: 7, key: [3; 32], tick: Duration::from_millis(20), poll: Duration::from_millis(200), keep: None }
}

/// Node `id` of a cluster of `MEMBERS`, its log and snapshots in memory.
async fn node(chat: FakeChat, id: u64) -> Arc<MetaStore> {
    node_with(chat, id, opts()).await
}

async fn node_with(chat: FakeChat, id: u64, opts: MetaOptions) -> Arc<MetaStore> {
    node_on(chat, id, opts, &Stores::new()).await
}

/// A node's log and snapshots, in memory: they outlive it, to restart it.
#[derive(Clone)]
struct Stores {
    log: Arc<MemoryBlobStore>,
    snapshots: Arc<MemoryBlobStore>,
}

impl Stores {
    fn new() -> Self {
        Self { log: Arc::new(MemoryBlobStore::new()), snapshots: Arc::new(MemoryBlobStore::new()) }
    }
}

async fn node_on(chat: FakeChat, id: u64, opts: MetaOptions, stores: &Stores) -> Arc<MetaStore> {
    let peers: HashMap<u64, Arc<dyn Mailbox>> =
        MEMBERS.iter().filter(|&&m| m != id).map(|&m| (m, chat.mailbox(id, m))).collect();
    let net = ChatNetwork::<TypeConfig>::new(id, settings(), chat.mailbox(id, id), peers);
    let log = Arc::new(crate::ChainLogStore::new(stores.log.clone()));
    let snapshots = Arc::new(crate::BlobSnapshotStore::new(stores.snapshots.clone()));
    let serving = net.clone();
    let cluster = Cluster {
        node: id,
        members: MEMBERS.into(),
        network: net,
        serve: Some(Box::new(move |raft| Box::pin(async move { serving.serve(Arc::new(raft)).await }))),
    };
    MetaStore::open_in(log, snapshots, counter(), opts, cluster).await.unwrap()
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

/// `node`'s gauges as `record_gauges` sets them, by `name` or `name{value}`
/// (the value of its one label).
fn gauges(node: &MetaStore) -> HashMap<String, f64> {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || node.record_gauges());
    let mut out = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let DebugValue::Gauge(g) = value else { continue };
        let name = match key.key().labels().next() {
            Some(l) => format!("{}{{{}}}", key.key().name(), l.value()),
            None => key.key().name().to_string(),
        };
        out.insert(name, g.into_inner());
    }
    out
}

#[tokio::test]
async fn raft_gauges_show_roles_and_a_lagging_peer() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    let mut followers = MEMBERS.into_iter().filter(|&m| m != leader);
    let (behind, other) = (followers.next().unwrap(), followers.next().unwrap());
    nodes[&leader].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    eventually("every node applied the commit", || nodes.values().all(|n| value(n) == 1)).await;

    let g = gauges(&nodes[&behind]);
    assert_eq!(g["pigeonhole_meta_raft_state{follower}"], 1.0);
    assert_eq!(g["pigeonhole_meta_raft_state{leader}"], 0.0);
    assert_eq!(g["pigeonhole_meta_raft_leader"], leader as f64);
    assert!(g["pigeonhole_meta_raft_quorum_ack_age_seconds"].is_nan(), "a follower has no quorum acks");
    assert!(g[&format!("pigeonhole_meta_raft_replication_lag{{{leader}}}")].is_nan());

    chat.isolate(behind, true);
    for i in 2..=4 {
        nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    let lag = |p: u64| format!("pigeonhole_meta_raft_replication_lag{{{p}}}");
    eventually("the leader sees the isolated peer 3 entries behind and the other caught up", || {
        let g = gauges(&nodes[&leader]);
        g[&lag(behind)] >= 3.0 && g[&lag(other)] == 0.0
    })
    .await;
    let g = gauges(&nodes[&leader]);
    assert_eq!(g["pigeonhole_meta_raft_state{leader}"], 1.0);
    assert_eq!(g["pigeonhole_meta_raft_node"], leader as f64);
    assert!(g["pigeonhole_meta_raft_term"] >= 1.0);
    assert!(g["pigeonhole_meta_raft_quorum_ack_age_seconds"] < 1.0, "the other follower acks");

    chat.isolate(behind, false);
    eventually("the healed peer catches up", || gauges(&nodes[&leader])[&lag(behind)] == 0.0).await;
}

#[tokio::test]
async fn only_the_leader_says_it_leads() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    eventually("the leader knows it leads", || nodes[&leader].is_leader()).await;
    assert!(nodes.iter().all(|(&id, n)| n.is_leader() == (id == leader)));

    // Cut off, it may think it leads a while; healed, it knows it does not.
    chat.isolate(leader, true);
    let rest: HashMap<_, _> = nodes.iter().filter(|(&id, _)| id != leader).map(|(&id, n)| (id, n.clone())).collect();
    let new = agreed_leader(&rest).await;
    chat.isolate(leader, false);
    eventually("the old leader follows", || nodes[&leader].leader() == Some(new)).await;
    eventually("one leader again", || nodes.iter().all(|(&id, n)| n.is_leader() == (id == new))).await;
}

#[tokio::test]
async fn a_follower_behind_the_purged_log_catches_up_from_a_snapshot() {
    let chat = FakeChat::new();
    let often = MetaOptions { snapshot_every: Some(4), keep_snapshots: 1, ..opts() };
    let started = futures::future::join_all(MEMBERS.map(|id| node_with(chat.clone(), id, often.clone()))).await;
    let nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();
    let leader = agreed_leader(&nodes).await;
    let behind = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    nodes[&leader].propose::<Counter>(op(0, 1)).await.unwrap().unwrap();
    eventually("all applied the first commit", || nodes.values().all(|n| value(n) == 1)).await;

    chat.isolate(behind, true);
    let applied_then = nodes[&behind].applied_position();
    for i in 1..20 {
        nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    eventually("the leader's log starts past what the cut-off follower has", || {
        nodes[&leader].base_position() > applied_then
    })
    .await;

    chat.isolate(behind, false);
    eventually("the follower caught up", || value(&nodes[&behind]) == 20).await;
    assert!(nodes[&behind].base_position() > applied_then, "from a snapshot, not entry by entry");
}

/// Position of the snapshot `node` holds now (0: none).
fn snapshot_at(node: &MetaStore) -> Position {
    node.shared.current.lock().unwrap().as_ref().map_or(0, |c| c.at())
}

#[tokio::test]
async fn a_follower_behind_by_less_than_the_kept_log_catches_up_from_it() {
    let chat = FakeChat::new();
    let kept = MetaOptions { snapshot_every: Some(4), keep_log: Some(1000), ..opts() };
    let started = futures::future::join_all(MEMBERS.map(|id| node_with(chat.clone(), id, kept.clone()))).await;
    let nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();
    let leader = agreed_leader(&nodes).await;
    let behind = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    nodes[&leader].propose::<Counter>(op(0, 1)).await.unwrap().unwrap();
    eventually("all applied the first commit", || nodes.values().all(|n| value(n) == 1)).await;

    chat.isolate(behind, true);
    let applied_then = nodes[&behind].applied_position();
    for i in 1..20 {
        nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    eventually("the leader snapshots past the cut-off follower", || snapshot_at(&nodes[&leader]) > applied_then).await;
    assert!(nodes[&leader].base_position() <= applied_then, "the leader keeps the log the follower lacks");

    chat.isolate(behind, false);
    eventually("the follower caught up", || value(&nodes[&behind]) == 20).await;
    assert!(nodes[&behind].base_position() <= applied_then, "from the log, not a snapshot");
}

/// A node whose log starts behind its snapshot (the log a cluster keeps)
/// boots from the snapshot and replays only what follows it.
#[tokio::test]
async fn a_node_restarts_from_a_base_behind_its_snapshot() {
    let chat = FakeChat::new();
    let kept = MetaOptions { snapshot_every: Some(4), keep_log: Some(3), ..opts() };
    let stores: HashMap<u64, Stores> = MEMBERS.into_iter().map(|id| (id, Stores::new())).collect();
    let started =
        futures::future::join_all(MEMBERS.map(|id| node_on(chat.clone(), id, kept.clone(), &stores[&id]))).await;
    let mut nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();
    let leader = agreed_leader(&nodes).await;
    for i in 0..20 {
        nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    let follower = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    eventually("the follower's log starts behind its snapshot", || {
        let n = &nodes[&follower];
        value(n) == 20 && n.base_position() > 0 && n.base_position() < snapshot_at(n)
    })
    .await;
    let (base, snapshot) = (nodes[&follower].base_position(), snapshot_at(&nodes[&follower]));

    nodes.remove(&follower).unwrap().close().await.unwrap();
    let again = node_on(chat.clone(), follower, kept.clone(), &stores[&follower]).await;
    assert_eq!((again.base_position(), snapshot_at(&again)), (base, snapshot));
    assert!(value(&again) <= 20, "from the snapshot on, nothing applied twice");
    eventually("the restarted node replays what follows its snapshot", || value(&again) == 20).await;
    nodes.insert(follower, again);
    nodes[&leader].propose::<Counter>(op(99, 1)).await.unwrap().unwrap();
    eventually("the restarted node follows on", || value(&nodes[&follower]) == 21).await;
}

/// gRPC between nodes 1 and 2 (or all, with `chat_only` empty), the chat
/// to the rest: each node serves both, its peers split by transport.
#[cfg(not(madsim))]
mod transports {
    use super::*;
    use openraft_grpc::{GrpcNetwork, Identity, Mixed};
    use std::collections::HashSet;

    async fn node_on(
        chat: FakeChat,
        id: u64,
        addrs: &HashMap<u64, String>,
        listener: Option<tokio::net::TcpListener>,
        opts: MetaOptions,
    ) -> Arc<MetaStore> {
        let via_chat: HashSet<u64> = match addrs.contains_key(&id) {
            true => MEMBERS.into_iter().filter(|m| *m != id && !addrs.contains_key(m)).collect(),
            false => MEMBERS.into_iter().filter(|m| *m != id).collect(),
        };
        let grpc_peers: HashMap<u64, String> =
            addrs.iter().filter(|(&m, _)| m != id && !via_chat.contains(&m)).map(|(&m, a)| (m, a.clone())).collect();
        let chat_peers: HashMap<u64, Arc<dyn Mailbox>> = via_chat.iter().map(|&m| (m, chat.mailbox(id, m))).collect();
        let identity = Identity { me: id, cluster: 7, key: [3; 32] };
        let chat_net = ChatNetwork::<TypeConfig>::new(id, settings(), chat.mailbox(id, id), chat_peers);
        let grpc_net = GrpcNetwork::<TypeConfig>::new(identity.clone(), grpc_peers);
        let serving = chat_net.clone();
        let serve: Serve = Box::new(move |raft| {
            Box::pin(async move {
                let raft = Arc::new(raft);
                if let Some(listener) = listener {
                    tokio::spawn(openraft_grpc::serve::<TypeConfig>(listener, identity, raft.clone()));
                }
                serving.serve(raft).await;
            })
        });
        let network = Mixed { a: grpc_net, b: chat_net, via_b: via_chat };
        let cluster = Cluster { node: id, members: MEMBERS.into(), network, serve: Some(serve) };
        let log = Arc::new(crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
        let snapshots = Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
        MetaStore::open_in(log, snapshots, counter(), opts, cluster).await.unwrap()
    }

    /// The three nodes, those in `grpc` with a gRPC listener each.
    async fn three_on(chat: &FakeChat, grpc: &[u64], opts: MetaOptions) -> HashMap<u64, Arc<MetaStore>> {
        let mut listeners = HashMap::new();
        let mut addrs = HashMap::new();
        for &id in grpc {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.insert(id, l.local_addr().unwrap().to_string());
            listeners.insert(id, l);
        }
        let started = futures::future::join_all(
            MEMBERS.map(|id| node_on(chat.clone(), id, &addrs, listeners.remove(&id), opts.clone())),
        )
        .await;
        MEMBERS.into_iter().zip(started).collect()
    }

    #[tokio::test]
    async fn three_nodes_over_grpc_elect_apply_and_survive_a_killed_leader() {
        let chat = FakeChat::new();
        let mut nodes = three_on(&chat, &MEMBERS, opts()).await;
        let leader = agreed_leader(&nodes).await;
        nodes[&leader].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
        let follower = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
        let err = nodes[&follower].propose::<Counter>(op(2, 1)).await.unwrap_err();
        assert!(err.downcast_ref::<NotLeader>().is_some(), "{err:#}");
        eventually("every node applied the commit", || nodes.values().all(|n| value(n) == 1)).await;
        assert_eq!(chat.posts_by(leader), 0, "nothing went through the chat");

        drop(nodes.remove(&leader));
        let new = agreed_leader(&nodes).await;
        nodes[&new].propose::<Counter>(op(3, 1)).await.unwrap().unwrap();
        eventually("both survivors applied both commits", || nodes.values().all(|n| value(n) == 2)).await;
    }

    #[tokio::test]
    async fn two_nodes_on_grpc_and_one_on_the_chat_make_one_cluster() {
        let chat = FakeChat::new();
        let often = MetaOptions { snapshot_every: Some(4), keep_snapshots: 1, ..opts() };
        let nodes = three_on(&chat, &[1, 2], often).await;
        let leader = agreed_leader(&nodes).await;
        nodes[&leader].propose::<Counter>(op(0, 1)).await.unwrap().unwrap();
        eventually("all applied the first commit, the chat node too", || nodes.values().all(|n| value(n) == 1)).await;

        // The chat node cut off: the gRPC pair goes on; healed, it catches up
        // from a snapshot over the chat.
        chat.isolate(3, true);
        let rest: HashMap<_, _> = nodes.iter().filter(|(&id, _)| id != 3).map(|(&id, n)| (id, n.clone())).collect();
        let leader = agreed_leader(&rest).await;
        let applied_then = nodes[&3].applied_position();
        for i in 1..20 {
            nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
        }
        eventually("the leader's log starts past the chat node", || nodes[&leader].base_position() > applied_then)
            .await;
        chat.isolate(3, false);
        eventually("the chat node caught up", || value(&nodes[&3]) == 20).await;
        let other = if leader == 1 { 2 } else { 1 };
        assert!(chat.posts_by(other) + chat.posts_by(leader) > 0, "the pair posts for the chat node");
    }
}

/// Nodes 1 and 2 on a direct network (gRPC in memory), 3 on the chat: what
/// the in-browser demo runs, and the simulation can.
mod direct {
    use super::*;
    use openraft_grpc::{Direct, Mixed};
    use std::collections::HashSet;

    const PAIR: [u64; 2] = [1, 2];

    async fn node_on(direct: Direct<TypeConfig>, chat: FakeChat, id: u64, opts: MetaOptions) -> Arc<MetaStore> {
        let via_chat: HashSet<u64> = match PAIR.contains(&id) {
            true => MEMBERS.into_iter().filter(|m| !PAIR.contains(m)).collect(),
            false => MEMBERS.into_iter().filter(|m| *m != id).collect(),
        };
        let chat_peers: HashMap<u64, Arc<dyn Mailbox>> = via_chat.iter().map(|&m| (m, chat.mailbox(id, m))).collect();
        let chat_net = ChatNetwork::<TypeConfig>::new(id, settings(), chat.mailbox(id, id), chat_peers);
        let serving = chat_net.clone();
        let hub = direct.clone();
        let serve: Serve = Box::new(move |raft| {
            Box::pin(async move {
                let raft = Arc::new(raft);
                hub.serve(id, raft.clone());
                serving.serve(raft).await;
            })
        });
        let network = Mixed { a: direct.network(id), b: chat_net, via_b: via_chat };
        let cluster = Cluster { node: id, members: MEMBERS.into(), network, serve: Some(serve) };
        let log = Arc::new(crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
        let snapshots = Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
        MetaStore::open_in(log, snapshots, counter(), opts, cluster).await.unwrap()
    }

    #[tokio::test]
    async fn a_direct_pair_and_a_chat_node_make_one_cluster() {
        let (direct, chat) = (Direct::<TypeConfig>::new(), FakeChat::new());
        direct.set_delay(Some((Duration::from_millis(5), Duration::from_millis(5))));
        let often = MetaOptions { snapshot_every: Some(4), keep_snapshots: 1, ..opts() };
        let started =
            futures::future::join_all(MEMBERS.map(|id| node_on(direct.clone(), chat.clone(), id, often.clone()))).await;
        let mut nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();
        let leader = agreed_leader(&nodes).await;
        nodes[&leader].propose::<Counter>(op(0, 1)).await.unwrap().unwrap();
        eventually("all applied, the chat node too", || nodes.values().all(|n| value(n) == 1)).await;

        // The chat node cut off and caught up from a snapshot over the chat.
        chat.isolate(3, true);
        let rest: HashMap<_, _> = nodes.iter().filter(|(&id, _)| id != 3).map(|(&id, n)| (id, n.clone())).collect();
        let leader = agreed_leader(&rest).await;
        let behind = nodes[&3].applied_position();
        for i in 1..12 {
            nodes[&leader].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
        }
        eventually("the log moved past the chat node", || nodes[&leader].base_position() > behind).await;
        chat.isolate(3, false);
        eventually("the chat node caught up", || value(&nodes[&3]) == 12).await;

        // The direct pair split: the leader cut off, the other two go on.
        direct.isolate(leader, true);
        chat.isolate(leader, true);
        drop(nodes.remove(&leader));
        let new = agreed_leader(&nodes).await;
        nodes[&new].propose::<Counter>(op(99, 1)).await.unwrap().unwrap();
        eventually("both survivors applied it", || nodes.values().all(|n| value(n) == 13)).await;
    }
}

#[tokio::test]
async fn each_node_sees_the_cluster_and_the_leader_sees_lags() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    nodes[&leader].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    eventually("every node applied the commit", || nodes.values().all(|n| value(n) == 1)).await;
    eventually("the leader knows everyone caught up", || {
        nodes[&leader].cluster_view().nodes.iter().all(|n| n.lag == Some(0))
    })
    .await;

    let view = nodes[&leader].cluster_view();
    assert_eq!((view.me, view.leader), (leader, Some(leader)));
    let roles: Vec<_> = view.nodes.iter().map(|n| (n.id, n.role)).collect();
    let want: Vec<_> = MEMBERS.iter().map(|&id| (id, if id == leader { "leader" } else { "follower" })).collect();
    assert_eq!(roles, want);

    let follower = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    let theirs = nodes[&follower].cluster_view();
    assert_eq!((theirs.me, theirs.leader), (follower, Some(leader)));
    for n in &theirs.nodes {
        assert_eq!(n.applied.is_some(), n.id == follower, "a follower knows only its own applied");
        assert_eq!(n.lag, None, "and no lags");
    }

    chat.isolate(follower, true);
    nodes[&leader].propose::<Counter>(op(2, 1)).await.unwrap().unwrap();
    eventually("the leader sees the cut-off node behind", || {
        nodes[&leader].cluster_view().nodes.iter().any(|n| n.id == follower && n.lag.unwrap_or(0) >= 1)
    })
    .await;
}

/// The followers `leader` has, cut off from everyone.
fn cut_off_followers(chat: &FakeChat, leader: u64) -> Vec<u64> {
    let followers: Vec<u64> = MEMBERS.into_iter().filter(|&m| m != leader).collect();
    for &f in &followers {
        chat.isolate(f, true);
    }
    followers
}

#[tokio::test]
async fn a_leader_left_alone_is_read_only_until_a_quorum_is_back() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    nodes[&leader].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    assert!(nodes[&leader].has_quorum() && !nodes[&leader].is_read_only());

    let followers = cut_off_followers(&chat, leader);
    let l = &nodes[&leader];
    eventually("the leader alone turns read-only", || l.is_read_only()).await;
    assert!(l.is_leader() && !l.has_quorum(), "it still leads, without a quorum");
    let t = std::time::Instant::now();
    let err = l.propose::<Counter>(op(2, 1)).await.unwrap_err();
    assert!(err.downcast_ref::<NoQuorum>().is_some(), "{err:#}");
    assert!(t.elapsed() < Duration::from_millis(100), "refused at once, not after a wait");
    assert_eq!(value(l), 1, "reads go on");
    assert!(followers.iter().all(|f| !nodes[f].is_read_only()), "only a leader is ever read-only");

    chat.isolate(followers[0], false);
    eventually("a quorum again: writable", || nodes.values().any(|n| n.has_quorum())).await;
    let writer = nodes.values().find(|n| n.has_quorum()).unwrap();
    writer.propose::<Counter>(op(3, 1)).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_write_in_flight_when_the_quorum_goes_has_an_unknown_outcome() {
    let chat = FakeChat::new();
    let nodes = three(&chat).await;
    let leader = agreed_leader(&nodes).await;
    nodes[&leader].propose::<Counter>(op(1, 1)).await.unwrap().unwrap();

    cut_off_followers(&chat, leader);
    let t = std::time::Instant::now();
    let err = nodes[&leader].propose::<Counter>(op(2, 1)).await.unwrap_err();
    assert!(err.downcast_ref::<CommitUnknown>().is_some(), "{err:#}");
    let lease = opts().election_timeout.1;
    assert!(t.elapsed() < lease * 2, "answered once read-only, in {:?}", t.elapsed());
}

#[tokio::test]
async fn a_cluster_of_one_is_never_read_only() {
    let log = Arc::new(crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let snapshots = Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    let solo = MetaStore::open(log, snapshots, counter(), opts()).await.unwrap();
    tokio::time::sleep(opts().election_timeout.1 * 3).await;
    assert!(solo.has_quorum() && !solo.is_read_only());
    solo.propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_new_leader_serves_only_with_its_predecessors_commits_applied() {
    let chat = FakeChat::new();
    let mut nodes = three(&chat).await;
    let old = agreed_leader(&nodes).await;
    for i in 0..5 {
        nodes[&old].propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    chat.isolate(old, true);
    drop(nodes.remove(&old));
    // Watch the survivors: whenever one serves, it applied all its
    // predecessor committed; while it is elected but catching up, it gives
    // out no term to prepare writes in.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let new = loop {
        assert!(tokio::time::Instant::now() < deadline, "never a new leader");
        if let Some((&id, n)) = nodes.iter().find(|(_, n)| n.is_leader()) {
            assert_eq!(value(n), 5, "node {id} serves before applying its predecessor's commits");
            break id;
        }
        for n in nodes.values() {
            if let Some(term) = n.catching_up() {
                let refused = match n.serving_term() {
                    // Or it lost the lead since.
                    Err(e) => e.downcast_ref::<CatchingUp>().is_some_and(|c| c.term == term) || e.is::<NotLeader>(),
                    Ok(_) => n.is_leader(),
                };
                assert!(refused, "a leader catching up in term {term} handed its term out");
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    assert_eq!(nodes[&new].raft_state(), openraft::ServerState::Leader);
    let term = nodes[&new].serving_term().unwrap();
    let follower = nodes.keys().copied().find(|&id| id != new).unwrap();
    let err = nodes[&follower].serving_term().unwrap_err();
    assert!(err.downcast_ref::<NotLeader>().is_some(), "{err:#}");
    assert!(term > 1);
    nodes[&new].propose::<Counter>(op(5, 1)).await.unwrap().unwrap();
}
