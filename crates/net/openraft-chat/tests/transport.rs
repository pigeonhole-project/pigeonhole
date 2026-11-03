//! The transport's own mechanics over [`FakeChat`], with a stand-in for the
//! node's `Raft`: delivery, coalescing, timeouts, partitions, duplicates,
//! foreign keys. Raft itself over this transport is tested where there are
//! real nodes (pigeonhole-meta).

use async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{BasicNode, Vote};
use openraft_chat::frame::{open, seal, Body, Envelope, Frame, FRAME_VERSION};
use openraft_chat::{ChatNetwork, FakeChat, Handler, Mailbox, Settings};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

openraft::declare_raft_types!(
    pub Config: D = String, R = String,
);

const KEY: [u8; 32] = [7; 32];

fn settings(key: [u8; 32]) -> Settings {
    Settings { cluster: 42, key, tick: Duration::from_millis(100), poll: Duration::from_millis(500), keep: None }
}

/// Grants every vote and accepts every append; counts the requests.
#[derive(Default)]
struct Yes {
    calls: AtomicU64,
}

#[async_trait]
impl Handler<Config> for Yes {
    async fn append_entries(
        &self,
        _: AppendEntriesRequest<Config>,
    ) -> Result<AppendEntriesResponse<u64>, RaftError<u64>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(AppendEntriesResponse::Success)
    }
    async fn vote(&self, req: VoteRequest<u64>) -> Result<VoteResponse<u64>, RaftError<u64>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(VoteResponse::new(req.vote, None, true))
    }
    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<Config>,
    ) -> Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(InstallSnapshotResponse { vote: req.vote })
    }
}

/// Nodes 1 and 2 on `chat`, node 2 answering with `handler`.
async fn pair(chat: &FakeChat, key2: [u8; 32], handler: Arc<Yes>) -> (ChatNetwork<Config>, ChatNetwork<Config>) {
    let net = |me: u64, other: u64, key| {
        let peers: HashMap<u64, Arc<dyn Mailbox>> = [(other, chat.mailbox(me, other))].into();
        ChatNetwork::<Config>::new(me, settings(key), chat.mailbox(me, me), peers)
    };
    let (one, two) = (net(1, 2, KEY), net(2, 1, key2));
    two.serve(handler).await;
    one.serve(Arc::new(Yes::default())).await;
    (one, two)
}

#[allow(clippy::result_large_err)] // openraft's RPCError, as its API returns it
async fn vote(
    net: &mut ChatNetwork<Config>,
    target: u64,
    term: u64,
    ttl: Duration,
) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
    let mut client = net.new_client(target, &BasicNode::default()).await;
    client.vote(VoteRequest::new(Vote::new(term, 1), None), RPCOption::new(ttl)).await
}

#[test]
fn a_frame_opens_only_with_its_key_and_intact() {
    let frame = Frame::<Config> {
        version: FRAME_VERSION,
        cluster: 42,
        from: 1,
        seq: 9,
        envelopes: vec![Envelope { to: 2, rpc: 3, body: Body::Vote(VoteRequest::new(Vote::new(5, 1), None)) }],
    };
    let sealed = seal(&frame, &KEY);
    let back = open::<Config>(&sealed, &KEY).unwrap();
    assert_eq!((back.from, back.seq, back.envelopes.len(), back.envelopes[0].rpc), (1, 9, 1, 3));
    assert!(open::<Config>(&sealed, &[8; 32]).is_err(), "another key");
    let mut bad = sealed.to_vec();
    let mid = bad.len() / 2;
    bad[mid] ^= 1;
    assert!(open::<Config>(&bad, &KEY).is_err(), "a flipped bit");
}

#[tokio::test(start_paused = true)]
async fn a_call_gets_its_answer() {
    let chat = FakeChat::new();
    let handler = Arc::new(Yes::default());
    let (mut one, _two) = pair(&chat, KEY, handler.clone()).await;
    let resp = vote(&mut one, 2, 7, Duration::from_secs(10)).await.unwrap();
    assert!(resp.vote_granted);
    assert_eq!(resp.vote, Vote::new(7, 1));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn concurrent_calls_share_posts() {
    let chat = FakeChat::new();
    let (one, _two) = pair(&chat, KEY, Arc::new(Yes::default())).await;
    let calls: Vec<_> = (0..20u64)
        .map(|i| {
            let mut net = one.clone();
            tokio::spawn(async move { vote(&mut net, 2, i + 1, Duration::from_secs(10)).await })
        })
        .collect();
    for c in calls {
        assert!(c.await.unwrap().unwrap().vote_granted);
    }
    // 20 requests out, 20 responses back, a few frames each way.
    assert!(chat.posts_by(1) <= 3, "node 1 posted {} frames", chat.posts_by(1));
    assert!(chat.posts_by(2) <= 3, "node 2 posted {} frames", chat.posts_by(2));
}

#[tokio::test(start_paused = true)]
async fn a_cut_off_peer_times_out_and_answers_once_healed() {
    let chat = FakeChat::new();
    let (mut one, _two) = pair(&chat, KEY, Arc::new(Yes::default())).await;
    chat.isolate(2, true);
    let err = vote(&mut one, 2, 1, Duration::from_secs(3)).await.unwrap_err();
    assert!(matches!(err, RPCError::Timeout(_)), "{err:?}");
    chat.isolate(2, false);
    assert!(vote(&mut one, 2, 2, Duration::from_secs(10)).await.unwrap().vote_granted);
}

#[tokio::test(start_paused = true)]
async fn a_frame_posted_twice_is_handled_once() {
    let chat = FakeChat::new();
    chat.set_duplication(Some(1));
    let handler = Arc::new(Yes::default());
    let (mut one, _two) = pair(&chat, KEY, handler.clone()).await;
    for term in 1..=5 {
        assert!(vote(&mut one, 2, term, Duration::from_secs(10)).await.unwrap().vote_granted);
    }
    assert_eq!(handler.calls.load(Ordering::SeqCst), 5);
}

#[tokio::test(start_paused = true)]
async fn lost_frames_are_timeouts_and_later_calls_go_through() {
    let chat = FakeChat::new();
    let (mut one, _two) = pair(&chat, KEY, Arc::new(Yes::default())).await;
    chat.set_loss(Some(3));
    let mut ok = 0;
    for term in 1..=10 {
        if vote(&mut one, 2, term, Duration::from_secs(2)).await.is_ok() {
            ok += 1;
        }
    }
    assert!(ok > 0 && ok < 10, "{ok} of 10 answered with every third post lost");
}

#[tokio::test(start_paused = true)]
async fn a_node_with_another_key_is_not_heard() {
    let chat = FakeChat::new();
    let handler = Arc::new(Yes::default());
    let (mut one, _two) = pair(&chat, [9; 32], handler.clone()).await;
    let err = vote(&mut one, 2, 1, Duration::from_secs(3)).await.unwrap_err();
    assert!(matches!(err, RPCError::Timeout(_)), "{err:?}");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn calls_frames_and_posts_are_counted() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // A current-thread runtime: every task records where the guard is set.
    let _guard = metrics::set_default_local_recorder(&recorder);
    let chat = FakeChat::new();
    chat.set_duplication(Some(1));
    let (mut one, _two) = pair(&chat, KEY, Arc::new(Yes::default())).await;
    for term in 1..=3 {
        vote(&mut one, 2, term, Duration::from_secs(10)).await.unwrap();
    }
    chat.isolate(2, true);
    vote(&mut one, 2, 4, Duration::from_secs(3)).await.unwrap_err();

    let snapshot = snapshotter.snapshot().into_vec();
    // The value of `name` whose labels include all of `labels`.
    let find = |name: &str, labels: &[(&str, &str)]| {
        snapshot
            .iter()
            .find(|(k, ..)| {
                k.key().name() == name
                    && labels.iter().all(|(l, v)| k.key().labels().any(|x| x.key() == *l && x.value() == *v))
            })
            .map_or_else(|| panic!("{name} {labels:?}"), |(.., v)| v)
    };
    let calls =
        |outcome| match find("openraft_chat_call_seconds", &[("peer", "2"), ("rpc", "vote"), ("outcome", outcome)]) {
            DebugValue::Histogram(h) => h.len(),
            other => panic!("{other:?}"),
        };
    assert_eq!((calls("ok"), calls("timeout")), (3, 1));
    let frames =
        |peer, outcome| match find("openraft_chat_frames_received_total", &[("peer", peer), ("outcome", outcome)]) {
            DebugValue::Counter(c) => *c,
            other => panic!("{other:?}"),
        };
    assert_eq!(frames("1", "ok"), 3, "node 2 got the 3 requests once");
    assert_eq!(frames("1", "duplicate"), 3, "and each a second time");
    assert!(matches!(find("openraft_chat_posts_total", &[("outcome", "ok")]), DebugValue::Counter(n) if *n >= 6));
}

/// Grants votes; a vote in a term listed in `delays` it answers only after
/// that delay.
struct Slow {
    delays: HashMap<u64, Duration>,
}

#[async_trait]
impl Handler<Config> for Slow {
    async fn append_entries(
        &self,
        _: AppendEntriesRequest<Config>,
    ) -> Result<AppendEntriesResponse<u64>, RaftError<u64>> {
        Ok(AppendEntriesResponse::Success)
    }
    async fn vote(&self, req: VoteRequest<u64>) -> Result<VoteResponse<u64>, RaftError<u64>> {
        if let Some(d) = self.delays.get(&req.vote.leader_id().term) {
            tokio::time::sleep(*d).await;
        }
        Ok(VoteResponse::new(req.vote, None, true))
    }
    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<Config>,
    ) -> Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>> {
        Ok(InstallSnapshotResponse { vote: req.vote })
    }
}

#[tokio::test(start_paused = true)]
async fn a_response_from_another_peer_does_not_answer_a_call() {
    // Node 1 asks node 3, which never answers. Node 2 posts a well-sealed
    // response to the very rpc id node 1 used: node 1 must not take it.
    let chat = FakeChat::new();
    let peers: HashMap<u64, Arc<dyn Mailbox>> = [(2, chat.mailbox(1, 2)), (3, chat.mailbox(1, 3))].into();
    let mut one = ChatNetwork::<Config>::new(1, settings(KEY), chat.mailbox(1, 1), peers);
    one.serve(Arc::new(Yes::default())).await;
    let call = tokio::spawn(async move { vote(&mut one, 3, 7, Duration::from_secs(5)).await });

    // The request node 1 posted: its rpc id.
    let mut rpc = None;
    while rpc.is_none() {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let frames = chat.mailbox(9, 1).read_after(0, 10).await.unwrap();
        rpc = frames.first().map(|(_, b)| open::<Config>(b, &KEY).unwrap().envelopes[0].rpc);
    }
    let forged = Frame::<Config> {
        version: FRAME_VERSION,
        cluster: 42,
        from: 2,
        seq: 1,
        envelopes: vec![Envelope {
            to: 1,
            rpc: rpc.unwrap(),
            body: Body::Voted(Ok(VoteResponse::new(Vote::new(7, 1), None, true))),
        }],
    };
    chat.mailbox(2, 2).post(seal(&forged, &KEY)).await.unwrap();

    let err = call.await.unwrap().unwrap_err();
    assert!(matches!(err, RPCError::Timeout(_)), "node 2's response answered a call to node 3: {err:?}");
}

#[tokio::test(start_paused = true)]
async fn a_response_to_an_earlier_run_is_not_taken_for_this_ones() {
    // Node 1 asks node 2 for a vote in term 7, and restarts before node 2
    // answers; its new run asks in term 8. Node 2 answers the old request
    // first (at 2 s, the new one at 4 s): the new run must not take that
    // answer for its own.
    let chat = FakeChat::new();
    let two = ChatNetwork::<Config>::new(2, settings(KEY), chat.mailbox(2, 2), [(1, chat.mailbox(2, 1))].into());
    let delays = [(7, Duration::from_secs(2)), (8, Duration::from_secs(4))].into();
    two.serve(Arc::new(Slow { delays })).await;
    let node1 = || ChatNetwork::<Config>::new(1, settings(KEY), chat.mailbox(1, 1), [(2, chat.mailbox(1, 2))].into());

    let mut first = node1();
    first.serve(Arc::new(Yes::default())).await;
    let old = tokio::spawn(async move { vote(&mut first, 2, 7, Duration::from_secs(30)).await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    old.abort(); // the restart: the old run and its call are gone
    let _ = old.await;
    std::thread::sleep(Duration::from_millis(2)); // a run starts at a later instant

    let mut second = node1();
    second.serve(Arc::new(Yes::default())).await;
    let resp = vote(&mut second, 2, 8, Duration::from_secs(30)).await.unwrap();
    assert_eq!(resp.vote, Vote::new(8, 1), "the new run took the answer meant for the old one");
}

#[tokio::test(start_paused = true)]
async fn an_outbox_keeps_only_its_recent_frames() {
    // Frames live 5 s: over 30 s of calls the outboxes hold the last few
    // seconds' frames, and the calls go through as before.
    let chat = FakeChat::new();
    let net = |me: u64, other: u64| {
        let peers: HashMap<u64, Arc<dyn Mailbox>> = [(other, chat.mailbox(me, other))].into();
        let settings = Settings { keep: Some(Duration::from_secs(5)), ..settings(KEY) };
        ChatNetwork::<Config>::new(me, settings, chat.mailbox(me, me), peers)
    };
    let (mut one, two) = (net(1, 2), net(2, 1));
    two.serve(Arc::new(Yes::default())).await;
    one.serve(Arc::new(Yes::default())).await;
    for term in 1..=60 {
        assert!(vote(&mut one, 2, term, Duration::from_secs(10)).await.unwrap().vote_granted);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for node in [1, 2] {
        let (posted, kept) = (chat.posts_by(node), chat.kept(node));
        assert!(posted >= 60, "node {node} posted {posted}");
        assert!(kept <= 25, "node {node} keeps {kept} of {posted} frames");
    }
}

#[tokio::test(start_paused = true)]
async fn a_call_dropped_by_an_outer_timeout_stops_waiting_and_counts() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let chat = FakeChat::new();
    let (mut one, _two) = pair(&chat, KEY, Arc::new(Yes::default())).await;
    chat.isolate(2, true);
    // openraft's own timeout around the call, shorter than the call's ttl.
    for term in 1..=5 {
        let outer = tokio::time::timeout(Duration::from_secs(1), vote(&mut one, 2, term, Duration::from_secs(60)));
        assert!(outer.await.is_err(), "the outer timeout fired first");
    }
    assert_eq!(one.pending_calls(), 0, "no dropped call is left waiting");

    let timeouts = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(k, ..)| {
            k.key().name() == "openraft_chat_call_seconds" && k.key().labels().any(|l| l.value() == "timeout")
        })
        .map(|(.., v)| v);
    assert!(matches!(timeouts, Some(DebugValue::Histogram(ref h)) if h.len() == 5), "{timeouts:?}");
}
