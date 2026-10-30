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
    Settings { cluster: 42, key, tick: Duration::from_millis(100), poll: Duration::from_millis(500) }
}

/// Grants every vote and accepts every append; counts the requests.
#[derive(Default)]
struct Yes {
    calls: AtomicU64,
}

#[async_trait]
impl Handler<Config> for Yes {
    async fn append_entries(&self, _: AppendEntriesRequest<Config>) -> Result<AppendEntriesResponse<u64>, RaftError<u64>> {
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

async fn vote(net: &mut ChatNetwork<Config>, target: u64, term: u64, ttl: Duration) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
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
