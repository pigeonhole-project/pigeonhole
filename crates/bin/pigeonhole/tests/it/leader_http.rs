//! The S3 port's leader gate and health checks on a cluster of three over a
//! fake chat: a leader left alone turns read-only, serves reads, and turns
//! writes away before reading their bodies (docs/cluster.md § Losing the
//! quorum).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use openraft_chat::{ChatNetwork, FakeChat, Mailbox, Settings};
use pigeonhole::leader_http;
use pigeonhole_meta::raft::TypeConfig;
use pigeonhole_meta::{
    BlobSnapshotStore, ChainLogStore, Cluster, MetaOptions, MetaStore, Namespaces, Never, Outcome, StateMachine,
};
use pigeonhole_storage_memory::MemoryBlobStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

const MEMBERS: [u64; 3] = [1, 2, 3];

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

async fn node(chat: FakeChat, id: u64) -> Arc<MetaStore> {
    let peers: HashMap<u64, Arc<dyn Mailbox>> =
        MEMBERS.iter().filter(|&&m| m != id).map(|&m| (m, chat.mailbox(id, m))).collect();
    let settings = Settings {
        cluster: 7,
        key: [3; 32],
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
        heartbeat: Duration::from_millis(100),
        election_timeout: (Duration::from_millis(800), Duration::from_millis(1600)),
        ..MetaOptions::default()
    };
    let log = Arc::new(ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let snapshots = Arc::new(BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    MetaStore::open_in(log, snapshots, Namespaces::new().with::<Sum>(), opts, cluster).await.unwrap()
}

/// The cluster's deadline for anything to settle.
const SETTLE: Duration = Duration::from_secs(20);

/// The gate and the health routes, as `serve` layers them, in front of a
/// stand-in for S3 that counts the requests it got and reads their bodies.
fn app(meta: Arc<MetaStore>, served: Arc<AtomicUsize>) -> Router {
    let s3 = tower::service_fn(move |req: Request<Body>| {
        let served = served.clone();
        async move {
            served.fetch_add(1, Ordering::SeqCst);
            let _ = axum::body::to_bytes(req.into_body(), usize::MAX).await;
            Ok::<_, std::convert::Infallible>(axum::response::Response::new(Body::empty()))
        }
    });
    leader_http::routes(meta.clone())
        .fallback_service(s3)
        .layer(axum::middleware::from_fn_with_state(meta, leader_http::only_on_the_leader))
}

async fn call(app: &Router, method: &str, path: &str) -> (StatusCode, String) {
    let req = Request::builder().method(method).uri(path).body(Body::from("payload")).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn a_leader_without_its_quorum_serves_reads_and_turns_writes_away() {
    let chat = FakeChat::new();
    let started = futures::future::join_all(MEMBERS.map(|id| node(chat.clone(), id))).await;
    let nodes: HashMap<u64, Arc<MetaStore>> = MEMBERS.into_iter().zip(started).collect();
    pigeonhole_testkit::eventually("a leader", SETTLE, || nodes.values().filter(|n| n.is_leader()).count() == 1).await;
    let leader = *nodes.iter().find(|(_, n)| n.is_leader()).unwrap().0;
    let follower = *MEMBERS.iter().find(|&&m| m != leader).unwrap();
    let served = Arc::new(AtomicUsize::new(0));
    let (l, f) = (app(nodes[&leader].clone(), served.clone()), app(nodes[&follower].clone(), served.clone()));

    assert_eq!(call(&l, "PUT", "/b/k").await.0, StatusCode::OK);
    assert_eq!(call(&l, "GET", "/_pigeonhole/leader").await.0, StatusCode::OK);
    assert_eq!(call(&f, "GET", "/b/k").await.0, StatusCode::SERVICE_UNAVAILABLE, "a follower serves nothing");
    assert_eq!(call(&f, "GET", "/_pigeonhole/readable").await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(served.load(Ordering::SeqCst), 1);

    for m in MEMBERS.into_iter().filter(|&m| m != leader) {
        chat.isolate(m, true);
    }
    pigeonhole_testkit::eventually("the leader alone turns read-only", SETTLE, || nodes[&leader].is_read_only()).await;

    for method in ["PUT", "POST", "DELETE"] {
        let (status, body) = call(&l, method, "/b/k").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{method}");
        assert!(body.contains("<Code>ServiceUnavailable</Code>") && body.contains("read-only"), "{body}");
    }
    assert_eq!(served.load(Ordering::SeqCst), 1, "no write reached S3, no body was read");
    assert_eq!(call(&l, "GET", "/b/k").await.0, StatusCode::OK);
    assert_eq!(call(&l, "HEAD", "/b/k").await.0, StatusCode::OK);
    assert_eq!(served.load(Ordering::SeqCst), 3, "reads did");

    let (status, body) = call(&l, "GET", "/_pigeonhole/leader").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "writes don't work here: {body}");
    let health: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!((health["leading"].as_bool(), health["read_only"].as_bool()), (Some(true), Some(true)));
    assert_eq!(call(&l, "GET", "/_pigeonhole/readable").await.0, StatusCode::OK, "reads do");
}
