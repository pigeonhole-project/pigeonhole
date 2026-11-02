//! A write whose request goes away (the client hangs up, a timeout) while
//! its entry is being written still finishes what follows the commit: the
//! chunks it replaced are released, and so are those of a refused write.

use bytes::Bytes;
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole_chunk_store::{default_layer_opts, BlobDb, ChunkStore};
use pigeonhole_gateway_s3::{build_s3_gateway, unique_chunk_ids, Index, S3Gateway, S3Options};
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_testkit::GatedLog;
use s3s::dto::*;
use s3s::{S3Request, S3};
use std::sync::Arc;
use std::time::Duration;

const BUCKET: &str = "b";

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn put(key: &str, body: &'static [u8], if_none_match: bool) -> S3Request<PutObjectInput> {
    req(PutObjectInput {
        bucket: BUCKET.into(),
        key: key.into(),
        body: Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(body)))),
        if_none_match: if_none_match.then_some(ETagCondition::Any),
        ..Default::default()
    })
}

struct Node {
    _dir: tempfile::TempDir,
    s3: Arc<S3Gateway>,
    store: Arc<ChunkStore>,
    gate: Arc<GatedLog>,
}

async fn open() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let (meta, gate) = pigeonhole_testkit::gated_tablet(dir.path(), |n| Index::register(BlobDb::register(n))).await;
    let mut opts = default_layer_opts();
    opts.chunk_size = 64 * 1024;
    let db = BlobDb::open(meta.clone()).await.unwrap();
    let store = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
    let s3_opts = S3Options { inline_max: None, ..S3Options::for_test() };
    let s3 = Arc::new(build_s3_gateway(s3_opts, Index::open(meta).await.unwrap(), store.clone()));
    s3.create_bucket(req(CreateBucketInput { bucket: BUCKET.into(), ..Default::default() })).await.unwrap();
    Node { _dir: dir, s3, store, gate }
}

async fn chunks_of(n: &Node, key: &str) -> Vec<i64> {
    let src = n.s3.index.get_copy_source(BUCKET, key).await.unwrap().expect("the object");
    unique_chunk_ids(src.body.extents())
}

/// Chunks nothing points at any more (refs 0), once there are some.
async fn released(n: &Node) -> Vec<i64> {
    for _ in 0..500 {
        let ids = n.store.db().list_zero_ref_chunks().await.unwrap();
        if !ids.is_empty() {
            return ids;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

#[tokio::test]
async fn an_overwrite_dropped_mid_commit_still_releases_what_it_replaced() {
    let n = open().await;
    n.s3.put_object(put("k", b"first", false)).await.unwrap();
    let old = chunks_of(&n, "k").await;
    assert!(!old.is_empty());

    n.gate.close();
    let mut overwrite = Box::pin(n.s3.put_object(put("k", b"second", false)));
    tokio::select! {
        _ = &mut overwrite => panic!("committed through a closed log"),
        () = n.gate.parked() => {}
    }
    drop(overwrite);
    n.gate.open();

    assert_eq!(released(&n).await, old, "the replaced chunks are released");
    assert_ne!(chunks_of(&n, "k").await, old, "the overwrite applied");
}

#[tokio::test]
async fn a_refused_write_dropped_mid_commit_still_releases_its_chunks() {
    let n = open().await;
    n.gate.close();
    // The unconditional write first, held at the log; the conditional one
    // passes the early check (nothing there yet) and is refused once it
    // applies after it.
    let first = tokio::spawn({
        let s3 = n.s3.clone();
        async move { s3.put_object(put("k", b"first", false)).await.map(|_| ()) }
    });
    n.gate.parked().await;
    let conditional = n.s3.put_object(put("k", b"second", true));
    // Its body ingested and its proposal queued behind the first, or not
    // yet: either way the request is gone before anything is applied.
    let _ = tokio::time::timeout(Duration::from_millis(100), conditional).await;
    n.gate.open();
    first.await.unwrap().unwrap();

    let kept = chunks_of(&n, "k").await;
    let refused = released(&n).await;
    assert!(!refused.is_empty(), "the refused write's chunks are released");
    assert!(refused.iter().all(|id| !kept.contains(id)), "{refused:?} vs {kept:?}");
}
