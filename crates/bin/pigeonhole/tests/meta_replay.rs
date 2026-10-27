//! The command log is the source of truth: replaying it into an empty tablet
//! rebuilds the chunk tables and the s3 state exactly.

use bytes::Bytes;
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::{build_s3_gateway, BlobDb, ChunkStore, IngestOptions};
use pigeonhole_gateway_s3::S3State;
use pigeonhole_meta::{dump_tables, LocalLogStore, MetaOptions, MetaStore, Namespaces, StateMachine};
use s3s::dto::*;
use s3s::{S3Request, S3};
use std::path::Path;
use std::sync::Arc;

/// Written directly by the node, not through commands (until they move out
/// of the replicated file, see docs/roadmap.md).
const NODE_LOCAL: &[&str] = &[
    "chunk_put_watermarks",
    "chunk_repair_queue",
    "chunk_sweep_cursor",
];

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn tablet(dir: &Path, db: &str) -> Arc<MetaStore> {
    let url = format!("sqlite:{}", dir.join(db).display());
    let log = LocalLogStore::open(dir.join("log")).unwrap();
    MetaStore::open(
        &url,
        Arc::new(log),
        Index::register(Namespaces::new().with_sql(BlobDb::namespace())),
        MetaOptions::default(),
    )
    .await
    .unwrap()
}

async fn replicated_state(meta: &MetaStore) -> (Vec<(String, Vec<String>)>, Vec<u8>) {
    let mut dump = dump_tables(meta.pool()).await.unwrap();
    dump.retain(|(table, _)| !NODE_LOCAL.contains(&table.as_str()));
    (dump, meta.read::<S3State, _>(|s| s.snapshot()))
}

#[tokio::test]
async fn replay_rebuilds_chunk_and_s3_tables() {
    let dir = tempfile::tempdir().unwrap();
    let meta = tablet(dir.path(), "a.db").await;
    let cfg = Config::for_test(dir.path());
    let db = BlobDb::open(meta.clone()).await.unwrap();
    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;
    let store = Arc::new(ChunkStore::open(db, MemoryBlobStore::new(), opts).await.unwrap());
    let s3 = build_s3_gateway(cfg, Index::open(meta.clone()).await.unwrap(), store);

    s3.create_bucket(req(CreateBucketInput { bucket: "b".into(), ..Default::default() }))
        .await
        .unwrap();
    for i in 0..20u8 {
        s3.put_object(req(PutObjectInput {
            bucket: "b".into(),
            key: format!("k{}", i % 7),
            body: Some(StreamingBlob::from_bytes(Bytes::from(vec![i; 1000 + i as usize]))),
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    let copy = CopyObjectInput::builder()
        .bucket("b".into())
        .key("copy".into())
        .copy_source(CopySource::Bucket { bucket: "b".into(), key: "k1".into(), version_id: None })
        .build()
        .unwrap();
    s3.copy_object(req(copy)).await.unwrap();
    s3.delete_object(req(DeleteObjectInput { bucket: "b".into(), key: "k2".into(), ..Default::default() }))
        .await
        .unwrap();
    // Rejected command: deleting a missing key still takes a log position.
    let _ = s3
        .delete_object(req(DeleteObjectInput { bucket: "b".into(), key: "nope".into(), ..Default::default() }))
        .await;

    let before = replicated_state(&meta).await;
    let position = meta.applied_position().await.unwrap();
    assert!(position > 20, "every metadata change went through the log");
    drop(s3);

    let replayed = tablet(dir.path(), "b.db").await;
    assert_eq!(replayed.applied_position().await.unwrap(), position);
    assert_eq!(replicated_state(&replayed).await, before);
    let snap = &before.1;
    assert_eq!(&S3State::restore(snap).unwrap().snapshot(), snap, "restore inverts snapshot");
}
