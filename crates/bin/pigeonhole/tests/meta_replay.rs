//! The command log is the source of truth: replaying it into an empty tablet
//! rebuilds the chunk and s3 state exactly.

use bytes::Bytes;
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::{build_s3_gateway, BlobDb, ChunkStore, IngestOptions};
use pigeonhole_gateway_s3::S3State;
use pigeonhole_chunk_store::ChunkState;
use pigeonhole_meta::{
    decode_state, encode_state, MetaOptions, MetaStore, Namespaces,
};
use s3s::dto::*;
use s3s::{S3Request, S3};
use std::path::Path;
use std::sync::Arc;

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

async fn tablet(dir: &Path, snapshot_every: u64) -> Arc<MetaStore> {
    let (log, snapshots) = pigeonhole_testkit::disk_meta(dir);
    MetaStore::open(
        log,
        snapshots,
        Index::register(BlobDb::register(Namespaces::new())),
        MetaOptions { snapshot_every, ..MetaOptions::default() },
    )
    .await
    .unwrap()
}

/// Logical snapshots of both namespaces: equal bytes = equal state.
fn replicated_state(meta: &MetaStore) -> (Vec<u8>, Vec<u8>) {
    (meta.read::<ChunkState, _>(encode_state), meta.read::<S3State, _>(encode_state))
}

#[tokio::test]
async fn replay_rebuilds_chunk_and_s3_state() {
    rebuild(0).await;
}

#[tokio::test]
async fn snapshot_and_tail_rebuild_chunk_and_s3_state() {
    rebuild(7).await;
}

async fn rebuild(snapshot_every: u64) {
    let dir = tempfile::tempdir().unwrap();
    let meta = tablet(dir.path(), snapshot_every).await;
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

    let before = replicated_state(&meta);
    let position = meta.applied_position();
    assert!(position > 20, "every metadata change went through the log");
    // Snapshots are stored in the background.
    while snapshot_every > 0 && meta.base_position() == 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    drop(s3);

    let replayed = tablet(dir.path(), snapshot_every).await;
    let snapshot = pigeonhole_testkit::disk_meta(dir.path()).0.discover().await.unwrap().unwrap().base;
    assert_eq!(snapshot.is_some(), snapshot_every > 0);
    assert_eq!(replayed.applied_position(), position);
    assert_eq!(replicated_state(&replayed), before);
    let (chunk, s3) = &before;
    assert_eq!(&encode_state(&decode_state::<ChunkState>(chunk).unwrap()), chunk, "decode inverts encode");
    assert_eq!(&encode_state(&decode_state::<S3State>(s3).unwrap()), s3, "decode inverts encode");
}
