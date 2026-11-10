//! E2E (memory): start → PUT → restart (all metadata state is gone) → boot
//! replays the metadata logs → GET; and losing the log itself → restore from a
//! snapshot export → GET.

use bytes::Bytes;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::service::S3Options;
use pigeonhole_codec::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};
use pigeonhole_meta::{MetaOptions, MetaStore, Namespaces};
use pigeonhole_testkit::s3::{self as s3kit, collect_body, req, Node};
use s3s::dto::*;
use s3s::S3;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// A node over the tablet in `dir` and the store `mem`, both of which
/// outlive it.
async fn open_with_shared_mem(dir: &Path, mem: Arc<MemoryBlobStore>) -> pigeonhole::S3Gateway {
    let meta = pigeonhole_testkit::tablet(dir, s3kit::register).await;
    let ingest = s3kit::ingest(DEFAULT_CHUNK_SIZE, DEFAULT_BLOCK_SIZE);
    Node::open(meta, vec![s3kit::member(mem)], ingest, S3Options::for_test()).await.s3
}

#[tokio::test]
async fn memory_put_restart_replay_get() {
    let dir = tempfile::tempdir().unwrap();

    // The backend survives the restart; so do the metadata logs.
    let mem = Arc::new(MemoryBlobStore::new());

    let s3 = open_with_shared_mem(dir.path(), mem.clone()).await;

    s3.create_bucket(req(CreateBucketInput { bucket: "demo".into(), ..Default::default() })).await.unwrap();

    let payload = Bytes::from_static(b"stage-g-restore-payload");
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "obj.bin".into(),
        body: Some(StreamingBlob::from_bytes(payload.clone())),
        ..Default::default()
    }))
    .await
    .unwrap();

    drop(s3);

    let s32 = open_with_shared_mem(dir.path(), mem).await;
    let got = s3s2_get(&s32).await;
    assert_eq!(got, payload.as_ref());
}

async fn s3s2_get(s3: &pigeonhole::S3Gateway) -> Vec<u8> {
    let got = s3
        .get_object(req(GetObjectInput { bucket: "demo".into(), key: "obj.bin".into(), ..Default::default() }))
        .await
        .unwrap()
        .output;
    collect_body(got.body).await
}

/// One tablet for both namespaces, as the binary runs it, with snapshots in
/// another directory, taken after every batch (and when idle).
async fn open_snapshotting(dir: &Path, mem: Arc<MemoryBlobStore>) -> (pigeonhole::S3Gateway, Arc<MetaStore>) {
    let (log, snapshots) = pigeonhole_testkit::disk_meta(dir);
    let opts = MetaOptions {
        snapshot_every: Some(1),
        snapshot_interval: Some(std::time::Duration::from_millis(10)),
        snapshots_outlive_log: true,
        ..MetaOptions::default()
    };
    let meta = MetaStore::open(log, snapshots, s3kit::register(Namespaces::new()), opts).await.unwrap();
    let ingest = s3kit::ingest(DEFAULT_CHUNK_SIZE, DEFAULT_BLOCK_SIZE);
    let node = Node::open(meta.clone(), vec![s3kit::member(mem)], ingest, S3Options::for_test()).await;
    (node.s3, meta)
}

#[tokio::test]
async fn lost_log_restores_from_stored_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryBlobStore::new());
    let (s3, meta) = open_snapshotting(dir.path(), mem.clone()).await;
    s3.create_bucket(req(CreateBucketInput { bucket: "demo".into(), ..Default::default() })).await.unwrap();
    let payload = Bytes::from_static(b"exported-payload");
    s3.put_object(req(PutObjectInput {
        bucket: "demo".into(),
        key: "obj.bin".into(),
        body: Some(StreamingBlob::from_bytes(payload.clone())),
        ..Default::default()
    }))
    .await
    .unwrap();
    // Snapshots are stored in the background; wait for the one covering the PUT.
    pigeonhole_testkit::eventually("a snapshot covering the PUT", Duration::from_secs(10), || {
        meta.base_position() == meta.applied_position()
    })
    .await;
    drop((s3, meta));

    // The disk with the log is gone; the snapshot directory survived.
    std::fs::remove_dir_all(dir.path().join("log")).unwrap();
    let (log, snapshots) = pigeonhole_testkit::disk_meta(dir.path());
    pigeonhole_meta::restore_lossy(log.as_ref(), snapshots.as_ref()).await.unwrap();
    drop((log, snapshots));

    let (s3, _meta) = open_snapshotting(dir.path(), mem).await;
    assert_eq!(s3s2_get(&s3).await, payload.as_ref());
}
