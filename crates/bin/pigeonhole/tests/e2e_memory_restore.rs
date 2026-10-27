//! E2E (memory): start → PUT → lose the local SQLite files → boot replays the
//! metadata log → GET.

use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Method, Uri};
use http::Extensions;
use pigeonhole::config::Config;
use pigeonhole::index::Index;
use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::{build_s3_gateway, BlobDb, ChunkStore, IngestOptions};
use pigeonhole_blob::{
    erase_sweep, CheapestFirst, Replicated, SharedBackend,
};
use s3s::dto::*;
use s3s::{S3, S3Request};
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

async fn collect_body(body: Option<StreamingBlob>) -> Vec<u8> {
    let mut out = Vec::new();
    let Some(mut body) = body else {
        return out;
    };
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

async fn open_with_shared_mem(
    dir: &std::path::Path,
    idx_url: &str,
    blob_url: &str,
    mem: Arc<MemoryBlobStore>,
) -> pigeonhole::S3Gateway {
    let cfg = Config::for_test(dir);
    let index = Index::connect(idx_url).await.unwrap();
    let db = BlobDb::connect(blob_url).await.unwrap();
    let mut opts = IngestOptions::new(cfg.chunk_size, cfg.chunk_codec);
    opts.block_size = cfg.block_size;

    let backend: SharedBackend = Arc::new(erase_sweep(mem));
    let rep = Arc::new(Replicated::new(vec![backend], 1, Arc::new(CheapestFirst::new())).unwrap());
    let store = Arc::new(ChunkStore::open_replicated(db, rep, opts).await.unwrap());
    build_s3_gateway(cfg, index, store)
}

#[tokio::test]
async fn memory_put_lose_state_replay_get() {
    let dir = tempfile::tempdir().unwrap();
    let idx_path = dir.path().join("s3.db");
    let blob_path = dir.path().join("blob.db");
    let idx_url = format!("sqlite:{}?mode=rwc", idx_path.display());
    let blob_url = format!("sqlite:{}?mode=rwc", blob_path.display());

    // The backend survives the restart; so do the metadata logs.
    let mem = Arc::new(MemoryBlobStore::new());

    let s3 = open_with_shared_mem(dir.path(), &idx_url, &blob_url, mem.clone()).await;

    s3.create_bucket(req(CreateBucketInput {
        bucket: "demo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();

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

    // Lose the SQLite files; the logs next to them stay.
    for db in ["s3.db", "blob.db"] {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(dir.path().join(format!("{db}{suffix}")));
        }
    }
    assert!(!idx_path.exists() && !blob_path.exists());

    let s32 = open_with_shared_mem(dir.path(), &idx_url, &blob_url, mem).await;
    let got = s3s2_get(&s32).await;
    assert_eq!(got, payload.as_ref());
}

async fn s3s2_get(s3: &pigeonhole::S3Gateway) -> Vec<u8> {
    let got = s3
        .get_object(req(GetObjectInput {
            bucket: "demo".into(),
            key: "obj.bin".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .output;
    collect_body(got.body).await
}
