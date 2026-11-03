//! Stress: concurrent S3 put / copy / delete / get against an aggressive sweeper
//! (no pause between passes).
//!
//! Invariants checked (docs/sweep-invariants.md):
//! - every object visible in the index reads back intact (no reclaimed live parts);
//! - after quiescence, backend keys == committed live parts (garbage, including a
//!   dead writer's uncommitted uploads, is fully reclaimed);
//! - a restart that replays the metadata logs reads every object;
//! - a GET racing an overwrite / delete of the same key returns some whole
//!   version or NoSuchKey, never an error or a torn body (reads pin chunks).

use pigeonhole::memory::MemoryBlobStore;
use pigeonhole::service::S3Options;
use pigeonhole_blob::OrderedKey;
use pigeonhole_testkit::s3::{self as s3kit, get_object as get, payload, req, verify_payload, Node, HEADER};
use s3s::dto::*;
use s3s::{S3ErrorCode, S3};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const BUCKET: &str = "stress";
const KEYS: u64 = 12;
const WORKERS: u64 = 8;
const OPS_PER_WORKER: u64 = 400;
const CHUNK_SIZE: usize = 64 * 1024;
const BLOCK_SIZE: usize = 16 * 1024;
const MAX_OBJECT: u64 = 200 * 1024;

/// xorshift64*: deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn key(i: u64) -> String {
    format!("obj-{i:02}")
}

/// The node of the binary over a tablet in `dir` and the one store `mem`;
/// reopening `dir` replays what is there.
async fn open_node(dir: &Path, mem: Arc<MemoryBlobStore>) -> Node {
    let meta = pigeonhole_testkit::tablet(dir, s3kit::register).await;
    Node::open(meta, vec![s3kit::member(mem)], s3kit::ingest(CHUNK_SIZE, BLOCK_SIZE), S3Options::for_test()).await
}

async fn get_object(s3: &pigeonhole::S3Gateway, k: &str) -> Result<Option<Vec<u8>>, String> {
    get(s3, BUCKET, k).await
}

#[derive(Default)]
struct Counters {
    puts: AtomicU64,
    copies: AtomicU64,
    copies_src_missing: AtomicU64,
    deletes: AtomicU64,
    gets: AtomicU64,
}

async fn worker(s3: Arc<pigeonhole::S3Gateway>, id: u64, c: Arc<Counters>) -> Vec<String> {
    let mut rng = Rng::new(0x5EED + id);
    let mut failures = Vec::new();
    for op in 0..OPS_PER_WORKER {
        let k = key(rng.below(KEYS));
        match rng.below(100) {
            0..=44 => {
                let len = (HEADER as u64 + rng.below(MAX_OBJECT)) as usize;
                let body = payload(id << 32 | op, len);
                match s3
                    .put_object(req(PutObjectInput {
                        bucket: BUCKET.into(),
                        key: k.clone(),
                        body: Some(StreamingBlob::from_bytes(body)),
                        ..Default::default()
                    }))
                    .await
                {
                    Ok(_) => c.puts.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        failures.push(format!("put {k}: {e:?}"));
                        continue;
                    }
                };
            }
            45..=69 => {
                let src = key(rng.below(KEYS));
                let input = CopyObjectInput::builder()
                    .bucket(BUCKET.into())
                    .key(k.clone())
                    .copy_source(CopySource::Bucket {
                        bucket: BUCKET.into(),
                        key: src.clone().into(),
                        version_id: None,
                    })
                    .build()
                    .unwrap();
                match s3.copy_object(req(input)).await {
                    Ok(_) => c.copies.fetch_add(1, Ordering::Relaxed),
                    Err(e) if *e.code() == S3ErrorCode::NoSuchKey => {
                        c.copies_src_missing.fetch_add(1, Ordering::Relaxed)
                    }
                    Err(e) => {
                        failures.push(format!("copy {src} -> {k}: {e:?}"));
                        continue;
                    }
                };
            }
            70..=89 => {
                match s3
                    .delete_object(req(DeleteObjectInput {
                        bucket: BUCKET.into(),
                        key: k.clone(),
                        ..Default::default()
                    }))
                    .await
                {
                    Ok(_) => c.deletes.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        failures.push(format!("delete {k}: {e:?}"));
                        continue;
                    }
                };
            }
            _ => {
                c.gets.fetch_add(1, Ordering::Relaxed);
                match get_object(&s3, &k).await {
                    Ok(Some(data)) => {
                        if let Err(e) = verify_payload(&data) {
                            failures.push(format!("get {k}: {e}"));
                        }
                    }
                    Ok(None) => {}
                    Err(e) => failures.push(e),
                }
            }
        }
        tokio::task::yield_now().await;
    }
    failures
}

/// Read every key; returns `key -> etag` for objects that exist.
async fn verify_all(s3: &pigeonhole::S3Gateway, stage: &str) -> HashMap<String, Vec<u8>> {
    let mut live = HashMap::new();
    let mut failures = Vec::new();
    for i in 0..KEYS {
        let k = key(i);
        match get_object(s3, &k).await {
            Ok(Some(data)) => match verify_payload(&data) {
                Ok(()) => {
                    live.insert(k, data);
                }
                Err(e) => failures.push(format!("{k}: {e}")),
            },
            Ok(None) => {}
            Err(e) => failures.push(e),
        }
    }
    assert!(failures.is_empty(), "{stage}: unreadable objects: {failures:#?}");
    live
}

async fn backend_keys(mem: &MemoryBlobStore) -> BTreeSet<Vec<u8>> {
    mem.message_keys().iter().map(|k| k.to_bytes()).collect()
}

async fn expected_keys(node: &Node) -> BTreeSet<Vec<u8>> {
    let instance = &node.backends[0].instance().id;
    node.store.db().live_part_keys(instance).await.unwrap().into_iter().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ops_with_aggressive_sweeper() {
    let dir = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemoryBlobStore::new());
    let node = open_node(dir.path(), mem.clone()).await;

    node.s3.create_bucket(req(CreateBucketInput { bucket: BUCKET.into(), ..Default::default() })).await.unwrap();

    // A dead writer's uploads: on the backend, never committed to chunk_parts.
    let ours = node.store.replicated().pins().owner();
    for i in 0..5u64 {
        node.backends[0]
            .put(payload(0xDEAD + i, 1024), &pigeonhole_blob::BlobId::random_of(ours).name())
            .await
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let sweep_passes = Arc::new(AtomicU64::new(0));
    let sweep_deleted = Arc::new(AtomicU64::new(0));

    let sweep_task = {
        let sw = node.sweeper(7);
        let (stop, passes, deleted) = (stop.clone(), sweep_passes.clone(), sweep_deleted.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let stats = sw.sweep_once().await.expect("sweep pass");
                passes.fetch_add(1, Ordering::Relaxed);
                deleted.fetch_add(stats.keys_deleted, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        })
    };

    let s3 = Arc::new(node.s3);
    let counters = Arc::new(Counters::default());
    let workers: Vec<_> = (0..WORKERS).map(|id| tokio::spawn(worker(s3.clone(), id, counters.clone()))).collect();
    let mut failures = Vec::new();
    for w in workers {
        failures.extend(w.await.unwrap());
    }

    stop.store(true, Ordering::Relaxed);
    sweep_task.await.unwrap();

    eprintln!(
        "puts={} copies={} copies_src_missing={} deletes={} gets={} \
         sweep_passes={} swept_keys={}",
        counters.puts.load(Ordering::Relaxed),
        counters.copies.load(Ordering::Relaxed),
        counters.copies_src_missing.load(Ordering::Relaxed),
        counters.deletes.load(Ordering::Relaxed),
        counters.gets.load(Ordering::Relaxed),
        sweep_passes.load(Ordering::Relaxed),
        sweep_deleted.load(Ordering::Relaxed),
    );
    assert!(failures.is_empty(), "operation failures during stress: {failures:#?}");
    assert!(sweep_passes.load(Ordering::Relaxed) > 0, "sweeper never ran");

    let s3 = Arc::try_unwrap(s3).ok().expect("workers done");
    let node = Node { s3, ..node };
    let live = verify_all(&node.s3, "after stress").await;

    // Quiesce: no upload is pinned, so everything up to the last returned key
    // is a candidate.
    let sw = node.sweeper(7);
    for _ in 0..3 {
        sw.sweep_once().await.unwrap();
    }
    assert_eq!(verify_all(&node.s3, "after final sweep").await, live, "final sweep changed object contents");

    let on_backend = backend_keys(&mem).await;
    let expected = expected_keys(&node).await;
    let garbage: Vec<_> = on_backend.difference(&expected).collect();
    let missing: Vec<_> = expected.difference(&on_backend).collect();
    assert!(garbage.is_empty(), "unreclaimed backend keys: {garbage:?}");
    assert!(missing.is_empty(), "live keys deleted from backend: {missing:?}");

    // Restart: all metadata state is gone, boot replays the metadata logs.
    drop(sw);
    drop(node);
    let restored = open_node(dir.path(), mem).await;
    assert_eq!(verify_all(&restored.s3, "after replay").await, live, "replay lost or changed objects");
}
