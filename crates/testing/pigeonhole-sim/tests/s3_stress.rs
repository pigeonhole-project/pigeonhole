//! sweep_stress under store faults: concurrent S3 put / copy / delete / get
//! with an aggressive sweeper, the blob store failing, failing after the
//! fact, landing late and rate limiting. Operations may fail; what may never
//! happen: a GET that returns a damaged body, a live part deleted, garbage
//! left once faults stop, a restart that loses an object.

use bytes::Bytes;
use futures::StreamExt;
use http::{Extensions, HeaderMap, Method, Uri};
use pigeonhole_blob::{erase, CheapestFirst, OrderedKey, Replicated, SharedBackend};
use pigeonhole_chunk_store::{BlobDb, ChunkStore, IngestOptions, SweepConfig, Sweeper};
use pigeonhole_gateway_s3::{build_s3_gateway, Index, S3Gateway, S3Options};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaOptions, MetaStore, Namespaces};
use pigeonhole_sim::faults::{Fault, FaultPlan, Faulty};
use pigeonhole_sim::{random_ms, random_u64};
use pigeonhole_storage_memory::MemoryBlobStore;
use s3s::dto::*;
use s3s::{S3ErrorCode, S3Request, S3};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BUCKET: &str = "stress";
const KEYS: u64 = 6;
const WORKERS: u64 = 4;
const OPS: u64 = 40;
const HEADER: usize = 16;

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

/// `[seed][len][bytes from seed]`: an intact object checks itself.
fn payload(seed: u64, len: usize) -> Bytes {
    let len = len.max(HEADER);
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&seed.to_le_bytes());
    out.extend_from_slice(&(len as u64).to_le_bytes());
    let mut x = seed | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

fn intact(data: &[u8]) -> bool {
    data.len() >= HEADER && {
        let seed = u64::from_le_bytes(data[..8].try_into().unwrap());
        let len = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        len == data.len() && payload(seed, len).as_ref() == data
    }
}

struct Node {
    s3: S3Gateway,
    store: Arc<ChunkStore>,
    backends: Vec<SharedBackend>,
}

/// Every chunk goes to every member; one success is enough.
async fn open_node(log: &Arc<MemoryBlobStore>, snaps: &Arc<MemoryBlobStore>, members: &[Arc<Faulty<MemoryBlobStore>>]) -> Node {
    let meta = MetaStore::open(
        Arc::new(ChainLogStore::new(log.clone())),
        Arc::new(BlobSnapshotStore::new(snaps.clone())),
        Index::register(BlobDb::register(Namespaces::new())),
        MetaOptions { snapshot_every: Some(50), ..MetaOptions::default() },
    )
    .await
    .unwrap();
    let s3_opts = S3Options { chunk_size: 16 * 1024, block_size: 4 * 1024, ..S3Options::for_test() };
    let mut opts = IngestOptions::new(s3_opts.chunk_size, s3_opts.chunk_codec);
    opts.block_size = s3_opts.block_size;
    let backends: Vec<SharedBackend> = members.iter().map(|m| Arc::new(erase(m.clone())) as SharedBackend).collect();
    let rep = Arc::new(Replicated::new(backends.clone(), 1, Arc::new(CheapestFirst::new())).unwrap());
    let store =
        Arc::new(ChunkStore::open_replicated(BlobDb::open(meta.clone()).await.unwrap(), rep, opts).await.unwrap());
    let s3 = build_s3_gateway(s3_opts, Index::open(meta).await.unwrap(), store.clone());
    Node { s3, store, backends }
}

fn sweeper(node: &Node) -> Sweeper {
    Sweeper::new(
        node.store.db().clone(),
        node.backends.clone(),
        SweepConfig { batch_size: 5, interval: Duration::ZERO },
        node.store.replicated().pins().clone(),
    )
}

async fn get(s3: &S3Gateway, k: &str) -> Result<Option<Vec<u8>>, String> {
    let out = match s3.get_object(req(GetObjectInput { bucket: BUCKET.into(), key: k.into(), ..Default::default() })).await {
        Ok(r) => r.output,
        Err(e) if *e.code() == S3ErrorCode::NoSuchKey => return Ok(None),
        Err(e) => return Err(format!("{e:?}")),
    };
    let mut body = out.body.ok_or("no body")?;
    let mut data = Vec::new();
    while let Some(chunk) = body.next().await {
        data.extend_from_slice(&chunk.map_err(|e| e.to_string())?);
    }
    Ok(Some(data))
}

async fn worker(s3: Arc<S3Gateway>, id: u64, damaged: Arc<Mutex<Vec<String>>>) {
    for op in 0..OPS {
        tokio::time::sleep(random_ms(5)).await;
        let k = format!("k{}", random_u64() % KEYS);
        match random_u64() % 10 {
            0..=3 => {
                let body = payload(id << 32 | op, HEADER + (random_u64() % 60_000) as usize);
                let input = PutObjectInput { bucket: BUCKET.into(), key: k, body: Some(StreamingBlob::from_bytes(body)), ..Default::default() };
                let _ = s3.put_object(req(input)).await;
            }
            4..=5 => {
                let src = format!("k{}", random_u64() % KEYS);
                let input = CopyObjectInput::builder()
                    .bucket(BUCKET.into())
                    .key(k)
                    .copy_source(CopySource::Bucket { bucket: BUCKET.into(), key: src.into(), version_id: None })
                    .build()
                    .unwrap();
                let _ = s3.copy_object(req(input)).await;
            }
            6 => {
                let _ = s3.delete_object(req(DeleteObjectInput { bucket: BUCKET.into(), key: k, ..Default::default() })).await;
            }
            _ => {
                if let Ok(Some(data)) = get(&s3, &k).await {
                    if !intact(&data) {
                        damaged.lock().unwrap().push(format!("{k}: {} bytes", data.len()));
                    }
                }
            }
        }
    }
}

async fn read_all(s3: &S3Gateway) -> BTreeMap<String, Vec<u8>> {
    let mut live = BTreeMap::new();
    for k in (0..KEYS).map(|i| format!("k{i}")).chain(["marker".to_string()]) {
        match get(s3, &k).await {
            Ok(Some(data)) => {
                assert!(intact(&data), "{k} damaged after the faults stopped");
                live.insert(k, data);
            }
            Ok(None) => {}
            Err(e) => panic!("{k} unreadable after the faults stopped: {e}"),
        }
    }
    live
}

async fn run(members: Vec<Arc<Faulty<MemoryBlobStore>>>) {
    let (log, snaps) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
    let node = open_node(&log, &snaps, &members).await;
    node.s3.create_bucket(req(CreateBucketInput { bucket: BUCKET.into(), ..Default::default() })).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let sweep = {
        let (sw, stop) = (sweeper(&node), stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = sw.sweep_once().await; // may fail under faults
                tokio::time::sleep(random_ms(10)).await;
            }
        })
    };
    let s3 = Arc::new(node.s3);
    let damaged = Arc::new(Mutex::new(Vec::new()));
    let workers: Vec<_> = (0..WORKERS).map(|id| tokio::spawn(worker(s3.clone(), id, damaged.clone()))).collect();
    for w in workers {
        w.await.unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    sweep.await.unwrap();
    assert!(damaged.lock().unwrap().is_empty(), "damaged reads: {:?}", damaged.lock().unwrap());

    // Faults stop (a damaged copy stays damaged); late puts land.
    for m in &members {
        m.quiesce();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let s3 = Arc::try_unwrap(s3).ok().expect("workers done");
    let node = Node { s3, ..node };
    // A put whose answer was lost (applied, then failed; or landed late)
    // holds a key above every key returned, so above the sweep horizon: it
    // is garbage the sweeper reaches once a newer upload returns a higher
    // key (sweep-invariants.md). Make one.
    let marker = payload(u64::MAX, 100);
    let input = PutObjectInput { bucket: BUCKET.into(), key: "marker".into(), body: Some(StreamingBlob::from_bytes(marker)), ..Default::default() };
    node.s3.put_object(req(input)).await.unwrap();
    let live = read_all(&node.s3).await;
    let sw = sweeper(&node);
    for _ in 0..3 {
        sw.sweep_once().await.unwrap();
    }
    assert_eq!(read_all(&node.s3).await, live, "the final sweeps changed objects");
    for (m, backend) in members.iter().zip(&node.backends) {
        let instance = backend.instance().id.clone();
        let on_backend: BTreeSet<Vec<u8>> = m.inner().message_keys().iter().map(|k| k.to_bytes()).collect();
        let expected: BTreeSet<Vec<u8>> =
            node.store.db().live_part_keys(&instance).await.unwrap().into_iter().collect();
        assert_eq!(on_backend.difference(&expected).count(), 0, "{instance}: garbage left after faults stopped");
        assert_eq!(expected.difference(&on_backend).count(), 0, "{instance}: live parts deleted");
    }

    // A restart replays the log and reads the same objects.
    drop(sw);
    drop(node);
    let again = open_node(&log, &snaps, &members).await;
    assert_eq!(read_all(&again.s3).await, live, "replay lost or changed objects");
}

fn member(id: &str, plan: FaultPlan) -> Arc<Faulty<MemoryBlobStore>> {
    Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new().with_instance_id(id)), plan))
}

#[tokio::test]
async fn s3_under_store_faults_keeps_objects_whole_and_reclaims_garbage() {
    run(vec![member("a", FaultPlan::mild())]).await;
}

/// One member also returns damaged copies of a third of its blobs: reads
/// must notice (the block checksums) and read the other member.
#[tokio::test]
async fn a_damaged_replica_is_read_around() {
    let damaged = FaultPlan { corrupt: 0.3, ..FaultPlan::mild() };
    let members = vec![member("a", damaged), member("b", FaultPlan::none())];
    run(members.clone()).await;
    let served = members[0].faults().iter().filter(|(_, f)| matches!(f, Fault::Corrupt)).count();
    assert!(served > 0, "no damaged copy was ever read");
}
