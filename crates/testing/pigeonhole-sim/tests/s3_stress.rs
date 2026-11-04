//! sweep_stress under store faults: concurrent S3 put / copy / delete / get
//! with an aggressive sweeper and the repairer, the blob store failing,
//! failing after the fact, landing late and rate limiting; and, in the
//! crash runs, the node killed at a random moment (its tasks dropped, the
//! disk under its log losing what was not synced, a write maybe torn), then
//! reopened, the workers going on. Operations may fail; what may never
//! happen: a GET that returns a damaged body, a live part deleted, garbage
//! left once faults stop, a chunk left short of a replica once repair ran,
//! a restart that loses an object.
//!
//! And the cooler racing rewrites and deletes of the inline objects it moves
//! out: no rewrite lost, no chunk left that nothing points at.

use bytes::Bytes;
use pigeonhole_blob::{OrderedKey, SharedBackend};
use pigeonhole_chunk_store::{RepairConfig, Repairer, REPAIR_BACKOFF_MAX};
use pigeonhole_gateway_s3::{S3Gateway, S3Options};
use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaOptions, MetaStore, Namespaces};
use pigeonhole_sim::faults::{Fault, FaultPlan, Faulty};
use pigeonhole_sim::{random_ms, random_u64};
use pigeonhole_storage_disk::{DiskBlobStore, SimFs};
use pigeonhole_storage_memory::MemoryBlobStore;
use pigeonhole_testkit::s3::{self as s3kit, intact, payload, req, Node, HEADER};
use s3s::dto::*;
use s3s::S3;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BUCKET: &str = "stress";
const KEYS: u64 = 6;
const WORKERS: u64 = 4;
const OPS: u64 = 40;

/// The ops of each worker: `OPS`, or ten times as many when
/// `PIGEONHOLE_SIM_LONG` is set (`make sim-all`).
fn ops() -> u64 {
    if std::env::var_os("PIGEONHOLE_SIM_LONG").is_some() {
        OPS * 10
    } else {
        OPS
    }
}

/// Every chunk goes to every member; one success is enough. The tablet's
/// log and snapshots are on the simulated disk `fs`. `inline_max`: objects
/// up to it stay in the metadata (docs/inline-objects.md).
async fn open_node(fs: &SimFs, members: &[Arc<Faulty<MemoryBlobStore>>], inline_max: Option<usize>) -> Node {
    let open = |dir: &str| Arc::new(DiskBlobStore::open_on(Arc::new(fs.clone()), dir, 64 * 1024).unwrap());
    let meta = MetaStore::open(
        Arc::new(ChainLogStore::new(open("/meta/log"))),
        Arc::new(BlobSnapshotStore::new(open("/meta/snapshots"))),
        s3kit::register(Namespaces::new()),
        MetaOptions { snapshot_every: Some(50), ..MetaOptions::default() },
    )
    .await
    .unwrap();
    let backends: Vec<SharedBackend> = members.iter().map(|m| s3kit::member(m.clone())).collect();
    let opts = S3Options { inline_max, ..S3Options::for_test() };
    Node::open(meta, backends, s3kit::ingest(16 * 1024, 4 * 1024), opts).await
}

fn repairer(node: &Node) -> Repairer {
    let config = RepairConfig { interval: None, batch_size: 8, concurrency: 2, max_cost_wait_secs: 0.0, scrub: true };
    Repairer::new(node.store.db().clone(), node.store.replicated().clone(), config)
}

async fn get(s3: &S3Gateway, k: &str) -> Result<Option<Vec<u8>>, String> {
    s3kit::get_object(s3, BUCKET, k).await
}

/// Run worker `id`'s ops from where `done` says it got to; an op cut off
/// by a crash runs again in the next life.
async fn worker(s3: Arc<S3Gateway>, id: usize, done: Arc<[AtomicU64]>, damaged: Arc<Mutex<Vec<String>>>) {
    let id64 = id as u64;
    loop {
        let op = done[id].load(Ordering::SeqCst);
        if op >= ops() {
            return;
        }
        tokio::time::sleep(random_ms(5)).await;
        let k = format!("k{}", random_u64() % KEYS);
        match random_u64() % 10 {
            0..=3 => {
                let body = payload(id64 << 32 | op, HEADER + (random_u64() % 60_000) as usize);
                let input = PutObjectInput {
                    bucket: BUCKET.into(),
                    key: k,
                    body: Some(StreamingBlob::from_bytes(body)),
                    ..Default::default()
                };
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
                let _ = s3
                    .delete_object(req(DeleteObjectInput { bucket: BUCKET.into(), key: k, ..Default::default() }))
                    .await;
            }
            _ => {
                if let Ok(Some(data)) = get(&s3, &k).await {
                    if !intact(&data) {
                        damaged.lock().unwrap().push(format!("{k}: {} bytes", data.len()));
                    }
                }
            }
        }
        done[id].fetch_add(1, Ordering::SeqCst);
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

/// One life of a node: its workers, sweeper and repairer.
struct Life {
    workers: Vec<tokio::task::JoinHandle<()>>,
    background: Vec<tokio::task::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl Life {
    fn start(node: &Node, done: &Arc<[AtomicU64]>, damaged: &Arc<Mutex<Vec<String>>>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let sweep = {
            let (sw, stop) = (node.sweeper(5), stop.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let _ = sw.sweep_once().await; // may fail under faults
                    tokio::time::sleep(random_ms(10)).await;
                }
            })
        };
        let repair = {
            let (rp, stop) = (repairer(node), stop.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let _ = rp.repair_once().await; // may fail under faults
                    tokio::time::sleep(random_ms(10)).await;
                }
            })
        };
        let s3 = Arc::new(node.s3.clone());
        let workers = (0..WORKERS as usize)
            .map(|id| tokio::spawn(worker(s3.clone(), id, done.clone(), damaged.clone())))
            .collect();
        Self { workers, background: vec![sweep, repair], stop }
    }

    /// The process dies: every task of it at once, wherever it was.
    fn kill(self) {
        for t in self.workers.iter().chain(&self.background) {
            t.abort();
        }
    }

    /// The workers finish; the background loops stop after their pass.
    async fn finish(self) {
        for w in self.workers {
            w.await.unwrap();
        }
        self.stop.store(true, Ordering::SeqCst);
        for t in self.background {
            t.await.unwrap();
        }
    }
}

/// Wait until the workers did `n` ops in all (or all of theirs).
async fn ops_done(done: &[AtomicU64], n: u64) {
    let total = || done.iter().map(|d| d.load(Ordering::SeqCst)).sum::<u64>();
    while total() < n.min(WORKERS * ops()) {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// The run; with `crash`, the node dies at a random op and is reopened
/// from what its disk kept. The node reopened from the log at the end,
/// faults stopped.
async fn run(members: Vec<Arc<Faulty<MemoryBlobStore>>>, crash: bool) -> Node {
    let fs = SimFs::new(Duration::from_millis(1));
    let mut node = open_node(&fs, &members, None).await;
    node.s3.create_bucket(req(CreateBucketInput { bucket: BUCKET.into(), ..Default::default() })).await.unwrap();

    let done: Arc<[AtomicU64]> = (0..WORKERS).map(|_| AtomicU64::new(0)).collect();
    let damaged = Arc::new(Mutex::new(Vec::new()));
    let mut life = Life::start(&node, &done, &damaged);
    if crash {
        ops_done(&done, 1 + random_u64() % (WORKERS * ops())).await;
        life.kill();
        drop(node);
        let mut torn = |n: usize| (random_u64() as usize) % (n + 1);
        fs.crash(Some(&mut torn));
        node = open_node(&fs, &members, None).await;
        life = Life::start(&node, &done, &damaged);
    }
    life.finish().await;
    assert!(damaged.lock().unwrap().is_empty(), "damaged reads: {:?}", damaged.lock().unwrap());

    // Faults stop (a damaged copy stays damaged); late puts land.
    for m in &members {
        m.quiesce();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    // A put whose answer was lost (applied, then failed; or landed late)
    // holds a key above every key returned, so above the sweep horizon: it
    // is garbage the sweeper reaches once a newer upload returns a higher
    // key (sweep-invariants.md). Make one.
    let marker = payload(u64::MAX, 100);
    let input = PutObjectInput {
        bucket: BUCKET.into(),
        key: "marker".into(),
        body: Some(StreamingBlob::from_bytes(marker)),
        ..Default::default()
    };
    node.s3.put_object(req(input)).await.unwrap();
    let live = read_all(&node.s3).await;
    // Repair until nothing is missing: every live chunk on every member. A
    // job that failed while the faults were on waits out its backoff (the
    // clock moved on past it), one parked after too many failures is queued
    // again, as an operator does once the stores are back.
    node.store.db().retry_unrepairable();
    let rp = repairer(&node);
    for _ in 0..10 {
        rp.repair_once().await.unwrap();
        if node.store.db().repair_jobs().await.is_empty() {
            break;
        }
        tokio::time::sleep(REPAIR_BACKOFF_MAX).await;
    }
    for backend in &node.backends {
        let id = backend.instance().id.clone();
        let peers: Vec<String> = node.backends.iter().map(|b| b.instance().id.clone()).filter(|p| *p != id).collect();
        let peers: Vec<&str> = peers.iter().map(String::as_str).collect();
        let missing = node.store.db().chunks_missing_instance(&id, &peers).await.unwrap();
        assert!(missing.is_empty(), "{id}: chunks short of a replica after repair: {missing:?}");
    }
    let sw = node.sweeper(5);
    for _ in 0..3 {
        let stats = sw.sweep_once().await.unwrap();
        assert!(stats.failed.is_empty(), "a sweep with the faults off left {:?} out", stats.failed);
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
    let again = open_node(&fs, &members, None).await;
    assert_eq!(read_all(&again.s3).await, live, "replay lost or changed objects");
    again
}

fn member(id: &str, plan: FaultPlan) -> Arc<Faulty<MemoryBlobStore>> {
    Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new().with_instance_id(id)), plan))
}

#[tokio::test]
async fn s3_under_store_faults_keeps_objects_whole_and_reclaims_garbage() {
    run(vec![member("a", FaultPlan::mild())], false).await;
}

/// Two members, one failing: the repairer fills in what the writes missed
/// while the node is killed mid-run and reopened.
#[tokio::test]
async fn a_node_killed_mid_run_reopens_and_goes_on() {
    run(vec![member("a", FaultPlan::mild()), member("b", FaultPlan::none())], true).await;
}

/// One member also returns damaged copies of a third of its blobs: reads
/// must notice (the block checksums) and read the other member. Which copy
/// a read tries first is the selector's coin, so at the end one object has
/// its copy on `a` damaged on purpose: read from `a` alone it is refused,
/// never served, and a GET reads around it.
#[tokio::test]
async fn a_damaged_replica_is_read_around() {
    let damaged = FaultPlan { corrupt: 0.3, ..FaultPlan::mild() };
    let members = vec![member("a", damaged), member("b", FaultPlan::none())];
    let node = run(members.clone(), false).await;

    // Written with faults stopped: on both members.
    let body = payload(u64::MAX - 1, 40_000);
    let input = PutObjectInput {
        bucket: BUCKET.into(),
        key: "damaged".into(),
        body: Some(StreamingBlob::from_bytes(body.clone())),
        ..Default::default()
    };
    node.s3.put_object(req(input)).await.unwrap();
    let extents = node.s3.index.get_extents(BUCKET, "damaged").await.unwrap().unwrap();
    for ext in &extents {
        let layouts = node.store.db().get_replica_layouts(ext.chunk).await.unwrap();
        let on_a = layouts.iter().find(|l| l.instance == "a").expect("a copy on a");
        for part in &on_a.parts {
            members[0].damage(&pigeonhole_blob::load_id::<MemoryBlobStore>(&part.locator).unwrap());
        }
        let alone = node.store.read_from_replicas(ext.chunk, 0, ext.len as usize, std::slice::from_ref(on_a)).await;
        assert!(alone.is_err(), "chunk {}: a damaged copy was served", ext.chunk);
    }
    assert_eq!(get(&node.s3, "damaged").await.unwrap().as_deref(), Some(&body[..]), "not read around the damage");
    let served = members[0].faults().iter().filter(|(_, f)| matches!(f, Fault::Corrupt)).count();
    assert!(served > 0, "no damaged copy was ever read");
}

/// Which write a body is (the seed in its header), for messages.
fn which(body: Option<&[u8]>) -> String {
    match body {
        None => "nothing".into(),
        Some(d) if intact(d) => format!("write {:#x}", u64::from_le_bytes(d[..8].try_into().unwrap())),
        Some(d) => format!("{} damaged bytes", d.len()),
    }
}

/// A writer that owns `keys`, so the last write of each is known: rewrites
/// and deletes, small enough to stay inline. Returns what each key holds.
async fn inline_writer(s3: Arc<S3Gateway>, id: u64, keys: Vec<String>) -> BTreeMap<String, Option<Bytes>> {
    let mut last: BTreeMap<String, Option<Bytes>> = keys.iter().map(|k| (k.clone(), None)).collect();
    for op in 0..ops() {
        tokio::time::sleep(random_ms(5)).await;
        let k = &keys[(random_u64() % keys.len() as u64) as usize];
        if random_u64().is_multiple_of(4) {
            let input = DeleteObjectInput { bucket: BUCKET.into(), key: k.clone(), ..Default::default() };
            s3.delete_object(req(input)).await.unwrap();
            last.insert(k.clone(), None);
        } else {
            let body = payload(id << 32 | op, HEADER + (random_u64() % 500) as usize);
            let input = PutObjectInput {
                bucket: BUCKET.into(),
                key: k.clone(),
                content_length: Some(body.len() as i64),
                body: Some(StreamingBlob::from_bytes(body.clone())),
                ..Default::default()
            };
            s3.put_object(req(input)).await.unwrap();
            last.insert(k.clone(), Some(body));
        }
    }
    last
}

/// The cooler moves an inline object out in two steps: it uploads the bytes
/// as a chunk, then commits `Cool` naming the version it picked. A rewrite or
/// a delete in between must win (`Cool` rejected, its chunk released), and
/// stores that answer late stretch that window: the seed decides where it
/// falls. Checked: the race did happen (some moves rejected), a pass once
/// the writes stop moves out all that is left, every key holds its last
/// write, and every live chunk is pointed at.
#[tokio::test]
async fn the_cooler_never_loses_a_rewrite_or_leaks_a_chunk() {
    let slow = FaultPlan { delay: 0.5, max_delay: Duration::from_millis(30), ..FaultPlan::none() };
    let members = vec![member("a", slow)];
    let node = open_node(&SimFs::new(Duration::from_millis(1)), &members, Some(1024)).await;
    node.s3.create_bucket(req(CreateBucketInput { bucket: BUCKET.into(), ..Default::default() })).await.unwrap();
    let s3 = Arc::new(node.s3);

    let stop = Arc::new(AtomicBool::new(false));
    let policy = pigeonhole_gateway_s3::CoolPolicy {
        cool_after: Some(Duration::ZERO),
        min_inline: Duration::ZERO,
        batch: usize::MAX,
        ..Default::default()
    };
    let cooler = {
        let (s3, stop, policy) = (s3.clone(), stop.clone(), policy.clone());
        tokio::spawn(async move {
            let mut rejected = 0;
            while !stop.load(Ordering::SeqCst) {
                rejected += s3.cool_pass(&policy).await.unwrap().rejected;
                tokio::time::sleep(random_ms(5)).await;
            }
            rejected
        })
    };
    let writers: Vec<_> = (0..WORKERS)
        .map(|id| {
            let keys = (0..KEYS).map(|k| format!("w{id}k{k}")).collect();
            tokio::spawn(inline_writer(s3.clone(), id, keys))
        })
        .collect();
    let mut last = BTreeMap::new();
    for w in writers {
        last.extend(w.await.unwrap());
    }
    stop.store(true, Ordering::SeqCst);
    assert!(cooler.await.unwrap() > 0, "no move was ever rejected: the race did not happen");
    let quiet = s3.cool_pass(&policy).await.unwrap();
    assert_eq!(quiet.rejected, 0, "nothing writes any more");

    let mut referenced = BTreeSet::new();
    for (k, want) in &last {
        let got = get(&s3, k).await.unwrap();
        assert!(
            got.as_deref() == want.as_deref(),
            "{k} holds {}, but its last was {}",
            which(got.as_deref()),
            which(want.as_deref())
        );
        let extents = s3.index.get_extents(BUCKET, k).await.unwrap().unwrap_or_default();
        assert_eq!(extents.is_empty(), want.is_none(), "{k}: still inline after a quiet pass");
        referenced.extend(extents.iter().map(|e| e.chunk));
    }
    // A rejected move releases its chunk once its commit is answered.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while s3.store.db().stats().live as usize != referenced.len() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "live chunks {} != referenced {}",
            s3.store.db().stats().live,
            referenced.len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
