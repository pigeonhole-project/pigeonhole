use super::*;
use std::collections::BTreeMap;
use std::path::Path;

/// Toy namespace: a counter; every committed op is recorded; going negative
/// is rejected.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Counter {
    v: i64,
    ops: BTreeMap<u64, i64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Op {
    id: u64,
    delta: i64,
}

impl StateMachine for Counter {
    const NAME: &'static str = "counter";
    const VERSION: u32 = 1;
    type Command = Op;
    type Reply = i64;
    type Reject = &'static str;

    fn apply(&mut self, op: Op) -> Result<Outcome<Self>> {
        if self.v + op.delta < 0 {
            return Ok(Err("negative"));
        }
        self.v += op.delta;
        self.ops.insert(op.id, op.delta);
        Ok(Ok(self.v))
    }
}

fn op(id: u64, delta: i64) -> Op {
    Op { id, delta }
}

fn counter() -> Namespaces {
    Namespaces::new().with::<Counter>()
}

async fn open_on(dir: &Path) -> Arc<MetaStore> {
    MetaStore::open_local(dir.join("log"), counter()).await.unwrap()
}

fn value(store: &MetaStore) -> i64 {
    store.read::<Counter, _>(|c| c.v)
}

fn op_ids(store: &MetaStore) -> Vec<u64> {
    store.read::<Counter, _>(|c| c.ops.keys().copied().collect())
}

fn snapshot(store: &MetaStore) -> Vec<u8> {
    store.read::<Counter, _>(encode_state)
}

#[tokio::test]
async fn commit_and_reject_both_consume_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    assert_eq!(store.generation(), 1);
    assert_eq!(store.propose_at::<Counter>(op(1, 5)).await.unwrap(), (1, Ok(5)));
    assert_eq!(store.propose_at::<Counter>(op(2, -7)).await.unwrap(), (2, Err("negative")));
    assert_eq!(value(&store), 5, "rejected command must not change state");
    assert_eq!(op_ids(&store), vec![1]);
    assert_eq!(store.applied_position(), 2);
}

#[tokio::test]
async fn unknown_namespace_fails_without_a_position() {
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Other;
    impl StateMachine for Other {
        const NAME: &'static str = "other";
        const VERSION: u32 = 1;
        type Command = ();
        type Reply = ();
        type Reject = ();
        fn apply(&mut self, _: ()) -> Result<Outcome<Self>> {
            Ok(Ok(()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    assert!(store.propose::<Other>(()).await.is_err());
    assert_eq!(store.applied_position(), 0);

    let twice = Namespaces::new().with::<Counter>().with::<Counter>();
    assert!(MetaStore::open_local(dir.path().join("log2"), twice).await.is_err());
}

#[tokio::test]
async fn concurrent_proposals_get_distinct_positions() {
    let dir = tempfile::tempdir().unwrap();
    for delay in [Duration::ZERO, Duration::from_millis(20)] {
        let log = LocalLogStore::open(dir.path().join(format!("log-{}", delay.as_millis()))).unwrap();
        let opts = MetaOptions { batch_max_delay: delay, batch_max_entries: 8, snapshot_every: 5 };
        let store = MetaStore::open(Arc::new(log), counter(), opts).await.unwrap();
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let s = store.clone();
                tokio::spawn(async move { s.propose_at::<Counter>(op(i, 1)).await })
            })
            .collect();
        let mut positions = Vec::new();
        for t in tasks {
            positions.push(t.await.unwrap().unwrap().0);
        }
        positions.sort_unstable();
        assert_eq!(positions, (1..=32).collect::<Vec<_>>());
        assert_eq!(value(&store), 32);
    }
}

#[tokio::test]
async fn replay_yields_identical_state() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path()).await;
    for i in 0..300u64 {
        // Every fifth op would go negative and is rejected.
        let delta = if i % 5 == 4 { -1000 } else { i as i64 % 7 };
        let _ = a.propose::<Counter>(op(i, delta)).await.unwrap();
    }
    assert_eq!(a.applied_position(), 300);
    let snap = snapshot(&a);
    assert_eq!(encode_state(&decode_state::<Counter>(&snap).unwrap()), snap, "decode inverts encode");

    let b = open_on(dir.path()).await;
    assert_eq!(b.generation(), 2);
    assert_eq!(b.applied_position(), 300);
    assert_eq!(snapshot(&b), snap);
    assert_eq!(op_ids(&b).len(), 240);
}

#[tokio::test]
async fn second_incarnation_fences_the_first_without_losing_acknowledged_entries() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path()).await;

    // Keep proposing on A while B boots on the same log.
    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut acked = Vec::new();
            for id in 0.. {
                match a.propose::<Counter>(op(id, 1)).await {
                    Ok(_) => acked.push(id),
                    Err(e) => {
                        let stop = e.downcast::<Stop>().expect("a Stop error");
                        return (acked, stop);
                    }
                }
            }
            unreachable!()
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b = open_on(dir.path()).await;
    let (acked, stop) = writer.await.unwrap();

    assert_eq!(stop, Stop::Fenced { current: 2 });
    assert_eq!(a.stopped().await, Stop::Fenced { current: 2 });
    assert!(a.propose::<Counter>(op(u64::MAX >> 1, 1)).await.is_err());
    assert!(!acked.is_empty(), "A acknowledged something before the fence");

    // B holds exactly A's acknowledged entries: nothing lost, nothing extra.
    assert_eq!(op_ids(&b), acked);
    assert_eq!(snapshot(&a), snapshot(&b));

    b.propose::<Counter>(op(1 << 40, 1)).await.unwrap().unwrap();
}

#[tokio::test]
async fn crash_between_append_and_apply_is_applied_once_on_boot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path()).await;
        a.propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    }
    // An incarnation that appended a batch and died before applying it.
    let log = LocalLogStore::open(dir.path().join("log")).unwrap();
    assert_eq!(log.claim(2).await.unwrap().position, 1);
    assert_eq!(log.append(2, &[entry::<Counter>(&op(2, 5))]).await.unwrap(), 2);

    for expected_gen in [3, 4] {
        let a = open_on(dir.path()).await;
        assert_eq!(a.generation(), expected_gen);
        assert_eq!(a.applied_position(), 2);
        assert_eq!(value(&a), 6);
        assert_eq!(op_ids(&a), vec![1, 2]);
    }
}

#[tokio::test]
async fn entries_of_another_command_format_refuse_to_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path()).await;
        a.propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    }
    let log = LocalLogStore::open(dir.path().join("log")).unwrap();
    log.claim(2).await.unwrap();
    let mut old = entry::<Counter>(&op(2, 1));
    old.version = Counter::VERSION + 1;
    log.append(2, &[old]).await.unwrap();

    let err = MetaStore::open_local(dir.path().join("log"), counter()).await.err().expect("boot must fail");
    assert!(format!("{err:#}").contains("counter command in format 2, this binary reads 1"), "{err:#}");
}

fn segments(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    names.sort();
    names
}

async fn open_snapshotting(dir: &Path) -> Arc<MetaStore> {
    let log = LocalLogStore::with_segment_bytes(dir, 64).unwrap();
    let opts = MetaOptions { snapshot_every: 10, ..MetaOptions::default() };
    MetaStore::open(Arc::new(log), counter(), opts).await.unwrap()
}

#[tokio::test]
async fn snapshots_collect_the_log_and_boot_from_them() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let a = open_snapshotting(&log_dir).await;
    for i in 0..35u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    let state = snapshot(&a);
    drop(a);

    let d = LocalLogStore::open(&log_dir).unwrap().discover().await.unwrap().unwrap();
    let (at, snap) = d.snapshot.expect("a snapshot was taken");
    assert_eq!(at, 30);
    assert_eq!(TabletSnapshot::decode(&snap.data).unwrap().position, 30);
    let segs = segments(&log_dir);
    assert!(segs[0] > "seg-00000000000000000001.log".to_string(), "log behind the snapshot collected: {segs:?}");

    let b = open_snapshotting(&log_dir).await;
    assert_eq!(b.applied_position(), 35);
    assert_eq!(snapshot(&b), state, "snapshot + tail rebuild the same state");
    b.propose::<Counter>(op(100, 1)).await.unwrap().unwrap();
    assert_eq!(value(&b), 36);
}

#[tokio::test]
async fn snapshot_of_another_state_version_refuses_to_load() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    {
        let a = open_on(dir.path()).await;
        a.propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    }
    let log = LocalLogStore::open(&log_dir).unwrap();
    log.claim(2).await.unwrap();
    let snap = TabletSnapshot {
        format: SNAPSHOT_FORMAT,
        position: 1,
        states: vec![("counter".into(), Counter::VERSION + 1, encode_state(&Counter::default()))],
    };
    log.install_snapshot(2, 1, Snapshot { data: snap.encode() }).await.unwrap();

    let err = MetaStore::open_local(&log_dir, counter()).await.err().expect("boot must fail");
    assert!(format!("{err:#}").contains("counter state in format 2, this binary reads 1"), "{err:#}");
}
