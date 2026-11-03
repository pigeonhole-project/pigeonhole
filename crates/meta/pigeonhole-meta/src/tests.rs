use super::*;
use pigeonhole_storage_disk::DiskBlobStore;
use pigeonhole_storage_memory::MemoryBlobStore;
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
    type Reject = String;

    fn apply(&mut self, op: Op) -> Result<Outcome<Self>> {
        if self.v + op.delta < 0 {
            return Ok(Err("negative".into()));
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

/// A disk log in `<meta_dir>/log`, as a `local_disk` `[meta] log`.
fn log_of(meta_dir: &Path) -> Arc<crate::ChainLogStore<DiskBlobStore>> {
    Arc::new(crate::ChainLogStore::new(Arc::new(DiskBlobStore::open(meta_dir.join("log")).unwrap())))
}

/// Snapshots in `<meta_dir>/snapshots`, as a `local_disk` `[meta] snapshot`.
fn snaps_of(meta_dir: &Path) -> Arc<crate::BlobSnapshotStore<DiskBlobStore>> {
    Arc::new(crate::BlobSnapshotStore::new(Arc::new(DiskBlobStore::open(meta_dir.join("snapshots")).unwrap())))
}

/// Live snapshot blobs in `<meta_dir>/snapshots`.
async fn snapshot_count(meta_dir: &Path) -> usize {
    let store = DiskBlobStore::open(meta_dir.join("snapshots")).unwrap();
    store.live_keys().await.unwrap().len()
}

/// Wait until there are at most `n` snapshots in `meta_dir`.
async fn snapshots_down_to(meta_dir: &Path, n: usize, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while snapshot_count(meta_dir).await > n {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn open_dir(meta_dir: &Path, namespaces: Namespaces) -> Result<Arc<MetaStore>> {
    MetaStore::open(log_of(meta_dir), snaps_of(meta_dir), namespaces, MetaOptions::default()).await
}

async fn open_on(dir: &Path) -> Arc<MetaStore> {
    open_dir(&dir.join("log"), counter()).await.unwrap()
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

/// Log and snapshots of one `local_disk` `[meta]` store at `dir`.
fn local(dir: &Path) -> (Arc<dyn LogStore>, Arc<dyn SnapshotStore>) {
    (log_of(dir), snaps_of(dir))
}

/// Snapshots are taken in the background; wait until the log starts at or
/// after `at`.
async fn wait_base(store: &MetaStore, at: Position) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.base_position() < at {
        assert!(tokio::time::Instant::now() < deadline, "no base at {at}, still {}", store.base_position());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// An entry as a crashed incarnation would have appended it: the openraft
/// entry for `cmd` at `position` in `term`, led by the only node.
fn raw_entry(term: u64, position: Position, cmd: Command) -> Entry {
    let id = openraft::LogId::new(openraft::CommittedLeaderId::new(term, raft::SOLO), position - 1);
    let entry = raft::RaftEntry {
        log_id: id,
        payload: openraft::EntryPayload::Normal(vec![crate::Group { fence: None, commands: vec![cmd] }]),
    };
    Entry { term, payload: postcard::to_stdvec(&entry).unwrap() }
}

/// The term of the entry at `head`.
async fn head_term(log: &dyn LogStore, head: Head) -> u64 {
    let last = log.read(head.position - 1, head).await.unwrap();
    last[0].1.term
}

/// Wait until `f` holds (a background removal finished).
async fn eventually(what: &str, f: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn commit_and_reject_both_consume_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    assert_eq!(store.generation(), 1);
    let booted = store.applied_position();
    assert!(booted > 0, "Raft's own entries (membership, the leader's blank) come first");
    assert_eq!(store.propose_at::<Counter>(op(1, 5)).await.unwrap(), (booted + 1, Ok(5)));
    assert_eq!(store.propose_at::<Counter>(op(2, -7)).await.unwrap(), (booted + 2, Err("negative".into())));
    assert_eq!(value(&store), 5, "rejected command must not change state");
    assert_eq!(op_ids(&store), vec![1]);
    assert_eq!(store.applied_position(), booted + 2);
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
    let booted = store.applied_position();
    assert!(store.propose::<Other>(()).await.is_err());
    assert_eq!(store.applied_position(), booted);

    let twice = Namespaces::new().with::<Counter>().with::<Counter>();
    assert!(open_dir(&dir.path().join("log2"), twice).await.is_err());
}

/// A second namespace: names seen, in order.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Names(Vec<String>);

impl StateMachine for Names {
    const NAME: &'static str = "names";
    const VERSION: u32 = 1;
    type Command = String;
    type Reply = usize;
    type Reject = Never;

    fn apply(&mut self, name: String) -> Result<Outcome<Self>> {
        self.0.push(name);
        Ok(Ok(self.0.len()))
    }
}

/// A snapshot without a namespace (one added since it was taken) leaves
/// that namespace empty, whatever it held before the restore.
#[test]
fn a_restore_empties_a_namespace_the_snapshot_lacks() {
    let states = collect_states(counter().with::<Names>()).unwrap();
    apply_command(&states, 1, &command::<Counter>(&op(1, 5))).unwrap();
    let snap = take_snapshot(&states, 1);
    apply_command(&states, 2, &command::<Names>(&"a".to_string())).unwrap();
    apply_command(&states, 3, &command::<Counter>(&op(2, 1))).unwrap();
    assert_eq!(snap.states.iter().map(|s| s.0.as_str()).collect::<Vec<_>>(), ["counter", "names"]);
    let without_names = TabletSnapshot { states: snap.states.into_iter().filter(|s| s.0 != "names").collect(), ..snap };

    restore_states(&states, &without_names).unwrap();
    let names = states["names"].as_any().downcast_ref::<StateCell<Names>>().unwrap().0.read().unwrap().0.clone();
    let counter = states["counter"].as_any().downcast_ref::<StateCell<Counter>>().unwrap().0.read().unwrap().v;
    assert_eq!((names, counter), (Vec::<String>::new(), 5));
}

#[tokio::test]
async fn propose_all_commits_commands_of_several_namespaces_as_one_entry() {
    let dir = tempfile::tempdir().unwrap();
    let ns = Namespaces::new().with::<Counter>().with::<Names>();
    let store = open_dir(&dir.path().join("log"), ns).await.unwrap();
    let booted = store.applied_position();

    let mut p = Proposal::new();
    let ups = p.extend([op(1, 5), op(2, -9), op(3, 1)].iter().map(Prepared::<Counter>::new));
    let named = p.push(Prepared::<Names>::new(&"a".to_string()));
    let (position, mut outcomes) = store.propose_all_at(p).await.unwrap();
    assert_eq!(position, booted + 1, "one entry for all four");
    assert_eq!(outcomes.take(ups[0]).unwrap(), Ok(5));
    assert_eq!(outcomes.take(ups[1]).unwrap(), Err("negative".into()), "rejected on its own");
    assert_eq!(outcomes.take(ups[2]).unwrap(), Ok(6), "applied after the rejection, in order");
    assert_eq!(outcomes.take(named).unwrap(), Ok(1));
    assert!(outcomes.take(named).is_err(), "an outcome is taken once");
    assert_eq!(op_ids(&store), vec![1, 3]);

    // Durable together: a new incarnation replays all of them.
    drop(store);
    let ns = Namespaces::new().with::<Counter>().with::<Names>();
    let again = open_dir(&dir.path().join("log"), ns).await.unwrap();
    assert_eq!(value(&again), 6);
    assert_eq!(again.read::<Names, _>(|n| n.0.clone()), vec!["a".to_string()]);

    // A fenced tablet fails the whole proposal.
    let ns = Namespaces::new().with::<Counter>().with::<Names>();
    let newer = open_dir(&dir.path().join("log"), ns).await.unwrap();
    let mut p = Proposal::new();
    p.push(Prepared::<Counter>::new(&op(4, 1)));
    p.push(Prepared::<Names>::new(&"b".to_string()));
    assert!(again.propose_all(p).await.is_err());
    assert_eq!(value(&newer), 6);
}

#[tokio::test]
async fn a_proposal_larger_than_a_batch_stays_one_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    let booted = store.applied_position();
    let mut p = Proposal::new();
    p.extend((0..MAX_BATCH as u64 + 10).map(|i| Prepared::<Counter>::new(&op(i, 1))));
    let small = store.propose_at::<Counter>(op(u64::MAX, 1));
    let (big, small) = tokio::join!(store.propose_all_at(p), small);
    let (big, small) = (big.unwrap().0, small.unwrap().0);
    assert!(big == booted + 1 || big == booted + 2, "{big}");
    assert_ne!(big, small);
    assert_eq!(value(&store), MAX_BATCH as i64 + 11);
}

#[tokio::test]
async fn concurrent_proposals_get_distinct_positions() {
    let dir = tempfile::tempdir().unwrap();
    let (log, snapshots) = local(dir.path());
    let opts = MetaOptions { snapshot_every: Some(5), ..MetaOptions::default() };
    let store = MetaStore::open(log, snapshots, counter(), opts).await.unwrap();
    let booted = store.applied_position();
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
    // Concurrent proposals share entries (group commit); positions only grow.
    positions.sort_unstable();
    assert!(positions[0] > booted && positions[31] <= booted + 32, "{positions:?}");
    assert!(positions.windows(2).filter(|w| w[0] != w[1]).count() < 31, "some proposals were batched: {positions:?}");
    assert_eq!(value(&store), 32);
}

#[tokio::test]
async fn replay_yields_identical_state() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path()).await;
    let booted = a.applied_position();
    for i in 0..300u64 {
        // Every fifth op would go negative and is rejected.
        let delta = if i % 5 == 4 { -1000 } else { i as i64 % 7 };
        let _ = a.propose::<Counter>(op(i, delta)).await.unwrap();
    }
    assert_eq!(a.applied_position(), booted + 300);
    let snap = snapshot(&a);
    assert_eq!(encode_state(&decode_state::<Counter>(&snap).unwrap()), snap, "decode inverts encode");
    drop(a);

    let b = open_on(dir.path()).await;
    assert_eq!(b.generation(), 2);
    assert!(b.applied_position() >= booted + 300, "everything the log holds is applied");
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
                        return (acked, id, stop);
                    }
                }
            }
            unreachable!()
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b = open_on(dir.path()).await;
    let (acked, failed, stop) = writer.await.unwrap();

    assert_eq!(stop, Stop::Fenced { current: 2 });
    assert_eq!(a.stopped().await, Stop::Fenced { current: 2 });
    assert!(a.propose::<Counter>(op(u64::MAX >> 1, 1)).await.is_err());
    assert!(!acked.is_empty(), "A acknowledged something before the fence");

    // B holds every entry A acknowledged; at most the one that failed is
    // there too (written, then fenced before its answer: outcome unknown).
    let held = op_ids(&b);
    assert_eq!(&held[..acked.len()], &acked[..]);
    assert!(held.len() <= acked.len() + 1 && held.iter().all(|&id| id <= failed), "{held:?} vs {acked:?}");

    b.propose::<Counter>(op(1 << 40, 1)).await.unwrap().unwrap();
}

/// The last handle to a tablet dropped after its runtime ended (a test's or
/// a tool's) has nothing to shut down on, and does not panic.
#[test]
fn a_tablet_dropped_after_its_runtime_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let store = runtime.block_on(open_on(dir.path()));
    drop(runtime);
    drop(store);
}

#[tokio::test]
async fn crash_between_append_and_apply_is_applied_once_on_boot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path()).await;
        a.propose::<Counter>(op(1, 1)).await.unwrap().unwrap();
    }
    // An incarnation that appended an entry and died before committing it.
    let log = log_of(&dir.path().join("log"));
    let head = log.claim(2).await.unwrap();
    let term = head_term(log.as_ref(), head).await;
    let next = head.position + 1;
    assert_eq!(log.append(2, &[raw_entry(term, next, command::<Counter>(&op(2, 5)))]).await.unwrap(), next);

    for expected_gen in [3, 4] {
        let a = open_on(dir.path()).await;
        assert_eq!(a.generation(), expected_gen);
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
    let log = log_of(&dir.path().join("log"));
    let head = log.claim(2).await.unwrap();
    let term = head_term(log.as_ref(), head).await;
    let mut old = command::<Counter>(&op(2, 1));
    old.version = Counter::VERSION + 1;
    log.append(2, &[raw_entry(term, head.position + 1, old)]).await.unwrap();

    let err = open_dir(&dir.path().join("log"), counter()).await.err().expect("boot must fail");
    assert!(format!("{err:#}").contains("counter command in format 2, this binary reads 1"), "{err:#}");
}

fn segments(dir: &Path) -> Vec<String> {
    let dir = &dir.join("log");
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    names.sort();
    names
}

async fn open_snapshotting(dir: &Path) -> Arc<MetaStore> {
    let log = crate::ChainLogStore::new(Arc::new(DiskBlobStore::with_segment_bytes(dir.join("log"), 64).unwrap()));
    let opts = MetaOptions { snapshot_every: Some(10), ..MetaOptions::default() };
    MetaStore::open(Arc::new(log), snaps_of(dir), counter(), opts).await.unwrap()
}

#[tokio::test]
async fn snapshots_collect_the_log_and_boot_from_them() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let a = open_snapshotting(&log_dir).await;
    for i in 0..35u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    wait_base(&a, 20).await;
    let (state, applied) = (snapshot(&a), a.applied_position());
    drop(a);

    let d = log_of(&log_dir).discover().await.unwrap().unwrap();
    let base = d.base.expect("the log starts from a snapshot");
    let stored = snaps_of(&log_dir).get(&base.snapshot).await.unwrap();
    assert_eq!(raft::RaftSnapshot::decode(&stored.data).unwrap().tablet.position, base.at);
    snapshots_down_to(&log_dir, 2, "older snapshots removed").await;
    let segs = segments(&log_dir);
    assert!(segs[0].as_str() > "seg-0000000001", "log behind the base collected: {segs:?}");

    let b = open_snapshotting(&log_dir).await;
    assert!(b.applied_position() >= applied, "everything the log holds is applied");
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
    let log = log_of(&log_dir);
    let head = log.claim(2).await.unwrap();
    let meta = raft::solo_meta(head.position - 1);
    let tablet = TabletSnapshot {
        format: SNAPSHOT_FORMAT,
        position: head.position,
        states: vec![("counter".into(), Counter::VERSION + 1, encode_state(&Counter::default()))],
    };
    let data = raft::RaftSnapshot { meta: meta.clone(), tablet }.encode();
    let stored = StoredSnapshot { log_id: head.log_id, at: head.position, data };
    let snapshot = snaps_of(&log_dir).put(&stored, None).await.unwrap();
    log.rebase(2, raft::base_of(&meta, snapshot), 1, &[]).await.unwrap();
    drop(log);

    let err = open_dir(&log_dir, counter()).await.err().expect("boot must fail");
    assert!(format!("{err:#}").contains("counter state in format 2, this binary reads 1"), "{err:#}");
}

#[tokio::test]
async fn a_lost_log_boots_only_through_a_lossy_restore() {
    let dir = tempfile::tempdir().unwrap();
    let (log_dir, snap_dir) = (dir.path().join("log"), dir.path().join("snapshots"));
    let snapshots: Arc<dyn SnapshotStore> = snaps_of(&snap_dir);
    let open = |snapshots: Arc<dyn SnapshotStore>| {
        let log = log_of(&log_dir);
        let opts = MetaOptions { snapshot_every: Some(10), snapshots_outlive_log: true, ..MetaOptions::default() };
        MetaStore::open(log, snapshots, counter(), opts)
    };
    let a = open(snapshots.clone()).await.unwrap();
    assert_eq!(a.restorable_position(), Some(0));
    for i in 0..45u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    wait_base(&a, 30).await;
    let restorable = a.restorable_position().unwrap();
    let kept = a.read::<Counter, _>(|c| c.v);
    drop(a);

    // The disk with the log is lost; the snapshots survive.
    std::fs::remove_dir_all(&log_dir).unwrap();
    let err = open(snapshots.clone()).await.err().expect("an empty log next to snapshots must not boot");
    assert!(format!("{err:#}").contains("restore --lossy"), "{err:#}");

    let log = log_of(&log_dir);
    let restored_at = restore_lossy(log.as_ref(), snapshots.as_ref()).await.unwrap();
    assert!(restored_at >= restorable, "the newest snapshot: {restored_at} < {restorable}");
    assert!(restore_lossy(log.as_ref(), snapshots.as_ref()).await.is_err(), "only an empty log is seeded");

    let b = open(snapshots.clone()).await.unwrap();
    let restored = value(&b);
    assert!(restored <= kept && restored >= 20, "state as of the snapshot: {restored} of {kept}");
    b.propose::<Counter>(op(1000, 1)).await.unwrap().unwrap();
    drop(b);
    assert_eq!(value(&open(snapshots).await.unwrap()), restored + 1);
}

#[tokio::test]
async fn confirm_active_detects_a_fence_without_any_proposal() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path()).await;
    let booted = a.applied_position();
    a.confirm_active().await.unwrap();
    assert_eq!(a.applied_position(), booted, "a confirmation takes no position");

    let _b = open_on(dir.path()).await;
    assert!(!a.is_stopped(), "A has not touched the log since B claimed it");
    let err = a.confirm_active().await.unwrap_err();
    assert_eq!(err.downcast::<Stop>().unwrap(), Stop::Fenced { current: 2 });
    assert!(a.is_stopped());
}

#[tokio::test]
async fn the_tablet_runs_over_a_chain_log_with_snapshots_in_another_store() {
    use crate::{BlobSnapshotStore, ChainLogStore};
    let (log_chat, snap_chat) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
    let open = || {
        let log = Arc::new(ChainLogStore::new(log_chat.clone()));
        let snapshots = Arc::new(BlobSnapshotStore::new(snap_chat.clone()));
        MetaStore::open(log, snapshots, counter(), MetaOptions { snapshot_every: Some(7), ..MetaOptions::default() })
    };
    let a = open().await.unwrap();
    for i in 0..20u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    wait_base(&a, 14).await;
    let state = snapshot(&a);
    eventually("older snapshots removed", || snap_chat.message_keys().len() <= 2).await;

    let b = open().await.unwrap();
    assert_eq!(snapshot(&b), state, "snapshot + tail rebuild the state");
    let err = a.propose::<Counter>(op(100, 1)).await.unwrap_err();
    assert_eq!(err.downcast::<Stop>().unwrap(), Stop::Fenced { current: 2 });
    b.propose::<Counter>(op(100, 1)).await.unwrap().unwrap();
    assert_eq!(value(&b), 21);
    let batches = log_chat
        .message_keys()
        .into_iter()
        .filter(|k| log_chat.name_of(*k).is_some_and(|n| n.starts_with("log-batch-")))
        .count();
    assert!(batches < 15, "batches behind the base are collected: {batches} left");
}

/// A snapshot store whose `get` fails for one chosen snapshot (a damaged
/// or lost copy).
struct Unreadable {
    inner: Arc<dyn SnapshotStore>,
    bad: Mutex<Option<SnapshotRef>>,
}

#[async_trait::async_trait]
impl SnapshotStore for Unreadable {
    async fn put(&self, s: &StoredSnapshot, after: Option<&SnapshotRef>) -> Result<SnapshotRef> {
        self.inner.put(s, after).await
    }
    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        if self.bad.lock().unwrap().as_ref() == Some(r) {
            bail!("snapshot {:?} fails its checksum", r.0);
        }
        self.inner.get(r).await
    }
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        self.inner.newest().await
    }
    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        self.inner.remove(r).await
    }
}

/// The newest snapshot cannot be read: a boot starts from the older base the
/// log keeps and replays from there; with one base kept it cannot boot.
#[tokio::test]
async fn a_boot_falls_back_to_an_older_base_when_the_newest_snapshot_is_unreadable() {
    use crate::{BlobSnapshotStore, ChainLogStore};
    for keep in [2, 1] {
        let (log_chat, snap_chat) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
        let snapshots =
            Arc::new(Unreadable { inner: Arc::new(BlobSnapshotStore::new(snap_chat)), bad: Mutex::default() });
        let opts = MetaOptions { snapshot_every: Some(5), keep_snapshots: keep, ..MetaOptions::default() };
        let open = || {
            MetaStore::open(Arc::new(ChainLogStore::new(log_chat.clone())), snapshots.clone(), counter(), opts.clone())
        };
        let a = open().await.unwrap();
        for i in 0..23u64 {
            a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
        }
        wait_base(&a, 15).await;
        let state = snapshot(&a);
        let d = a.log.discover().await.unwrap().unwrap();
        assert_eq!(d.older.len(), keep - 1, "keep {keep}: {d:?}");
        *snapshots.bad.lock().unwrap() = d.base.map(|b| b.snapshot);
        drop(a);

        match (keep, open().await) {
            (2, Ok(b)) => {
                assert_eq!(snapshot(&b), state, "an older snapshot + a longer replay rebuild the state");
                b.propose::<Counter>(op(100, 1)).await.unwrap().unwrap();
                assert_eq!(value(&b), 24);
            }
            (1, Err(e)) => assert!(format!("{e:#}").contains("fails its checksum"), "{e:#}"),
            (_, other) => panic!("keep {keep}: {:?}", other.map(|_| ())),
        }
    }
}

/// After a fallback boot the next rebase drops the damaged base, not the
/// one that loaded: with two kept, the log keeps the new base and the one
/// the boot started from, and the damaged snapshot is removed.
#[tokio::test]
async fn after_a_fallback_boot_the_base_that_loaded_is_kept_and_the_damaged_one_dropped() {
    use crate::{BlobSnapshotStore, ChainLogStore};
    let (log_chat, snap_chat) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
    let snapshots = Arc::new(Unreadable { inner: Arc::new(BlobSnapshotStore::new(snap_chat)), bad: Mutex::default() });
    let opts = MetaOptions { snapshot_every: Some(5), keep_snapshots: 2, ..MetaOptions::default() };
    let open =
        || MetaStore::open(Arc::new(ChainLogStore::new(log_chat.clone())), snapshots.clone(), counter(), opts.clone());
    let a = open().await.unwrap();
    for i in 0..23u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    wait_base(&a, 15).await;
    let d = a.log.discover().await.unwrap().unwrap();
    let (damaged, good) = (d.base.clone().unwrap(), d.older[0].clone());
    *snapshots.bad.lock().unwrap() = Some(damaged.snapshot.clone());
    drop(a);

    let b = open().await.unwrap();
    // It booted from the older base and asked for a snapshot at once: the
    // log moves onto it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let d = loop {
        let d = b.log.discover().await.unwrap().unwrap();
        if d.base.as_ref().is_some_and(|b| b.snapshot != damaged.snapshot) {
            break d;
        }
        assert!(tokio::time::Instant::now() < deadline, "the log never moves off the damaged base");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(d.older, vec![good.clone()], "kept: the new base and the one that loaded");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while snapshots.inner.get(&damaged.snapshot).await.is_ok() {
        assert!(tokio::time::Instant::now() < deadline, "the damaged snapshot is never removed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    snapshots.inner.get(&good.snapshot).await.expect("the base that loaded stays");
    drop(b);

    // The new base damaged too: the next boot still has one to fall back on.
    *snapshots.bad.lock().unwrap() = d.base.map(|b| b.snapshot);
    let c = open().await.unwrap();
    assert_eq!(value(&c), 23);
}

/// A follower starts building a snapshot of its own (at 4), then installs a
/// newer one from its leader (at 8) before the build is stored: the build
/// comes too late to be current, and the leader's stays stored.
#[tokio::test]
async fn a_build_stored_after_an_install_of_a_newer_snapshot_stays_out() {
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
    let snapshots: Arc<dyn SnapshotStore> = Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())));
    let (stop, _) = watch::channel(None);
    let shared = Arc::new(raft::Shared::new(stop, snapshots.clone()));
    // Raft's index 3 is position 4, 7 is 8.
    let (at4, at8) = (raft::solo_meta(3), raft::solo_meta(7));
    let mut sm = raft::StateMachine {
        states: Arc::new(collect_states(counter()).unwrap()),
        snapshots: snapshots.clone(),
        history: crate::log::LogId(1),
        last_applied: at4.last_log_id,
        membership: at4.last_membership.clone(),
        shared: shared.clone(),
    };
    let mut build = sm.get_snapshot_builder().await;

    let leaders = raft::RaftSnapshot { meta: at8.clone(), tablet: take_snapshot(&sm.states, 8) };
    sm.install_snapshot(&at8, Box::new(std::io::Cursor::new(leaders.encode()))).await.unwrap();
    let built = build.build_snapshot().await.unwrap();

    assert_eq!(built.meta.last_log_id, at8.last_log_id, "the build answers with the newer one");
    let current = shared.current.lock().unwrap().clone().unwrap();
    assert_eq!(current.meta.last_log_id, at8.last_log_id, "the leader's is current");
    let stored = snapshots.get(&current.snapshot).await.unwrap();
    assert_eq!(stored.at, 8, "the leader's snapshot is still stored");
}

/// A snapshot store whose `put` takes two minutes (a snapshot in many parts
/// on a chat).
struct SlowPut(Arc<dyn SnapshotStore>);

#[async_trait::async_trait]
impl SnapshotStore for SlowPut {
    async fn put(&self, s: &StoredSnapshot, after: Option<&SnapshotRef>) -> Result<SnapshotRef> {
        tokio::time::sleep(Duration::from_secs(120)).await;
        self.0.put(s, after).await
    }
    async fn get(&self, r: &SnapshotRef) -> Result<StoredSnapshot> {
        self.0.get(r).await
    }
    async fn newest(&self) -> Result<Option<StoredSnapshot>> {
        self.0.newest().await
    }
    async fn remove(&self, r: &SnapshotRef) -> Result<()> {
        self.0.remove(r).await
    }
}

/// A follower installs its leader's snapshot (at 8, past its empty log)
/// into a slow store, and Raft asks to purge behind it meanwhile: the purge
/// returns at once, the log is rebased once the snapshot is stored however
/// long that takes, and a write queued behind the purge waits for it.
#[tokio::test(start_paused = true)]
async fn a_purge_waits_for_a_slow_snapshot_store_without_a_deadline() {
    use openraft::storage::{RaftLogStorage, RaftStateMachine};
    let log = Arc::new(crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new())));
    let head = log.claim(1).await.unwrap();
    let snapshots: Arc<dyn SnapshotStore> =
        Arc::new(SlowPut(Arc::new(crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new())))));
    let (stop, stopped) = watch::channel(None);
    let shared = Arc::new(raft::Shared::new(stop, snapshots.clone()));
    let mut adapter = raft::LogAdapter::new(log.clone(), 1, head.log_id, 0, shared.clone(), raft::Keep(2));
    let mut sm = raft::StateMachine {
        states: Arc::new(collect_states(counter()).unwrap()),
        snapshots,
        history: head.log_id,
        last_applied: None,
        membership: Default::default(),
        shared: shared.clone(),
    };
    let at8 = raft::solo_meta(7);
    let data = raft::RaftSnapshot { meta: at8.clone(), tablet: take_snapshot(&sm.states, 8) }.encode();
    let t0 = tokio::time::Instant::now();
    let install = tokio::spawn(async move {
        sm.install_snapshot(&at8, Box::new(std::io::Cursor::new(data))).await.map(|_| sm.last_applied)
    });
    tokio::task::yield_now().await;

    adapter.purge(at8_id()).await.unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1), "the purge does not wait for the store");
    let vote = openraft::Vote::new(3, raft::SOLO);
    let mut voter = adapter.clone();
    let voted = tokio::spawn(async move { voter.save_vote(&vote).await });
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!((shared.based.load(Ordering::SeqCst), voted.is_finished()), (0, false), "nothing before it is stored");

    assert!(install.await.unwrap().is_ok());
    voted.await.unwrap().unwrap();
    assert!(t0.elapsed() >= Duration::from_secs(120));
    assert_eq!(shared.based.load(Ordering::SeqCst), 8);
    let d = log.discover().await.unwrap().unwrap();
    assert_eq!((d.base.map(|b| b.at), d.head.position, d.vote.is_some()), (Some(8), 8, true));
    assert!(stopped.borrow().is_none(), "the tablet goes on");
}

/// Raft's id of the entry at position 8 in a cluster of one (term 1).
fn at8_id() -> raft::RaftLogId {
    raft::solo_meta(7).last_log_id.unwrap()
}

#[tokio::test]
async fn a_local_log_restores_from_snapshots_in_a_blob_store() {
    let dir = tempfile::tempdir().unwrap();
    let chat = Arc::new(MemoryBlobStore::new());
    let snapshots: Arc<dyn SnapshotStore> = Arc::new(crate::BlobSnapshotStore::new(chat.clone()));
    let opts = MetaOptions { snapshot_every: Some(10), snapshots_outlive_log: true, ..MetaOptions::default() };
    let a = MetaStore::open(log_of(dir.path()), snapshots.clone(), counter(), opts).await.unwrap();
    for i in 0..35u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    wait_base(&a, 20).await;
    drop(a);

    let fresh = tempfile::tempdir().unwrap();
    let log = log_of(fresh.path());
    restore_lossy(log.as_ref(), snapshots.as_ref()).await.unwrap();
    let b = MetaStore::open(log, snapshots, counter(), MetaOptions::default()).await.unwrap();
    assert!(value(&b) >= 17, "the newest snapshot's state, got {}", value(&b));
}

#[tokio::test]
async fn an_idle_tablet_snapshots_by_time() {
    let dir = tempfile::tempdir().unwrap();
    let opts = MetaOptions {
        snapshot_every: None,
        snapshot_interval: Some(Duration::from_millis(50)),
        keep_snapshots: 1,
        ..MetaOptions::default()
    };
    let (log, snapshots) = local(dir.path());
    let a = MetaStore::open(log.clone(), snapshots, counter(), opts).await.unwrap();
    for i in 0..3u64 {
        a.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
    }
    // No more traffic: the interval alone takes the snapshot.
    let applied = a.applied_position();
    wait_base(&a, applied).await;
    let d = log.discover().await.unwrap().unwrap();
    assert_eq!(d.base.map(|b| b.at), Some(applied));
    // Nothing new: no more snapshots (an older one may still be on its way out).
    snapshots_down_to(dir.path(), 1, "only the base snapshot is kept").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(snapshot_count(dir.path()).await, 1);
}

/// Latency of the tablet over a disk and a memory log (`--ignored --nocapture`).
#[tokio::test]
#[ignore]
async fn timing_probe() {
    for kind in ["disk", "memory"] {
        let dir = tempfile::tempdir().unwrap();
        let (log, snapshots): (Arc<dyn LogStore>, Arc<dyn SnapshotStore>) = match kind {
            "disk" => local(dir.path()),
            _ => {
                use pigeonhole_storage_memory::MemoryBlobStore;
                let log = crate::ChainLogStore::new(Arc::new(MemoryBlobStore::new()));
                let snapshots = crate::BlobSnapshotStore::new(Arc::new(MemoryBlobStore::new()));
                (Arc::new(log), Arc::new(snapshots))
            }
        };
        let store = MetaStore::open(log, snapshots, counter(), MetaOptions::default()).await.unwrap();
        let t = std::time::Instant::now();
        for i in 0..200u64 {
            store.propose::<Counter>(op(i, 1)).await.unwrap().unwrap();
        }
        let seq = t.elapsed();
        let t = std::time::Instant::now();
        let tasks: Vec<_> = (1000..1200u64)
            .map(|i| {
                let s = store.clone();
                tokio::spawn(async move { s.propose::<Counter>(op(i, 1)).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap().unwrap();
        }
        let par = t.elapsed();
        let t = std::time::Instant::now();
        for _ in 0..50 {
            store.confirm_active().await.unwrap();
        }
        let conf = t.elapsed();
        println!(
            "{kind}: 200 sequential {seq:?} ({:?}/op), 200 concurrent {par:?}, 50 confirm_active {conf:?}",
            seq / 200
        );
    }
}

/// Proposals queued together share entries; every one is counted in and out
/// of the queue and its wait recorded.
#[test]
#[cfg(not(madsim))] // builds a runtime of its own (a thread-local recorder)
fn batcher_metrics_add_up() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    const N: usize = 50;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        // One thread: the batcher and Raft run where the recorder is set.
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = open_on(dir.path()).await;
            let all = (0..N as u64).map(|i| store.propose::<Counter>(op(i, 1)));
            for r in futures::future::join_all(all).await {
                assert!(r.unwrap().is_ok());
            }
        });
    });
    let snapshot = snapshotter.snapshot().into_vec();
    let value = |name: &str| snapshot.iter().find(|(k, ..)| k.key().name() == name).map(|(.., v)| v);
    // A histogram's (count, sum).
    let hist = |name: &str| match value(name) {
        Some(DebugValue::Histogram(h)) => (h.len(), h.iter().map(|x| x.into_inner()).sum::<f64>()),
        other => panic!("{name}: {other:?}"),
    };
    match value("pigeonhole_meta_queue_depth") {
        Some(DebugValue::Gauge(g)) => assert_eq!(g.into_inner(), 0.0, "every proposal left the queue"),
        other => panic!("queue depth: {other:?}"),
    }
    assert_eq!(hist("pigeonhole_meta_queue_wait_seconds").0, N);
    assert_eq!(hist("pigeonhole_meta_propose_seconds").0, N);
    let (entries, proposals) = hist("pigeonhole_meta_proposals_per_entry");
    assert_eq!(proposals as usize, N);
    assert!(entries < N, "{N} proposals queued at once share entries, got {entries}");
}

mod cluster;

/// A namespace with a check: `false` is refused, by `check` and by `apply`.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Strict(u64);

impl StateMachine for Strict {
    const NAME: &'static str = "strict";
    const VERSION: u32 = 1;
    type Command = bool;
    type Reply = u64;
    type Reject = String;

    fn apply(&mut self, ok: bool) -> Result<Outcome<Self>> {
        if !ok {
            return Ok(Err("refused".into()));
        }
        self.0 += 1;
        Ok(Ok(self.0))
    }

    fn check(&self, ok: &bool) -> Option<String> {
        (!ok).then(|| "refused".into())
    }
}

async fn open_strict(dir: &Path) -> Arc<MetaStore> {
    open_dir(&dir.join("log"), Namespaces::new().with::<Counter>().with::<Strict>()).await.unwrap()
}

#[tokio::test]
async fn a_proposal_fenced_to_another_term_is_committed_but_not_applied() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_strict(dir.path()).await;
    let term = store.serving_term().unwrap();
    let booted = store.applied_position();

    let mut p = Proposal::new();
    let fenced = p.push(Prepared::<Counter>::new(&op(1, 5)).fenced(term + 1));
    let along = p.push(Prepared::<Counter>::new(&op(2, 1)));
    let (position, mut outcomes) = store.propose_all_at(p).await.unwrap();
    assert_eq!(position, booted + 1, "the entry takes its position");
    for slot in [fenced, along] {
        let err = outcomes.take(slot).unwrap_err();
        let not = err.downcast_ref::<NotApplied>();
        assert_eq!(not, Some(&NotApplied::Fenced { fence: term + 1, term }), "{err:#}");
        assert!(unavailable(&err), "a client asks again");
    }
    assert_eq!(value(&store), 0, "none of the proposal applied");

    let mut p = Proposal::new();
    let up = p.push(Prepared::<Counter>::new(&op(3, 5)).fenced(term));
    assert_eq!(store.propose_all(p).await.unwrap().take(up).unwrap(), Ok(5), "its own term applies");

    // Replay reaches the same verdicts.
    drop(store);
    let again = open_strict(dir.path()).await;
    assert_eq!(op_ids(&again), vec![3]);
}

#[tokio::test]
async fn a_command_failing_its_check_applies_nothing_of_its_proposal() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_strict(dir.path()).await;

    let mut p = Proposal::new();
    let before = p.push(Prepared::<Counter>::new(&op(1, 5)));
    let refused = p.push(Prepared::<Strict>::new(&false));
    let after = p.push(Prepared::<Counter>::new(&op(2, 1)));
    let mut outcomes = store.propose_all(p).await.unwrap();
    assert_eq!(outcomes.take(refused).unwrap(), Err("refused".into()));
    for slot in [before, after] {
        let err = outcomes.take(slot).unwrap_err();
        assert_eq!(err.downcast_ref::<NotApplied>(), Some(&NotApplied::Skipped), "{err:#}");
        assert!(!unavailable(&err), "asking again would not help");
    }
    assert_eq!(value(&store), 0);
    assert_eq!(store.read::<Strict, _>(|s| s.0), 0);

    // Alone, the command is rejected by apply, as any.
    assert_eq!(store.propose::<Strict>(false).await.unwrap(), Err("refused".into()));
    assert_eq!(store.propose::<Strict>(true).await.unwrap(), Ok(1));
}

/// Raft's report of node 1 leading in `term` with `applied` applied (its
/// term, and its index) among `voters`.
fn leading(term: u64, applied: Option<u64>, voters: &[u64]) -> raft::Metrics {
    let mut m = raft::Metrics::new_initial(1);
    m.state = openraft::ServerState::Leader;
    m.current_term = term;
    m.current_leader = Some(1);
    m.last_applied = applied.map(|t| openraft::LogId::new(openraft::CommittedLeaderId::new(t, 1), 7));
    let membership = openraft::Membership::new(vec![voters.iter().copied().collect()], None);
    m.membership_config = Arc::new(openraft::StoredMembership::new(None, membership));
    m
}

#[test]
fn a_leader_serves_once_it_applied_an_entry_of_its_own_term() {
    assert!(!serving(&leading(3, None, &[1, 2, 3])), "nothing applied yet");
    assert!(!serving(&leading(3, Some(2), &[1, 2, 3])), "its predecessor's entries only: maybe not all of them");
    assert!(serving(&leading(3, Some(3), &[1, 2, 3])), "its blank entry, and so all before it");
    let mut follower = leading(3, Some(3), &[1, 2, 3]);
    follower.state = openraft::ServerState::Follower;
    assert!(!serving(&follower));
}

#[test]
fn a_leader_no_quorum_acked_yet_counts_from_its_election() {
    let lease = Duration::from_secs(3);
    let t0 = tokio::time::Instant::now();
    let mut clock = LeadClock::default();
    let three = |term, applied| leading(term, Some(applied), &[1, 2, 3]);
    assert_eq!(clock.acked_at(&three(4, 4), t0), Some(t0), "elected: its lease starts");
    let later = t0 + lease * 2;
    assert_eq!(clock.acked_at(&three(4, 4), later), Some(t0), "no ack known: still its election");
    // Read-only, as spawn_quorum_watch tells: cut off since its election.
    assert!(later - t0 > lease);

    let mut acked = three(4, 4);
    acked.millis_since_quorum_ack = Some(10);
    assert_eq!(clock.acked_at(&acked, later), Some(later - Duration::from_millis(10)), "an ack known: from it");

    let next = later + lease;
    assert_eq!(clock.acked_at(&three(5, 4), next), Some(next), "a new term starts its own clock");
    assert_eq!(clock.acked_at(&leading(5, Some(5), &[1]), next), None, "alone: always a quorum");
    let mut follower = three(5, 5);
    follower.state = openraft::ServerState::Follower;
    assert_eq!(clock.acked_at(&follower, next), None);
}
