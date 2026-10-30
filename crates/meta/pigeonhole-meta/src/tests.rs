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
    pigeonhole_storage_api::BlobBackend::candidates(&store, None, u64::MAX, 1000).await.unwrap().len()
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
    let entry = raft::RaftEntry { log_id: id, payload: openraft::EntryPayload::Normal(vec![cmd]) };
    Entry { term, payload: postcard::to_stdvec(&entry).unwrap() }
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
    let term = log.discover().await.unwrap().unwrap().head_term;
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
    let term = log.discover().await.unwrap().unwrap().head_term;
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
    assert!(segs[0] > "seg-0000000001".to_string(), "log behind the base collected: {segs:?}");

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
    let snapshot = snaps_of(&log_dir).put(&stored).await.unwrap();
    log.rebase(2, raft::base_of(&meta, snapshot), 1).await.unwrap();
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
    assert_eq!(value(&*open(snapshots).await.unwrap()), restored + 1);
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
    async fn put(&self, s: &StoredSnapshot) -> Result<SnapshotRef> {
        self.inner.put(s).await
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
        let open = || MetaStore::open(Arc::new(ChainLogStore::new(log_chat.clone())), snapshots.clone(), counter(), opts.clone());
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

#[tokio::test]
async fn a_local_log_restores_from_snapshots_in_a_blob_store() {
    let dir = tempfile::tempdir().unwrap();
    let chat = Arc::new(MemoryBlobStore::new());
    let snapshots: Arc<dyn SnapshotStore> = Arc::new(crate::BlobSnapshotStore::new(chat.clone()));
    let opts = MetaOptions { snapshot_every: Some(10), snapshots_outlive_log: true, ..MetaOptions::default() };
    let a = MetaStore::open(log_of(dir.path()), snapshots.clone(), counter(), opts)
        .await
        .unwrap();
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
        let tasks: Vec<_> = (1000..1200u64).map(|i| { let s = store.clone(); tokio::spawn(async move { s.propose::<Counter>(op(i, 1)).await }) }).collect();
        for t in tasks { t.await.unwrap().unwrap().unwrap(); }
        let par = t.elapsed();
        let t = std::time::Instant::now();
        for _ in 0..50 { store.confirm_active().await.unwrap(); }
        let conf = t.elapsed();
        println!("{kind}: 200 sequential {seq:?} ({:?}/op), 200 concurrent {par:?}, 50 confirm_active {conf:?}", seq / 200);
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
