use super::*;
use std::collections::BTreeMap;
use std::path::Path;

/// Toy namespace. Command = op id (u64) + signed delta (i64) for a counter;
/// every committed op is recorded in `counter_ops`; going negative is rejected.
struct Counter;

#[async_trait]
impl Namespace for Counter {
    fn name(&self) -> &'static str {
        "counter"
    }

    async fn init(&self, conn: &mut SqliteConnection) -> Result<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS counter (v INTEGER NOT NULL)")
            .execute(&mut *conn)
            .await?;
        sqlx::query("INSERT INTO counter SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counter)")
            .execute(&mut *conn)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS counter_ops (id INTEGER PRIMARY KEY, delta INTEGER NOT NULL)")
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    async fn apply(&self, conn: &mut SqliteConnection, command: &[u8]) -> Result<Applied> {
        let id = u64::from_le_bytes(command[..8].try_into().context("op id")?);
        let delta = i64::from_le_bytes(command[8..].try_into().context("8-byte delta")?);
        sqlx::query("INSERT INTO counter_ops (id, delta) VALUES (?, ?)")
            .bind(id as i64)
            .bind(delta)
            .execute(&mut *conn)
            .await?;
        sqlx::query("UPDATE counter SET v = v + ?")
            .bind(delta)
            .execute(&mut *conn)
            .await?;
        let (v,): (i64,) = sqlx::query_as("SELECT v FROM counter")
            .fetch_one(&mut *conn)
            .await?;
        if v < 0 {
            return Ok(Applied::Reject(b"negative".to_vec()));
        }
        Ok(Applied::Commit(v.to_le_bytes().to_vec()))
    }
}

fn sql_counter() -> Namespaces {
    Namespaces::new().with_sql(Arc::new(Counter))
}

fn op(id: u64, delta: i64) -> Vec<u8> {
    [id.to_le_bytes(), delta.to_le_bytes()].concat()
}

fn url(dir: &Path, name: &str) -> String {
    format!("sqlite:{}", dir.join(name).display())
}

async fn open_on(dir: &Path, db: &str) -> Arc<MetaStore> {
    let log = LocalLogStore::open(dir.join("log")).unwrap();
    MetaStore::open(&url(dir, db), Arc::new(log), sql_counter(), MetaOptions::default())
        .await
        .unwrap()
}

async fn value(store: &MetaStore) -> i64 {
    sqlx::query_as::<_, (i64,)>("SELECT v FROM counter")
        .fetch_one(store.pool())
        .await
        .unwrap()
        .0
}

async fn op_ids(store: &MetaStore) -> Vec<u64> {
    sqlx::query_as::<_, (i64,)>("SELECT id FROM counter_ops ORDER BY id")
        .fetch_all(store.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|(id,)| id as u64)
        .collect()
}

#[tokio::test]
async fn commit_and_reject_both_consume_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path(), "a.db").await;
    assert_eq!(store.generation(), 1);
    let r = store.propose("counter", &op(1, 5)).await.unwrap();
    assert_eq!(r.position, 1);
    assert_eq!(r.applied, Applied::Commit(5i64.to_le_bytes().to_vec()));

    let r = store.propose("counter", &op(2, -7)).await.unwrap();
    assert_eq!(r.position, 2);
    assert_eq!(r.applied, Applied::Reject(b"negative".to_vec()));
    assert_eq!(value(&store).await, 5, "rejected command must not leave writes");
    assert_eq!(op_ids(&store).await, vec![1]);
    assert_eq!(store.applied_position().await.unwrap(), 2);
}

#[tokio::test]
async fn unknown_namespace_fails_without_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path(), "a.db").await;
    assert!(store.propose("nope", &[]).await.is_err());
    assert_eq!(store.applied_position().await.unwrap(), 0);

    let log = Arc::new(LocalLogStore::open(dir.path().join("log2")).unwrap());
    let twice: Vec<Arc<dyn Namespace>> = vec![Arc::new(Counter), Arc::new(Counter)];
    assert!(MetaStore::open(&url(dir.path(), "b.db"), log, twice, MetaOptions::default())
        .await
        .is_err());
}

#[tokio::test]
async fn concurrent_proposals_get_distinct_positions() {
    let dir = tempfile::tempdir().unwrap();
    for delay in [Duration::ZERO, Duration::from_millis(20)] {
        let log = LocalLogStore::open(dir.path().join(format!("log-{}", delay.as_millis()))).unwrap();
        let opts = MetaOptions { batch_max_delay: delay, batch_max_entries: 8 };
        let db = url(dir.path(), &format!("{}.db", delay.as_millis()));
        let store = MetaStore::open(&db, Arc::new(log), sql_counter(), opts)
            .await
            .unwrap();
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let s = store.clone();
                tokio::spawn(async move { s.propose("counter", &op(i, 1)).await })
            })
            .collect();
        let mut positions = Vec::new();
        for t in tasks {
            positions.push(t.await.unwrap().unwrap().position);
        }
        positions.sort_unstable();
        assert_eq!(positions, (1..=32).collect::<Vec<_>>());
        assert_eq!(value(&store).await, 32);
    }
}

#[tokio::test]
async fn replay_into_an_empty_file_yields_identical_tables() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path(), "a.db").await;
    for i in 0..300u64 {
        // Every fifth op would go negative and is rejected.
        let delta = if i % 5 == 4 { -1000 } else { i as i64 % 7 };
        a.propose("counter", &op(i, delta)).await.unwrap();
    }
    let a_pos = a.applied_position().await.unwrap();
    assert_eq!(a_pos, 300);

    let b = open_on(dir.path(), "b.db").await;
    assert_eq!(b.generation(), 2);
    assert_eq!(b.applied_position().await.unwrap(), a_pos);
    let a_dump = dump_tables(a.pool()).await.unwrap();
    assert_eq!(a_dump, dump_tables(b.pool()).await.unwrap());
    assert!(a_dump.iter().any(|(t, rows)| t == "counter_ops" && rows.len() == 240));
}

#[tokio::test]
async fn second_incarnation_fences_the_first_without_losing_acknowledged_entries() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_on(dir.path(), "a.db").await;

    // Keep proposing on A while B boots on the same log.
    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut acked = Vec::new();
            for id in 0.. {
                match a.propose("counter", &op(id, 1)).await {
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
    let b = open_on(dir.path(), "b.db").await;
    let (acked, stop) = writer.await.unwrap();

    assert_eq!(stop, Stop::Fenced { current: 2 });
    assert_eq!(a.stopped().await, Stop::Fenced { current: 2 });
    assert!(a.propose("counter", &op(u64::MAX >> 1, 1)).await.is_err());
    assert!(!acked.is_empty(), "A acknowledged something before the fence");

    let in_b = op_ids(&b).await;
    assert!(acked.iter().all(|id| in_b.contains(id)), "acknowledged entry lost");
    // B holds A's acknowledged prefix and nothing A did not append before the claim.
    assert_eq!(in_b.len(), acked.len());
    assert_eq!(dump_tables(a.pool()).await.unwrap(), dump_tables(b.pool()).await.unwrap());

    b.propose("counter", &op(1 << 40, 1)).await.unwrap();
}

#[tokio::test]
async fn crash_between_append_and_apply_is_applied_once_on_boot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path(), "a.db").await;
        a.propose("counter", &op(1, 1)).await.unwrap();
    }
    // An incarnation that appended a batch and died before applying it.
    let log = LocalLogStore::open(dir.path().join("log")).unwrap();
    assert_eq!(log.claim(2).await.unwrap().position, 1);
    let entry = Entry { namespace: "counter".into(), command: op(2, 5) };
    assert_eq!(log.append(2, &[entry]).await.unwrap(), 2);

    for expected_gen in [3, 4] {
        let a = open_on(dir.path(), "a.db").await;
        assert_eq!(a.generation(), expected_gen);
        assert_eq!(a.applied_position().await.unwrap(), 2);
        assert_eq!(value(&a).await, 6);
        assert_eq!(op_ids(&a).await, vec![1, 2]);
    }
}

#[tokio::test]
async fn state_from_another_log_is_rebuilt_from_this_one() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path(), "a.db").await;
        a.propose("counter", &op(1, 1)).await.unwrap();
        a.propose("counter", &op(2, 1)).await.unwrap();
    }
    // A recreated log reuses positions 1, 2, 3 for different entries.
    std::fs::remove_dir_all(dir.path().join("log")).unwrap();
    {
        let other = open_on(dir.path(), "other.db").await;
        for id in 10..13 {
            other.propose("counter", &op(id, 5)).await.unwrap();
        }
    }
    let a = open_on(dir.path(), "a.db").await;
    assert_eq!(a.applied_position().await.unwrap(), 3);
    assert_eq!(op_ids(&a).await, vec![10, 11, 12], "nothing of the old history survives");
    assert_eq!(value(&a).await, 15);
}

/// In-memory twin of [`Counter`]: same commands, state in a struct.
#[derive(Default)]
struct MemCounter {
    v: i64,
    ops: BTreeMap<u64, i64>,
}

impl StateMachine for MemCounter {
    const NAME: &'static str = "mem";

    fn apply(&mut self, command: &[u8]) -> Result<Applied> {
        let id = u64::from_le_bytes(command[..8].try_into().context("op id")?);
        let delta = i64::from_le_bytes(command[8..].try_into().context("8-byte delta")?);
        if self.v + delta < 0 {
            return Ok(Applied::Reject(b"negative".to_vec()));
        }
        self.v += delta;
        self.ops.insert(id, delta);
        Ok(Applied::Commit(self.v.to_le_bytes().to_vec()))
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.v.to_le_bytes().to_vec();
        for (id, delta) in &self.ops {
            out.extend_from_slice(&op(*id, *delta));
        }
        out
    }

    fn restore(data: &[u8]) -> Result<Self> {
        let v = i64::from_le_bytes(data[..8].try_into()?);
        let ops = data[8..]
            .chunks(16)
            .map(|c| Ok((u64::from_le_bytes(c[..8].try_into()?), i64::from_le_bytes(c[8..].try_into()?))))
            .collect::<Result<_>>()?;
        Ok(Self { v, ops })
    }
}

async fn open_mixed(dir: &Path, db: &str) -> Arc<MetaStore> {
    let log = LocalLogStore::open(dir.join("log")).unwrap();
    let namespaces = Namespaces::new().with_sql(Arc::new(Counter)).with::<MemCounter>();
    MetaStore::open(&url(dir, db), Arc::new(log), namespaces, MetaOptions::default())
        .await
        .unwrap()
}

fn mem_snapshot(store: &MetaStore) -> Vec<u8> {
    store.read::<MemCounter, _>(|s| s.snapshot())
}

#[tokio::test]
async fn in_memory_namespace_commits_rejects_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_mixed(dir.path(), "a.db").await;
    let r = store.propose("mem", &op(1, 5)).await.unwrap();
    assert_eq!((r.position, r.applied), (1, Applied::Commit(5i64.to_le_bytes().to_vec())));
    let r = store.propose("mem", &op(2, -7)).await.unwrap();
    assert_eq!((r.position, r.applied), (2, Applied::Reject(b"negative".to_vec())));
    assert_eq!(store.read::<MemCounter, _>(|s| (s.v, s.ops.len())), (5, 1));

    // Both kinds share one log and one sequence of positions.
    assert_eq!(store.propose("counter", &op(3, 2)).await.unwrap().position, 3);
    assert_eq!(value(&store).await, 2);
}

#[tokio::test]
async fn every_boot_rebuilds_memory_and_sql_state_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let a = open_mixed(dir.path(), "a.db").await;
    for i in 0..100u64 {
        let delta = if i % 5 == 4 { -1000 } else { i as i64 % 7 };
        a.propose("mem", &op(i, delta)).await.unwrap();
        a.propose("counter", &op(i, delta)).await.unwrap();
    }
    let (snap, tables) = (mem_snapshot(&a), dump_tables(a.pool()).await.unwrap());
    assert_eq!(MemCounter::restore(&snap).unwrap().snapshot(), snap, "restore inverts snapshot");
    drop(a);

    // Same file (state discarded and rebuilt) and an empty file give the same state.
    for db in ["a.db", "b.db"] {
        let b = open_mixed(dir.path(), db).await;
        assert_eq!(b.applied_position().await.unwrap(), 200);
        assert_eq!(mem_snapshot(&b), snap);
        assert_eq!(dump_tables(b.pool()).await.unwrap(), tables);
    }
}

#[test]
fn local_log_dir_sits_next_to_the_database() {
    assert_eq!(local_log_dir("sqlite:/x/meta.db").unwrap(), PathBuf::from("/x/meta.db.log"));
    assert_eq!(local_log_dir("sqlite://rel.db?mode=rwc").unwrap(), PathBuf::from("rel.db.log"));
    assert!(local_log_dir("sqlite::memory:").is_err());
}
