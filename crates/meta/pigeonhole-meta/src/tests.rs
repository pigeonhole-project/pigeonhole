use super::*;
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

fn op(id: u64, delta: i64) -> Vec<u8> {
    [id.to_le_bytes(), delta.to_le_bytes()].concat()
}

fn url(dir: &Path, name: &str) -> String {
    format!("sqlite:{}", dir.join(name).display())
}

async fn open_on(dir: &Path, db: &str) -> Arc<MetaStore> {
    let log = LocalLogStore::open(dir.join("log")).unwrap();
    MetaStore::open(&url(dir, db), Arc::new(log), vec![Arc::new(Counter)], MetaOptions::default())
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
        let store = MetaStore::open(&db, Arc::new(log), vec![Arc::new(Counter)], opts)
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
    assert_eq!(log.claim(2).await.unwrap(), Head { position: 1 });
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
async fn state_ahead_of_the_log_refuses_to_boot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path(), "a.db").await;
        a.propose("counter", &op(1, 1)).await.unwrap();
    }
    std::fs::remove_dir_all(dir.path().join("log")).unwrap();
    let log = Arc::new(LocalLogStore::open(dir.path().join("log")).unwrap());
    let err = MetaStore::open(&url(dir.path(), "a.db"), log, vec![Arc::new(Counter)], MetaOptions::default())
        .await
        .err()
        .expect("boot must fail");
    assert!(matches!(err.downcast_ref::<LogError>(), Some(LogError::Lost(_))), "{err:#}");
}

#[test]
fn local_log_dir_sits_next_to_the_database() {
    assert_eq!(local_log_dir("sqlite:/x/meta.db").unwrap(), PathBuf::from("/x/meta.db.log"));
    assert_eq!(local_log_dir("sqlite://rel.db?mode=rwc").unwrap(), PathBuf::from("rel.db.log"));
    assert!(local_log_dir("sqlite::memory:").is_err());
}
