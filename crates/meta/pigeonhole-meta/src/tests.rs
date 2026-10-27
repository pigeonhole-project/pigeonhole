use super::*;
use std::collections::BTreeMap;
use std::path::Path;

/// Toy namespace. Command = op id (u64) + signed delta (i64) for a counter;
/// every committed op is recorded; going negative is rejected.
#[derive(Default)]
struct Counter {
    v: i64,
    ops: BTreeMap<u64, i64>,
}

impl StateMachine for Counter {
    const NAME: &'static str = "counter";

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

fn op(id: u64, delta: i64) -> Vec<u8> {
    [id.to_le_bytes(), delta.to_le_bytes()].concat()
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
    store.read::<Counter, _>(|c| c.snapshot())
}

#[tokio::test]
async fn commit_and_reject_both_consume_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    assert_eq!(store.generation(), 1);
    let r = store.propose("counter", &op(1, 5)).await.unwrap();
    assert_eq!(r.position, 1);
    assert_eq!(r.applied, Applied::Commit(5i64.to_le_bytes().to_vec()));

    let r = store.propose("counter", &op(2, -7)).await.unwrap();
    assert_eq!(r.position, 2);
    assert_eq!(r.applied, Applied::Reject(b"negative".to_vec()));
    assert_eq!(value(&store), 5, "rejected command must not change state");
    assert_eq!(op_ids(&store), vec![1]);
    assert_eq!(store.applied_position(), 2);
}

#[tokio::test]
async fn unknown_namespace_fails_without_a_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = open_on(dir.path()).await;
    assert!(store.propose("nope", &[]).await.is_err());
    assert_eq!(store.applied_position(), 0);

    let twice = Namespaces::new().with::<Counter>().with::<Counter>();
    assert!(MetaStore::open_local(dir.path().join("log2"), twice).await.is_err());
}

#[tokio::test]
async fn concurrent_proposals_get_distinct_positions() {
    let dir = tempfile::tempdir().unwrap();
    for delay in [Duration::ZERO, Duration::from_millis(20)] {
        let log = LocalLogStore::open(dir.path().join(format!("log-{}", delay.as_millis()))).unwrap();
        let opts = MetaOptions { batch_max_delay: delay, batch_max_entries: 8 };
        let store = MetaStore::open(Arc::new(log), counter(), opts).await.unwrap();
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
        a.propose("counter", &op(i, delta)).await.unwrap();
    }
    assert_eq!(a.applied_position(), 300);
    let snap = snapshot(&a);
    assert_eq!(Counter::restore(&snap).unwrap().snapshot(), snap, "restore inverts snapshot");

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
    let b = open_on(dir.path()).await;
    let (acked, stop) = writer.await.unwrap();

    assert_eq!(stop, Stop::Fenced { current: 2 });
    assert_eq!(a.stopped().await, Stop::Fenced { current: 2 });
    assert!(a.propose("counter", &op(u64::MAX >> 1, 1)).await.is_err());
    assert!(!acked.is_empty(), "A acknowledged something before the fence");

    // B holds exactly A's acknowledged entries: nothing lost, nothing extra.
    assert_eq!(op_ids(&b), acked);
    assert_eq!(snapshot(&a), snapshot(&b));

    b.propose("counter", &op(1 << 40, 1)).await.unwrap();
}

#[tokio::test]
async fn crash_between_append_and_apply_is_applied_once_on_boot() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = open_on(dir.path()).await;
        a.propose("counter", &op(1, 1)).await.unwrap();
    }
    // An incarnation that appended a batch and died before applying it.
    let log = LocalLogStore::open(dir.path().join("log")).unwrap();
    assert_eq!(log.claim(2).await.unwrap().position, 1);
    let entry = Entry { namespace: "counter".into(), command: op(2, 5) };
    assert_eq!(log.append(2, &[entry]).await.unwrap(), 2);

    for expected_gen in [3, 4] {
        let a = open_on(dir.path()).await;
        assert_eq!(a.generation(), expected_gen);
        assert_eq!(a.applied_position(), 2);
        assert_eq!(value(&a), 6);
        assert_eq!(op_ids(&a), vec![1, 2]);
    }
}
