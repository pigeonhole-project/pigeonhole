//! A model check of the chain log under store faults (fault-injection.md
//! § model check): writers of rising generations overlap, each appends until
//! fenced or until a fault leaves its outcome unknown; afterwards a reader
//! must find every acknowledged entry where it was acknowledged.

use futures::TryStreamExt;
use pigeonhole_blob::Chain;
use pigeonhole_meta::{ChainLogStore, Entry, LogError, LogStore, Position};
use pigeonhole_sim::faults::{FaultPlan, Faulty};
use pigeonhole_sim::{random_ms, random_u64};
use pigeonhole_storage_disk::{DiskBlobStore, SimFs};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WRITERS: u64 = 5;

/// Acknowledged entries: position → payload, and who acknowledged what.
#[derive(Default)]
struct Acks {
    at: BTreeMap<Position, (u64, Vec<u8>)>,
}

impl Acks {
    fn record(&mut self, gen: u64, first: Position, entries: &[Entry]) {
        for (k, e) in entries.iter().enumerate() {
            let pos = first + k as Position;
            if let Some((g, p)) = self.at.get(&pos) {
                assert_eq!(p, &e.payload, "generations {g} and {gen} both acknowledged position {pos}");
            }
            self.at.insert(pos, (gen, e.payload.clone()));
        }
    }
}

async fn claim<C: Chain>(log: &ChainLogStore<C>, floor: &AtomicU64) -> Option<u64> {
    let mut gen = floor.fetch_add(1, Ordering::SeqCst) + 1;
    for _ in 0..8 {
        match log.claim(gen).await {
            Ok(_) => return Some(gen),
            Err(LogError::Fenced { current }) => {
                floor.fetch_max(current, Ordering::SeqCst);
                gen = floor.fetch_add(1, Ordering::SeqCst) + 1;
            }
            Err(_) => tokio::time::sleep(random_ms(20)).await,
        }
    }
    None
}

async fn writer<C: Chain>(id: u64, chain: Arc<C>, floor: Arc<AtomicU64>, acks: Arc<Mutex<Acks>>) {
    tokio::time::sleep(random_ms(400)).await;
    // A process that fails (an outcome it cannot know) restarts as a new
    // incarnation, as the tablet does; a fenced one is done.
    for incarnation in 0..4u64 {
        let log = ChainLogStore::new(chain.clone());
        let Some(gen) = claim(&log, &floor).await else { continue };
        for n in 0..30u64 {
            tokio::time::sleep(random_ms(15)).await;
            let len = 1 + random_u64() % 3;
            let entries: Vec<Entry> = (0..len)
                .map(|k| Entry { term: gen, payload: format!("w{id}i{incarnation}g{gen}n{n}k{k}").into_bytes() })
                .collect();
            match log.append(gen, &entries).await {
                Ok(last) => acks.lock().unwrap().record(gen, last + 1 - len, &entries),
                Err(LogError::Fenced { .. }) => return,
                Err(_) => break,
            }
        }
    }
}

async fn check<C: Chain>(chain: Arc<C>, quiesce: impl FnOnce(), floor: &AtomicU64, acks: &Acks) {
    quiesce();
    let log = ChainLogStore::new(chain);
    let gen = claim(&log, floor).await.expect("a quiet store can be claimed");
    let d = log.discover().await.unwrap().unwrap();
    assert_eq!(d.gen, gen);
    let history: BTreeMap<Position, Vec<u8>> =
        log.read(0, d.head).map_ok(|(p, e)| (p, e.payload)).try_collect().await.expect("the history reads back");
    for (pos, (g, payload)) in &acks.at {
        assert_eq!(
            history.get(pos),
            Some(payload),
            "position {pos} acknowledged by generation {g} is not in the history"
        );
    }
    // The log goes on.
    log.append(gen, &[Entry { term: gen, payload: b"after".to_vec() }]).await.unwrap();
}

async fn run<C: Chain>(store: Arc<Faulty<C>>) {
    let floor = Arc::new(AtomicU64::new(0));
    let acks = Arc::new(Mutex::new(Acks::default()));
    let tasks: Vec<_> =
        (0..WRITERS).map(|id| tokio::spawn(writer(id, store.clone(), floor.clone(), acks.clone()))).collect();
    for t in tasks {
        t.await.unwrap();
    }
    // Late puts land in the meantime.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let acks = acks.lock().unwrap().at.clone();
    let quiet = store.clone();
    check(store.clone(), move || quiet.quiesce(), &floor, &Acks { at: acks }).await;
}

#[tokio::test]
async fn acknowledged_entries_survive_faults_on_memory() {
    run(Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new()), FaultPlan::mild()))).await;
}

#[tokio::test]
async fn acknowledged_entries_survive_faults_on_disk() {
    let fs = SimFs::new(Duration::from_millis(2));
    let disk = DiskBlobStore::open_on(Arc::new(fs), "/log", 1 << 16).unwrap();
    run(Arc::new(Faulty::new(Arc::new(disk), FaultPlan::mild()))).await;
}

#[tokio::test]
async fn acknowledged_entries_survive_harsh_faults_on_disk() {
    let fs = SimFs::new(Duration::from_millis(2));
    let disk = DiskBlobStore::open_on(Arc::new(fs), "/log", 1 << 12).unwrap();
    run(Arc::new(Faulty::new(Arc::new(disk), FaultPlan::harsh()))).await;
}

#[tokio::test]
async fn acknowledged_entries_survive_harsh_faults_on_memory() {
    run(Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new()), FaultPlan::harsh()))).await;
}
