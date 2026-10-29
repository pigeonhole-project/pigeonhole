//! The tablet over a simulated disk (`SimFs`): a sync costs virtual time,
//! a power failure keeps only what was synced.

use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaOptions, MetaStore, Namespaces, Stop};
use pigeonhole_sim::{random_ms, random_u64, Journal, Record};
use pigeonhole_storage_disk::{DiskBlobStore, SimFs};
use std::sync::Arc;
use std::time::Duration;

async fn tablet(fs: &SimFs) -> anyhow::Result<Arc<MetaStore>> {
    let open = |dir: &str| DiskBlobStore::open_on(Arc::new(fs.clone()), dir, 4096).map(Arc::new);
    MetaStore::open(
        Arc::new(ChainLogStore::new(open("/meta/log")?)),
        Arc::new(BlobSnapshotStore::new(open("/meta/snapshots")?)),
        Namespaces::new().with::<Journal>(),
        MetaOptions { snapshot_every: 11, ..MetaOptions::default() },
    )
    .await
}

fn held(meta: &MetaStore) -> Vec<u64> {
    meta.read::<Journal, _>(|j| j.entries.keys().copied().collect())
}

/// A writer that never waits for anything but its disk: the clock moves
/// only because a sync costs time, so the second process gets to start.
#[tokio::test]
async fn a_takeover_of_a_busy_disk_log() {
    let fs = SimFs::new(Duration::from_millis(4));
    let a = tablet(&fs).await.unwrap();
    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut acked = Vec::new();
            for id in 0.. {
                match a.propose::<Journal>(Record { id, writer: "a".into() }).await {
                    Ok(_) => acked.push(id),
                    Err(e) => return (acked, e.downcast::<Stop>().expect("a Stop")),
                }
            }
            unreachable!()
        })
    };
    tokio::time::sleep(Duration::from_millis(30 + random_u64() % 300)).await;
    let b = tablet(&fs).await.unwrap();
    let (acked, stop) = writer.await.unwrap();
    assert_eq!(stop, Stop::Fenced { current: 2 });
    let held = held(&b);
    assert!(held.len() >= acked.len() && held[..acked.len()] == acked[..], "{held:?} lost some of {acked:?}");
}

/// Power fails at a random moment, mid-write or not: a new process boots
/// from what was synced and holds every acknowledged command.
#[tokio::test]
async fn a_power_failure_loses_nothing_acknowledged() {
    let fs = SimFs::new(Duration::from_millis(4));
    let a = tablet(&fs).await.unwrap();
    let acked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = {
        let (a, acked) = (a.clone(), acked.clone());
        tokio::spawn(async move {
            for id in 0.. {
                tokio::time::sleep(random_ms(3)).await;
                if a.propose::<Journal>(Record { id, writer: "a".into() }).await.is_ok() {
                    acked.lock().unwrap().push(id);
                }
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(20 + random_u64() % 400)).await;
    // The process dies (its tasks are gone), then the power.
    writer.abort();
    drop(a);
    let mut cut = |n: usize| (random_u64() as usize) % (n + 1);
    fs.crash(Some(&mut cut));

    let b = tablet(&fs).await.unwrap();
    let acked = acked.lock().unwrap().clone();
    let held = held(&b);
    assert!(held.len() >= acked.len() && held[..acked.len()] == acked[..], "{held:?} lost some of {acked:?}");
}
