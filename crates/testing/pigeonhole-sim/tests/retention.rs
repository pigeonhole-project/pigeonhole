//! Log retention (docs/log-retention.md): a tablet keeps its `keep` newest
//! bases. A writer is taken over at a random moment, the newest base's
//! snapshot is then damaged, and a third boot must still hold every
//! acknowledged command, from an older base and a longer replay. Afterwards
//! no snapshot older than the oldest base kept is left.

use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, LogStore, MetaOptions, MetaStore, Namespaces, Stop};
use pigeonhole_sim::faults::{FaultPlan, Faulty};
use pigeonhole_sim::{random_ms, random_u64, Journal, Record};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::sync::Arc;
use std::time::Duration;

type Snaps = Faulty<MemoryBlobStore>;

async fn tablet(log: &Arc<MemoryBlobStore>, snaps: &Arc<Snaps>, keep: usize) -> anyhow::Result<Arc<MetaStore>> {
    let opts = MetaOptions { snapshot_every: 5, keep_snapshots: keep, ..MetaOptions::default() };
    MetaStore::open(
        Arc::new(ChainLogStore::new(log.clone())),
        Arc::new(BlobSnapshotStore::new(snaps.clone())),
        Namespaces::new().with::<Journal>(),
        opts,
    )
    .await
}

/// Propose until fenced; the ids acknowledged.
fn writer(t: Arc<MetaStore>, name: &'static str, from: u64) -> tokio::task::JoinHandle<Vec<u64>> {
    tokio::spawn(async move {
        let mut acked = Vec::new();
        for id in from.. {
            tokio::time::sleep(random_ms(5)).await;
            match t.propose::<Journal>(Record { id, writer: name.into() }).await {
                Ok(_) => acked.push(id),
                Err(e) => {
                    assert!(matches!(e.downcast::<Stop>(), Ok(Stop::Fenced { .. })), "{name} stopped otherwise");
                    return acked;
                }
            }
        }
        unreachable!()
    })
}

/// Wait until the log keeps `keep` bases.
async fn bases_kept(log: &Arc<MemoryBlobStore>, keep: usize) -> pigeonhole_meta::Discovered {
    let reader = ChainLogStore::new(log.clone());
    for _ in 0..1000 {
        let d = reader.discover().await.unwrap().unwrap();
        if d.base.is_some() && d.older.len() == keep - 1 {
            return d;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the log never kept {keep} bases");
}

#[tokio::test]
async fn a_damaged_newest_snapshot_loses_nothing_acknowledged() {
    let keep = 2 + (random_u64() % 2) as usize;
    let log = Arc::new(MemoryBlobStore::new());
    let snaps = Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new()), FaultPlan::none()));

    let a = tablet(&log, &snaps, keep).await.unwrap();
    let wa = writer(a.clone(), "a", 0);
    tokio::time::sleep(Duration::from_millis(20 + random_u64() % 200)).await;
    let b = tablet(&log, &snaps, keep).await.unwrap();
    let mut acked = wa.await.unwrap();
    drop(a);
    for id in 1_000_000..1_000_000 + 10 * keep as u64 {
        b.propose::<Journal>(Record { id, writer: "b".into() }).await.unwrap().unwrap();
        acked.push(id);
    }
    bases_kept(&log, keep).await;
    // Fence B, so the base the damage hits stays the newest.
    let fence = ChainLogStore::new(log.clone());
    fence.claim(fence.discover().await.unwrap().unwrap().gen + 1).await.unwrap();
    let newest = fence.discover().await.unwrap().unwrap().base.unwrap();
    snaps.damage(&BlobSnapshotStore::<Snaps>::blob_id(&newest.snapshot).unwrap());
    drop(b);

    let c = tablet(&log, &snaps, keep).await.expect("boots from an older base");
    let held: Vec<u64> = c.read::<Journal, _>(|j| j.entries.keys().copied().collect());
    let missing: Vec<&u64> = acked.iter().filter(|id| !held.contains(id)).collect();
    assert!(missing.is_empty(), "lost {missing:?}");
    c.propose::<Journal>(Record { id: u64::MAX, writer: "c".into() }).await.unwrap().unwrap();

    // The fallback snapshots again; the bases beyond `keep` lose their
    // snapshots. (Snapshots a fenced incarnation stored but never based on
    // stay: log-retention.md, open questions.)
    tokio::time::sleep(Duration::from_secs(1)).await;
    let d = ChainLogStore::new(log.clone()).discover().await.unwrap().unwrap();
    let watermark = d.older.last().or(d.base.as_ref()).unwrap().at;
    let stale: Vec<String> = snaps
        .inner()
        .message_keys()
        .into_iter()
        .filter_map(|k| snaps.inner().name_of(k))
        .filter(|name| {
            let at: u64 = name.trim_end_matches(".snap").rsplit('-').next().unwrap().parse().unwrap();
            at < watermark
        })
        .collect();
    assert!(stale.is_empty(), "snapshots behind the oldest base kept ({watermark}): {stale:?}");
}

/// With one base kept there is nothing to fall back to: the same damage
/// stops the boot (the scenario above really tests the fallback).
#[tokio::test]
async fn with_one_base_kept_a_damaged_snapshot_stops_the_boot() {
    let log = Arc::new(MemoryBlobStore::new());
    let snaps = Arc::new(Faulty::new(Arc::new(MemoryBlobStore::new()), FaultPlan::none()));
    let a = tablet(&log, &snaps, 1).await.unwrap();
    for id in 0..12 {
        a.propose::<Journal>(Record { id, writer: "a".into() }).await.unwrap().unwrap();
    }
    bases_kept(&log, 1).await;
    let fence = ChainLogStore::new(log.clone());
    fence.claim(fence.discover().await.unwrap().unwrap().gen + 1).await.unwrap();
    let newest = fence.discover().await.unwrap().unwrap().base.unwrap();
    snaps.damage(&BlobSnapshotStore::<Snaps>::blob_id(&newest.snapshot).unwrap());
    drop(a);
    assert!(tablet(&log, &snaps, 1).await.is_err());
}
