//! A second process takes the log over while the first keeps writing.

use pigeonhole_meta::{BlobSnapshotStore, ChainLogStore, MetaOptions, MetaStore, Namespaces, Stop};
use pigeonhole_sim::{random_ms, random_u64, Journal, Record};
use pigeonhole_storage_memory::MemoryBlobStore;
use std::sync::Arc;
use std::time::Duration;

async fn tablet(log: &Arc<MemoryBlobStore>, snaps: &Arc<MemoryBlobStore>) -> Arc<MetaStore> {
    let opts = MetaOptions { snapshot_every: 7, ..MetaOptions::default() };
    MetaStore::open(
        Arc::new(ChainLogStore::new(log.clone())),
        Arc::new(BlobSnapshotStore::new(snaps.clone())),
        Namespaces::new().with::<Journal>(),
        opts,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_takeover_keeps_every_acknowledged_command() {
    let (log, snaps) = (Arc::new(MemoryBlobStore::new()), Arc::new(MemoryBlobStore::new()));
    let a = tablet(&log, &snaps).await;
    let writer = {
        let a = a.clone();
        tokio::spawn(async move {
            let mut acked = Vec::new();
            for id in 0.. {
                tokio::time::sleep(random_ms(5)).await;
                match a.propose::<Journal>(Record { id, writer: "a".into() }).await {
                    Ok(_) => acked.push(id),
                    Err(e) => return (acked, e.downcast::<Stop>().expect("a Stop")),
                }
            }
            unreachable!()
        })
    };
    tokio::time::sleep(Duration::from_millis(20 + random_u64() % 200)).await;
    let b = tablet(&log, &snaps).await;
    let (acked, stop) = writer.await.unwrap();
    assert_eq!(stop, Stop::Fenced { current: 2 });
    let held: Vec<u64> = b.read::<Journal, _>(|j| j.entries.keys().copied().collect());
    assert!(held.len() >= acked.len() && held[..acked.len()] == acked[..], "{held:?} lost some of {acked:?}");
    b.propose::<Journal>(Record { id: u64::MAX, writer: "b".into() }).await.unwrap().unwrap();
}
