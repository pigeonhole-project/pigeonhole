use super::*;
use pigeonhole_storage_api::inspect::RecordView;
use pigeonhole_storage_api::{collect_stream, BlobId};
use std::fs;
use std::io::Write;

fn small(dir: &Path, segment_bytes: u64) -> DiskBlobStore {
    DiskBlobStore::with_segment_bytes(dir, segment_bytes).unwrap()
}

async fn read(store: &DiskBlobStore, id: &DiskId) -> Vec<u8> {
    collect_stream(store.get(id, None).await.unwrap()).await.unwrap().to_vec()
}

fn segment_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("seg-"))
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn keys_are_positions_and_keep_growing_across_reopen_and_segments() {
    let dir = tempfile::tempdir().unwrap();
    let a = small(dir.path(), 100);
    let first = a.put(Bytes::from_static(b"one"), &BlobId::random().name()).await.unwrap();
    let mut last = first.clone();
    for i in 0..10u8 {
        let id = a.put(Bytes::from(vec![i; 40]), &BlobId::random().name()).await.unwrap();
        assert!(id.key > last.key);
        last = id;
    }
    assert!(segment_files(dir.path()).len() > 2, "rolled over: {:?}", segment_files(dir.path()));
    drop(a);

    let b = small(dir.path(), 100);
    let next = b.put(Bytes::from_static(b"two"), &BlobId::random().name()).await.unwrap();
    assert!(next.key > last.key);
    assert_eq!(read(&b, &first).await, b"one");
    assert_eq!(read(&b, &last).await, vec![9; 40]);
    let part = collect_stream(b.get(&last, Some(3..7)).await.unwrap()).await.unwrap();
    assert_eq!(part.as_ref(), &[9; 4]);
}

#[tokio::test]
async fn a_torn_record_at_the_end_is_dropped_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let a = small(dir.path(), 1 << 20);
    let kept = a.put(Bytes::from_static(b"kept"), &BlobId::random().name()).await.unwrap();
    let torn = a.put(Bytes::from(vec![5u8; 1000]), &BlobId::random().name()).await.unwrap();
    drop(a);
    let seg = dir.path().join(&segment_files(dir.path())[0]);
    let len = fs::metadata(&seg).unwrap().len();
    fs::OpenOptions::new().write(true).open(&seg).unwrap().set_len(len - 10).unwrap();

    let b = small(dir.path(), 1 << 20);
    assert_eq!(read(&b, &kept).await, b"kept");
    assert!(b.get(&torn, None).await.is_err(), "never acknowledged");
    let again = b.put(Bytes::from_static(b"next"), &BlobId::random().name()).await.unwrap();
    assert_eq!(again.key, torn.key, "the torn space is reused");
    assert_eq!(read(&small(dir.path(), 1 << 20), &again).await, b"next");
}

#[tokio::test]
async fn a_last_record_failing_its_checksum_is_dropped_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let a = small(dir.path(), 1 << 20);
    let kept = a.put(Bytes::from_static(b"kept"), &BlobId::random().name()).await.unwrap();
    let torn = a.put(Bytes::from(vec![5u8; 1000]), &BlobId::random().name()).await.unwrap();
    drop(a);
    let seg = dir.path().join(&segment_files(dir.path())[0]);
    let mut bytes = fs::read(&seg).unwrap();
    *bytes.last_mut().unwrap() ^= 1; // the length is whole, the data is not
    fs::write(&seg, bytes).unwrap();

    let b = small(dir.path(), 1 << 20);
    assert_eq!(read(&b, &kept).await, b"kept");
    assert!(b.get(&torn, None).await.is_err(), "never acknowledged");
}

#[tokio::test]
async fn deletes_free_space_and_drop_dead_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = small(dir.path(), 64 << 10);
    let mut ids = Vec::new();
    for i in 0..6u8 {
        ids.push(store.put(Bytes::from(vec![i; 40 << 10]), &BlobId::random().name()).await.unwrap());
    }
    let segs = segment_files(dir.path());
    assert_eq!(segs.len(), 3, "{segs:?}");
    // The first segment dies whole; a record of the second is punched out.
    let second = dir.path().join(&segs[1]);
    let blocks = || std::os::unix::fs::MetadataExt::blocks(&fs::metadata(&second).unwrap());
    let before = blocks();
    store.delete(&[ids[0].key, ids[1].key, ids[2].key]).await.unwrap();
    assert_eq!(segment_files(dir.path()).len(), 2);
    assert!(blocks() < before, "a hole: {} -> {} blocks", before, blocks());
    let gone = store.get(&ids[2], None).await;
    assert_eq!(pigeonhole_storage_api::Failure::of(&gone.err().unwrap()), pigeonhole_storage_api::Failure::NotFound);
    assert_eq!(read(&store, &ids[3]).await, vec![3; 40 << 10]);
    store.delete(&[ids[2].key]).await.unwrap(); // again: fine

    let reopened = small(dir.path(), 64 << 10);
    let gone = reopened.get(&ids[2], None).await;
    assert_eq!(
        pigeonhole_storage_api::Failure::of(&gone.err().unwrap()),
        pigeonhole_storage_api::Failure::NotFound,
        "dead after reopen"
    );
    assert_eq!(
        reopened.candidates(pigeonhole_storage_api::Owner(0), None, u64::MAX, 100).await.unwrap(),
        vec![ids[3].key, ids[4].key, ids[5].key]
    );
}

#[tokio::test]
async fn the_first_successor_wins_across_handles_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (small(dir.path(), 1 << 20), small(dir.path(), 1 << 20));
    assert_eq!(a.tail().await.unwrap(), None);
    let After::Won(root) = a.put_after(None, "root", Bytes::from_static(b"r")).await.unwrap() else { panic!() };
    // Another process (another handle) sees it at once.
    let After::Lost(w) = b.put_after(None, "root2", Bytes::from_static(b"r")).await.unwrap() else { panic!() };
    assert_eq!((w.id.clone(), w.label.as_str()), (root.clone(), "root"));
    let After::Won(x) = b.put_after(Some(&root), "x", Bytes::from_static(b"x")).await.unwrap() else { panic!() };
    let After::Lost(w) = a.put_after(Some(&root), "y", Bytes::from_static(b"y")).await.unwrap() else { panic!() };
    assert_eq!(w.id, x);
    assert_eq!(a.successor(&root).await.unwrap().map(|l| l.id), Some(x.clone()));
    assert_eq!(a.tail().await.unwrap().map(|l| (l.id, l.label)), Some((x.clone(), "x".to_string())));

    let c = small(dir.path(), 1 << 20);
    assert_eq!(c.tail().await.unwrap().map(|l| l.id), Some(x.clone()));
    assert_eq!(c.successor(&root).await.unwrap().map(|l| l.label), Some("x".into()));
    assert_eq!(read(&c, &x).await, b"x");

    let After::Won(y) = c.put_after(Some(&x), "y", Bytes::from_static(b"y")).await.unwrap() else { panic!() };
    c.prune(&y).await.unwrap();
    assert!(c.get(&root, None).await.is_err());
    assert!(c.get(&x, None).await.is_err());
    assert_eq!(c.tail().await.unwrap().map(|l| l.id), Some(y));
}

mod sim {
    use super::*;
    use crate::io::SimFs;
    use std::time::Duration;

    fn on(fs: &SimFs) -> DiskBlobStore {
        DiskBlobStore::open_on(Arc::new(fs.clone()), "/d", 1 << 20).unwrap()
    }

    #[tokio::test]
    async fn what_was_acknowledged_survives_a_crash_and_a_torn_write_is_dropped() {
        let fs = SimFs::new(Duration::ZERO);
        let a = on(&fs);
        let kept = a.put(Bytes::from_static(b"kept"), &BlobId::random().name()).await.unwrap();
        let After::Won(root) = a.put_after(None, "root", Bytes::from_static(b"r")).await.unwrap() else { panic!() };
        // A write in flight when the power goes: half of it reaches the disk.
        let seg = std::path::Path::new("/d").join("seg-0000000001");
        let len = fs.file_len(&seg).unwrap();
        let file = crate::io::Fs::open(&fs, &seg, false).unwrap();
        file.write_all_at(&[0x47, 0x53, 0x48, 0x50, 0, 0, 1, 0, 200, 0, 0, 0, 0, 0, 0, 0], len).unwrap();
        drop(a);
        fs.crash(Some(&mut |n| n / 2));

        let b = on(&fs);
        assert_eq!(read(&b, &kept).await, b"kept");
        assert_eq!(b.tail().await.unwrap().map(|l| l.id), Some(root.clone()));
        let next = b.put(Bytes::from_static(b"next"), &BlobId::random().name()).await.unwrap();
        assert_eq!(next.key, crate::segments::key_of(1, len), "the torn bytes were cut off");
        assert_eq!(read(&on(&fs), &next).await, b"next");
    }

    #[tokio::test]
    async fn an_unsynced_segment_is_gone_after_a_crash() {
        let fs = SimFs::new(Duration::ZERO);
        let a = on(&fs);
        a.put(Bytes::from_static(b"one"), &BlobId::random().name()).await.unwrap();
        // A segment created but never synced (the process died right after).
        crate::io::Fs::open(&fs, std::path::Path::new("/d/seg-0000000002"), true).unwrap();
        drop(a);
        fs.crash(None);
        assert_eq!(on(&fs).candidates(pigeonhole_storage_api::Owner(0), None, u64::MAX, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_first_successor_wins_between_two_handles() {
        let fs = SimFs::new(Duration::ZERO);
        let (a, b) = (on(&fs), on(&fs));
        let After::Won(root) = a.put_after(None, "r", Bytes::from_static(b"r")).await.unwrap() else { panic!() };
        let After::Won(x) = b.put_after(Some(&root), "x", Bytes::from_static(b"x")).await.unwrap() else { panic!() };
        let After::Lost(w) = a.put_after(Some(&root), "y", Bytes::from_static(b"y")).await.unwrap() else { panic!() };
        assert_eq!(w.id, x);
    }

    /// A delete syncs the segments it touched, and the directory when it
    /// removed one: after a crash the removed segment stays gone.
    #[tokio::test]
    async fn a_delete_syncs_what_it_touched_and_a_removal_survives_a_crash() {
        let fs = SimFs::new(Duration::ZERO);
        // Two records a segment: [0, 1], [2, 3].
        let a = DiskBlobStore::open_on(Arc::new(fs.clone()), "/d", 200).unwrap();
        let mut ids = Vec::new();
        for i in 0..4u8 {
            ids.push(a.put(Bytes::from(vec![i + 1; 40]), &BlobId::random().name()).await.unwrap());
        }
        assert_eq!(ids.iter().map(|id| id.key >> 40).collect::<Vec<_>>(), [1, 1, 2, 2]);
        let (files, dirs) = fs.syncs();
        // A record of the second segment: that segment only (was: both).
        a.delete(&[ids[2].key]).await.unwrap();
        assert_eq!(fs.syncs(), (files + 1, dirs), "one segment synced");
        // The whole first segment: removed, the directory synced.
        a.delete(&[ids[0].key, ids[1].key]).await.unwrap();
        assert_eq!(fs.syncs(), (files + 1, dirs + 1), "no segment left to sync, the directory once");
        drop(a);
        fs.crash(None);
        let b = on(&fs);
        assert!(b.get(&ids[0], None).await.is_err(), "the removed segment stays removed");
        assert!(b.get(&ids[1], None).await.is_err());
        assert_eq!(read(&b, &ids[3]).await, vec![4; 40]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_sync_costs_virtual_time() {
        let fs = SimFs::new(Duration::from_millis(4));
        let a = on(&fs);
        let t = tokio::time::Instant::now();
        a.put(Bytes::from_static(b"one"), &BlobId::random().name()).await.unwrap();
        assert!(t.elapsed() >= Duration::from_millis(4), "{:?}", t.elapsed());
    }
}

/// A header whose magic did not make it (a page of the append torn off) at
/// the end of the last segment is cut off on open; the same bytes with a
/// whole record after them are corruption.
#[tokio::test]
async fn a_header_without_its_magic_is_torn_at_the_end_and_corrupt_before_a_record() {
    let dir = tempfile::tempdir().unwrap();
    let a = small(dir.path(), 1 << 20);
    let kept = a.put(Bytes::from_static(b"kept"), &BlobId::random().name()).await.unwrap();
    drop(a);
    let seg = dir.path().join(&segment_files(dir.path())[0]);
    let whole = fs::read(&seg).unwrap();
    let garbage = [
        0x47u8, 0x53, 0x01, 0x02, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
    ];
    fs::write(&seg, [&whole[..], &garbage[..]].concat()).unwrap();
    let b = small(dir.path(), 1 << 20);
    assert_eq!(read(&b, &kept).await, b"kept");
    assert_eq!(fs::metadata(&seg).unwrap().len(), whole.len() as u64, "cut off");
    drop(b);

    fs::write(&seg, [&whole[..], &garbage[..], &whole[..]].concat()).unwrap();
    assert!(DiskBlobStore::with_segment_bytes(dir.path(), 1 << 20).is_err(), "a record after it: corrupt");
}

/// A deposed writer whose tip was pruned with its whole segment still
/// learns who won, by label: the log turns that into `Fenced`.
#[tokio::test]
async fn a_put_after_a_pruned_segment_loses_to_a_named_winner() {
    let dir = tempfile::tempdir().unwrap();
    let store = small(dir.path(), 64);
    let After::Won(tip) = store.put_after(None, "log-claim-1", Bytes::from(vec![1; 40])).await.unwrap() else {
        panic!()
    };
    let After::Won(next) = store.put_after(Some(&tip), "log-claim-2", Bytes::from(vec![2; 40])).await.unwrap() else {
        panic!()
    };
    let After::Won(last) = store.put_after(Some(&next), "log-batch-2", Bytes::from(vec![3; 40])).await.unwrap() else {
        panic!()
    };
    store.prune(&last).await.unwrap();
    assert_eq!(segment_files(dir.path()).len(), 1, "the pruned segments are gone");
    let After::Lost(w) = store.put_after(Some(&tip), "log-batch-1", Bytes::from_static(b"x")).await.unwrap() else {
        panic!("the deposed writer won")
    };
    assert_eq!((w.id, w.label.as_str()), (next, "log-claim-2"));
}

/// Another process's deletes are seen: a record it deleted is not read
/// (its data is a hole), nor offered to the sweeper.
#[tokio::test]
async fn another_handles_deletes_are_seen() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (small(dir.path(), 1 << 20), small(dir.path(), 1 << 20));
    let doomed = a.put(Bytes::from(vec![7u8; 10_000]), &BlobId::random().name()).await.unwrap();
    let kept = a.put(Bytes::from_static(b"kept"), &BlobId::random().name()).await.unwrap();
    assert_eq!(read(&b, &doomed).await, vec![7u8; 10_000]);
    a.delete(&[doomed.key]).await.unwrap();
    let gone = b.get(&doomed, None).await;
    assert_eq!(pigeonhole_storage_api::Failure::of(&gone.err().unwrap()), pigeonhole_storage_api::Failure::NotFound);
    assert_eq!(b.candidates(pigeonhole_storage_api::Owner(0), None, u64::MAX, 10).await.unwrap(), vec![kept.key]);
}

#[tokio::test]
async fn inspect_shows_segments_and_records_without_touching_them() {
    let dir = tempfile::tempdir().unwrap();
    let store = small(dir.path(), 100);
    let a = store.put(Bytes::from_static(b"alpha"), "a.bin").await.unwrap();
    let names: Vec<String> = (0..4).map(|i| format!("b{i}.bin")).collect();
    for n in &names {
        store.put(Bytes::from(vec![7; 40]), n).await.unwrap();
    }
    store.delete(&[a.key]).await.unwrap();
    // A torn record at the end, as a writer caught mid-append would leave it.
    let last = segment_files(dir.path()).pop().unwrap();
    let path = dir.path().join(&last);
    let before = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&[0x47, 0x53, 0x48]).unwrap();

    let segs = inspect(&StdFs, dir.path(), |label| label == "b0.bin").unwrap();
    assert_eq!(segs.iter().map(|s| s.name.clone()).collect::<Vec<_>>(), segment_files(dir.path()));
    let records: Vec<&RecordView> = segs.iter().flat_map(|s| &s.records).collect();
    assert_eq!(
        records.iter().map(|r| r.label.as_str()).collect::<Vec<_>>(),
        ["a.bin", "b0.bin", "b1.bin", "b2.bin", "b3.bin"]
    );
    assert!(records[0].dead && records[0].data.is_none(), "deleted, so no data");
    assert_eq!(records[0].key, a.key);
    assert_eq!(records[1].data.as_deref(), Some(&[7u8; 40][..]), "asked for");
    assert!(records[2].data.is_none() && records[2].size == 40);
    assert!(records.iter().all(|r| !r.chained));
    assert_eq!(fs::metadata(&path).unwrap().len(), before + 3, "the torn tail is left alone");
}
