use super::*;
use pigeonhole_blob::collect_stream;
use std::fs;

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
    let first = a.put(Bytes::from_static(b"one"), "x").await.unwrap();
    let mut last = first.clone();
    for i in 0..10u8 {
        let id = a.put(Bytes::from(vec![i; 40]), "x").await.unwrap();
        assert!(id.key > last.key);
        last = id;
    }
    assert!(segment_files(dir.path()).len() > 2, "rolled over: {:?}", segment_files(dir.path()));
    drop(a);

    let b = small(dir.path(), 100);
    let next = b.put(Bytes::from_static(b"two"), "x").await.unwrap();
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
    let kept = a.put(Bytes::from_static(b"kept"), "x").await.unwrap();
    let torn = a.put(Bytes::from(vec![5u8; 1000]), "x").await.unwrap();
    drop(a);
    let seg = dir.path().join(&segment_files(dir.path())[0]);
    let len = fs::metadata(&seg).unwrap().len();
    fs::OpenOptions::new().write(true).open(&seg).unwrap().set_len(len - 10).unwrap();

    let b = small(dir.path(), 1 << 20);
    assert_eq!(read(&b, &kept).await, b"kept");
    assert!(b.get(&torn, None).await.is_err(), "never acknowledged");
    let again = b.put(Bytes::from_static(b"next"), "x").await.unwrap();
    assert_eq!(again.key, torn.key, "the torn space is reused");
    assert_eq!(read(&small(dir.path(), 1 << 20), &again).await, b"next");
}

#[tokio::test]
async fn deletes_free_space_and_drop_dead_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = small(dir.path(), 64 << 10);
    let mut ids = Vec::new();
    for i in 0..6u8 {
        ids.push(store.put(Bytes::from(vec![i; 40 << 10]), "x").await.unwrap());
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
    assert_eq!(pigeonhole_blob::Failure::of(&gone.err().unwrap()), pigeonhole_blob::Failure::NotFound);
    assert_eq!(read(&store, &ids[3]).await, vec![3; 40 << 10]);
    store.delete(&[ids[2].key]).await.unwrap(); // again: fine

    let reopened = small(dir.path(), 64 << 10);
    let gone = reopened.get(&ids[2], None).await;
    assert_eq!(pigeonhole_blob::Failure::of(&gone.err().unwrap()), pigeonhole_blob::Failure::NotFound, "dead after reopen");
    assert_eq!(reopened.candidates(None, u64::MAX, 100).await.unwrap(), vec![ids[3].key, ids[4].key, ids[5].key]);
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
        let kept = a.put(Bytes::from_static(b"kept"), "x").await.unwrap();
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
        let next = b.put(Bytes::from_static(b"next"), "x").await.unwrap();
        assert_eq!(next.key, crate::segments::key_of(1, len), "the torn bytes were cut off");
        assert_eq!(read(&on(&fs), &next).await, b"next");
    }

    #[tokio::test]
    async fn an_unsynced_segment_is_gone_after_a_crash() {
        let fs = SimFs::new(Duration::ZERO);
        let a = on(&fs);
        a.put(Bytes::from_static(b"one"), "x").await.unwrap();
        // A segment created but never synced (the process died right after).
        crate::io::Fs::open(&fs, std::path::Path::new("/d/seg-0000000002"), true).unwrap();
        drop(a);
        fs.crash(None);
        assert_eq!(on(&fs).candidates(None, u64::MAX, 10).await.unwrap().len(), 1);
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

    #[tokio::test(start_paused = true)]
    async fn a_sync_costs_virtual_time() {
        let fs = SimFs::new(Duration::from_millis(4));
        let a = on(&fs);
        let t = tokio::time::Instant::now();
        a.put(Bytes::from_static(b"one"), "x").await.unwrap();
        assert!(t.elapsed() >= Duration::from_millis(4), "{:?}", t.elapsed());
    }
}
