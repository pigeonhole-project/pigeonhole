//! `local` LogStore: a directory of segmented append-only files.
//!
//! ```text
//! <dir>/lock                  flock'ed around every claim / append / snapshot
//! <dir>/claimed_gen           newest claimed generation (tmp + fsync + rename)
//! <dir>/seg-<first>.log       records; <first> = position of its first entry
//! <dir>/snapshot-<at>.snap    newest snapshot; segments wholly ≤ <at> are dropped
//! ```
//!
//! A record is `MAGIC | len | crc32 | Batch` (little endian u32s). An append
//! re-reads `claimed_gen` under the lock, writes one record and fsyncs before
//! returning, so after a claim no older generation can acknowledge anything.
//! A torn record at the very end of the last segment was never acknowledged;
//! `claim` truncates it. Anything else that does not parse is [`LogError::Lost`].
//! Fences only processes on this host; durability = the disk.

use super::pb::Batch;
use super::{Discovered, Entry, EntryStream, Head, LogError, LogResult, LogStore, Position, Snapshot};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use prost::Message;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAGIC: u32 = 0x5048_4c47; // "PHLG"
const HEADER: usize = 12;

/// Default segment size before rolling over to a new file.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 << 20;

pub struct LocalLogStore {
    shared: Arc<Shared>,
}

struct Shared {
    dir: PathBuf,
    segment_bytes: u64,
    /// Tail of the log as seen by the generation this handle claimed.
    owner: Mutex<Option<Owner>>,
}

struct Owner {
    gen: u64,
    head: Position,
    /// Last segment open for append: file, length, first position. `None` on
    /// an empty log; a new segment starts at `head + 1`.
    seg: Option<(File, u64, Position)>,
}

impl LocalLogStore {
    /// Open (or create) the log directory.
    pub fn open(dir: impl Into<PathBuf>) -> LogResult<Self> {
        Self::with_segment_bytes(dir, DEFAULT_SEGMENT_BYTES)
    }

    pub fn with_segment_bytes(dir: impl Into<PathBuf>, segment_bytes: u64) -> LogResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).with_context(|| format!("create log dir {}", dir.display()))?;
        Ok(Self {
            shared: Arc::new(Shared {
                dir,
                segment_bytes: segment_bytes.max(1),
                owner: Mutex::new(None),
            }),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.shared.dir
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Shared) -> LogResult<T> + Send + 'static,
    ) -> LogResult<T> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || f(&shared))
            .await
            .map_err(|e| LogError::Other(anyhow!("log task: {e}")))?
    }
}

#[async_trait]
impl LogStore for LocalLogStore {
    async fn claim(&self, gen: u64) -> LogResult<Head> {
        self.blocking(move |s| s.claim(gen)).await
    }

    async fn append(&self, gen: u64, batch: &[Entry]) -> LogResult<Position> {
        let entries = batch.to_vec();
        self.blocking(move |s| s.append(gen, entries)).await
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        self.blocking(|s| s.discover()).await
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let shared = self.shared.clone();
        // One segment per step, so replay never holds the whole log in memory.
        let state = ReadState { from, next: from + 1, head: head.position, segs: None };
        let steps = futures::stream::unfold(Some(state), move |state| {
            let shared = shared.clone();
            async move {
                let mut state = state?;
                if state.next > state.head {
                    return None;
                }
                let step = tokio::task::spawn_blocking(move || {
                    let r = shared.read_step(&mut state);
                    (r, state)
                })
                .await;
                let (items, state) = match step {
                    Ok((Ok(entries), state)) => (entries.into_iter().map(Ok).collect(), Some(state)),
                    Ok((Err(e), _)) => (vec![Err(e)], None),
                    Err(e) => (vec![Err(LogError::Other(anyhow!("log task: {e}")))], None),
                };
                Some((futures::stream::iter(items), state))
            }
        });
        Box::pin(steps.flatten())
    }

    async fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> LogResult<()> {
        self.blocking(move |s| s.install_snapshot(gen, at, snapshot)).await
    }
}

struct ReadState {
    from: Position,
    next: Position,
    head: Position,
    /// Segments not yet read, oldest first.
    segs: Option<Vec<(Position, PathBuf)>>,
}

impl Shared {
    fn lock(&self) -> LogResult<File> {
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.dir.join("lock"))?;
        f.lock()?;
        Ok(f)
    }

    fn claimed_gen(&self) -> LogResult<Option<u64>> {
        match fs::read_to_string(self.dir.join("claimed_gen")) {
            Ok(s) => Ok(Some(s.trim().parse().map_err(|e| {
                LogError::Lost(format!("claimed_gen {s:?}: {e}"))
            })?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn sync_dir(&self) -> LogResult<()> {
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Write `name` atomically: tmp file, fsync, rename, fsync the directory.
    fn write_atomic(&self, name: &str, data: &[u8]) -> LogResult<()> {
        let tmp = self.dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        fs::rename(&tmp, self.dir.join(name))?;
        self.sync_dir()
    }

    fn segments(&self) -> LogResult<Vec<(Position, PathBuf)>> {
        numbered(&self.dir, "seg-", ".log")
    }

    fn snapshot_at(&self) -> LogResult<Option<(Position, PathBuf)>> {
        Ok(numbered(&self.dir, "snapshot-", ".snap")?.pop())
    }

    /// Validate the whole log and return its head. With `repair`, a torn
    /// record at the end of the last segment is truncated away.
    fn scan(&self, repair: bool) -> LogResult<Position> {
        let mut head = self.snapshot_at()?.map_or(0, |(at, _)| at);
        let segs = self.segments()?;
        let mut last_gen = 0;
        for (i, (first, path)) in segs.iter().enumerate() {
            let last_seg = i + 1 == segs.len();
            if *first > head + 1 {
                return Err(LogError::Lost(format!("hole before {}", path.display())));
            }
            let data = fs::read(path)?;
            let mut off = 0;
            let mut seen_first = None;
            while off < data.len() {
                match decode_record(&data[off..]) {
                    Record::Ok(batch, len) => {
                        if batch.gen < last_gen {
                            return Err(LogError::Lost(format!(
                                "{}: generation {} after {last_gen}",
                                path.display(),
                                batch.gen
                            )));
                        }
                        last_gen = batch.gen;
                        seen_first.get_or_insert(batch.first);
                        // Segments wholly behind a snapshot overlap it; skip them.
                        if batch.first + batch.entries.len() as u64 > head {
                            if batch.first != head + 1 {
                                return Err(LogError::Lost(format!(
                                    "{}: batch at {} after head {head}",
                                    path.display(),
                                    batch.first
                                )));
                            }
                            head += batch.entries.len() as u64;
                        }
                        off += len;
                    }
                    Record::Torn if last_seg => {
                        if repair {
                            let f = OpenOptions::new().write(true).open(path)?;
                            f.set_len(off as u64)?;
                            f.sync_all()?;
                        }
                        break;
                    }
                    Record::Torn | Record::Corrupt => {
                        return Err(LogError::Lost(format!(
                            "{}: bad record at offset {off}",
                            path.display()
                        )));
                    }
                }
            }
            if let Some(f) = seen_first {
                if f != *first {
                    return Err(LogError::Lost(format!(
                        "{}: first batch at {f}",
                        path.display()
                    )));
                }
            }
        }
        Ok(head)
    }

    fn claim(&self, gen: u64) -> LogResult<Head> {
        let _lock = self.lock()?;
        if let Some(current) = self.claimed_gen()? {
            if gen <= current {
                return Err(LogError::Fenced { current });
            }
        }
        self.write_atomic("claimed_gen", gen.to_string().as_bytes())?;
        let head = self.scan(true)?;
        let seg = match self.segments()?.pop() {
            Some((first, path)) => {
                let f = OpenOptions::new().append(true).open(path)?;
                let len = f.metadata()?.len();
                Some((f, len, first))
            }
            None => None,
        };
        *self.owner.lock().unwrap() = Some(Owner { gen, head, seg });
        Ok(Head { position: head })
    }

    fn append(&self, gen: u64, entries: Vec<Entry>) -> LogResult<Position> {
        let _lock = self.lock()?;
        let current = self.claimed_gen()?.unwrap_or(0);
        if current != gen {
            if current > gen {
                return Err(LogError::Fenced { current });
            }
            return Err(LogError::Other(anyhow!("append for unclaimed generation {gen}")));
        }
        let mut owner = self.owner.lock().unwrap();
        let o = owner
            .as_mut()
            .filter(|o| o.gen == gen)
            .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;

        // Roll over only if the new segment gets a fresh name: a segment that
        // holds only zero entries keeps growing.
        let roll = o
            .seg
            .as_ref()
            .is_none_or(|(_, len, first)| *len >= self.segment_bytes && o.head + 1 > *first);
        if roll {
            let path = self.dir.join(format!("seg-{:020}.log", o.head + 1));
            let f = OpenOptions::new().create_new(true).append(true).open(&path)?;
            self.sync_dir()?;
            o.seg = Some((f, 0, o.head + 1));
        }

        let n = entries.len() as u64;
        let record = encode_record(&Batch { gen, first: o.head + 1, entries });
        let (f, len, _) = o.seg.as_mut().unwrap();
        let write = f.write_all(&record).and_then(|()| f.sync_data());
        if let Err(e) = write {
            // The record may be partially on disk; only a new claim can repair it.
            *owner = None;
            return Err(e.into());
        }
        *len += record.len() as u64;
        o.head += n;
        Ok(o.head)
    }

    fn discover(&self) -> LogResult<Option<Discovered>> {
        let _lock = self.lock()?;
        let Some(gen) = self.claimed_gen()? else {
            return Ok(None);
        };
        let head = self.scan(false)?;
        let snapshot = match self.snapshot_at()? {
            Some((at, path)) => Some((at, Snapshot { data: fs::read(path)? })),
            None => None,
        };
        Ok(Some(Discovered { snapshot, head: Head { position: head }, gen }))
    }

    fn read_step(&self, st: &mut ReadState) -> LogResult<Vec<(Position, Entry)>> {
        if st.segs.is_none() {
            let snap = self.snapshot_at()?.map_or(0, |(at, _)| at);
            if st.from < snap {
                return Err(LogError::Lost(format!(
                    "entries after {} collected up to snapshot {snap}",
                    st.from
                )));
            }
            let mut segs = self.segments()?;
            // Keep the segment containing `next` and everything after it.
            let start = segs.iter().rposition(|(first, _)| *first <= st.next).unwrap_or(0);
            segs.drain(..start);
            segs.reverse();
            st.segs = Some(segs);
        }
        let Some((_, path)) = st.segs.as_mut().unwrap().pop() else {
            return Err(LogError::Lost(format!("log ends before position {}", st.next)));
        };
        let data = fs::read(&path)?;
        let mut out = Vec::new();
        let mut off = 0;
        while off < data.len() && st.next <= st.head {
            let Record::Ok(batch, len) = decode_record(&data[off..]) else {
                return Err(LogError::Lost(format!(
                    "{}: bad record at offset {off}",
                    path.display()
                )));
            };
            off += len;
            for (i, entry) in batch.entries.into_iter().enumerate() {
                let pos = batch.first + i as u64;
                if pos == st.next && pos <= st.head {
                    out.push((pos, entry));
                    st.next += 1;
                }
            }
        }
        Ok(out)
    }

    fn install_snapshot(&self, gen: u64, at: Position, snapshot: Snapshot) -> LogResult<()> {
        let _lock = self.lock()?;
        let current = self.claimed_gen()?.unwrap_or(0);
        if current != gen {
            return Err(LogError::Fenced { current });
        }
        let head = self.scan(false)?;
        if at > head {
            return Err(LogError::Other(anyhow!("snapshot at {at} beyond head {head}")));
        }
        let old = numbered(&self.dir, "snapshot-", ".snap")?;
        self.write_atomic(&format!("snapshot-{at:020}.snap"), &snapshot.data)?;
        for (pos, path) in old {
            if pos < at {
                fs::remove_file(path)?;
            }
        }
        // A segment is garbage once the next one starts at or before `at + 1`.
        let segs = self.segments()?;
        for pair in segs.windows(2) {
            if pair[1].0 <= at + 1 {
                fs::remove_file(&pair[0].1)?;
            }
        }
        self.sync_dir()
    }
}

/// Files `<prefix><number><suffix>` in `dir`, sorted by number.
fn numbered(dir: &Path, prefix: &str, suffix: &str) -> LogResult<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name();
        let Some(n) = name
            .to_str()
            .and_then(|n| n.strip_prefix(prefix))
            .and_then(|n| n.strip_suffix(suffix))
            .and_then(|n| n.parse().ok())
        else {
            continue;
        };
        out.push((n, e.path()));
    }
    out.sort_unstable_by_key(|(n, _)| *n);
    Ok(out)
}

fn encode_record(batch: &Batch) -> Vec<u8> {
    let payload = batch.encode_to_vec();
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

enum Record {
    Ok(Batch, usize),
    /// Ends past the end of the data or fails its checksum as the last record.
    Torn,
    Corrupt,
}

fn decode_record(data: &[u8]) -> Record {
    if data.len() < HEADER {
        return Record::Torn;
    }
    let word = |i: usize| u32::from_le_bytes(data[i..i + 4].try_into().unwrap());
    if word(0) != MAGIC {
        return Record::Corrupt;
    }
    let len = word(4) as usize;
    let Some(payload) = data.get(HEADER..HEADER + len) else {
        return Record::Torn;
    };
    if crc32fast::hash(payload) != word(8) {
        return if data.len() == HEADER + len { Record::Torn } else { Record::Corrupt };
    }
    match Batch::decode(payload) {
        Ok(b) => Record::Ok(b, HEADER + len),
        Err(_) => Record::Corrupt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn entry(i: u64) -> Entry {
        Entry { namespace: "t".into(), command: i.to_le_bytes().to_vec() }
    }

    async fn read_all(log: &dyn LogStore, from: Position, head: Position) -> LogResult<Vec<(Position, Entry)>> {
        log.read(from, Head { position: head }).try_collect().await
    }

    #[tokio::test]
    async fn append_read_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::with_segment_bytes(dir.path(), 64).unwrap();
        assert_eq!(log.discover().await.unwrap(), None);
        assert_eq!(log.claim(1).await.unwrap(), Head { position: 0 });
        assert_eq!(log.append(1, &[]).await.unwrap(), 0, "zero entry takes no position");
        let mut pos = 0;
        for i in 1..=10 {
            pos = log.append(1, &[entry(2 * i - 1), entry(2 * i)]).await.unwrap();
        }
        assert_eq!(pos, 20);
        assert!(log.shared.segments().unwrap().len() > 1, "small segments roll over");

        let got = read_all(&log, 5, 12).await.unwrap();
        assert_eq!(got, (6..=12).map(|i| (i, entry(i))).collect::<Vec<_>>());

        let again = LocalLogStore::open(dir.path()).unwrap();
        let d = again.discover().await.unwrap().unwrap();
        assert_eq!((d.gen, d.head, d.snapshot), (1, Head { position: 20 }, None));
        assert_eq!(again.claim(2).await.unwrap(), Head { position: 20 });
        assert_eq!(again.append(2, &[entry(21)]).await.unwrap(), 21);
        assert_eq!(read_all(&again, 0, 21).await.unwrap().len(), 21);
    }

    #[tokio::test]
    async fn newer_claim_fences_older_generation() {
        let dir = tempfile::tempdir().unwrap();
        let a = LocalLogStore::open(dir.path()).unwrap();
        let b = LocalLogStore::open(dir.path()).unwrap();
        a.claim(1).await.unwrap();
        a.append(1, &[entry(1)]).await.unwrap();

        assert!(matches!(b.claim(1).await, Err(LogError::Fenced { current: 1 })));
        assert_eq!(b.claim(2).await.unwrap(), Head { position: 1 }, "acknowledged entry ≤ head");
        assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
        assert!(matches!(
            a.install_snapshot(1, 1, Snapshot { data: vec![] }).await,
            Err(LogError::Fenced { current: 2 })
        ));
        assert_eq!(b.append(2, &[entry(2)]).await.unwrap(), 2);
        assert_eq!(read_all(&b, 0, 2).await.unwrap(), vec![(1, entry(1)), (2, entry(2))]);
    }

    #[tokio::test]
    async fn claim_truncates_a_torn_tail_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::open(dir.path()).unwrap();
        log.claim(1).await.unwrap();
        log.append(1, &[entry(1)]).await.unwrap();
        let (_, seg) = log.shared.segments().unwrap().pop().unwrap();
        let mut torn = encode_record(&Batch { gen: 1, first: 2, entries: vec![entry(2)] });
        torn.truncate(torn.len() - 3);
        OpenOptions::new().append(true).open(&seg).unwrap().write_all(&torn).unwrap();

        let log = LocalLogStore::open(dir.path()).unwrap();
        assert_eq!(log.discover().await.unwrap().unwrap().head, Head { position: 1 });
        assert_eq!(log.claim(2).await.unwrap(), Head { position: 1 });
        assert_eq!(log.append(2, &[entry(2)]).await.unwrap(), 2);
        assert_eq!(read_all(&log, 0, 2).await.unwrap().len(), 2);

        // Flip a byte inside the first record: not a torn tail, the log is damaged.
        let mut data = fs::read(&seg).unwrap();
        data[HEADER + 2] ^= 0xff;
        fs::write(&seg, data).unwrap();
        assert!(matches!(log.discover().await, Err(LogError::Lost(_))));
        assert!(matches!(read_all(&log, 0, 2).await, Err(LogError::Lost(_))));
    }

    #[tokio::test]
    async fn snapshot_collects_segments_behind_it() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
        log.claim(1).await.unwrap();
        for i in 1..=6 {
            log.append(1, &[entry(i)]).await.unwrap();
        }
        assert_eq!(log.shared.segments().unwrap().len(), 6);
        log.install_snapshot(1, 4, Snapshot { data: b"s4".to_vec() }).await.unwrap();
        let firsts: Vec<_> = log.shared.segments().unwrap().into_iter().map(|(f, _)| f).collect();
        assert_eq!(firsts, vec![5, 6]);

        let d = log.discover().await.unwrap().unwrap();
        assert_eq!(d.snapshot, Some((4, Snapshot { data: b"s4".to_vec() })));
        assert_eq!(d.head, Head { position: 6 });
        assert_eq!(read_all(&log, 4, 6).await.unwrap(), vec![(5, entry(5)), (6, entry(6))]);
        assert!(matches!(read_all(&log, 2, 6).await, Err(LogError::Lost(_))));

        let log = LocalLogStore::open(dir.path()).unwrap();
        assert_eq!(log.claim(2).await.unwrap(), Head { position: 6 });
    }

    #[tokio::test]
    async fn zero_entries_of_many_generations_share_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        for gen in 1..=3 {
            let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
            assert_eq!(log.claim(gen).await.unwrap(), Head { position: 0 });
            assert_eq!(log.append(gen, &[]).await.unwrap(), 0);
        }
        let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
        log.claim(4).await.unwrap();
        assert_eq!(log.append(4, &[entry(1)]).await.unwrap(), 1);
        assert_eq!(log.append(4, &[entry(2)]).await.unwrap(), 2);
        let firsts: Vec<_> = log.shared.segments().unwrap().into_iter().map(|(f, _)| f).collect();
        assert_eq!(firsts, vec![1, 2]);
        assert_eq!(read_all(&log, 0, 2).await.unwrap(), vec![(1, entry(1)), (2, entry(2))]);
    }

    #[tokio::test]
    async fn read_past_the_end_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::open(dir.path()).unwrap();
        log.claim(1).await.unwrap();
        log.append(1, &[entry(1)]).await.unwrap();
        assert!(matches!(read_all(&log, 0, 3).await, Err(LogError::Lost(_))));
        assert_eq!(read_all(&log, 1, 1).await.unwrap(), vec![]);
    }
}
