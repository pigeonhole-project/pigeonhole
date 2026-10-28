//! `local` LogStore: a directory of segmented append-only files.
//!
//! ```text
//! <dir>/lock                  flock'ed around every claim / append / rebase
//! <dir>/log_id                identity of this history, written by the first claim
//! <dir>/claimed_gen           newest claimed generation (tmp + fsync + rename)
//! <dir>/base                  postcard [`Base`]: where the log starts
//! <dir>/vote                  the last saved vote (tmp + fsync + rename)
//! <dir>/seg-<seq>.log         records, in write order; <seq> grows by one per segment
//! ```
//!
//! A record is `MAGIC | len | crc32 | Batch` (little endian u32s). An append
//! re-reads `claimed_gen` under the lock, writes one record and fsyncs before
//! returning, so after a claim no older generation can acknowledge anything.
//! A batch replaces whatever the log held from its `first` on, so a truncate
//! is a record too; the log is the result of applying every record in order.
//! A torn record at the very end of the last segment was never acknowledged;
//! `claim` truncates it. Anything else that does not parse is [`LogError::Lost`].
//! Fences only processes on this host; durability = the disk.

use super::{
    Base, Batch, Discovered, Entry, EntryStream, Head, LogError, LogId, LogResult, LogStore, Position,
};
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::BTreeMap;
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
    /// Last segment open for append: file, length, sequence number.
    seg: Option<(File, u64, u64)>,
}

/// The log as the records on disk make it.
struct Walk {
    head: Position,
    /// Term of every entry after the base, in order.
    terms: Vec<(Position, u64)>,
    /// Segments in order with the highest position any of their records reached.
    segs: Vec<(u64, PathBuf, Position)>,
    /// Entries after the `collect_from` the walk was asked for.
    entries: BTreeMap<Position, Entry>,
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
        self.blocking(move |s| s.write(gen, None, entries)).await
    }

    async fn truncate(&self, gen: u64, from: Position) -> LogResult<()> {
        self.blocking(move |s| s.write(gen, Some(from), Vec::new()).map(|_| ())).await
    }

    async fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        let vote = vote.to_vec();
        self.blocking(move |s| s.save_vote(gen, &vote)).await
    }

    async fn confirm(&self, gen: u64) -> LogResult<()> {
        self.blocking(move |s| {
            let _lock = s.lock()?;
            s.check_owner(gen)
        })
        .await
    }

    async fn discover(&self) -> LogResult<Option<Discovered>> {
        self.blocking(|s| s.discover()).await
    }

    fn read(&self, from: Position, head: Head) -> EntryStream {
        let shared = self.shared.clone();
        // The whole tail after `from` is assembled first: a later record may
        // replace entries of an earlier one (a truncate), so nothing is final
        // before the last record is read. Snapshots bound its size.
        let fut = async move {
            tokio::task::spawn_blocking(move || shared.read_range(from, head))
                .await
                .map_err(|e| LogError::Other(anyhow!("log task: {e}")))?
        };
        Box::pin(
            futures::stream::once(fut)
                .map(|r| match r {
                    Ok(items) => futures::stream::iter(items.into_iter().map(Ok).collect::<Vec<_>>()),
                    Err(e) => futures::stream::iter(vec![Err(e)]),
                })
                .flatten(),
        )
    }

    async fn rebase(&self, gen: u64, base: Base) -> LogResult<()> {
        self.blocking(move |s| s.rebase(gen, base)).await
    }
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

    /// The log's identity; the first claim of an empty directory creates it.
    fn log_id(&self, create: bool) -> LogResult<LogId> {
        match fs::read_to_string(self.dir.join("log_id")) {
            Ok(s) => s.parse().map_err(|e| LogError::Lost(format!("log_id {s:?}: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let empty = self.claimed_gen()?.is_none()
                    && self.segments()?.is_empty()
                    && self.base()?.is_none();
                if !(create && empty) {
                    return Err(LogError::Lost(format!("{}: no log_id", self.dir.display())));
                }
                let id = LogId::random();
                self.write_atomic("log_id", id.to_string().as_bytes())?;
                Ok(id)
            }
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

    fn segments(&self) -> LogResult<Vec<(u64, PathBuf)>> {
        numbered(&self.dir, "seg-", ".log")
    }

    fn base(&self) -> LogResult<Option<Base>> {
        match fs::read(self.dir.join("base")) {
            Ok(data) => Ok(Some(postcard::from_bytes(&data).map_err(|e| LogError::Lost(format!("base: {e}")))?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn vote(&self) -> LogResult<Option<Vec<u8>>> {
        match fs::read(self.dir.join("vote")) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Apply every record in order. With `repair`, a torn record at the end
    /// of the last segment is truncated away; entries after `collect_from`
    /// are kept in [`Walk::entries`].
    fn walk(&self, repair: bool, collect_from: Option<Position>) -> LogResult<Walk> {
        let base = self.base()?;
        let base_at = base.as_ref().map_or(0, |b| b.at);
        let mut w = Walk { head: base_at, terms: Vec::new(), segs: Vec::new(), entries: BTreeMap::new() };
        let segs = self.segments()?;
        let mut last_gen = 0;
        for (i, (seq, path)) in segs.iter().enumerate() {
            let last_seg = i + 1 == segs.len();
            let data = fs::read(path)?;
            let mut off = 0;
            let mut seg_max = 0;
            while off < data.len() {
                let (batch, len) = match decode_record(&data[off..]) {
                    Record::Ok(batch, len) => (batch, len),
                    Record::Torn if last_seg => {
                        if repair {
                            let f = OpenOptions::new().write(true).open(path)?;
                            f.set_len(off as u64)?;
                            f.sync_all()?;
                        }
                        break;
                    }
                    Record::Torn | Record::Corrupt => {
                        return Err(LogError::Lost(format!("{}: bad record at offset {off}", path.display())));
                    }
                    Record::Unreadable(why) => return Err(LogError::Lost(format!("{}: {why}", path.display()))),
                };
                off += len;
                if batch.gen < last_gen {
                    return Err(LogError::Lost(format!(
                        "{}: generation {} after {last_gen}",
                        path.display(),
                        batch.gen
                    )));
                }
                last_gen = batch.gen;
                if batch.first == 0 || batch.first > w.head + 1 {
                    return Err(LogError::Lost(format!(
                        "{}: batch at {} after head {}",
                        path.display(),
                        batch.first,
                        w.head
                    )));
                }
                let end = batch.first - 1 + batch.entries.len() as u64;
                w.terms.retain(|(p, _)| *p < batch.first);
                if let Some(from) = collect_from {
                    let first = batch.first;
                    let _ = w.entries.split_off(&first.max(from + 1));
                }
                for (k, entry) in batch.entries.into_iter().enumerate() {
                    let pos = batch.first + k as u64;
                    if pos > base_at {
                        w.terms.push((pos, entry.term));
                        if collect_from.is_some_and(|from| pos > from) {
                            w.entries.insert(pos, entry);
                        }
                    }
                }
                w.head = end.max(base_at);
                seg_max = seg_max.max(end);
            }
            w.segs.push((*seq, path.clone(), seg_max));
        }
        Ok(w)
    }

    fn head_term(&self, w: &Walk) -> LogResult<u64> {
        Ok(match w.terms.last() {
            Some((_, t)) => *t,
            None => self.base()?.map_or(0, |b| b.term),
        })
    }

    fn claim(&self, gen: u64) -> LogResult<Head> {
        let _lock = self.lock()?;
        if let Some(current) = self.claimed_gen()? {
            if gen <= current {
                return Err(LogError::Fenced { current });
            }
        }
        let log_id = self.log_id(true)?;
        self.write_atomic("claimed_gen", gen.to_string().as_bytes())?;
        let head = self.walk(true, None)?.head;
        let seg = match self.segments()?.pop() {
            Some((seq, path)) => {
                let f = OpenOptions::new().append(true).open(path)?;
                let len = f.metadata()?.len();
                Some((f, len, seq))
            }
            None => None,
        };
        *self.owner.lock().unwrap() = Some(Owner { gen, head, seg });
        Ok(Head { log_id, position: head })
    }

    /// Check under the lock that `gen` still holds the log and this handle
    /// claimed it.
    fn check_owner(&self, gen: u64) -> LogResult<()> {
        let current = self.claimed_gen()?.unwrap_or(0);
        if current != gen {
            if current > gen {
                return Err(LogError::Fenced { current });
            }
            return Err(LogError::Other(anyhow!("write for unclaimed generation {gen}")));
        }
        Ok(())
    }

    /// Append `entries` after the head, or (`from`) truncate to `from - 1`.
    fn write(&self, gen: u64, from: Option<Position>, entries: Vec<Entry>) -> LogResult<Position> {
        let _lock = self.lock()?;
        self.check_owner(gen)?;
        let base_at = self.base()?.map_or(0, |b| b.at);
        let mut owner = self.owner.lock().unwrap();
        let o = owner
            .as_mut()
            .filter(|o| o.gen == gen)
            .ok_or_else(|| LogError::Other(anyhow!("generation {gen} not claimed by this handle")))?;
        let first = match from {
            None => o.head + 1,
            Some(from) => {
                if from <= base_at || from > o.head + 1 {
                    return Err(LogError::Other(anyhow!("truncate at {from} outside ({base_at}, {}]", o.head + 1)));
                }
                from
            }
        };

        // Roll over only for a record with entries: a segment holding only
        // zero entries and barriers keeps growing.
        let roll = o.seg.as_ref().is_none_or(|(_, len, _)| *len >= self.segment_bytes && !entries.is_empty());
        if roll {
            let seq = o.seg.as_ref().map_or_else(
                || self.segments().map(|s| s.last().map_or(1, |(n, _)| n + 1)),
                |(_, _, n)| Ok(n + 1),
            )?;
            let path = self.dir.join(format!("seg-{seq:020}.log"));
            let f = OpenOptions::new().create_new(true).append(true).open(&path)?;
            self.sync_dir()?;
            o.seg = Some((f, 0, seq));
        }

        let n = entries.len() as u64;
        let record = encode_record(&Batch::new(gen, first, entries));
        let (f, len, _) = o.seg.as_mut().unwrap();
        let write = f.write_all(&record).and_then(|()| f.sync_data());
        if let Err(e) = write {
            // The record may be partially on disk; only a new claim can repair it.
            *owner = None;
            return Err(e.into());
        }
        *len += record.len() as u64;
        o.head = first - 1 + n;
        Ok(o.head)
    }

    fn save_vote(&self, gen: u64, vote: &[u8]) -> LogResult<()> {
        let _lock = self.lock()?;
        self.check_owner(gen)?;
        self.write_atomic("vote", vote)
    }

    fn discover(&self) -> LogResult<Option<Discovered>> {
        let _lock = self.lock()?;
        let Some(gen) = self.claimed_gen()? else {
            return Ok(None);
        };
        let log_id = self.log_id(false)?;
        let w = self.walk(false, None)?;
        Ok(Some(Discovered {
            base: self.base()?,
            head: Head { log_id, position: w.head },
            head_term: self.head_term(&w)?,
            vote: self.vote()?,
            gen,
        }))
    }

    fn read_range(&self, from: Position, head: Head) -> LogResult<Vec<(Position, Entry)>> {
        let _lock = self.lock()?;
        if self.log_id(false)? != head.log_id {
            return Err(LogError::Lost(format!("log {} is not this one", head.log_id)));
        }
        let base_at = self.base()?.map_or(0, |b| b.at);
        if from < base_at {
            return Err(LogError::Lost(format!("entries after {from} collected up to base {base_at}")));
        }
        let w = self.walk(false, Some(from))?;
        if head.position > w.head {
            return Err(LogError::Lost(format!("log ends at {}, before {}", w.head, head.position)));
        }
        let out: Vec<_> = w.entries.into_iter().filter(|(p, _)| *p <= head.position).collect();
        let want = head.position.saturating_sub(from) as usize;
        if out.len() != want {
            return Err(LogError::Lost(format!("entries ({from}, {}] incomplete", head.position)));
        }
        Ok(out)
    }

    fn rebase(&self, gen: u64, base: Base) -> LogResult<()> {
        let _lock = self.lock()?;
        let current = self.claimed_gen()?.unwrap_or(0);
        if current != gen {
            return Err(LogError::Fenced { current });
        }
        let w = self.walk(false, None)?;
        let old = self.base()?.map_or(0, |b| b.at);
        let at = base.at;
        if at < old {
            return Err(LogError::Other(anyhow!("rebase at {at} behind the base {old}")));
        }
        self.write_atomic("base", &postcard::to_stdvec(&base).expect("a base always encodes"))?;
        if let Some(o) = self.owner.lock().unwrap().as_mut().filter(|o| o.gen == gen) {
            o.head = o.head.max(at);
        }
        // Leading segments whose every record ends at or before the base are
        // garbage; keep the last one (it is open for append).
        let keep_from = w.segs.iter().position(|(_, _, max)| *max > at).unwrap_or(w.segs.len());
        let keep_from = keep_from.min(w.segs.len().saturating_sub(1));
        for (_, path, _) in &w.segs[..keep_from] {
            fs::remove_file(path)?;
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
    let payload = batch.encode();
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
    /// Intact (checksum matches) but not a batch this binary reads, e.g.
    /// another log format.
    Unreadable(String),
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
        Err(LogError::Lost(why)) => Record::Unreadable(why),
        Err(e) => Record::Unreadable(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    fn entry(i: u64) -> Entry {
        Entry { term: 1, payload: i.to_le_bytes().to_vec() }
    }

    fn base(at: Position) -> Base {
        Base { at, term: 1, leader: 0, snapshot: crate::snapshot::SnapshotRef(format!("s{at}")) }
    }

    async fn read_all(log: &dyn LogStore, from: Position, head: Position) -> LogResult<Vec<(Position, Entry)>> {
        let log_id = log.discover().await?.expect("claimed").head.log_id;
        log.read(from, Head { log_id, position: head }).try_collect().await
    }

    #[tokio::test]
    async fn append_read_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::with_segment_bytes(dir.path(), 64).unwrap();
        assert_eq!(log.discover().await.unwrap(), None);
        assert_eq!(log.claim(1).await.unwrap().position, 0);
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
        assert_eq!((d.gen, d.head.position, d.base), (1, 20, None));
        let head = again.claim(2).await.unwrap();
        assert_eq!((head.log_id, head.position), (d.head.log_id, 20), "identity survives claims");
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
        assert_eq!(b.claim(2).await.unwrap().position, 1, "acknowledged entry ≤ head");
        assert!(matches!(a.append(1, &[entry(2)]).await, Err(LogError::Fenced { current: 2 })));
        assert!(matches!(
            a.rebase(1, base(1)).await,
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
        let mut torn = encode_record(&Batch::new(1, 2, vec![entry(2)]));
        torn.truncate(torn.len() - 3);
        OpenOptions::new().append(true).open(&seg).unwrap().write_all(&torn).unwrap();

        let log = LocalLogStore::open(dir.path()).unwrap();
        assert_eq!(log.discover().await.unwrap().unwrap().head.position, 1);
        assert_eq!(log.claim(2).await.unwrap().position, 1);
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
    async fn rebase_collects_segments_behind_it() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
        log.claim(1).await.unwrap();
        for i in 1..=6 {
            log.append(1, &[entry(i)]).await.unwrap();
        }
        assert_eq!(log.shared.segments().unwrap().len(), 6);
        log.rebase(1, base(4)).await.unwrap();
        let seqs: Vec<_> = log.shared.segments().unwrap().into_iter().map(|(n, _)| n).collect();
        assert_eq!(seqs, vec![5, 6], "segments wholly behind the base are gone");

        let d = log.discover().await.unwrap().unwrap();
        assert_eq!(d.base, Some(base(4)));
        assert!(log.rebase(1, base(3)).await.is_err(), "a base never moves back");
        assert_eq!(d.head.position, 6);
        assert_eq!(read_all(&log, 4, 6).await.unwrap(), vec![(5, entry(5)), (6, entry(6))]);
        assert!(matches!(read_all(&log, 2, 6).await, Err(LogError::Lost(_))));

        let log = LocalLogStore::open(dir.path()).unwrap();
        assert_eq!(log.claim(2).await.unwrap().position, 6);
    }

    #[tokio::test]
    async fn zero_entries_of_many_generations_share_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        for gen in 1..=3 {
            let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
            assert_eq!(log.claim(gen).await.unwrap().position, 0);
            assert_eq!(log.append(gen, &[]).await.unwrap(), 0);
        }
        let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
        log.claim(4).await.unwrap();
        assert_eq!(log.append(4, &[entry(1)]).await.unwrap(), 1);
        assert_eq!(log.append(4, &[entry(2)]).await.unwrap(), 2);
        let seqs: Vec<_> = log.shared.segments().unwrap().into_iter().map(|(n, _)| n).collect();
        assert_eq!(seqs, vec![1, 2, 3], "zero entries share the first segment");
        assert_eq!(read_all(&log, 0, 2).await.unwrap(), vec![(1, entry(1)), (2, entry(2))]);
    }

    #[tokio::test]
    async fn log_id_is_new_for_a_new_log_and_required_for_an_old_one() {
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let a = LocalLogStore::open(d1.path()).unwrap();
        let b = LocalLogStore::open(d2.path()).unwrap();
        let (ha, hb) = (a.claim(1).await.unwrap(), b.claim(1).await.unwrap());
        assert_ne!(ha.log_id, hb.log_id);

        a.append(1, &[entry(1)]).await.unwrap();
        fs::remove_file(d1.path().join("log_id")).unwrap();
        assert!(matches!(a.claim(2).await, Err(LogError::Lost(_))));
        assert!(matches!(a.discover().await, Err(LogError::Lost(_))));
    }

    #[tokio::test]
    async fn batches_of_another_log_format_are_lost() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::open(dir.path()).unwrap();
        log.claim(1).await.unwrap();
        log.append(1, &[entry(1)]).await.unwrap();
        let (_, seg) = log.shared.segments().unwrap().pop().unwrap();
        let mut future = Batch::new(1, 2, vec![entry(2)]);
        future.format = crate::LOG_FORMAT + 1;
        let mut tail = encode_record(&future);
        tail.extend_from_slice(&encode_record(&Batch::new(1, 3, vec![entry(3)])));
        OpenOptions::new().append(true).open(&seg).unwrap().write_all(&tail).unwrap();

        let err = log.discover().await.unwrap_err();
        let want = format!("log format {}, this binary reads {}", crate::LOG_FORMAT + 1, crate::LOG_FORMAT);
        assert!(err.to_string().contains(&want), "{err}");
    }

    #[tokio::test]
    async fn rebase_inside_a_segment_keeps_it_readable() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::open(dir.path()).unwrap();
        log.claim(1).await.unwrap();
        for i in 1..=6 {
            log.append(1, &[entry(i)]).await.unwrap();
        }
        // One segment holds everything; the base covers half of it.
        log.rebase(1, base(3)).await.unwrap();
        let d = log.discover().await.unwrap().unwrap();
        assert_eq!(d.head.position, 6);
        assert_eq!(read_all(&log, 3, 6).await.unwrap().len(), 3);

        let log = LocalLogStore::open(dir.path()).unwrap();
        assert_eq!(log.claim(2).await.unwrap().position, 6);
        assert_eq!(log.append(2, &[]).await.unwrap(), 6);
        assert_eq!(log.append(2, &[entry(7)]).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn a_truncate_reaches_into_older_segments() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLogStore::with_segment_bytes(dir.path(), 1).unwrap();
        log.claim(1).await.unwrap();
        for i in 1..=6 {
            log.append(1, &[entry(i)]).await.unwrap();
        }
        // Positions 3..=6 sit in four older segments; the truncate is a record
        // in the newest one and replaces them all.
        log.truncate(1, 3).await.unwrap();
        let other = |i: u64| Entry { term: 2, payload: i.to_le_bytes().to_vec() };
        assert_eq!(log.append(1, &[other(30)]).await.unwrap(), 3);
        let want = vec![(1, entry(1)), (2, entry(2)), (3, other(30))];
        assert_eq!(read_all(&log, 0, 3).await.unwrap(), want);

        log.rebase(1, base(2)).await.unwrap();
        let again = LocalLogStore::open(dir.path()).unwrap();
        let d = again.discover().await.unwrap().unwrap();
        assert_eq!((d.head.position, d.head_term), (3, 2));
        assert_eq!(read_all(&again, 2, 3).await.unwrap(), vec![(3, other(30))]);
        assert_eq!(again.claim(2).await.unwrap().position, 3);
        assert_eq!(again.append(2, &[entry(4)]).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn passes_the_logstore_conformance_suite() {
        crate::log::conformance::run_all(|| {
            let dir = Arc::new(tempfile::tempdir().unwrap());
            move || {
                let dir = dir.clone();
                async move { Arc::new(LocalLogStore::open(dir.path()).unwrap()) as Arc<dyn LogStore> }
            }
        })
        .await;
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
