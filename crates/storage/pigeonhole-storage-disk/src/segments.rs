//! Append-only segment files of records (Haystack / Bitcask style).
//!
//! ```text
//! <dir>/lock          flock'ed around every write and every chain decision
//! <dir>/deletes       how many deletes ran (u64): other processes re-read headers
//! <dir>/seg-<seq>     records in write order; <seq> grows by one per segment
//! ```
//!
//! A record is a fixed header, a label and the data:
//!
//! ```text
//! magic u32 | flags u8 | pad u8 | label_len u16 | data_len u64 | prev u64 | crc u32 | pad u32 | label | data
//! ```
//!
//! (little endian). `crc` covers the flags (without `DEAD`), the lengths,
//! `prev`, the label and the data. A record's key is its byte position,
//! `(seq << 40) | offset`: keys grow with every put, and a blob is read with
//! one `pread`, no index on disk.
//!
//! Every write holds the `flock` and first catches up with what another
//! process appended (the length of the last segment, a newer segment), so the
//! chain's "first successor wins" holds across processes. A delete sets
//! `DEAD` in the header and punches a hole over the data; a segment whose
//! records are all dead is removed. Only the segments a delete touched are
//! synced, and the directory once a segment went. A delete also bumps a
//! counter in `<dir>/deletes`: a process that finds it moved since it last
//! looked reads the headers of its live records again (and drops the
//! segments that are gone), so it never reads a hole as a blob.
//!
//! A torn record was never acknowledged (each append is synced before it is
//! answered, so nothing acknowledged follows one): at the end of the last
//! segment, anything that is not a whole record (a short or zeroed header,
//! one without its magic, a record past the end or failing its checksum) is
//! cut off by the next writer or open. A header without its magic that a
//! whole record follows is corruption, not a crash: an error.

use crate::io::{DirLock, Fs, FsFile};
use anyhow::{anyhow, bail, Context, Result};
use pigeonhole_storage_api::inspect::{RecordView, SegmentView};
use pigeonhole_storage_api::{BlobId, NotFound, Owner};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: u32 = 0x5048_5347; // "PHSG"
pub(crate) const HEADER: u64 = 32;
const OFFSET_BITS: u32 = 40;
const NO_PREV: u64 = u64::MAX;

/// The record is deleted (its data may be a hole).
const DEAD: u8 = 1;
/// The record is a chain member (put with `put_after`).
const CHAINED: u8 = 2;

/// Default segment size before rolling over to a new file.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 << 20;

pub(crate) fn key_of(seq: u64, offset: u64) -> u64 {
    (seq << OFFSET_BITS) | offset
}

fn split(key: u64) -> (u64, u64) {
    (key >> OFFSET_BITS, key & ((1 << OFFSET_BITS) - 1))
}

fn seg_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("seg-{seq:010}"))
}

/// The owner a record's name tells, if it names a blob.
fn owner_of(label: &str) -> Option<Owner> {
    BlobId::from_name(label).map(|b| b.owner())
}

/// One record as the index knows it.
#[derive(Debug, Clone)]
pub(crate) struct Rec {
    pub data_len: u64,
    label_len: u16,
    pub dead: bool,
    /// The owner a blob's name tells ([`BlobId::owner`]), if it names one.
    pub owner: Option<Owner>,
    /// Chain members only.
    pub chain: Option<ChainRec>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChainRec {
    pub prev: Option<u64>,
    pub label: String,
}

impl Rec {
    fn data_offset(&self, key: u64) -> u64 {
        split(key).1 + HEADER + self.label_len as u64
    }
}

struct Seg {
    file: Arc<dyn FsFile>,
    /// Bytes scanned (= the end of the last whole record).
    len: u64,
    live: usize,
}

/// The index of a directory of segments, kept in memory.
pub(crate) struct Segments {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
    segment_bytes: u64,
    lock: Arc<dyn DirLock>,
    /// The delete counter (`<dir>/deletes`) and its value when we last looked.
    deletes: Arc<dyn FsFile>,
    deletes_seen: u64,
    segs: BTreeMap<u64, Seg>,
    pub records: BTreeMap<u64, Rec>,
    /// Chain: the winning successor of each predecessor (`None`: the start).
    pub next: HashMap<Option<u64>, u64>,
    pub tail: Option<u64>,
}

/// The process-wide `flock`, held while it lives.
pub(crate) struct Locked<'a>(&'a mut Segments);

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        self.0.lock.unlock();
    }
}

impl std::ops::Deref for Locked<'_> {
    type Target = Segments;
    fn deref(&self) -> &Segments {
        self.0
    }
}

impl std::ops::DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut Segments {
        self.0
    }
}

impl Segments {
    pub fn open(fs: Arc<dyn Fs>, dir: &Path, segment_bytes: u64) -> Result<Self> {
        fs.create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let lock = fs.dir_lock(dir).with_context(|| format!("lock file in {}", dir.display()))?;
        let deletes = open_or_create(fs.as_ref(), &dir.join("deletes"))?;
        let mut this = Self {
            fs,
            dir: dir.to_path_buf(),
            segment_bytes: segment_bytes.max(1),
            lock,
            deletes,
            deletes_seen: 0,
            segs: BTreeMap::new(),
            records: BTreeMap::new(),
            next: HashMap::new(),
            tail: None,
        };
        {
            let mut locked = this.locked()?;
            locked.deletes_seen = read_counter(locked.deletes.as_ref())?;
            locked.catch_up()?;
        }
        Ok(this)
    }

    /// Take the `flock` (blocking).
    pub fn locked(&mut self) -> Result<Locked<'_>> {
        self.lock.lock().context("flock the segment directory")?;
        Ok(Locked(self))
    }

    pub fn file_of(&self, key: u64) -> Option<Arc<dyn FsFile>> {
        self.segs.get(&split(key).0).map(|s| s.file.clone())
    }

    /// Read `len` bytes of record `key`'s data at `from`.
    pub fn read(&self, key: u64, from: u64, len: u64) -> Result<Vec<u8>> {
        let rec = self.records.get(&key).filter(|r| !r.dead).ok_or_else(|| NotFound(format!("blob {key}")))?;
        if from + len > rec.data_len {
            bail!("byte range {from}..{} outside blob of {} bytes", from + len, rec.data_len);
        }
        let file = self.file_of(key).ok_or_else(|| anyhow!("no segment for blob {key}"))?;
        let mut buf = vec![0; len as usize];
        file.read_exact_at(&mut buf, rec.data_offset(key) + from)?;
        Ok(buf)
    }

    /// Whether another process deleted since we last looked (one read of
    /// the counter, without the lock): a reader then catches up.
    pub fn deletes_moved(&self) -> Result<bool> {
        Ok(read_counter(self.deletes.as_ref())? != self.deletes_seen)
    }

    /// Every segment sequence number in the directory.
    fn seqs_on_disk(&self) -> Result<Vec<u64>> {
        let mut seqs = Vec::new();
        for name in self.fs.list(&self.dir).with_context(|| format!("read {}", self.dir.display()))? {
            if let Some(seq) = name.strip_prefix("seg-").and_then(|n| n.parse().ok()) {
                seqs.push(seq);
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }
}

impl Locked<'_> {
    /// Index whatever was appended since we last looked (by us at open, or
    /// by another process), truncating a torn record at the very end.
    pub fn catch_up(&mut self) -> Result<()> {
        // Another process deleted since we looked: re-read what died.
        let deletes = read_counter(self.deletes.as_ref())?;
        if deletes != self.deletes_seen {
            self.reread_deaths()?;
            self.deletes_seen = deletes;
        }
        // Nothing new if the active segment did not grow and is not full
        // (only a full segment rolls over): one fstat, no directory listing.
        if let Some(s) = self.segs.values().next_back() {
            if s.len < self.segment_bytes && s.file.len()? == s.len {
                return Ok(());
            }
        }
        let last_known = self.segs.keys().next_back().copied();
        let mut seqs = self.seqs_on_disk()?;
        seqs.retain(|s| last_known.is_none_or(|l| *s >= l));
        let newest = seqs.last().copied();
        for seq in seqs {
            if !self.segs.contains_key(&seq) {
                let path = seg_path(&self.dir, seq);
                let file = self.fs.open(&path, false).with_context(|| format!("open {}", path.display()))?;
                self.segs.insert(seq, Seg { file, len: 0, live: 0 });
            }
            self.scan(seq, Some(seq) == newest)?;
        }
        Ok(())
    }

    /// Index the records of segment `seq` after what is known of it.
    fn scan(&mut self, seq: u64, last: bool) -> Result<()> {
        let (file, mut pos) = {
            let s = &self.segs[&seq];
            (s.file.clone(), s.len)
        };
        let size = file.len()?;
        while pos < size {
            let found = read_record(file.as_ref(), pos, size, last).with_context(|| format!("segment {seq}"))?;
            let (header, label) = match found {
                Found::Record(header, label) => (header, label),
                Found::Unwritten if last => break,
                Found::Unwritten => bail!("segment {seq}: bad record magic at {pos}"),
                Found::Torn(_) if last => break,
                Found::Torn(what) => bail!("segment {seq}: {what} at {pos} in a segment that is not the last"),
            };
            self.index(key_of(seq, pos), &header, &label);
            pos += header.record_len();
        }
        if pos < size {
            // A torn tail: never acknowledged, drop it.
            file.set_len(pos)?;
            file.sync()?;
        }
        self.segs.get_mut(&seq).unwrap().len = pos;
        Ok(())
    }

    /// Add a record found by [`Self::scan`] to the index; the first
    /// successor of a chain record wins.
    fn index(&mut self, key: u64, header: &Header, label: &[u8]) {
        let chain = (header.flags & CHAINED != 0).then(|| ChainRec {
            prev: (header.prev != NO_PREV).then_some(header.prev),
            label: String::from_utf8_lossy(label).into_owned(),
        });
        if let Some(c) = &chain {
            if let std::collections::hash_map::Entry::Vacant(e) = self.next.entry(c.prev) {
                e.insert(key);
                self.tail = Some(key);
            }
        }
        if !header.dead() {
            self.segs.get_mut(&split(key).0).unwrap().live += 1;
        }
        let owner = owner_of(&String::from_utf8_lossy(label));
        let rec = Rec { data_len: header.data_len, label_len: header.label_len, dead: header.dead(), owner, chain };
        self.records.insert(key, rec);
    }

    /// Append one record and fsync it; its key.
    pub fn append(&mut self, chain: Option<(Option<u64>, &str)>, label: &str, data: &[u8]) -> Result<u64> {
        if label.len() > u16::MAX as usize {
            bail!("label of {} bytes", label.len());
        }
        let seq = match self.segs.iter().next_back() {
            Some((seq, s)) if s.len < self.segment_bytes => *seq,
            last => {
                let seq = last.map_or(1, |(s, _)| s + 1);
                let path = seg_path(&self.dir, seq);
                let file = self.fs.open(&path, true).with_context(|| format!("create {}", path.display()))?;
                self.fs.sync_dir(&self.dir)?;
                self.segs.insert(seq, Seg { file, len: 0, live: 0 });
                seq
            }
        };
        let (flags, prev) = match chain {
            Some((prev, _)) => (CHAINED, prev.unwrap_or(NO_PREV)),
            None => (0, NO_PREV),
        };
        let mut header = Header { flags, label_len: label.len() as u16, data_len: data.len() as u64, prev, crc: 0 };
        header.crc = header.checksum(label.as_bytes(), data);

        let seg = self.segs.get_mut(&seq).unwrap();
        let pos = seg.len;
        let mut head = header.encode().to_vec();
        head.extend_from_slice(label.as_bytes());
        seg.file.write_all_at(&head, pos)?;
        seg.file.write_all_at(data, pos + head.len() as u64)?;
        seg.file.sync()?;
        seg.len = pos + head.len() as u64 + data.len() as u64;
        seg.live += 1;

        let key = key_of(seq, pos);
        let chain = chain.map(|(prev, label)| ChainRec { prev, label: label.to_string() });
        if let Some(c) = &chain {
            self.next.entry(c.prev).or_insert(key);
            self.tail = Some(key);
        }
        self.records.insert(
            key,
            Rec {
                data_len: data.len() as u64,
                label_len: label.len() as u16,
                dead: false,
                owner: owner_of(label),
                chain,
            },
        );
        Ok(key)
    }

    /// Delete records: `DEAD` in the header, a hole over the data; a segment
    /// left with no live record is removed (not the one being appended to).
    /// Syncs the segments it touched, and the directory if one went.
    pub fn delete(&mut self, keys: &[u64]) -> Result<()> {
        let active = self.segs.keys().next_back().copied();
        let (mut touched, mut removed, mut changed) = (BTreeSet::new(), false, false);
        for &key in keys {
            let Some(rec) = self.records.get_mut(&key).filter(|r| !r.dead) else { continue };
            rec.dead = true;
            changed = true;
            let (data_off, data_len) = (rec.data_offset(key), rec.data_len);
            let (seq, offset) = split(key);
            let Some(seg) = self.segs.get_mut(&seq) else { continue };
            seg.live -= 1;
            if seg.live == 0 && Some(seq) != active {
                let path = seg_path(&self.dir, seq);
                self.fs.remove(&path).with_context(|| format!("remove {}", path.display()))?;
                self.forget_segment(seq);
                touched.remove(&seq);
                removed = true;
                continue;
            }
            let mut flags = [0u8; 1];
            seg.file.read_exact_at(&mut flags, offset + 4)?;
            seg.file.write_all_at(&[flags[0] | DEAD], offset + 4)?;
            punch_hole(seg.file.as_ref(), data_off, data_len)?;
            touched.insert(seq);
        }
        for seq in touched {
            self.segs[&seq].file.sync()?;
        }
        if removed {
            self.fs.sync_dir(&self.dir)?;
        }
        if changed {
            self.deletes_seen = read_counter(self.deletes.as_ref())? + 1;
            self.deletes.write_all_at(&self.deletes_seen.to_le_bytes(), 0)?;
        }
        Ok(())
    }

    /// Segment `seq` is gone: drop it and its records, but keep a chain
    /// member's label (dead): a deposed writer putting after a pruned blob
    /// still learns who won.
    fn forget_segment(&mut self, seq: u64) {
        self.segs.remove(&seq);
        let gone: Vec<u64> = self.records.range(key_of(seq, 0)..key_of(seq + 1, 0)).map(|(k, _)| *k).collect();
        for k in gone {
            match self.records.get_mut(&k) {
                Some(r) if r.chain.is_some() => r.dead = true,
                _ => {
                    self.records.remove(&k);
                }
            }
        }
    }

    /// Another process deleted records: mark what died (one read of a
    /// header's flags per live record) and drop the segments it removed.
    fn reread_deaths(&mut self) -> Result<()> {
        let on_disk: BTreeSet<u64> = self.seqs_on_disk()?.into_iter().collect();
        let known: Vec<u64> = self.segs.keys().copied().collect();
        for seq in known {
            if !on_disk.contains(&seq) {
                self.forget_segment(seq);
                continue;
            }
            let seg = self.0.segs.get_mut(&seq).unwrap();
            for (key, rec) in self.0.records.range_mut(key_of(seq, 0)..key_of(seq + 1, 0)) {
                if rec.dead {
                    continue;
                }
                let mut flags = [0u8; 1];
                seg.file.read_exact_at(&mut flags, split(*key).1 + 4)?;
                if flags[0] & DEAD != 0 {
                    rec.dead = true;
                    seg.live -= 1;
                }
            }
        }
        Ok(())
    }
}

/// Open `path`, creating it if it is not there.
fn open_or_create(fs: &dyn Fs, path: &Path) -> Result<Arc<dyn FsFile>> {
    match fs.open(path, false) {
        Ok(f) => Ok(f),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => match fs.open(path, true) {
            Ok(f) => Ok(f),
            // Another process created it meanwhile.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(fs.open(path, false)?),
            Err(e) => Err(e).with_context(|| format!("create {}", path.display())),
        },
        Err(e) => Err(e).with_context(|| format!("open {}", path.display())),
    }
}

/// The delete counter (0 for an empty file).
fn read_counter(file: &dyn FsFile) -> Result<u64> {
    if file.len()? < 8 {
        return Ok(0);
    }
    let mut b = [0u8; 8];
    file.read_exact_at(&mut b, 0)?;
    Ok(u64::from_le_bytes(b))
}

/// A record's fixed header (see the module doc for the layout).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Header {
    flags: u8,
    label_len: u16,
    data_len: u64,
    prev: u64,
    crc: u32,
}

impl Header {
    fn encode(&self) -> [u8; HEADER as usize] {
        let mut h = [0u8; HEADER as usize];
        h[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        h[4] = self.flags;
        h[6..8].copy_from_slice(&self.label_len.to_le_bytes());
        h[8..16].copy_from_slice(&self.data_len.to_le_bytes());
        h[16..24].copy_from_slice(&self.prev.to_le_bytes());
        h[24..28].copy_from_slice(&self.crc.to_le_bytes());
        h
    }

    /// `None` without the magic.
    fn decode(h: &[u8; HEADER as usize]) -> Option<Self> {
        let u64_at = |i: usize| u64::from_le_bytes(h[i..i + 8].try_into().unwrap());
        if h[0..4] != MAGIC.to_le_bytes() {
            return None;
        }
        Some(Self {
            flags: h[4],
            label_len: u16::from_le_bytes([h[6], h[7]]),
            data_len: u64_at(8),
            prev: u64_at(16),
            crc: u32::from_le_bytes(h[24..28].try_into().unwrap()),
        })
    }

    fn dead(&self) -> bool {
        self.flags & DEAD != 0
    }

    /// The header, the label and the data.
    fn record_len(&self) -> u64 {
        HEADER + self.label_len as u64 + self.data_len
    }

    /// What `crc` should be: the flags (without `DEAD`), the lengths,
    /// `prev`, the label and the data.
    fn checksum(&self, label: &[u8], data: &[u8]) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(&[self.flags & !DEAD]);
        h.update(&self.label_len.to_le_bytes());
        h.update(&self.data_len.to_le_bytes());
        h.update(&self.prev.to_le_bytes());
        h.update(label);
        h.update(data);
        h.finalize()
    }
}

/// What a scan finds at a position of a segment.
enum Found {
    /// A whole record and its label.
    Record(Header, Vec<u8>),
    /// A torn write: what is wrong with it.
    Torn(&'static str),
    /// Zeros: preallocated, never written.
    Unwritten,
}

/// The record at `pos` of a segment file of `size` bytes. A bad magic that
/// is not all zeros is an error. Only the last record of the last segment
/// can be torn with a whole length, so only its data is checked (a dead
/// one may be a hole).
fn read_record(file: &dyn FsFile, pos: u64, size: u64, last: bool) -> Result<Found> {
    if size - pos < HEADER {
        return Ok(Found::Torn("a short header"));
    }
    let mut h = [0u8; HEADER as usize];
    file.read_exact_at(&mut h, pos)?;
    let Some(header) = Header::decode(&h) else {
        if h.iter().all(|b| *b == 0) {
            return Ok(Found::Unwritten);
        }
        if last && !whole_record_after(file, pos, size)? {
            return Ok(Found::Torn("a header without its magic"));
        }
        bail!("bad record magic at {pos}");
    };
    let end = pos + header.record_len();
    if end > size {
        return Ok(Found::Torn("a record past the end"));
    }
    let mut label = vec![0; header.label_len as usize];
    file.read_exact_at(&mut label, pos + HEADER)?;
    if last && end == size && !header.dead() {
        let mut data = vec![0; header.data_len as usize];
        file.read_exact_at(&mut data, pos + HEADER + header.label_len as u64)?;
        if header.checksum(&label, &data) != header.crc {
            return Ok(Found::Torn("a record failing its checksum"));
        }
    }
    Ok(Found::Record(header, label))
}

/// Whether a whole record (magic, length within the file, checksum) starts
/// somewhere in `(pos, size)`: then a bad header at `pos` is corruption,
/// not a torn tail.
fn whole_record_after(file: &dyn FsFile, pos: u64, size: u64) -> Result<bool> {
    let mut rest = vec![0; (size - pos) as usize];
    file.read_exact_at(&mut rest, pos)?;
    let magic = MAGIC.to_le_bytes();
    for at in (1..rest.len()).filter(|&i| rest[i..].starts_with(&magic)) {
        if rest.len() - at < HEADER as usize {
            break;
        }
        let Some(header) = Header::decode(rest[at..at + HEADER as usize].try_into().unwrap()) else { continue };
        let end = at as u64 + header.record_len();
        if end > rest.len() as u64 || header.dead() {
            continue;
        }
        let label = &rest[at + HEADER as usize..at + HEADER as usize + header.label_len as usize];
        let data = &rest[at + HEADER as usize + header.label_len as usize..end as usize];
        if header.checksum(label, data) == header.crc {
            return Ok(true);
        }
    }
    Ok(false)
}

/// [`inspect`] on the real file system: what the admin's view of a disk reads.
pub fn inspect_dir(dir: &Path, want: &dyn Fn(&str) -> bool) -> Result<Vec<SegmentView>> {
    inspect(&crate::StdFs, dir, want)
}

/// What a directory of segments holds, for showing it: read without the
/// lock and without changing anything, so a writer may be at work; a
/// record still being written ends the scan of its segment.
pub fn inspect(fs: &dyn Fs, dir: &Path, want: impl Fn(&str) -> bool) -> Result<Vec<SegmentView>> {
    let mut names: Vec<String> = fs.list(dir).with_context(|| format!("read {}", dir.display()))?;
    names.retain(|n| n.strip_prefix("seg-").is_some_and(|s| s.parse::<u64>().is_ok()));
    names.sort();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let seq: u64 = name["seg-".len()..].parse().expect("checked above");
        let file = fs.open(&dir.join(&name), false).with_context(|| format!("open {name}"))?;
        let size = file.len()?;
        let (mut pos, mut records) = (0, Vec::new());
        while pos < size {
            let Ok(Found::Record(header, label)) = read_record(file.as_ref(), pos, size, false) else { break };
            let label = String::from_utf8_lossy(&label).into_owned();
            let data = (!header.dead() && want(&label))
                .then(|| {
                    let mut buf = vec![0; header.data_len as usize];
                    file.read_exact_at(&mut buf, pos + HEADER + header.label_len as u64).map(|()| buf)
                })
                .transpose()?;
            let chained = header.flags & CHAINED != 0;
            records.push(RecordView {
                key: key_of(seq, pos),
                label,
                size: header.data_len,
                dead: header.dead(),
                chained,
                data,
            });
            pos += header.record_len();
        }
        out.push(SegmentView { name, size, records });
    }
    Ok(out)
}

/// Free the blocks fully inside `[off, off + len)`; the file keeps its size.
fn punch_hole(file: &dyn FsFile, off: u64, len: u64) -> std::io::Result<()> {
    const BLOCK: u64 = 4096;
    let start = off.div_ceil(BLOCK) * BLOCK;
    let end = (off + len) / BLOCK * BLOCK;
    if end <= start {
        return Ok(());
    }
    file.punch(start, end - start)
}
