//! Append-only segment files of records (Haystack / Bitcask style).
//!
//! ```text
//! <dir>/lock          flock'ed around every write and every chain decision
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
//! chain's "first successor wins" holds across processes. A torn record at
//! the end of the last segment was never acknowledged: the next writer (or
//! open) truncates it. A delete sets `DEAD` in the header and punches a hole
//! over the data; a segment whose records are all dead is removed.

use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
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

/// One record as the index knows it.
#[derive(Debug, Clone)]
pub(crate) struct Rec {
    pub data_len: u64,
    label_len: u16,
    pub dead: bool,
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
    file: Arc<File>,
    /// Bytes scanned (= the end of the last whole record).
    len: u64,
    live: usize,
}

/// The index of a directory of segments, kept in memory.
pub(crate) struct Segments {
    dir: PathBuf,
    segment_bytes: u64,
    lock: File,
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
        let _ = self.0.lock.unlock();
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
    pub fn open(dir: &Path, segment_bytes: u64) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let lock_path = dir.join("lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("open {}", lock_path.display()))?;
        let mut this = Self {
            dir: dir.to_path_buf(),
            segment_bytes: segment_bytes.max(1),
            lock,
            segs: BTreeMap::new(),
            records: BTreeMap::new(),
            next: HashMap::new(),
            tail: None,
        };
        this.locked()?.catch_up()?;
        Ok(this)
    }

    /// Take the `flock` (blocking).
    pub fn locked(&mut self) -> Result<Locked<'_>> {
        self.lock.lock().context("flock the segment directory")?;
        Ok(Locked(self))
    }

    pub fn file_of(&self, key: u64) -> Option<Arc<File>> {
        self.segs.get(&split(key).0).map(|s| s.file.clone())
    }

    /// Read `len` bytes of record `key`'s data at `from`.
    pub fn read(&self, key: u64, from: u64, len: u64) -> Result<Vec<u8>> {
        let rec = self.records.get(&key).filter(|r| !r.dead).ok_or_else(|| anyhow!("no blob {key}"))?;
        if from + len > rec.data_len {
            bail!("byte range {from}..{} outside blob of {} bytes", from + len, rec.data_len);
        }
        let file = self.file_of(key).ok_or_else(|| anyhow!("no segment for blob {key}"))?;
        let mut buf = vec![0; len as usize];
        file.read_exact_at(&mut buf, rec.data_offset(key) + from)?;
        Ok(buf)
    }

    /// Every segment sequence number in the directory.
    fn seqs_on_disk(&self) -> Result<Vec<u64>> {
        let mut seqs = Vec::new();
        for e in fs::read_dir(&self.dir).with_context(|| format!("read {}", self.dir.display()))? {
            let name = e?.file_name();
            if let Some(seq) = name.to_str().and_then(|n| n.strip_prefix("seg-")).and_then(|n| n.parse().ok()) {
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
        let last_known = self.segs.keys().next_back().copied();
        let mut seqs = self.seqs_on_disk()?;
        seqs.retain(|s| last_known.is_none_or(|l| *s >= l));
        let newest = seqs.last().copied();
        for seq in seqs {
            if !self.segs.contains_key(&seq) {
                let path = seg_path(&self.dir, seq);
                let file = OpenOptions::new().read(true).write(true).open(&path)
                    .with_context(|| format!("open {}", path.display()))?;
                self.segs.insert(seq, Seg { file: Arc::new(file), len: 0, live: 0 });
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
        let size = file.metadata()?.len();
        let mut reader = io::BufReader::with_capacity(64 << 10, &*file);
        reader.seek(SeekFrom::Start(pos))?;
        while pos < size {
            let torn = |what: &str| -> Result<()> {
                if last {
                    Ok(())
                } else {
                    bail!("segment {seq}: {what} at {pos} in a segment that is not the last")
                }
            };
            if size - pos < HEADER {
                torn("a short header")?;
                break;
            }
            let mut h = [0u8; HEADER as usize];
            reader.read_exact(&mut h)?;
            let magic = u32::from_le_bytes(h[0..4].try_into().unwrap());
            let flags = h[4];
            let label_len = u16::from_le_bytes(h[6..8].try_into().unwrap());
            let data_len = u64::from_le_bytes(h[8..16].try_into().unwrap());
            let prev = u64::from_le_bytes(h[16..24].try_into().unwrap());
            let crc = u32::from_le_bytes(h[24..28].try_into().unwrap());
            if magic != MAGIC {
                if last && h.iter().all(|b| *b == 0) {
                    break; // preallocated, never written
                }
                bail!("segment {seq}: bad record magic at {pos}");
            }
            let end = pos + HEADER + label_len as u64 + data_len;
            if end > size {
                torn("a record past the end")?;
                break;
            }
            let mut label = vec![0; label_len as usize];
            reader.read_exact(&mut label)?;
            let dead = flags & DEAD != 0;
            // Only the last record can be torn with a whole length: check its
            // data (a dead one may be a hole).
            if last && end == size && !dead {
                let mut data = vec![0; data_len as usize];
                reader.read_exact(&mut data)?;
                if record_crc(flags, &h, &label, &data) != crc {
                    torn("a record failing its checksum")?;
                    break;
                }
            } else {
                reader.seek_relative(data_len as i64)?;
            }
            let key = key_of(seq, pos);
            let chain = (flags & CHAINED != 0).then(|| ChainRec {
                prev: (prev != NO_PREV).then_some(prev),
                label: String::from_utf8_lossy(&label).into_owned(),
            });
            if let Some(c) = &chain {
                if let std::collections::hash_map::Entry::Vacant(e) = self.next.entry(c.prev) {
                    e.insert(key);
                    self.tail = Some(key);
                }
            }
            if !dead {
                self.segs.get_mut(&seq).unwrap().live += 1;
            }
            self.records.insert(key, Rec { data_len, label_len, dead, chain });
            pos = end;
        }
        if pos < size {
            // A torn tail: never acknowledged, drop it.
            file.set_len(pos)?;
            file.sync_all()?;
        }
        self.segs.get_mut(&seq).unwrap().len = pos;
        Ok(())
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
                let file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)
                    .with_context(|| format!("create {}", path.display()))?;
                File::open(&self.dir)?.sync_all()?;
                self.segs.insert(seq, Seg { file: Arc::new(file), len: 0, live: 0 });
                seq
            }
        };
        let (flags, prev) = match chain {
            Some((prev, _)) => (CHAINED, prev.unwrap_or(NO_PREV)),
            None => (0, NO_PREV),
        };
        let mut h = [0u8; HEADER as usize];
        h[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        h[4] = flags;
        h[6..8].copy_from_slice(&(label.len() as u16).to_le_bytes());
        h[8..16].copy_from_slice(&(data.len() as u64).to_le_bytes());
        h[16..24].copy_from_slice(&prev.to_le_bytes());
        let crc = record_crc(flags, &h, label.as_bytes(), data);
        h[24..28].copy_from_slice(&crc.to_le_bytes());

        let seg = self.segs.get_mut(&seq).unwrap();
        let pos = seg.len;
        let mut head = h.to_vec();
        head.extend_from_slice(label.as_bytes());
        seg.file.write_all_at(&head, pos)?;
        seg.file.write_all_at(data, pos + head.len() as u64)?;
        seg.file.sync_data()?;
        seg.len = pos + head.len() as u64 + data.len() as u64;
        seg.live += 1;

        let key = key_of(seq, pos);
        let chain = chain.map(|(prev, label)| ChainRec { prev, label: label.to_string() });
        if let Some(c) = &chain {
            self.next.entry(c.prev).or_insert(key);
            self.tail = Some(key);
        }
        self.records.insert(key, Rec { data_len: data.len() as u64, label_len: label.len() as u16, dead: false, chain });
        Ok(key)
    }

    /// Delete records: `DEAD` in the header, a hole over the data; a segment
    /// left with no live record is removed (not the one being appended to).
    pub fn delete(&mut self, keys: &[u64]) -> Result<()> {
        let active = self.segs.keys().next_back().copied();
        for &key in keys {
            let Some(rec) = self.records.get_mut(&key).filter(|r| !r.dead) else { continue };
            rec.dead = true;
            let (data_off, data_len) = (rec.data_offset(key), rec.data_len);
            let (seq, offset) = split(key);
            let Some(seg) = self.segs.get_mut(&seq) else { continue };
            seg.live -= 1;
            if seg.live == 0 && Some(seq) != active {
                let path = seg_path(&self.dir, seq);
                fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
                self.segs.remove(&seq);
                let lo = key_of(seq, 0);
                let hi = key_of(seq + 1, 0);
                let gone: Vec<u64> = self.records.range(lo..hi).map(|(k, _)| *k).collect();
                for k in gone {
                    self.records.remove(&k);
                }
                continue;
            }
            let mut flags = [0u8; 1];
            seg.file.read_exact_at(&mut flags, offset + 4)?;
            seg.file.write_all_at(&[flags[0] | DEAD], offset + 4)?;
            punch_hole(&seg.file, data_off, data_len)?;
        }
        for seg in self.segs.values() {
            seg.file.sync_data()?;
        }
        Ok(())
    }
}

fn record_crc(flags: u8, header: &[u8], label: &[u8], data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&[flags & !DEAD]);
    h.update(&header[6..24]);
    h.update(label);
    h.update(data);
    h.finalize()
}

/// Free the blocks fully inside `[off, off + len)`; the file keeps its size.
fn punch_hole(file: &File, off: u64, len: u64) -> io::Result<()> {
    const BLOCK: u64 = 4096;
    let start = off.div_ceil(BLOCK) * BLOCK;
    let end = (off + len) / BLOCK * BLOCK;
    if end <= start {
        return Ok(());
    }
    punch(file, start, end - start)
}

#[cfg(target_os = "macos")]
fn punch(file: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let arg = libc::fpunchhole_t { fp_flags: 0, reserved: 0, fp_offset: off as libc::off_t, fp_length: len as libc::off_t };
    // SAFETY: a valid descriptor and a pointer to a live fpunchhole_t.
    let r = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &arg) };
    if r == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

#[cfg(target_os = "linux")]
fn punch(file: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: a valid descriptor.
    let r = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            off as libc::off_t,
            len as libc::off_t,
        )
    };
    if r == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Without hole punching the space stays until the whole segment dies.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn punch(_file: &File, _off: u64, _len: u64) -> io::Result<()> {
    Ok(())
}
