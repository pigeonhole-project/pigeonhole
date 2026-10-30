//! The file operations the segment store uses, behind a trait: [`StdFs`]
//! is the host file system; [`SimFs`] a disk in memory for tests and the
//! simulation (docs/simulation.md), where a write is durable only after a
//! sync and a crash drops the rest, optionally leaving a torn last write.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// A directory tree the store keeps its segments in.
pub trait Fs: Send + Sync + 'static {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Names of the entries of `dir`.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    /// Open for reading and writing; `create_new` fails if it exists.
    fn open(&self, path: &Path, create_new: bool) -> io::Result<Arc<dyn FsFile>>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Make the directory's entries (a created or removed file) durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// The lock that keeps processes apart in `dir` (`flock` on the host).
    fn dir_lock(&self, dir: &Path) -> io::Result<Arc<dyn DirLock>>;
    /// Time the I/O since the last call would have taken (a simulated disk
    /// charges it as virtual time; the host's took it already).
    fn take_cost(&self) -> Duration {
        Duration::ZERO
    }
}

#[allow(clippy::len_without_is_empty)] // a file's length, not a collection
pub trait FsFile: Send + Sync {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    /// Make everything written durable (fsync).
    fn sync(&self) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// Free `[offset, offset + len)`; it reads back as zeros.
    fn punch(&self, offset: u64, len: u64) -> io::Result<()>;
}

pub trait DirLock: Send + Sync {
    fn lock(&self) -> io::Result<()>;
    fn unlock(&self);
}

/// The host file system.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdFs;

impl Fs for StdFs {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for e in std::fs::read_dir(dir)? {
            names.push(e?.file_name().to_string_lossy().into_owned());
        }
        Ok(names)
    }

    fn open(&self, path: &Path, create_new: bool) -> io::Result<Arc<dyn FsFile>> {
        let file = OpenOptions::new().read(true).write(true).create_new(create_new).open(path)?;
        Ok(Arc::new(StdFile(file)))
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn dir_lock(&self, dir: &Path) -> io::Result<Arc<dyn DirLock>> {
        let file = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("lock"))?;
        Ok(Arc::new(StdLock(file)))
    }
}

struct StdFile(File);

impl FsFile for StdFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    fn sync(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn punch(&self, offset: u64, len: u64) -> io::Result<()> {
        punch(&self.0, offset, len)
    }
}

struct StdLock(File);

impl DirLock for StdLock {
    fn lock(&self) -> io::Result<()> {
        self.0.lock()
    }

    fn unlock(&self) {
        let _ = self.0.unlock();
    }
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn punch(file: &File, off: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let arg = libc::fpunchhole_t { fp_flags: 0, reserved: 0, fp_offset: off as libc::off_t, fp_length: len as libc::off_t };
    // SAFETY: a valid descriptor and a pointer to a live fpunchhole_t.
    let r = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &arg) };
    if r == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
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

/// A disk in memory. Every handle onto one `SimFs` (two "processes")
/// sees the same files; [`SimFs::crash`] keeps only what was synced.
#[derive(Clone)]
pub struct SimFs {
    inner: Arc<SimInner>,
}

struct SimInner {
    state: Mutex<SimState>,
    /// Virtual time one sync costs.
    sync_cost: Duration,
    cost: Mutex<Duration>,
}

#[derive(Default)]
struct SimState {
    dirs: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, Arc<SimFile>>,
    /// Names whose creation (or removal) a directory sync made durable.
    durable_names: BTreeSet<PathBuf>,
    locks: BTreeMap<PathBuf, Arc<SimLock>>,
}

struct SimFile {
    data: Mutex<SimData>,
    inner: std::sync::Weak<SimInner>,
}

#[derive(Default, Clone)]
struct SimData {
    /// What reads see.
    now: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
}

impl SimFs {
    /// A disk whose sync takes `sync_cost` of virtual time.
    pub fn new(sync_cost: Duration) -> Self {
        Self {
            inner: Arc::new(SimInner {
                state: Mutex::default(),
                sync_cost,
                cost: Mutex::new(Duration::ZERO),
            }),
        }
    }

    /// Power loss: every file falls back to what was synced; a file whose
    /// creation was never made durable is gone. With `torn`, a file whose
    /// unsynced bytes extend its synced ones keeps a prefix of them
    /// (`torn(unsynced len)` → how many bytes survive): a write cut short.
    pub fn crash(&self, mut torn: Option<&mut dyn FnMut(usize) -> usize>) {
        let mut st = self.inner.state.lock().unwrap();
        let durable_names = st.durable_names.clone();
        st.files.retain(|path, _| durable_names.contains(path));
        for file in st.files.values() {
            let mut d = file.data.lock().unwrap();
            let mut kept = d.durable.clone();
            if let Some(torn) = torn.as_mut() {
                if d.now.len() > d.durable.len() && d.now.starts_with(&d.durable) {
                    let extra = &d.now[d.durable.len()..];
                    let keep = torn(extra.len()).min(extra.len());
                    kept.extend_from_slice(&extra[..keep]);
                }
            }
            d.now = kept.clone();
            d.durable = kept;
        }
        for lock in st.locks.values() {
            lock.unlock();
        }
    }

    /// Bytes on the disk now (not what survives a crash).
    pub fn file_len(&self, path: &Path) -> Option<u64> {
        let st = self.inner.state.lock().unwrap();
        st.files.get(path).map(|f| f.data.lock().unwrap().now.len() as u64)
    }

    fn charge(&self) {
        *self.inner.cost.lock().unwrap() += self.inner.sync_cost;
    }
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("{} not found", path.display()))
}

impl Fs for SimFs {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.inner.state.lock().unwrap().dirs.insert(dir.to_path_buf());
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let st = self.inner.state.lock().unwrap();
        if !st.dirs.contains(dir) {
            return Err(not_found(dir));
        }
        Ok(st
            .files
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect())
    }

    fn open(&self, path: &Path, create_new: bool) -> io::Result<Arc<dyn FsFile>> {
        let mut st = self.inner.state.lock().unwrap();
        match (st.files.get(path), create_new) {
            (Some(_), true) => Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("{} exists", path.display()))),
            (Some(f), false) => Ok(f.clone() as Arc<dyn FsFile>),
            (None, false) => Err(not_found(path)),
            (None, true) => {
                let file = Arc::new(SimFile { data: Mutex::default(), inner: Arc::downgrade(&self.inner) });
                st.files.insert(path.to_path_buf(), file.clone());
                Ok(file)
            }
        }
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut st = self.inner.state.lock().unwrap();
        st.files.remove(path).ok_or_else(|| not_found(path))?;
        st.durable_names.remove(path);
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut st = self.inner.state.lock().unwrap();
        let names: Vec<PathBuf> = st.files.keys().filter(|p| p.parent() == Some(dir)).cloned().collect();
        st.durable_names.extend(names);
        drop(st);
        self.charge();
        Ok(())
    }

    fn dir_lock(&self, dir: &Path) -> io::Result<Arc<dyn DirLock>> {
        let mut st = self.inner.state.lock().unwrap();
        Ok(st.locks.entry(dir.to_path_buf()).or_insert_with(|| Arc::new(SimLock::default())).clone())
    }

    fn take_cost(&self) -> Duration {
        std::mem::take(&mut *self.inner.cost.lock().unwrap())
    }
}

impl FsFile for SimFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.data.lock().unwrap().now.len() as u64)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let d = self.data.lock().unwrap();
        let (start, end) = (offset as usize, offset as usize + buf.len());
        if end > d.now.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end"));
        }
        buf.copy_from_slice(&d.now[start..end]);
        Ok(())
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut d = self.data.lock().unwrap();
        let end = offset as usize + buf.len();
        if d.now.len() < end {
            d.now.resize(end, 0);
        }
        d.now[offset as usize..end].copy_from_slice(buf);
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        let mut d = self.data.lock().unwrap();
        d.durable = d.now.clone();
        drop(d);
        if let Some(inner) = self.inner.upgrade() {
            *inner.cost.lock().unwrap() += inner.sync_cost;
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.data.lock().unwrap().now.resize(len as usize, 0);
        Ok(())
    }

    fn punch(&self, offset: u64, len: u64) -> io::Result<()> {
        let mut d = self.data.lock().unwrap();
        let end = ((offset + len) as usize).min(d.now.len());
        let start = (offset as usize).min(end);
        d.now[start..end].fill(0);
        Ok(())
    }
}

/// A lock two handles (processes) of one `SimFs` exclude each other with.
#[derive(Default)]
struct SimLock {
    held: Mutex<bool>,
    freed: Condvar,
}

impl DirLock for SimLock {
    fn lock(&self) -> io::Result<()> {
        let mut held = self.held.lock().unwrap();
        while *held {
            held = self.freed.wait(held).unwrap();
        }
        *held = true;
        Ok(())
    }

    fn unlock(&self) {
        *self.held.lock().unwrap() = false;
        self.freed.notify_one();
    }
}
