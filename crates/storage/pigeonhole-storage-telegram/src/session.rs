//! The MTProto session as a file: only a cache of the home DC and the auth
//! keys (a lost file means one more bot login, no interaction). Peers and
//! update state are not kept: updates are ignored and the one peer we use is
//! resolved at startup. An exclusive lock keeps two processes off one session.

use anyhow::{Context, Result};
use grammers_session::types::{DcOption, PeerId, PeerInfo, UpdateState, UpdatesState};
use grammers_session::{BoxFuture, Session, SessionData};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Debug)]
pub struct SessionError(String);

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "session file: {}", self.0)
    }
}

impl std::error::Error for SessionError {}

/// What the file holds.
#[derive(Default, Serialize, Deserialize)]
struct Stored {
    home_dc: Option<i32>,
    /// DC id → hex auth key.
    auth_keys: BTreeMap<i32, String>,
}

pub struct FileSession {
    path: PathBuf,
    data: Mutex<SessionData>,
    /// Held for the life of the session: the `flock` on `<path>.lock`.
    _lock: File,
}

impl FileSession {
    /// Open (or start) the session at `path`; fails if another process holds it.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        }
        let lock_path = path.with_extension("lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("open {}", lock_path.display()))?;
        lock.try_lock()
            .map_err(|_| anyhow::anyhow!("MTProto session {} is in use by another process", path.display()))?;

        let mut data = SessionData::default();
        match fs::read(path) {
            Ok(bytes) => {
                let stored: Stored =
                    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
                if let Some(dc) = stored.home_dc {
                    data.home_dc = dc;
                }
                for (dc, key) in stored.auth_keys {
                    let key: [u8; 256] = hex::decode(&key)
                        .ok()
                        .and_then(|k| k.try_into().ok())
                        .with_context(|| format!("{}: bad auth key for DC {dc}", path.display()))?;
                    if let Some(opt) = data.dc_options.get_mut(&dc) {
                        opt.auth_key = Some(key);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        }
        Ok(Self { path: path.to_path_buf(), data: Mutex::new(data), _lock: lock })
    }

    /// Whether the home DC has an auth key (a login may be cached).
    pub fn has_home_key(&self) -> bool {
        let d = self.data.lock().unwrap();
        d.dc_options.get(&d.home_dc).is_some_and(|o| o.auth_key.is_some())
    }

    /// Forget every auth key (a session of another bot).
    pub fn reset(&self) -> Result<(), SessionError> {
        {
            let mut d = self.data.lock().unwrap();
            for opt in d.dc_options.values_mut() {
                opt.auth_key = None;
            }
        }
        self.save()
    }

    fn save(&self) -> Result<(), SessionError> {
        let stored = {
            let d = self.data.lock().unwrap();
            Stored {
                home_dc: Some(d.home_dc),
                auth_keys: d
                    .dc_options
                    .iter()
                    .filter_map(|(id, o)| o.auth_key.map(|k| (*id, hex::encode(k))))
                    .collect(),
            }
        };
        let write = || -> std::io::Result<()> {
            let tmp = self.path.with_extension("tmp");
            let mut opts = OpenOptions::new();
            opts.create(true).truncate(true).write(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
            let mut f = opts.open(&tmp)?;
            f.write_all(&serde_json::to_vec(&stored).expect("session encodes"))?;
            f.sync_all()?;
            fs::rename(&tmp, &self.path)
        };
        write().map_err(|e| SessionError(format!("write {}: {e}", self.path.display())))
    }
}

impl Session for FileSession {
    type Error = SessionError;

    fn home_dc_id(&self) -> Result<i32, SessionError> {
        Ok(self.data.lock().unwrap().home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            self.data.lock().unwrap().home_dc = dc_id;
            self.save()
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, SessionError> {
        Ok(self.data.lock().unwrap().dc_options.get(&dc_id).cloned())
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), SessionError>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            let key_changed = {
                let mut d = self.data.lock().unwrap();
                let old = d.dc_options.insert(dc_option.id, dc_option.clone());
                old.map(|o| o.auth_key) != Some(dc_option.auth_key)
            };
            if key_changed {
                self.save()
            } else {
                Ok(())
            }
        })
    }

    fn peer(&self, _peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, SessionError>> {
        Box::pin(async { Ok(None) })
    }

    fn cache_peer(&self, _peer: PeerInfo) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async { Ok(()) })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, SessionError>> {
        Box::pin(async { Ok(UpdatesState::default()) })
    }

    fn set_update_state(&self, _update: UpdateState) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keeps_the_home_dc_and_keys_and_locks_out_a_second_process() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bot.session");
        {
            let s = FileSession::open(&path).unwrap();
            assert!(!s.has_home_key());
            assert!(FileSession::open(&path).is_err(), "one holder at a time");
            s.set_home_dc_id(4).await.unwrap();
            let mut opt = s.dc_option(4).unwrap().unwrap();
            opt.auth_key = Some([7; 256]);
            s.set_dc_option(&opt).await.unwrap();
            assert!(s.has_home_key());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
        let s = FileSession::open(&path).unwrap();
        assert_eq!(s.home_dc_id().unwrap(), 4);
        assert_eq!(s.dc_option(4).unwrap().unwrap().auth_key, Some([7; 256]));
        s.reset().unwrap();
        assert!(!FileSession::open(&dir.path().join("other.session")).unwrap().has_home_key());
        drop(s);
        assert!(!FileSession::open(&path).unwrap().has_home_key());
    }
}
