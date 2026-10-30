//! Blob stores as the chunk store sees them (resolved from `[[store]]`).

use anyhow::{bail, Context, Result};
use pigeonhole_blob::{InstanceInfo, InstanceRole};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

/// The kinds of store this binary builds (`[[store]] kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceKind {
    Telegram,
    Discord,
    Memory,
    LocalDisk,
}

impl InstanceKind {
    /// The store crate's `KIND`, recorded in [`InstanceInfo::kind`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Telegram => pigeonhole_storage_telegram::KIND,
            // The crate is optional; a test pins the two together.
            Self::Discord => "discord",
            Self::Memory => pigeonhole_storage_memory::KIND,
            Self::LocalDisk => pigeonhole_storage_disk::KIND,
        }
    }
}

/// One configured backend instance after secret resolution.
#[derive(Debug, Clone)]
pub struct InstanceConfig {
    pub kind: InstanceKind,
    pub info: InstanceInfo,
    /// Env var names holding the bots' tokens (never the secrets), the
    /// primary first; empty for memory and local_disk.
    pub bot_token_envs: Vec<String>,
    /// Resolved tokens (from env), as `bot_token_envs`.
    pub bot_tokens: Vec<String>,
    /// Telegram chat id, Discord channel id or local_disk path (string form).
    pub scope_id: String,
    /// Per-message size limit override (Discord); `None` = backend default.
    pub max_blob_size: Option<usize>,
    /// `telegram`: how each bot logs in, as `bot_tokens`.
    pub telegram: Vec<TelegramLogin>,
}

/// A Telegram bot logged in over MTProto, resolved from `[[store]]` and the
/// environment.
#[derive(Clone, PartialEq, Eq)]
pub struct TelegramLogin {
    pub bot_token: String,
    pub api_id: i32,
    pub api_hash: String,
    /// The MTProto session file (auth key cache).
    pub session: PathBuf,
}

impl std::fmt::Debug for TelegramLogin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bot = self.bot_token.split(':').next().unwrap_or_default();
        f.debug_struct("TelegramLogin").field("bot", &bot).field("api_id", &self.api_id).field("session", &self.session).finish()
    }
}

/// Telegram fingerprint: `{bot_id}:{chat_id}` where bot_id is the token prefix.
pub fn telegram_fingerprint(bot_token: &str, chat_id: &str) -> Result<String> {
    let bot_id = bot_token
        .split_once(':')
        .map(|(a, _)| a)
        .filter(|s| !s.is_empty())
        .context("telegram bot token missing bot_id prefix before ':'")?;
    Ok(format!("tg:{bot_id}:{chat_id}"))
}

pub fn telegram_location(chat_id: &str) -> String {
    format!("tg:chat:{chat_id}")
}

pub fn discord_fingerprint(app_id: &str, channel_id: &str) -> String {
    format!("dc:{app_id}:{channel_id}")
}

pub fn discord_location(channel_id: &str) -> String {
    format!("dc:channel:{channel_id}")
}

/// A directory; also its fingerprint (the place is the identity).
pub fn local_disk_location(path: &std::path::Path) -> String {
    format!("disk:{}", path.display())
}

/// Each memory instance is its own in-process store, so id = identity.
pub fn memory_fingerprint(id: &str) -> String {
    format!("memory:{id}")
}

pub fn memory_location(id: &str) -> String {
    format!("memory:{id}")
}

/// Discord bot tokens are `app_id.xxx.yyy` or just use full token prefix before `.`.
pub fn discord_app_id_from_token(token: &str) -> String {
    token
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

/// In-process memory instance (tests, local runs).
pub fn memory_instance(id: &str) -> InstanceConfig {
    InstanceConfig {
        kind: InstanceKind::Memory,
        info: InstanceInfo {
            id: id.into(),
            kind: InstanceKind::Memory.as_str(),
            fingerprint: memory_fingerprint(id),
            location: memory_location(id),
            role: InstanceRole::ReadWrite,
        },
        bot_token_envs: Vec::new(),
        bot_tokens: Vec::new(),
        scope_id: "local".into(),
        max_blob_size: None,
        telegram: Vec::new(),
    }
}

/// At most one read-write instance per location; unique instance ids.
pub fn validate_instances(instances: &[InstanceConfig]) -> Result<()> {
    let mut ids = HashSet::new();
    let mut writers: HashMap<&str, &str> = HashMap::new();
    for inst in instances {
        if !ids.insert(inst.info.id.as_str()) {
            bail!("duplicate instance id {:?}", inst.info.id);
        }
        if inst.info.role == InstanceRole::ReadWrite {
            if let Some(prev) = writers.insert(inst.info.location.as_str(), inst.info.id.as_str()) {
                bail!(
                    "multiple read-write instances for location {:?}: {prev:?} and {:?}",
                    inst.info.location,
                    inst.info.id
                );
            }
        }
    }
    Ok(())
}

/// Compare live config fingerprints against rows loaded from `instances` table.
/// Returns Ok if every configured id matches, or the id is new; Err on mismatch.
pub fn check_fingerprints(
    configured: &[InstanceConfig],
    stored: &[(String, String /* fingerprint */)],
) -> Result<()> {
    let map: HashMap<&str, &str> = stored
        .iter()
        .map(|(id, fp)| (id.as_str(), fp.as_str()))
        .collect();
    for inst in configured {
        if let Some(prev) = map.get(inst.info.id.as_str()) {
            if *prev != inst.info.fingerprint.as_str() {
                bail!(
                    "instance {:?} fingerprint mismatch: config has {:?}, database has {:?}. \
                     Refusing to start (wrong bot/chat would corrupt replicas).",
                    inst.info.id,
                    inst.info.fingerprint,
                    prev
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "discord")]
    #[test]
    fn discord_kind_is_the_crates() {
        assert_eq!(InstanceKind::Discord.as_str(), pigeonhole_storage_discord::KIND);
    }

    #[test]
    fn rejects_two_writers_same_location() {
        let a = InstanceConfig {
            kind: InstanceKind::Telegram,
            info: InstanceInfo {
                id: "a".into(),
                kind: "telegram",
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_envs: vec!["T".into()],
            bot_tokens: vec!["1:x".into()],
            scope_id: "-100".into(),
            max_blob_size: None,
            telegram: Vec::new(),
        };
        let mut b = a.clone();
        b.info.id = "b".into();
        assert!(validate_instances(&[a, b]).is_err());
    }

    #[test]
    fn allows_writer_plus_readonly_same_location() {
        let a = InstanceConfig {
            kind: InstanceKind::Telegram,
            info: InstanceInfo {
                id: "rw".into(),
                kind: "telegram",
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_envs: vec!["T".into()],
            bot_tokens: vec!["1:x".into()],
            scope_id: "-100".into(),
            max_blob_size: None,
            telegram: Vec::new(),
        };
        let mut b = a.clone();
        b.info.id = "ro".into();
        b.info.role = InstanceRole::ReadOnly;
        b.info.fingerprint = "tg:2:-100".into();
        assert!(validate_instances(&[a, b]).is_ok());
    }

    #[test]
    fn fingerprint_mismatch_refuses() {
        let cfg = InstanceConfig {
            kind: InstanceKind::Telegram,
            info: InstanceInfo {
                id: "tg-main".into(),
                kind: "telegram",
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_envs: vec!["T".into()],
            bot_tokens: vec!["1:x".into()],
            scope_id: "-100".into(),
            max_blob_size: None,
            telegram: Vec::new(),
        };
        let err = check_fingerprints(
            &[cfg],
            &[("tg-main".into(), "tg:9:-100".into())],
        )
        .unwrap_err();
        assert!(err.to_string().contains("fingerprint mismatch"));
    }

    #[test]
    fn telegram_fp_from_token() {
        assert_eq!(
            telegram_fingerprint("123456:ABC-DEF", "-1001").unwrap(),
            "tg:123456:-1001"
        );
    }
}
