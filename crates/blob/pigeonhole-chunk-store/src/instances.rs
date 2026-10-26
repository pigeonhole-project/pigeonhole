//! Backend instances from `[[instances]]`.

use anyhow::{bail, Context, Result};
use pigeonhole_blob::{InstanceInfo, InstanceKind, InstanceRole};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// One configured backend instance after secret resolution.
#[derive(Debug, Clone)]
pub struct InstanceConfig {
    pub info: InstanceInfo,
    /// Env var name holding the bot/app token (never the secret itself).
    pub bot_token_env: String,
    /// Resolved token (from env); empty for memory.
    pub bot_token: String,
    /// Telegram chat id or Discord channel id (string form).
    pub scope_id: String,
    /// Per-message size limit override (Discord); `None` = backend default.
    pub max_blob_size: Option<usize>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileInstance {
    pub id: String,
    pub kind: String,
    pub bot_token_env: Option<String>,
    pub chat_id: Option<i64>,
    pub channel_id: Option<u64>,
    pub max_blob_size: Option<usize>,
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "read-write".into()
}

fn parse_role(s: &str) -> Result<InstanceRole> {
    match s {
        "read-write" | "rw" => Ok(InstanceRole::ReadWrite),
        "read-only" | "ro" => Ok(InstanceRole::ReadOnly),
        "retired" => Ok(InstanceRole::Retired),
        other => bail!("unknown instance role {other:?}; expected read-write|read-only|retired"),
    }
}

fn parse_kind(s: &str) -> Result<InstanceKind> {
    match s {
        "telegram" | "tg" => Ok(InstanceKind::Telegram),
        "discord" | "dc" => Ok(InstanceKind::Discord),
        "memory" => Ok(InstanceKind::Memory),
        other => bail!("unknown instance kind {other:?}"),
    }
}

/// Resolve `[[instances]]` entries; secrets loaded from env.
pub(crate) fn resolve_instances(raw: &[FileInstance]) -> Result<Vec<InstanceConfig>> {
    let mut out = Vec::with_capacity(raw.len());
    for f in raw {
        let kind = parse_kind(&f.kind)?;
        let role = parse_role(&f.role)?;
        let id = f.id.trim();
        if id.is_empty() {
            bail!("instance id must be non-empty");
        }
        let (bot_token_env, bot_token, scope_id, fingerprint, location) = match kind {
            InstanceKind::Memory => (
                String::new(),
                String::new(),
                "local".into(),
                memory_fingerprint(id),
                memory_location(id),
            ),
            InstanceKind::Telegram => {
                let chat = f
                    .chat_id
                    .context("telegram instance requires chat_id")?;
                let env_name = f
                    .bot_token_env
                    .clone()
                    .unwrap_or_else(|| "BOT_TOKEN".into());
                let token = std::env::var(&env_name).with_context(|| {
                    format!("instance {id}: missing env {env_name}")
                })?;
                let scope = chat.to_string();
                let fp = telegram_fingerprint(&token, &scope)?;
                let loc = telegram_location(&scope);
                (env_name, token, scope, fp, loc)
            }
            InstanceKind::Discord => {
                let channel = f
                    .channel_id
                    .context("discord instance requires channel_id")?;
                let env_name = f
                    .bot_token_env
                    .clone()
                    .unwrap_or_else(|| "DISCORD_BOT_TOKEN".into());
                let token = std::env::var(&env_name).with_context(|| {
                    format!("instance {id}: missing env {env_name}")
                })?;
                let scope = channel.to_string();
                let app_id = discord_app_id_from_token(&token);
                let fp = discord_fingerprint(&app_id, &scope);
                let loc = discord_location(&scope);
                (env_name, token, scope, fp, loc)
            }
        };
        out.push(InstanceConfig {
            info: InstanceInfo {
                id: id.to_string(),
                kind,
                fingerprint,
                location,
                role,
            },
            bot_token_env,
            bot_token,
            scope_id,
            max_blob_size: f.max_blob_size,
        });
    }
    validate_instances(&out)?;
    Ok(out)
}

/// In-process memory instance (tests, local runs).
pub fn memory_instance(id: &str) -> InstanceConfig {
    InstanceConfig {
        info: InstanceInfo {
            id: id.into(),
            kind: InstanceKind::Memory,
            fingerprint: memory_fingerprint(id),
            location: memory_location(id),
            role: InstanceRole::ReadWrite,
        },
        bot_token_env: String::new(),
        bot_token: String::new(),
        scope_id: "local".into(),
        max_blob_size: None,
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

    #[test]
    fn rejects_two_writers_same_location() {
        let a = InstanceConfig {
            info: InstanceInfo {
                id: "a".into(),
                kind: InstanceKind::Telegram,
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_env: "T".into(),
            bot_token: "1:x".into(),
            scope_id: "-100".into(),
            max_blob_size: None,
        };
        let mut b = a.clone();
        b.info.id = "b".into();
        assert!(validate_instances(&[a, b]).is_err());
    }

    #[test]
    fn allows_writer_plus_readonly_same_location() {
        let a = InstanceConfig {
            info: InstanceInfo {
                id: "rw".into(),
                kind: InstanceKind::Telegram,
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_env: "T".into(),
            bot_token: "1:x".into(),
            scope_id: "-100".into(),
            max_blob_size: None,
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
            info: InstanceInfo {
                id: "tg-main".into(),
                kind: InstanceKind::Telegram,
                fingerprint: "tg:1:-100".into(),
                location: "tg:chat:-100".into(),
                role: InstanceRole::ReadWrite,
            },
            bot_token_env: "T".into(),
            bot_token: "1:x".into(),
            scope_id: "-100".into(),
            max_blob_size: None,
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
