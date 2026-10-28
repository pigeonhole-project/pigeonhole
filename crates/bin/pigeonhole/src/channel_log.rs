//! Binary-only: one MTProto login per bot for the whole process, shared by
//! its blob stores, the log and the snapshots.

use anyhow::{bail, Context};
use pigeonhole_chunk_store::TelegramLogin;
use pigeonhole_storage_telegram::{MtprotoConfig, TelegramMtproto};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

/// The process's MTProto clients by bot id: one login (and session file) per
/// bot, shared by its blob stores, the log and the snapshots.
static CLIENTS: OnceLock<tokio::sync::Mutex<HashMap<i64, (PathBuf, TelegramMtproto)>>> = OnceLock::new();

pub async fn client(login: &TelegramLogin) -> anyhow::Result<TelegramMtproto> {
    let bot_id = pigeonhole_storage_telegram::bot_id_of(&login.bot_token)?;
    let mut clients = CLIENTS.get_or_init(Default::default).lock().await;
    if let Some((session, client)) = clients.get(&bot_id) {
        if *session != login.session {
            bail!(
                "bot {bot_id} is configured with two session files ({} and {}): one bot, one session",
                session.display(),
                login.session.display()
            );
        }
        return Ok(client.clone());
    }
    let cfg = MtprotoConfig {
        api_id: login.api_id,
        api_hash: login.api_hash.clone(),
        bot_token: login.bot_token.clone(),
        session: login.session.clone(),
    };
    let client = TelegramMtproto::connect(&cfg).await.with_context(|| format!("MTProto login of bot {bot_id}"))?;
    clients.insert(bot_id, (login.session.clone(), client.clone()));
    Ok(client)
}
