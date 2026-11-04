//! Binary-only: one MTProto login per bot for the whole process, shared by
//! its blob stores, the log and the snapshots, and the posts Telegram pushes
//! to it handed to who waits for a channel's ([`on_post`]: the Raft outboxes).

use anyhow::{bail, Context};
use pigeonhole::instances::TelegramLogin;
use pigeonhole_storage_telegram::ChannelMessage;
use pigeonhole_telegram_mtproto::{new_channel_posts, MtprotoConfig, TelegramMtproto, UpdatesRx};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

/// The process's MTProto clients by bot id: one login (and session file) per
/// bot, shared by its blob stores, the log and the snapshots.
static CLIENTS: OnceLock<tokio::sync::Mutex<HashMap<i64, (PathBuf, TelegramMtproto)>>> = OnceLock::new();

/// Who to tell of a channel's new posts, by channel id (without `-100`).
type Listener = Arc<dyn Fn(i64, ChannelMessage) + Send + Sync>;
static LISTENERS: OnceLock<Mutex<HashMap<i64, Vec<Listener>>>> = OnceLock::new();

/// Call `f` with every post in channel `channel_id` (without the `-100`)
/// that Telegram pushes to any of this process's bots: its id, the message.
pub fn on_post(channel_id: i64, f: impl Fn(i64, ChannelMessage) + Send + Sync + 'static) {
    LISTENERS.get_or_init(Default::default).lock().unwrap().entry(channel_id).or_default().push(Arc::new(f));
}

/// Hand each pushed post to its channel's listeners, for as long as the
/// bot's connections push.
async fn dispatch(mut updates: UpdatesRx) {
    while let Some(update) = updates.recv().await {
        for (channel, id, message) in new_channel_posts(&update) {
            let listeners = LISTENERS.get_or_init(Default::default).lock().unwrap().get(&channel).cloned();
            for f in listeners.into_iter().flatten() {
                f(id, message.clone());
            }
        }
    }
}

pub async fn client(login: &TelegramLogin) -> anyhow::Result<TelegramMtproto> {
    let bot_id = pigeonhole_telegram_mtproto::bot_id_of(&login.bot_token)?;
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
    let (client, updates) =
        TelegramMtproto::connect_with_updates(&cfg).await.with_context(|| format!("MTProto login of bot {bot_id}"))?;
    tokio::spawn(dispatch(updates));
    clients.insert(bot_id, (login.session.clone(), client.clone()));
    Ok(client)
}
