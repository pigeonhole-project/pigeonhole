//! Binary-only: the MTProto side of the channel LogStore and snapshots
//! (`[meta] log` / `snapshot` naming a `telegram_mtproto` `[[store]]`), and
//! one MTProto login per bot for the whole process.

use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_chunk_store::TelegramLogin;
use pigeonhole_meta::log::channel::{ChannelApi, ChannelMsg};
use lru::LruCache;
use pigeonhole_storage_telegram_mtproto::{
    windows, ChannelApi as _, ChannelMessage, FileLocation, FileReferenceExpired, MtprotoChannel, MtprotoConfig,
    TelegramMtproto, IDS_PER_CALL, WINDOW,
};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// The process's MTProto clients by bot id: one login (and session file) per
/// bot, shared by its blob stores, the log and the snapshots.
static CLIENTS: OnceLock<tokio::sync::Mutex<HashMap<i64, (PathBuf, TelegramMtproto)>>> = OnceLock::new();

pub async fn client(login: &TelegramLogin) -> anyhow::Result<TelegramMtproto> {
    let bot_id = pigeonhole_storage_telegram_mtproto::bot_id_of(&login.bot_token)?;
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

/// A channel as the log and the snapshots see it.
pub struct LogChannel {
    channel: MtprotoChannel,
    /// Where recently read documents are: a replay follows its chain down
    /// message by message, and one read of the page below a missing one
    /// locates the next ones too.
    located: Mutex<LruCache<i64, (u64, FileLocation)>>,
}

impl LogChannel {
    pub fn new(channel: MtprotoChannel) -> Self {
        Self { channel, located: Mutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap())) }
    }

    fn remember(&self, ids: &[i64], msgs: &[ChannelMessage]) {
        let mut located = self.located.lock().unwrap();
        for (id, m) in ids.iter().zip(msgs) {
            if let ChannelMessage::Document { size, file, .. } = m {
                located.put(*id, (*size, file.clone()));
            }
        }
    }

    /// Where `message_id`'s document is; `fresh` reads it again.
    async fn locate(&self, message_id: i64, fresh: bool) -> anyhow::Result<(u64, FileLocation)> {
        if !fresh {
            if let Some(l) = self.located.lock().unwrap().get(&message_id) {
                return Ok(l.clone());
            }
        }
        let lo = if fresh { message_id } else { (message_id - IDS_PER_CALL as i64 + 1).max(1) };
        let ids: Vec<i64> = (lo..=message_id).collect();
        let msgs = self.channel.get_messages(&ids).await?;
        self.remember(&ids, &msgs);
        match msgs.last() {
            Some(ChannelMessage::Document { size, file, .. }) => Ok((*size, file.clone())),
            _ => bail!("message {message_id} is not a document"),
        }
    }
}

#[async_trait]
impl ChannelApi for LogChannel {
    async fn send_document(&self, name: &str, data: Vec<u8>) -> anyhow::Result<i64> {
        // One random_id for every retry of this send: it posts at most once.
        let random_id = uuid::Uuid::new_v4().as_u64_pair().0 as i64;
        Ok(self.channel.send_document(name, Bytes::from(data), random_id).await?.message_id)
    }

    async fn get_messages(&self, ids: &[i64]) -> anyhow::Result<Vec<ChannelMsg>> {
        let msgs = self.channel.get_messages(ids).await?;
        self.remember(ids, &msgs);
        Ok(msgs
            .into_iter()
            .map(|m| match m {
                ChannelMessage::Empty => ChannelMsg::Empty,
                ChannelMessage::Other => ChannelMsg::Other,
                ChannelMessage::Document { name, .. } => ChannelMsg::Doc { name },
            })
            .collect())
    }

    async fn download(&self, message_id: i64) -> anyhow::Result<Vec<u8>> {
        let (size, mut file) = self.locate(message_id, false).await?;
        let mut out = Vec::with_capacity(size as usize);
        for (offset, skip, take) in windows(0, size) {
            let bytes = match self.channel.get_file(&file, offset, WINDOW).await {
                Err(e) if e.is::<FileReferenceExpired>() => {
                    file = self.locate(message_id, true).await?.1;
                    self.channel.get_file(&file, offset, WINDOW).await?
                }
                other => other?,
            };
            let part = bytes.get(skip..skip + take).with_context(|| format!("message {message_id}: short window"))?;
            out.extend_from_slice(part);
        }
        Ok(out)
    }

    async fn delete(&self, message_ids: &[i64]) -> anyhow::Result<()> {
        self.channel.delete_messages(message_ids).await
    }
}
