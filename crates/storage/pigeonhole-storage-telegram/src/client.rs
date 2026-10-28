//! The MTProto client: one bot login per process, sender pools ("lanes")
//! sharing its session, and [`MtprotoChannel`], the [`ChannelApi`] over it.
//!
//! grammers' own flood-wait sleeping is off ([`NoRetries`]): a
//! `FLOOD_WAIT_X` becomes [`RateLimited`], cools the [`ChatLimiter`] bucket
//! of that call and is waited out here, like every other transient failure.
//! Every call we make is idempotent (a send carries its `random_id`), so all
//! of them are retried within [`CALL_BUDGET`].

use crate::channel::{ChannelApi, ChannelMessage, FileLocation, FileReferenceExpired, Sent, IDS_PER_CALL, WINDOW};
use crate::session::FileSession;
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use grammers_mtsender::{ConnectionParams, InvocationError, NoRetries, SenderPool, SenderPoolHandle};
use grammers_tl_types::{self as tl, enums, functions, types};
use pigeonhole_blob::{ChatLimiter, RateLimited};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// How long one call keeps retrying flood waits and network errors.
pub const CALL_BUDGET: Duration = Duration::from_secs(300);
/// Upload parts of one file in flight.
const PARTS_IN_FLIGHT: usize = 4;
/// Upload part size (the most Telegram takes).
const PART: usize = 512 * 1024;
/// Files above this go up as "big" files (`saveBigFilePart`).
const BIG_FILE: usize = 10 * 1024 * 1024;

/// What a bot login needs.
#[derive(Clone)]
pub struct MtprotoConfig {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
    pub session: PathBuf,
}

impl std::fmt::Debug for MtprotoConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtprotoConfig").field("api_id", &self.api_id).field("session", &self.session).finish()
    }
}

/// The bot id: the token's prefix before `:`.
pub fn bot_id_of(token: &str) -> Result<i64> {
    token
        .split_once(':')
        .and_then(|(id, _)| id.parse().ok())
        .context("bot token must look like <bot id>:<secret>")
}

/// Which sender pool (connection) a call uses: the log's and the snapshots'
/// small sends must not queue behind blob upload parts on one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    Blobs,
    Meta,
}

struct Pool {
    handle: SenderPoolHandle,
    runner: JoinHandle<()>,
    /// DCs other than the home one this pool's connections are logged in to.
    authorized_dcs: tokio::sync::Mutex<HashSet<i32>>,
}

struct Inner {
    session: Arc<FileSession>,
    bot_id: i64,
    blobs: Pool,
    meta: Pool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for pool in [&self.blobs, &self.meta] {
            pool.handle.quit();
            pool.runner.abort();
        }
    }
}

/// A logged-in bot; clones share the connections.
#[derive(Clone)]
pub struct TelegramMtproto {
    inner: Arc<Inner>,
}

fn pool(session: &Arc<FileSession>, api_id: i32) -> Pool {
    let params = ConnectionParams { retry_policy: Box::new(NoRetries), ..Default::default() };
    let pool = SenderPool::with_configuration(session.clone(), api_id, params);
    // Updates are ignored: with the receiver gone the pool drops them.
    drop(pool.updates);
    Pool {
        handle: pool.handle.thin.clone(),
        runner: tokio::spawn(pool.runner.run()),
        authorized_dcs: tokio::sync::Mutex::new(HashSet::new()),
    }
}

impl TelegramMtproto {
    /// Log in as the bot of `cfg.bot_token`, reusing the session's auth key
    /// when it belongs to that bot.
    pub async fn connect(cfg: &MtprotoConfig) -> Result<Self> {
        let bot_id = bot_id_of(&cfg.bot_token)?;
        let session = Arc::new(FileSession::open(&cfg.session)?);
        let blobs = pool(&session, cfg.api_id);
        let meta = pool(&session, cfg.api_id);
        let this = Self { inner: Arc::new(Inner { session, bot_id, blobs, meta }) };
        if !this.logged_in().await? {
            this.sign_in(cfg).await?;
        }
        Ok(this)
    }

    pub fn bot_id(&self) -> i64 {
        self.inner.bot_id
    }

    /// Whether the session's auth key is logged in as our bot.
    async fn logged_in(&self) -> Result<bool> {
        if !self.inner.session.has_home_key() {
            return Ok(false);
        }
        let me = functions::users::GetUsers { id: vec![enums::InputUser::UserSelf] };
        match self.home(Lane::Meta, &me).await {
            Ok(users) => {
                let id = users.first().map(|u| u.id());
                if id != Some(self.inner.bot_id) {
                    bail!(
                        "MTProto session belongs to user {id:?}, not bot {}: delete the session file",
                        self.inner.bot_id
                    );
                }
                Ok(true)
            }
            Err(InvocationError::Rpc(e)) if e.code == 401 => Ok(false),
            Err(e) => Err(anyhow!("check the MTProto login: {e}")),
        }
    }

    async fn sign_in(&self, cfg: &MtprotoConfig) -> Result<()> {
        let request = functions::auth::ImportBotAuthorization {
            flags: 0,
            api_id: cfg.api_id,
            api_hash: cfg.api_hash.clone(),
            bot_auth_token: cfg.bot_token.clone(),
        };
        let started = std::time::Instant::now();
        let auth = match self.home(Lane::Meta, &request).await {
            Err(InvocationError::Rpc(e)) if e.code == 303 => {
                // The bot lives in another DC: make it home and log in there.
                let dc = e.value.context("a DC migration without a DC")? as i32;
                let old = self.inner.session.home_dc_id().map_err(|e| anyhow!("{e}"))?;
                use grammers_session::Session as _;
                self.inner.session.set_home_dc_id(dc).await.map_err(|e| anyhow!("{e}"))?;
                for pool in [&self.inner.blobs, &self.inner.meta] {
                    pool.handle.disconnect_from_dc(old);
                }
                self.home(Lane::Meta, &request).await
            }
            other => other,
        }
        .map_err(|e| anyhow!("bot login (auth.importBotAuthorization): {e}"))?;
        let enums::auth::Authorization::Authorization(auth) = auth else {
            bail!("bot login answered sign-up required");
        };
        if auth.user.id() != self.inner.bot_id {
            bail!("bot login returned user {}, expected {}", auth.user.id(), self.inner.bot_id);
        }
        info!(bot_id = self.inner.bot_id, elapsed = ?started.elapsed(), "MTProto bot login");
        Ok(())
    }

    fn pool(&self, lane: Lane) -> &Pool {
        match lane {
            Lane::Blobs => &self.inner.blobs,
            Lane::Meta => &self.inner.meta,
        }
    }

    /// One attempt in the home DC.
    async fn home<R: tl::RemoteCall>(&self, lane: Lane, request: &R) -> Result<R::Return, InvocationError> {
        use grammers_session::Session as _;
        let dc = self.inner.session.home_dc_id().map_err(|e| InvocationError::Session(Box::new(e)))?;
        self.pool(lane).handle.invoke_in_dc(dc, request).await
    }

    /// One attempt in `dc`, logging the pool in there first if needed.
    async fn in_dc<R: tl::RemoteCall>(&self, lane: Lane, dc: i32, request: &R) -> Result<R::Return, InvocationError> {
        use grammers_session::Session as _;
        let home = self.inner.session.home_dc_id().map_err(|e| InvocationError::Session(Box::new(e)))?;
        if dc == home {
            return self.home(lane, request).await;
        }
        let pool = self.pool(lane);
        {
            let mut done = pool.authorized_dcs.lock().await;
            if !done.contains(&dc) {
                let enums::auth::ExportedAuthorization::Authorization(exported) =
                    self.home(lane, &functions::auth::ExportAuthorization { dc_id: dc }).await?;
                let import = functions::auth::ImportAuthorization { id: exported.id, bytes: exported.bytes };
                pool.handle.invoke_in_dc(dc, &import).await?;
                done.insert(dc);
            }
        }
        match pool.handle.invoke_in_dc(dc, request).await {
            Err(InvocationError::Rpc(e)) if e.name == "AUTH_KEY_UNREGISTERED" => {
                // A new connection to that DC got a new key: log in again.
                pool.authorized_dcs.lock().await.remove(&dc);
                Err(InvocationError::Rpc(e))
            }
            other => other,
        }
    }

    /// Resolve `chat_id` (Bot API form, `-100…`) to a channel this bot
    /// administers with post and delete rights; calls go through `lane` and
    /// are metered by `limiter`.
    pub async fn channel(&self, chat_id: i64, lane: Lane, limiter: Arc<ChatLimiter>) -> Result<MtprotoChannel> {
        let channel_id = channel_id_of(chat_id)?;
        // A bot may name a channel it is in with access_hash 0.
        let request = functions::channels::GetChannels {
            id: vec![types::InputChannel { channel_id, access_hash: 0 }.into()],
        };
        let chats = retry_call("channels.getChannels", CALL_BUDGET, None, || self.home(lane, &request))
            .await
            .with_context(|| format!("chat {chat_id}: is the bot a member of this channel?"))?;
        let channel = chats
            .chats()
            .into_iter()
            .find_map(|c| match c {
                enums::Chat::Channel(c) if c.id == channel_id => Some(c),
                _ => None,
            })
            .with_context(|| format!("chat {chat_id} is not a channel or supergroup the bot can see"))?;
        let rights = match &channel.admin_rights {
            Some(enums::ChatAdminRights::Rights(r)) => r.clone(),
            None if channel.creator => bail!("chat {chat_id}: unexpected creator without admin rights"),
            None => bail!("chat {chat_id}: the bot is not an admin"),
        };
        // Supergroup members post without rights; a channel needs post_messages.
        if (channel.broadcast && !rights.post_messages) || !rights.delete_messages {
            bail!("chat {chat_id}: the bot needs the post and delete messages admin rights");
        }
        let access_hash = channel.access_hash.context("channel without an access hash")?;
        debug!(chat_id, title = %channel.title, "resolved channel");
        Ok(MtprotoChannel { client: self.clone(), lane, channel_id, access_hash, limiter })
    }
}

/// `-100<id>` (Bot API) → `<id>`; basic groups and private chats are refused:
/// their message ids are not per chat, so ranges cannot be read.
pub fn channel_id_of(chat_id: i64) -> Result<i64> {
    const PREFIX: i64 = -1_000_000_000_000;
    if chat_id >= PREFIX {
        bail!("chat {chat_id} is not a channel or supergroup (-100… id); other chats have no per-chat message ids");
    }
    Ok(PREFIX - chat_id)
}

/// How a failed attempt is handled.
enum Failure {
    /// Rate limited for this long (the call was not applied).
    Flood(Duration),
    /// Worth another attempt after a backoff.
    Transient,
    /// Hand the error up as is.
    Fatal,
}

fn classify(e: &InvocationError) -> Failure {
    match e {
        InvocationError::Rpc(rpc) if rpc.code == 420 || rpc.name.ends_with("_WAIT") => {
            Failure::Flood(Duration::from_secs(rpc.value.unwrap_or(1).max(1) as u64))
        }
        InvocationError::Rpc(rpc) if rpc.code >= 500 || rpc.code == -503 || rpc.name == "AUTH_KEY_UNREGISTERED" => {
            Failure::Transient
        }
        InvocationError::Rpc(_) | InvocationError::InvalidDc => Failure::Fatal,
        InvocationError::Io(_)
        | InvocationError::Dropped(_)
        | InvocationError::Transport(_)
        | InvocationError::Deserialize(_)
        | InvocationError::Authentication(_)
        | InvocationError::Session(_) => Failure::Transient,
    }
}

/// Run `call` until it succeeds or fails for good, waiting out flood waits
/// (and cooling `penalize`'s bucket) and retrying transient failures within
/// `budget`. A flood wait that does not fit is [`RateLimited`].
async fn retry_call<T, Fut>(
    what: &'static str,
    budget: Duration,
    penalize: Option<&(dyn Fn(Duration) + Sync)>,
    mut call: impl FnMut() -> Fut,
) -> Result<T>
where
    Fut: std::future::Future<Output = Result<T, InvocationError>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = Duration::from_millis(500);
    loop {
        let err = match call().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        let wait = match classify(&err) {
            Failure::Flood(wait) => {
                if let Some(penalize) = penalize {
                    penalize(wait);
                }
                if tokio::time::Instant::now() + wait >= deadline {
                    return Err(anyhow::Error::new(RateLimited { method: what, retry_after: wait }));
                }
                wait
            }
            Failure::Transient => {
                let wait = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(16));
                if tokio::time::Instant::now() + wait >= deadline {
                    return Err(anyhow!("{what}: {err} (still failing after {budget:?})"));
                }
                wait
            }
            Failure::Fatal => return Err(map_fatal(what, err)),
        };
        warn!(what, ?wait, error = %err, "MTProto call failed; retrying");
        tokio::time::sleep(wait).await;
    }
}

fn map_fatal(what: &'static str, err: InvocationError) -> anyhow::Error {
    if err.is("FILE_REFERENCE_*") {
        return anyhow::Error::new(FileReferenceExpired);
    }
    anyhow!("{what}: {err}")
}

/// One channel over MTProto.
#[derive(Clone)]
pub struct MtprotoChannel {
    client: TelegramMtproto,
    lane: Lane,
    channel_id: i64,
    access_hash: i64,
    limiter: Arc<ChatLimiter>,
}

impl MtprotoChannel {
    fn input_channel(&self) -> enums::InputChannel {
        types::InputChannel { channel_id: self.channel_id, access_hash: self.access_hash }.into()
    }

    fn input_peer(&self) -> enums::InputPeer {
        types::InputPeerChannel { channel_id: self.channel_id, access_hash: self.access_hash }.into()
    }

    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        &self.limiter
    }

    /// The same channel through another lane.
    pub fn on_lane(&self, lane: Lane) -> Self {
        Self { lane, ..self.clone() }
    }

    /// Upload `data` as one file; returns what `sendMedia` takes.
    async fn upload(&self, name: &str, data: &Bytes) -> Result<enums::InputFile> {
        let file_id = random_i64();
        let parts = data.len().div_ceil(PART);
        let big = data.len() > BIG_FILE;
        let bot = self.limiter.bot().clone();
        let penalize = move |d: Duration| bot.penalize(d);
        futures::stream::iter(0..parts)
            .map(|k| {
                let bytes = data[k * PART..((k + 1) * PART).min(data.len())].to_vec();
                let penalize = &penalize;
                async move {
                    let ok = if big {
                        let request = functions::upload::SaveBigFilePart {
                            file_id,
                            file_part: k as i32,
                            file_total_parts: parts as i32,
                            bytes,
                        };
                        retry_call("upload.saveBigFilePart", CALL_BUDGET, Some(penalize), || {
                            self.client.home(self.lane, &request)
                        })
                        .await?
                    } else {
                        let request = functions::upload::SaveFilePart { file_id, file_part: k as i32, bytes };
                        retry_call("upload.saveFilePart", CALL_BUDGET, Some(penalize), || {
                            self.client.home(self.lane, &request)
                        })
                        .await?
                    };
                    if !ok {
                        bail!("upload part {k} of {name} refused");
                    }
                    Ok(())
                }
            })
            .buffer_unordered(PARTS_IN_FLIGHT)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(if big {
            types::InputFileBig { id: file_id, parts: parts as i32, name: name.into() }.into()
        } else {
            types::InputFile { id: file_id, parts: parts as i32, name: name.into(), md5_checksum: String::new() }.into()
        })
    }
}

fn random_i64() -> i64 {
    uuid::Uuid::new_v4().as_u64_pair().0 as i64
}

/// A message as [`ChannelMessage`].
fn channel_message(m: enums::Message) -> (i64, ChannelMessage) {
    match m {
        enums::Message::Empty(e) => (e.id as i64, ChannelMessage::Empty),
        enums::Message::Service(s) => (s.id as i64, ChannelMessage::Other),
        enums::Message::Message(m) => {
            let no_media = m.media.is_none();
            let doc = match m.media {
                Some(enums::MessageMedia::Document(types::MessageMediaDocument {
                    document: Some(enums::Document::Document(d)),
                    ..
                })) => Some(d),
                _ => None,
            };
            let Some(d) = doc else {
                if no_media && !m.message.is_empty() {
                    return (m.id as i64, ChannelMessage::Text { text: m.message });
                }
                return (m.id as i64, ChannelMessage::Other);
            };
            let name = d.attributes.iter().find_map(|a| match a {
                enums::DocumentAttribute::Filename(f) => Some(f.file_name.clone()),
                _ => None,
            });
            let file = FileLocation { id: d.id, access_hash: d.access_hash, file_reference: d.file_reference, dc_id: d.dc_id };
            let msg = ChannelMessage::Document { name: name.unwrap_or_default(), size: d.size.max(0) as u64, file };
            (m.id as i64, msg)
        }
    }
}

/// The posted message's id (and document) in a `sendMedia` answer.
fn sent_message(updates: enums::Updates, random_id: i64) -> Result<Sent> {
    let list = match updates {
        enums::Updates::Updates(u) => u.updates,
        enums::Updates::Combined(u) => u.updates,
        enums::Updates::UpdateShort(u) => vec![u.update],
        enums::Updates::UpdateShortSentMessage(m) => return Ok(Sent { message_id: m.id as i64, file: None }),
        other => bail!("sendMedia answered {other:?}"),
    };
    let mut id = None;
    let mut file = None;
    for u in list {
        match u {
            enums::Update::MessageId(m) if m.random_id == random_id => id = Some(m.id as i64),
            enums::Update::NewChannelMessage(m) => {
                if let (mid, ChannelMessage::Document { file: f, .. }) = channel_message(m.message) {
                    id.get_or_insert(mid);
                    if id == Some(mid) {
                        file = Some(f);
                    }
                }
            }
            _ => {}
        }
    }
    let message_id = id.context("sendMedia answer without the message id")?;
    Ok(Sent { message_id, file })
}

#[async_trait]
impl ChannelApi for MtprotoChannel {
    fn channel_id(&self) -> i64 {
        self.channel_id
    }

    async fn get_messages(&self, ids: &[i64]) -> Result<Vec<ChannelMessage>> {
        if ids.len() > IDS_PER_CALL {
            bail!("getMessages takes at most {IDS_PER_CALL} ids, got {}", ids.len());
        }
        let request = functions::channels::GetMessages {
            channel: self.input_channel(),
            id: ids.iter().map(|&id| types::InputMessageId { id: id as i32 }.into()).collect(),
        };
        let limiter = self.limiter.clone();
        let penalize = move |d: Duration| limiter.penalize_info(d);
        let answer = retry_call("channels.getMessages", CALL_BUDGET, Some(&penalize), || async {
            self.limiter.acquire_info().await;
            self.client.home(self.lane, &request).await
        })
        .await?;
        let messages = match answer {
            enums::messages::Messages::Messages(m) => m.messages,
            enums::messages::Messages::Slice(m) => m.messages,
            enums::messages::Messages::ChannelMessages(m) => m.messages,
            enums::messages::Messages::NotModified(_) => Vec::new(),
        };
        let mut by_id: std::collections::HashMap<i64, ChannelMessage> =
            messages.into_iter().map(channel_message).collect();
        Ok(ids.iter().map(|id| by_id.remove(id).unwrap_or(ChannelMessage::Empty)).collect())
    }

    async fn send_document(&self, name: &str, data: Bytes, random_id: i64) -> Result<Sent> {
        if data.is_empty() {
            bail!("Telegram does not take empty documents");
        }
        let file = self.upload(name, &data).await.with_context(|| format!("upload {name}"))?;
        let media: enums::InputMedia = types::InputMediaUploadedDocument {
            nosound_video: false,
            force_file: true,
            spoiler: false,
            file,
            thumb: None,
            mime_type: "application/octet-stream".into(),
            attributes: vec![types::DocumentAttributeFilename { file_name: name.into() }.into()],
            stickers: None,
            video_cover: None,
            video_timestamp: None,
            ttl_seconds: None,
        }
        .into();
        let request = functions::messages::SendMedia {
            silent: true,
            background: false,
            clear_draft: false,
            noforwards: false,
            update_stickersets_order: false,
            invert_media: false,
            allow_paid_floodskip: false,
            peer: self.input_peer(),
            reply_to: None,
            media,
            message: String::new(),
            random_id,
            reply_markup: None,
            entities: None,
            schedule_date: None,
            schedule_repeat_period: None,
            send_as: None,
            quick_reply_shortcut: None,
            effect: None,
            allow_paid_stars: None,
            suggested_post: None,
        };
        let limiter = self.limiter.clone();
        let penalize = move |d: Duration| limiter.penalize_send(d);
        // Repeats carry the same random_id: Telegram returns the message a
        // lost answer was about instead of posting another.
        let updates = retry_call("messages.sendMedia", CALL_BUDGET, Some(&penalize), || async {
            self.limiter.acquire_send().await;
            self.client.home(self.lane, &request).await
        })
        .await?;
        sent_message(updates, random_id)
    }

    async fn send_text(&self, text: &str, random_id: i64) -> Result<Sent> {
        let request = functions::messages::SendMessage {
            no_webpage: true,
            silent: true,
            background: false,
            clear_draft: false,
            noforwards: false,
            update_stickersets_order: false,
            invert_media: false,
            allow_paid_floodskip: false,
            peer: self.input_peer(),
            reply_to: None,
            message: text.into(),
            random_id,
            reply_markup: None,
            entities: None,
            schedule_date: None,
            schedule_repeat_period: None,
            send_as: None,
            quick_reply_shortcut: None,
            effect: None,
            allow_paid_stars: None,
            suggested_post: None,
            rich_message: None,
        };
        let limiter = self.limiter.clone();
        let penalize = move |d: Duration| limiter.penalize_send(d);
        let updates = retry_call("messages.sendMessage", CALL_BUDGET, Some(&penalize), || async {
            self.limiter.acquire_send().await;
            self.client.home(self.lane, &request).await
        })
        .await?;
        sent_message(updates, random_id)
    }

    async fn get_file(&self, file: &FileLocation, offset: u64, limit: u64) -> Result<Bytes> {
        if limit > WINDOW || offset % 4096 != 0 || limit % 4096 != 0 {
            bail!("getFile window {offset}+{limit} is not 4 KiB-aligned within {WINDOW}");
        }
        let request = functions::upload::GetFile {
            precise: true,
            cdn_supported: false,
            location: types::InputDocumentFileLocation {
                id: file.id,
                access_hash: file.access_hash,
                file_reference: file.file_reference.clone(),
                thumb_size: String::new(),
            }
            .into(),
            offset: offset as i64,
            limit: limit as i32,
        };
        let bot = self.limiter.bot().clone();
        let penalize = move |d: Duration| bot.penalize(d);
        let mut dc = file.dc_id;
        // A migration answer names the DC that serves the file.
        for _ in 0..3 {
            let answer = retry_call("upload.getFile", CALL_BUDGET, Some(&penalize), || {
                self.client.in_dc(self.lane, dc, &request)
            })
            .await;
            match answer {
                Ok(enums::upload::File::File(f)) => return Ok(Bytes::from(f.bytes)),
                Ok(enums::upload::File::CdnRedirect(_)) => bail!("getFile redirected to a CDN despite cdn_supported = false"),
                Err(e) => match migrate_dc(&e) {
                    Some(to) => dc = to,
                    None => return Err(e),
                },
            }
        }
        bail!("getFile kept migrating")
    }

    async fn delete_messages(&self, ids: &[i64]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        if ids.len() > IDS_PER_CALL {
            bail!("deleteMessages takes at most {IDS_PER_CALL} ids, got {}", ids.len());
        }
        let request = functions::channels::DeleteMessages {
            channel: self.input_channel(),
            id: ids.iter().map(|&id| id as i32).collect(),
        };
        let limiter = self.limiter.clone();
        let penalize = move |d: Duration| limiter.penalize_delete(d);
        retry_call("channels.deleteMessages", CALL_BUDGET, Some(&penalize), || async {
            self.limiter.acquire_delete().await;
            self.client.home(self.lane, &request).await
        })
        .await?;
        Ok(())
    }
}

/// `FILE_MIGRATE_X` → `X`.
fn migrate_dc(e: &anyhow::Error) -> Option<i32> {
    let text = e.to_string();
    let rest = text.split("FILE_MIGRATE").nth(1)?;
    rest.split(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty())?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use grammers_mtsender::RpcError;

    fn rpc(code: i32, message: &str) -> InvocationError {
        InvocationError::Rpc(RpcError::from(types::RpcError { error_code: code, error_message: message.into() }))
    }

    #[test]
    fn chat_ids_and_errors_map_as_expected() {
        assert_eq!(channel_id_of(-1003844711907).unwrap(), 3844711907);
        assert!(channel_id_of(-4012345).is_err(), "a basic group");
        assert!(channel_id_of(12345).is_err(), "a private chat");
        assert_eq!(bot_id_of("123456:ABC").unwrap(), 123456);

        assert!(matches!(classify(&rpc(420, "FLOOD_WAIT_7")), Failure::Flood(d) if d == Duration::from_secs(7)));
        assert!(matches!(classify(&rpc(500, "INTERDC_2_CALL_ERROR")), Failure::Transient));
        assert!(matches!(classify(&rpc(400, "CHANNEL_INVALID")), Failure::Fatal));
        assert!(matches!(classify(&InvocationError::Dropped("x")), Failure::Transient));
        let e = map_fatal("upload.getFile", rpc(400, "FILE_REFERENCE_EXPIRED"));
        assert!(e.is::<FileReferenceExpired>());
        let e = map_fatal("upload.getFile", rpc(303, "FILE_MIGRATE_4"));
        assert_eq!(migrate_dc(&e), Some(4));
    }

    #[tokio::test(start_paused = true)]
    async fn flood_waits_are_waited_out_until_the_budget_runs_out() {
        let tries = std::sync::atomic::AtomicU32::new(0);
        let penalized = std::sync::Mutex::new(Vec::new());
        let penalize = |d: Duration| penalized.lock().unwrap().push(d);
        let v = retry_call("x", Duration::from_secs(60), Some(&penalize), || {
            let n = tries.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { if n < 2 { Err(rpc(420, "FLOOD_WAIT_5")) } else { Ok(9) } }
        })
        .await
        .unwrap();
        assert_eq!(v, 9);
        assert_eq!(*penalized.lock().unwrap(), vec![Duration::from_secs(5); 2]);

        let err = retry_call("y", Duration::from_secs(10), None, || async { Err::<(), _>(rpc(420, "FLOOD_WAIT_30")) })
            .await
            .unwrap_err();
        assert!(err.is::<RateLimited>(), "{err:#}");
    }
}

/// Live: is a `sendMedia` whose caller gave up before it was written posted
/// anyway? Without the vendored grammers-mtsender patch it is (the request is
/// queued and goes out when its turn comes); with it, it must not be.
/// `cargo test -p pigeonhole-storage-telegram --lib abandoned -- --ignored --nocapture`
/// with `TG_BOT_TOKEN`, `TG_API_ID`, `TG_API_HASH`, `TG_BLOBS_CHAT_ID` in `.env`.
#[cfg(test)]
mod abandoned_send_live {
    use super::*;

    fn env(k: &str) -> String {
        std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
    }

    #[tokio::test]
    #[ignore = "needs a Telegram bot, api id and a test channel"]
    async fn an_abandoned_send_is_not_posted() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        dotenvy::from_path(root.join(".env")).ok();
        let cfg = MtprotoConfig {
            api_id: env("TG_API_ID").parse().unwrap(),
            api_hash: env("TG_API_HASH"),
            bot_token: env("TG_BOT_TOKEN"),
            session: root.join(".cache/tgm-live.session"),
        };
        let tg = TelegramMtproto::connect(&cfg).await.unwrap();
        let limiter = Arc::new(ChatLimiter::new(pigeonhole_blob::ChatLimiterConfig::default()));
        let ch = tg.channel(env("TG_BLOBS_CHAT_ID").parse().unwrap(), Lane::Blobs, limiter).await.unwrap();

        let probe_name = format!("abandoned-{}.bin", random_i64().unsigned_abs());
        let probe = ch.upload(&probe_name, &Bytes::from_static(b"x")).await.unwrap();

        let media: enums::InputMedia = types::InputMediaUploadedDocument {
            nosound_video: false,
            force_file: true,
            spoiler: false,
            file: probe,
            thumb: None,
            mime_type: "application/octet-stream".into(),
            attributes: vec![types::DocumentAttributeFilename { file_name: probe_name.clone() }.into()],
            stickers: None,
            video_cover: None,
            video_timestamp: None,
            ttl_seconds: None,
        }
        .into();
        let send = functions::messages::SendMedia {
            silent: true,
            background: false,
            clear_draft: false,
            noforwards: false,
            update_stickersets_order: false,
            invert_media: false,
            allow_paid_floodskip: false,
            peer: ch.input_peer(),
            reply_to: None,
            media,
            message: String::new(),
            random_id: random_i64(),
            reply_markup: None,
            entities: None,
            schedule_date: None,
            schedule_repeat_period: None,
            send_as: None,
            quick_reply_shortcut: None,
            effect: None,
            allow_paid_stars: None,
            suggested_post: None,
        };
        // Polled once (the request is handed to the pool), then given up
        // before the pool's task runs.
        use futures::FutureExt as _;
        assert!(ch.client.home(Lane::Blobs, &send).now_or_never().is_none());
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Where the top is now, and whether the probe landed below it.
        let marker = ch.send_document("abandoned-marker.bin", Bytes::from_static(b"m"), random_i64()).await.unwrap();
        let ids: Vec<i64> = ((marker.message_id - 20).max(1)..marker.message_id).collect();
        let msgs = ch.get_messages(&ids).await.unwrap();
        let posted: Vec<i64> = ids
            .iter()
            .zip(&msgs)
            .filter(|(_, m)| matches!(m, ChannelMessage::Document { name, .. } if *name == probe_name))
            .map(|(id, _)| *id)
            .collect();
        let mut ours = posted.clone();
        ours.push(marker.message_id);
        ch.delete_messages(&ours).await.unwrap();
        assert!(posted.is_empty(), "the abandoned sendMedia was posted as {posted:?}");
        println!("abandoned sendMedia posted: {}", !posted.is_empty());
    }
}
