//! Discord over HTTP: [`DiscordClient`] (one bot) and [`DiscordChannel`]
//! (one channel, a [`DiscordApi`]).

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_limits::chat::Member;
use pigeonhole_limits::{Call, ChatLimiter, Class, Level, Retry, Spend, Verdict};
use pigeonhole_storage_api::Unavailable;
use pigeonhole_storage_discord::limits::{DELETE, READ, SEND};
use pigeonhole_storage_discord::snowflake_bulk_deletable;
use pigeonhole_storage_discord::{CdnStale, DcAttachment, DcMessage, DiscordApi, ThreadStart, PAGE};
use reqwest::header::{HeaderMap, AUTHORIZATION};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

const DEFAULT_API_BASE: &str = "https://discord.com/api/v10";
/// `flags`: no link previews.
const SUPPRESS_EMBEDS: u64 = 1 << 2;
/// Attempts per request (network errors, 5xx and 429s); a 429's wait is
/// always waited out, however long.
const RETRY: Retry = Retry {
    deadline: Duration::from_secs(3600),
    attempts: 6,
    backoff: Duration::from_millis(200),
    max_backoff: Duration::from_millis(6400),
    give_way: false,
};
/// A member of a [`PooledDiscord`](crate::PooledDiscord): a 429 ends the
/// call at once, so that the pool may move it to another bot.
const RETRY_IN_POOL: Retry = RETRY.giving_way();

fn unix_now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Seconds a 429 asks to wait.
fn retry_after(headers: &HeaderMap, body: &serde_json::Value) -> f64 {
    body.get("retry_after")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            headers
                .get("retry-after")
                .or_else(|| headers.get("x-ratelimit-reset-after"))
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(3.0)
        .max(0.1)
}

/// An API request of `class`: a 429 cools that class in its channel (the
/// whole bot only when Discord says `global`, see [`classify`]).
const fn api(what: &'static str, class: Class) -> Call {
    Call { service: "discord", what, spend: Spend::Class(class), cools: Some(Level::Class) }
}

const GET_CHANNEL: Call = api("get channel", READ);
const READ_HISTORY: Call = api("read history", READ);
const GET_MESSAGE: Call = api("get message", READ);
const LIST_MESSAGES: Call = api("list messages", READ);
const GET_THREAD: Call = api("get thread", READ);
const SEND_MESSAGE: Call = api("send message", SEND);
const SEND_ATTACHMENT: Call = api("send attachment", SEND);
const START_THREAD: Call = api("start thread", SEND);
const DELETE_MESSAGE: Call = api("delete message", DELETE);
const BULK_DELETE: Call = api("bulk delete", DELETE);
const ARCHIVE_THREAD: Call = api("archive thread", DELETE);
const DELETE_THREAD: Call = api("delete thread", DELETE);
const LIST_PINS: Call = api("list pins", READ);
const PIN_MESSAGE: Call = api("pin message", SEND);

/// Why one attempt failed.
#[derive(Debug)]
enum AttemptFailed {
    Network(reqwest::Error),
    /// A 429: wait this long; `global` limits are the bot's.
    RateLimited {
        wait: Duration,
        global: bool,
    },
    Server {
        status: u16,
        text: String,
    },
}

impl std::fmt::Display for AttemptFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptFailed::Network(e) => write!(f, "{e}"),
            AttemptFailed::RateLimited { wait, global } => write!(f, "429, retry after {wait:?} (global: {global})"),
            AttemptFailed::Server { status, text } => write!(f, "{status} {text}"),
        }
    }
}

fn classify(e: &AttemptFailed) -> Verdict {
    match e {
        AttemptFailed::RateLimited { wait, global } => {
            Verdict::Wait { after: *wait, level: global.then_some(Level::Bot) }
        }
        AttemptFailed::Network(_) | AttemptFailed::Server { .. } => Verdict::Transient,
    }
}

/// One bot's HTTP client, shared by its channels.
#[derive(Clone)]
pub struct DiscordClient {
    http: reqwest::Client,
    bot_token: String,
    api_base: String,
}

/// One answer: status and JSON body (`Null` if none).
struct Answer {
    status: u16,
    body: serde_json::Value,
    /// `X-RateLimit-Remaining` / `-Reset-After` of the route.
    remaining: Option<u64>,
    reset_after: Option<f64>,
}

impl DiscordClient {
    pub fn new(bot_token: String) -> Result<Self> {
        Self::with_api_base(bot_token, DEFAULT_API_BASE.into())
    }

    pub fn with_api_base(bot_token: String, api_base: String) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bot {bot_token}").parse().context("authorization header")?);
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent("DiscordBot (https://github.com/pigeonhole-project/pigeonhole, 0.1)")
            .default_headers(headers)
            .build()?;
        Ok(Self { http, bot_token, api_base })
    }

    /// Application id prefix of the bot token (`app_id.xxx.yyy`); never logs the secret.
    pub fn app_id(&self) -> String {
        self.bot_token.split('.').next().filter(|s| !s.is_empty()).unwrap_or("unknown").to_string()
    }

    /// Channel `channel_id` through this client.
    pub fn channel(&self, channel_id: u64, limiter: Arc<ChatLimiter>) -> DiscordChannel {
        DiscordChannel { client: self.clone(), channel_id, limiter, retry: RETRY }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    /// Send `build()` through the limiter until it is not a 429, a 5xx or a
    /// network error; if every attempt was one of the latter two, the store
    /// is [`Unavailable`].
    async fn request(
        &self,
        limiter: &ChatLimiter,
        retry: &Retry,
        call: &Call,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Answer> {
        self.request_with(limiter, retry, call, false, build).await
    }

    /// [`Self::request`]; with `upload`, each attempt holds an upload permit
    /// while its body goes up and its answer comes, and no longer: a 429's
    /// wait (minutes, sometimes) between attempts holds back no other upload.
    async fn request_with(
        &self,
        limiter: &ChatLimiter,
        retry: &Retry,
        call: &Call,
        upload: bool,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Answer> {
        let what = call.what;
        let answer = pigeonhole_limits::run(Some(limiter), call, retry, classify, || async {
            let _permit = if upload { Some(limiter.acquire_upload().await) } else { None };
            let resp = build().send().await.map_err(AttemptFailed::Network)?;
            let status = resp.status().as_u16();
            let headers = resp.headers().clone();
            // A body cut off is a lost answer (worth a retry), not an empty one.
            let text = resp.text().await.map_err(AttemptFailed::Network)?;
            let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
            if status == 429 {
                let wait = Duration::from_secs_f64(retry_after(&headers, &body));
                let global = body.get("global").and_then(|g| g.as_bool()).unwrap_or(false);
                return Err(AttemptFailed::RateLimited { wait, global });
            }
            if status >= 500 {
                return Err(AttemptFailed::Server { status, text });
            }
            let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).map(str::to_string);
            let remaining = header("x-ratelimit-remaining").and_then(|s| s.parse().ok());
            let reset_after = header("x-ratelimit-reset-after").and_then(|s| s.parse().ok());
            Ok(Answer { status, body, remaining, reset_after })
        })
        .await;
        answer.map_err(|f| f.into_error(what, |last| anyhow!("{what}: {last}")))
    }

    /// Fail fast unless the bot sees `channel_id` as a text channel and can
    /// read its history. A bot cannot ask Discord for its own permissions in
    /// a channel (there is no such endpoint); send, attach, delete and thread
    /// rights (`chain`) show at the first use.
    pub async fn ensure_permissions(&self, channel_id: u64, chain: bool, limiter: &ChatLimiter) -> Result<()> {
        let path = format!("channels/{channel_id}");
        let a = self.request(limiter, &RETRY, &GET_CHANNEL, || self.http.get(self.url(&path))).await?;
        if a.status != 200 {
            bail!("channel {channel_id}: answered {} (is the bot in this guild and allowed to view it?)", a.status);
        }
        // Threads from messages exist in text (0) and announcement (5) channels.
        let kind = a.body["type"].as_u64().unwrap_or(u64::MAX);
        if chain && kind != 0 && kind != 5 {
            bail!("channel {channel_id} is of type {kind}: the log needs a text channel");
        }
        let path = format!("channels/{channel_id}/messages?limit=1");
        let a = self.request(limiter, &RETRY, &READ_HISTORY, || self.http.get(self.url(&path))).await?;
        if a.status != 200 {
            bail!("channel {channel_id}: cannot read the history ({})", a.status);
        }
        Ok(())
    }
}

/// One channel over HTTP.
#[derive(Clone)]
pub struct DiscordChannel {
    client: DiscordClient,
    channel_id: u64,
    limiter: Arc<ChatLimiter>,
    retry: Retry,
}

impl DiscordChannel {
    pub fn client(&self) -> &DiscordClient {
        &self.client
    }

    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        &self.limiter
    }

    fn path(&self, rest: &str) -> String {
        format!("channels/{}/{rest}", self.channel_id)
    }

    async fn delete_one(&self, id: u64) -> Result<()> {
        let path = self.path(&format!("messages/{id}"));
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &DELETE_MESSAGE, || c.http.delete(c.url(&path))).await?;
        match a.status {
            200..=299 | 404 => Ok(()),
            s => bail!("delete message {id}: {s} {}", a.body),
        }
    }
}

/// A member of a [`PooledDiscord`](crate::PooledDiscord).
impl Member for DiscordChannel {
    /// A 429 ends a call at once instead of being waited out (unless the
    /// call's outcome is unknown), so the pool can move it to another bot.
    fn in_pool(&self) -> Self {
        Self { retry: RETRY_IN_POOL, ..self.clone() }
    }

    fn limiter(&self) -> &Arc<ChatLimiter> {
        DiscordChannel::limiter(self)
    }
}

#[derive(Deserialize)]
struct RawMessage {
    id: String,
    #[serde(rename = "type", default)]
    kind: u8,
    #[serde(default)]
    content: String,
    #[serde(default)]
    attachments: Vec<RawAttachment>,
    #[serde(default)]
    thread: Option<RawThread>,
}

#[derive(Deserialize)]
struct RawAttachment {
    #[serde(default)]
    filename: String,
    #[serde(default)]
    size: u64,
    url: String,
}

#[derive(Deserialize)]
struct RawThread {
    #[serde(default)]
    name: String,
}

fn message(v: serde_json::Value) -> Result<DcMessage> {
    let m: RawMessage = serde_json::from_value(v).context("message json")?;
    Ok(DcMessage {
        id: m.id.parse().context("message id")?,
        kind: m.kind,
        content: m.content,
        attachment: m.attachments.into_iter().next().map(|a| DcAttachment {
            filename: a.filename,
            size: a.size,
            url: a.url,
        }),
        thread: m.thread.map(|t| t.name),
    })
}

/// Discord's JSON error code, if any.
fn code(body: &serde_json::Value) -> Option<u64> {
    body.get("code").and_then(|c| c.as_u64())
}

#[async_trait]
impl DiscordApi for DiscordChannel {
    fn channel_id(&self) -> u64 {
        self.channel_id
    }

    async fn send(&self, content: &str, file: Option<(&str, Bytes)>, nonce: &str) -> Result<DcMessage> {
        let mut payload = serde_json::json!({
            "content": content,
            "nonce": nonce,
            "enforce_nonce": true,
            "flags": SUPPRESS_EMBEDS,
            "allowed_mentions": { "parse": [] },
        });
        let path = self.path("messages");
        let c = &self.client;
        let a = match file {
            None => {
                c.request(&self.limiter, &self.retry, &SEND_MESSAGE, || c.http.post(c.url(&path)).json(&payload))
                    .await?
            }
            Some((name, data)) => {
                payload["attachments"] = serde_json::json!([{ "id": 0, "filename": name }]);
                let payload = payload.to_string();
                c.request_with(&self.limiter, &self.retry, &SEND_ATTACHMENT, true, || {
                    let part = Part::stream_with_length(reqwest::Body::from(data.clone()), data.len() as u64)
                        .file_name(name.to_string())
                        .mime_str("application/octet-stream")
                        .expect("a static mime type");
                    let form = Form::new().text("payload_json", payload.clone()).part("files[0]", part);
                    c.http.post(c.url(&path)).multipart(form)
                })
                .await?
            }
        };
        if !(200..300).contains(&a.status) {
            bail!("send message: {} {}", a.status, a.body);
        }
        message(a.body)
    }

    async fn get_message(&self, id: u64) -> Result<Option<DcMessage>> {
        let path = self.path(&format!("messages/{id}"));
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &GET_MESSAGE, || c.http.get(c.url(&path))).await?;
        match a.status {
            200 => Ok(Some(message(a.body)?)),
            404 => Ok(None),
            s => bail!("get message {id}: {s} {}", a.body),
        }
    }

    async fn list(&self, before: Option<u64>, after: Option<u64>, limit: usize) -> Result<Vec<DcMessage>> {
        let mut query = format!("messages?limit={}", limit.clamp(1, PAGE));
        if let Some(b) = before {
            query.push_str(&format!("&before={b}"));
        }
        if let Some(a) = after {
            query.push_str(&format!("&after={a}"));
        }
        let path = self.path(&query);
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &LIST_MESSAGES, || c.http.get(c.url(&path))).await?;
        if a.status != 200 {
            bail!("list messages: {} {}", a.status, a.body);
        }
        let mut out = match a.body {
            serde_json::Value::Array(items) => items.into_iter().map(message).collect::<Result<Vec<_>>>()?,
            other => bail!("list messages: not a list: {other}"),
        };
        out.retain(|m| before.is_none_or(|b| m.id < b) && after.is_none_or(|a| m.id > a));
        out.sort_by_key(|m| m.id);
        Ok(out)
    }

    async fn delete_messages(&self, ids: &[u64]) -> Result<()> {
        let now = unix_now_ms();
        let (mut young, mut old): (Vec<u64>, Vec<u64>) = ids.iter().partition(|&&id| snowflake_bulk_deletable(id, now));
        young.sort_unstable();
        young.dedup();
        old.sort_unstable();
        old.dedup();
        for chunk in young.chunks(PAGE) {
            if let [one] = chunk {
                self.delete_one(*one).await?;
                continue;
            }
            let body = serde_json::json!({ "messages": chunk.iter().map(|id| id.to_string()).collect::<Vec<_>>() });
            let path = self.path("messages/bulk-delete");
            let c = &self.client;
            let a =
                c.request(&self.limiter, &self.retry, &BULK_DELETE, || c.http.post(c.url(&path)).json(&body)).await?;
            if !(200..300).contains(&a.status) {
                debug!(status = a.status, "bulk delete refused; one by one");
                for &id in chunk {
                    self.delete_one(id).await?;
                }
            }
        }
        for id in old {
            self.delete_one(id).await?;
        }
        Ok(())
    }

    async fn start_thread(&self, message_id: u64, name: &str) -> Result<ThreadStart> {
        let path = self.path(&format!("messages/{message_id}/threads"));
        let body = serde_json::json!({ "name": name, "auto_archive_duration": 60 });
        let c = &self.client;
        // Thread creation has a budget of its own, shared by the bot's channels.
        let pacer = crate::pacer::threads_of(&c.app_id());
        pacer.wait().await;
        let a = c.request(&self.limiter, &self.retry, &START_THREAD, || c.http.post(c.url(&path)).json(&body)).await?;
        pacer.observe(a.remaining, a.reset_after);
        match (a.status, code(&a.body)) {
            (200..=299, _) => Ok(ThreadStart::Created),
            (_, Some(160004)) => Ok(ThreadStart::Taken),
            (404, _) | (_, Some(10008)) => Ok(ThreadStart::NoMessage),
            (s, _) => bail!("start thread on {message_id}: {s} {}", a.body),
        }
    }

    /// The legacy route's answer is a list of messages; the newer one's
    /// `{ items: [{ message, .. }] }`: either is read.
    async fn pinned(&self) -> Result<Vec<DcMessage>> {
        let path = self.path("pins");
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &LIST_PINS, || c.http.get(c.url(&path))).await?;
        if a.status != 200 {
            bail!("list pins: {} {}", a.status, a.body);
        }
        let items = match a.body {
            serde_json::Value::Array(items) => items,
            mut other => match other["items"].take() {
                serde_json::Value::Array(items) => items.into_iter().map(|mut i| i["message"].take()).collect(),
                _ => bail!("list pins: neither a list nor items: {other}"),
            },
        };
        items.into_iter().map(message).collect()
    }

    async fn pin(&self, id: u64) -> Result<()> {
        let path = self.path(&format!("pins/{id}"));
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &PIN_MESSAGE, || c.http.put(c.url(&path))).await?;
        match a.status {
            200..=299 => Ok(()),
            403 => bail!("pin message {id}: the bot needs the Manage Messages permission: {}", a.body),
            s => bail!("pin message {id}: {s} {}", a.body),
        }
    }

    async fn thread_name(&self, message_id: u64) -> Result<Option<String>> {
        let path = format!("channels/{message_id}");
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &GET_THREAD, || c.http.get(c.url(&path))).await?;
        match a.status {
            200 => Ok(a.body["name"].as_str().map(str::to_string)),
            404 => Ok(None),
            s => bail!("get thread {message_id}: {s} {}", a.body),
        }
    }

    async fn archive_thread(&self, message_id: u64) -> Result<()> {
        let path = format!("channels/{message_id}");
        let body = serde_json::json!({ "archived": true });
        let c = &self.client;
        let a =
            c.request(&self.limiter, &self.retry, &ARCHIVE_THREAD, || c.http.patch(c.url(&path)).json(&body)).await?;
        match a.status {
            200..=299 => Ok(()),
            s => bail!("archive thread {message_id}: {s} {}", a.body),
        }
    }

    async fn delete_thread(&self, message_id: u64) -> Result<()> {
        let path = format!("channels/{message_id}");
        let c = &self.client;
        let a = c.request(&self.limiter, &self.retry, &DELETE_THREAD, || c.http.delete(c.url(&path))).await?;
        match a.status {
            200..=299 | 404 => Ok(()),
            s => bail!("delete thread {message_id}: {s} {}", a.body),
        }
    }

    async fn download(&self, url: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let _permit = self.limiter.acquire_download().await;
        let mut req = self.client.http.get(url);
        if let Some((start, end)) = range {
            req = req.header("Range", format!("bytes={start}-{}", end - 1));
        }
        let resp = req.send().await.context("download attachment").context(Unavailable("download".into()))?;
        let status = resp.status().as_u16();
        if status >= 500 {
            return Err(anyhow!("download answered {status}").context(Unavailable("download".into())));
        }
        if matches!(status, 403 | 404) {
            return Err(CdnStale.into());
        }
        if let Some((start, _)) = range {
            if status != 206 {
                bail!("ranged download answered {status}, not 206");
            }
            let cr = resp.headers().get(reqwest::header::CONTENT_RANGE).and_then(|v| v.to_str().ok()).unwrap_or("");
            if !cr.starts_with(&format!("bytes {start}-")) {
                bail!("Content-Range {cr:?} does not start at {start}");
            }
        } else if !(200..300).contains(&status) {
            bail!("download answered {status}");
        }
        resp.bytes().await.context("attachment bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_read_429_cools_its_channel_and_the_bot_only_when_global() {
        let reg = pigeonhole_limits::LimiterRegistry::new(
            pigeonhole_storage_discord::limits(),
            pigeonhole_storage_discord::limits::CLASSES,
        )
        .unwrap();
        let call = GET_MESSAGE;
        let once = Retry { attempts: 1, ..RETRY };
        for global in [false, true] {
            let (a, b) = (reg.chat(&format!("bot-{global}"), "1"), reg.chat(&format!("bot-{global}"), "2"));
            let failed = pigeonhole_limits::run(Some(&a), &call, &once, classify, || async move {
                Err::<(), _>(AttemptFailed::RateLimited { wait: Duration::from_secs(5), global })
            })
            .await;
            assert!(matches!(failed, Err(pigeonhole_limits::Failed::RateLimited { .. })));
            assert!(a.peek_wait(READ) > Duration::from_secs(3), "the read's own bucket");
            let other = b.peek_wait(SEND);
            assert_eq!(other > Duration::from_secs(3), global, "another channel's sends (global: {global})");
        }
    }

    /// A local HTTP server (a thread): the first `fail` requests whose body
    /// holds `failing` answer 503, the rest a message. Each answer closes
    /// its connection.
    fn server(failing: &'static str, fail: usize) -> String {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::spawn(move || {
            for (n, conn) in listener.incoming().enumerate() {
                let Ok(mut conn) = conn else { return };
                let failed = failed.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(conn.try_clone().unwrap());
                    let (mut len, mut chunked) = (0, false);
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                            break;
                        }
                        let lower = line.to_ascii_lowercase();
                        if let Some(v) = lower.strip_prefix("content-length:") {
                            len = v.trim().parse().unwrap_or(0);
                        }
                        chunked |= lower.starts_with("transfer-encoding:") && lower.contains("chunked");
                    }
                    let mut body = Vec::new();
                    if chunked {
                        while !body.ends_with(b"0\r\n\r\n") {
                            let mut b = [0u8; 1];
                            if reader.read(&mut b).unwrap_or(0) == 0 {
                                break;
                            }
                            body.push(b[0]);
                        }
                    } else {
                        body.resize(len, 0);
                        reader.read_exact(&mut body).ok();
                    }
                    let ours = String::from_utf8_lossy(&body).contains(failing);
                    let fails = ours && failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < fail;
                    let (status, body) = if fails {
                        ("503 Service Unavailable", "{}".to_string())
                    } else {
                        ("200 OK", format!(r#"{{"id":"{}","type":0,"content":"x","attachments":[]}}"#, n + 1))
                    };
                    let answer = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    conn.write_all(answer.as_bytes()).ok();
                });
            }
        });
        format!("http://{addr}")
    }

    /// An attachment send whose first attempts fail waits its backoff
    /// without its upload permit: another upload of the bot in the channel
    /// goes on meanwhile (one permit; the retries back off 200 + 400 ms).
    #[tokio::test]
    async fn a_retried_upload_does_not_hold_the_permit_while_it_waits() {
        let cfg = pigeonhole_storage_discord::limits();
        let cfg = pigeonhole_limits::ChatLimiterConfig {
            upload_concurrency: 1,
            ..cfg.with_all(pigeonhole_limits::Rate::new(1000.0, 1000.0))
        };
        let reg = pigeonhole_limits::LimiterRegistry::new(cfg, pigeonhole_storage_discord::limits::CLASSES).unwrap();
        let client = DiscordClient::with_api_base("1.a.b".into(), server("\"n1\"", 2)).unwrap();
        let channel = client.channel(7, reg.chat("1", "7"));
        let file = || Some(("ph-1.bin", Bytes::from(vec![1u8; 1000])));
        let t0 = std::time::Instant::now();
        let first = async {
            channel.send("ph-1.bin", file(), "n1").await.unwrap();
            t0.elapsed()
        };
        let second = async {
            // After the first's first attempt took the permit and failed.
            tokio::time::sleep(Duration::from_millis(100)).await;
            channel.send("ph-1.bin", file(), "n2").await.unwrap();
            t0.elapsed()
        };
        let (first, second) = tokio::join!(first, second);
        assert!(first >= Duration::from_millis(500), "the first backed off twice: {first:?}");
        assert!(second < first, "the second went up while the first waited: {second:?} vs {first:?}");
    }

    #[test]
    fn app_id_from_token_prefix() {
        let dc = DiscordClient::new("1234567890.abc.def".into()).unwrap();
        assert_eq!(dc.app_id(), "1234567890");
    }

    #[test]
    fn messages_parse_with_attachment_and_thread() {
        let m = message(serde_json::json!({
            "id": "123", "type": 0, "content": "x",
            "attachments": [{ "id": "9", "filename": "ph-1.bin", "size": 3, "url": "https://cdn/x" }],
            "thread": { "id": "123", "name": "next 7 log-batch-1" }
        }))
        .unwrap();
        assert_eq!(m.id, 123);
        assert_eq!(m.attachment.unwrap().filename, "ph-1.bin");
        assert_eq!(m.thread.as_deref(), Some("next 7 log-batch-1"));
    }
}
