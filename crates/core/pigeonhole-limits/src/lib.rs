//! Rate limits a store paces its calls with. A chat service (Telegram,
//! Discord) limits on two levels, plus explicit cool-downs from 429s:
//!
//! - **Bot** ([`BotLimiter`]): every call of one bot token, across all its
//!   chats (Telegram: about 30 per second).
//! - **Chat** ([`ChatLimiter`]): per chat and per class of call. Sends and
//!   pins (a pin posts a service message: ~20 messages per minute in
//!   channels and groups), `getFile`, deletes, and info calls (`getChat`,
//!   descriptions) have separate buckets; each call also takes a bot token.
//! - **429**: `retry_after` freezes the bucket that was hit (the chat's for a
//!   chat-scoped call, the bot's for `getFile`) so that the other callers wait
//!   too; retrying is up to the caller.
//!
//! File CDN byte streams are not API-metered, only capped by a download
//! connection semaphore. [`LimiterRegistry`] hands out one limiter per
//! (bot, chat), all sharing their bot's.
//!
//! **Background work** (collecting the log) spends only spare budget:
//! [`ChatLimiter::wait_idle`] returns once nobody waits for the bucket and it
//! is at least half full; the caller then takes tokens as usual. Stores
//! without remote limits (disk, memory) have a [`StoreLimiter`]: there idle
//! means no other operation in flight.

use pigeonhole_types::MetricDoc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore, SemaphorePermit};
// The runtime's clock: simulated under madsim and in paused tests, so that
// a bucket's refill and the sleeps waiting for it agree.
use tokio::time::Instant;

mod run;
pub use run::{run, Call, Failed, Penalty, Retry, Verdict};
use tracing::debug;

/// The metrics this crate records.
pub const METRICS: &[MetricDoc] = &[
    MetricDoc::histogram(
        "pigeonhole_backend_throttle_wait_seconds",
        &["service", "method"],
        "Waits a chat service asked for (429 retry-after / FLOOD_WAIT), also the ones waited out inside a call",
    ),
    MetricDoc::histogram(
        "pigeonhole_limiter_wait_seconds",
        &["chat", "op"],
        "Time waiting for our own rate-limit buckets of a chat / channel (waits > 0 only)",
    ),
];

/// A chat service told us to wait `d` before calling `method` again.
pub fn record_throttle(service: &str, method: &str, d: Duration) {
    metrics::histogram!(
        "pigeonhole_backend_throttle_wait_seconds",
        "service" => service.to_owned(),
        "method" => method.to_owned()
    )
    .record(d.as_secs_f64());
}

pub fn record_limiter_wait(chat: &str, op: &str, d: Duration) {
    if d.is_zero() {
        return;
    }
    metrics::histogram!(
        "pigeonhole_limiter_wait_seconds",
        "chat" => chat.to_owned(),
        "op" => op.to_owned()
    )
    .record(d.as_secs_f64());
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    cool_down_until: Option<Instant>,
    rate_per_sec: f64,
    capacity: f64,
}

impl BucketState {
    fn new(rate_per_sec: f64, burst: f64) -> Self {
        let rate_per_sec = rate_per_sec.max(0.01);
        let capacity = burst.max(1.0);
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            cool_down_until: None,
            rate_per_sec,
            capacity,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let add = elapsed.as_secs_f64() * self.rate_per_sec;
        if add > 0.0 {
            self.tokens = (self.tokens + add).min(self.capacity);
            self.last_refill = now;
        }
    }

    /// Spare for background work: not cooling down, at least half full.
    fn spare(&mut self, now: Instant) -> bool {
        if self.cool_down_until.is_some_and(|until| now < until) {
            return false;
        }
        self.refill(now);
        self.tokens >= (self.capacity / 2.0).max(1.0)
    }

    /// Wait before a token is available now: none during a cool-down, else
    /// what the refill needs. Ends a cool-down that is over.
    fn wait(&mut self, now: Instant) -> Duration {
        if let Some(until) = self.cool_down_until {
            if now < until {
                return until.saturating_duration_since(now);
            }
            self.cool_down_until = None;
        }
        self.refill(now);
        self.time_until_token()
    }

    fn time_until_token(&self) -> Duration {
        if self.tokens >= 1.0 {
            return Duration::ZERO;
        }
        let need = 1.0 - self.tokens;
        let secs = if self.rate_per_sec > 0.0 {
            need / self.rate_per_sec
        } else {
            1.0
        };
        Duration::from_secs_f64(secs.max(0.001))
    }

    /// Estimated wait until a token is available (no capture, no state mutation).
    fn peek_wait(&self) -> Duration {
        let now = Instant::now();
        if let Some(until) = self.cool_down_until {
            if now < until {
                return until.saturating_duration_since(now);
            }
        }
        let elapsed = now.saturating_duration_since(self.last_refill);
        let tokens = (self.tokens + elapsed.as_secs_f64() * self.rate_per_sec).min(self.capacity);
        if tokens >= 1.0 {
            return Duration::ZERO;
        }
        let need = 1.0 - tokens;
        let secs = if self.rate_per_sec > 0.0 {
            need / self.rate_per_sec
        } else {
            1.0
        };
        Duration::from_secs_f64(secs.max(0.001))
    }
}

/// Which ChatLimiter bucket to peek / acquire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitBudget {
    Send,
    GetFile,
    Delete,
    Info,
}

struct TokenBucket {
    state: Mutex<BucketState>,
    /// Callers inside [`take_all`] with this bucket in their plan.
    waiting: AtomicUsize,
}

/// Counts a caller in [`take_all`] (also when its future is dropped).
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn new(n: &'a AtomicUsize) -> Self {
        n.fetch_add(1, Ordering::SeqCst);
        Self(n)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How often background work looks again at a busy budget.
const IDLE_POLL: Duration = Duration::from_millis(100);

impl TokenBucket {
    fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            state: Mutex::new(BucketState::new(rate_per_sec, burst)),
            waiting: AtomicUsize::new(0),
        }
    }

    /// Nobody waits and there is spare budget (see [`BucketState::spare`]).
    fn idle(&self) -> bool {
        self.waiting.load(Ordering::SeqCst) == 0 && self.state.lock().unwrap().spare(Instant::now())
    }

    fn peek_wait(&self) -> Duration {
        self.state.lock().unwrap().peek_wait()
    }

    fn penalize(&self, retry_after: Duration) {
        let retry_after = retry_after.max(Duration::from_secs(1));
        let mut st = self.state.lock().unwrap();
        let until = Instant::now() + retry_after;
        st.cool_down_until = Some(match st.cool_down_until {
            Some(prev) => prev.max(until),
            None => until,
        });
        st.tokens = 0.0;
    }
}

/// Take a token from every bucket of `plan` at once: all of them, or none.
/// A caller that waits holds no token meanwhile, so waiting for one bucket
/// (the bot's) never keeps another (the chat's) from the rest. Returns the
/// time waited.
///
/// Not a reservation: waiters retry when the latest bucket should have a
/// token, and are not served in the order they came (see
/// docs/shared-limiter.md, § Taking a plan at once).
async fn take_all(plan: &[&TokenBucket]) -> Duration {
    let started = Instant::now();
    let _waiting: Vec<Waiting<'_>> = plan.iter().map(|b| Waiting::new(&b.waiting)).collect();
    loop {
        match try_take_all(plan) {
            None => return started.elapsed(),
            Some(wait) => tokio::time::sleep(wait).await,
        }
    }
}

/// One attempt of [`take_all`]: `None` when the tokens were taken, else how
/// long until the latest bucket has one.
fn try_take_all(plan: &[&TokenBucket]) -> Option<Duration> {
    // Locked in one order (by address) so that two plans never deadlock.
    let mut buckets = plan.to_vec();
    buckets.sort_by_key(|b| *b as *const TokenBucket as usize);
    buckets.dedup_by_key(|b| *b as *const TokenBucket as usize);
    let mut states: Vec<MutexGuard<'_, BucketState>> = buckets.iter().map(|b| b.state.lock().unwrap()).collect();
    let now = Instant::now();
    let wait = states.iter_mut().map(|st| st.wait(now)).max().unwrap_or(Duration::ZERO);
    if !wait.is_zero() {
        return Some(wait);
    }
    for st in &mut states {
        st.tokens -= 1.0;
    }
    None
}

/// Every call of one bot, across its chats.
pub struct BotLimiter {
    calls: TokenBucket,
}

impl BotLimiter {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self { calls: TokenBucket::new(rate_per_sec, burst) }
    }

    fn idle(&self) -> bool {
        self.calls.idle()
    }

    /// A 429 not tied to one chat: every chat of the bot waits.
    pub fn penalize(&self, retry_after: Duration) {
        cool_down("", "bot", retry_after);
        self.calls.penalize(retry_after);
    }
}

/// Budgets for one chat; each call also takes a token from its bot.
pub struct ChatLimiter {
    /// The chat / channel id, a metrics label.
    chat: String,
    bot: Arc<BotLimiter>,
    send: TokenBucket,
    get_file: TokenBucket,
    delete: TokenBucket,
    info: TokenBucket,
    upload_sem: Semaphore,
    download_sem: Semaphore,
}

#[derive(Debug, Clone)]
pub struct ChatLimiterConfig {
    /// Bot level: all calls of one token.
    pub bot_rate_per_sec: f64,
    pub bot_burst: f64,
    pub send_rate_per_sec: f64,
    pub send_burst: f64,
    pub get_file_rate_per_sec: f64,
    pub get_file_burst: f64,
    pub delete_rate_per_sec: f64,
    pub delete_burst: f64,
    /// `getChat`, descriptions.
    pub info_rate_per_sec: f64,
    pub info_burst: f64,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
}

impl ChatLimiterConfig {
    /// Discord: ~50 requests a second per bot; per channel ~5 messages per
    /// 5 s (a log entry is a message and a thread: two sends) and route
    /// limits the client also reads from the headers.
    pub fn discord() -> Self {
        Self {
            bot_rate_per_sec: 40.0,
            bot_burst: 50.0,
            send_rate_per_sec: 1.0,
            send_burst: 5.0,
            get_file_rate_per_sec: 5.0,
            get_file_burst: 10.0,
            delete_rate_per_sec: 1.0,
            delete_burst: 5.0,
            info_rate_per_sec: 2.0,
            info_burst: 5.0,
            upload_concurrency: 2,
            download_concurrency: 8,
        }
    }
}

impl Default for ChatLimiterConfig {
    fn default() -> Self {
        Self {
            bot_rate_per_sec: 25.0,
            bot_burst: 30.0,
            // 18 a minute: under the ~20 messages a minute of a channel; a pin
            // counts (it posts a service message).
            send_rate_per_sec: 0.3,
            send_burst: 3.0,
            get_file_rate_per_sec: 15.0,
            get_file_burst: 30.0,
            delete_rate_per_sec: 1.0,
            delete_burst: 5.0,
            info_rate_per_sec: 2.0,
            info_burst: 5.0,
            upload_concurrency: 2,
            download_concurrency: 8,
        }
    }
}

impl ChatLimiter {
    /// A chat with a bot of its own (tests, single-chat tools).
    pub fn new(cfg: ChatLimiterConfig) -> Self {
        let bot = Arc::new(BotLimiter::new(cfg.bot_rate_per_sec, cfg.bot_burst));
        Self::for_bot(cfg, bot, "")
    }

    /// Chat `chat` of `bot`, sharing its bot-level budget with the bot's other chats.
    pub fn for_bot(cfg: ChatLimiterConfig, bot: Arc<BotLimiter>, chat: &str) -> Self {
        Self {
            chat: chat.to_owned(),
            bot,
            send: TokenBucket::new(cfg.send_rate_per_sec, cfg.send_burst),
            get_file: TokenBucket::new(cfg.get_file_rate_per_sec, cfg.get_file_burst),
            delete: TokenBucket::new(cfg.delete_rate_per_sec, cfg.delete_burst),
            info: TokenBucket::new(cfg.info_rate_per_sec, cfg.info_burst),
            upload_sem: Semaphore::new(cfg.upload_concurrency.max(1)),
            download_sem: Semaphore::new(cfg.download_concurrency.max(1)),
        }
    }

    pub fn bot(&self) -> &Arc<BotLimiter> {
        &self.bot
    }

    /// A token of `bucket` and one of the bot's, at once.
    async fn take(&self, bucket: &TokenBucket, name: &'static str) {
        let waited = take_all(&[bucket, &self.bot.calls]).await;
        record_limiter_wait(&self.chat, name, waited);
    }

    /// A token of `budget` (and one of the bot's).
    pub async fn acquire(&self, budget: LimitBudget) {
        let name = match budget {
            LimitBudget::Send => "send",
            LimitBudget::GetFile => "get_file",
            LimitBudget::Delete => "delete",
            LimitBudget::Info => "info",
        };
        self.take(self.bucket(budget), name).await;
    }

    /// Cool `budget`'s bucket in this chat only.
    pub fn penalize_budget(&self, budget: LimitBudget, retry_after: Duration) {
        let name = match budget {
            LimitBudget::Send => "send",
            LimitBudget::GetFile => "getFile",
            LimitBudget::Delete => "delete",
            LimitBudget::Info => "info",
        };
        cool_down(&self.chat, name, retry_after);
        self.bucket(budget).penalize(retry_after);
    }

    pub async fn acquire_upload(&self) -> SemaphorePermit<'_> {
        self.upload_sem
            .acquire()
            .await
            .expect("upload semaphore closed")
    }

    /// Cap parallel CDN downloads (not an API token).
    pub async fn acquire_download(&self) -> SemaphorePermit<'_> {
        self.download_sem
            .acquire()
            .await
            .expect("download semaphore closed")
    }

    fn bucket(&self, budget: LimitBudget) -> &TokenBucket {
        match budget {
            LimitBudget::Send => &self.send,
            LimitBudget::GetFile => &self.get_file,
            LimitBudget::Delete => &self.delete,
            LimitBudget::Info => &self.info,
        }
    }

    /// Background work: wait until `budget` and the bot's have spare tokens
    /// and nobody waits for them. Takes no token; under steady foreground
    /// load it waits as long as the load lasts.
    pub async fn wait_idle(&self, budget: LimitBudget) {
        let started = Instant::now();
        while !(self.bucket(budget).idle() && self.bot.idle()) {
            tokio::time::sleep(IDLE_POLL).await;
        }
        record_limiter_wait(&self.chat, "idle", started.elapsed());
    }

    /// Estimated wait before the next token for `budget` (does not capture a token).
    pub fn peek_wait(&self, budget: LimitBudget) -> Duration {
        self.bucket(budget).peek_wait().max(self.bot.calls.peek_wait())
    }
}

/// The budget of a store without remote limits (disk, memory): every
/// operation holds an [`Op`] while it runs and, with a rate set, takes a
/// token first. Background work waits for [`Self::wait_idle`]: no operation
/// in flight (and spare tokens).
pub struct StoreLimiter {
    store: String,
    in_flight: AtomicUsize,
    quiet: Notify,
    rate: Option<TokenBucket>,
}

/// One store operation in flight; see [`StoreLimiter::op`].
pub struct Op<'a>(&'a StoreLimiter);

impl Drop for Op<'_> {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.quiet.notify_waiters();
        }
    }
}

impl StoreLimiter {
    /// No rate: operations never wait, background work waits for quiet.
    pub fn new(store: &str) -> Self {
        Self { store: store.to_owned(), in_flight: AtomicUsize::new(0), quiet: Notify::new(), rate: None }
    }

    /// At most `rate_per_sec` operations a second, `burst` at once.
    pub fn with_rate(mut self, rate_per_sec: f64, burst: f64) -> Self {
        self.rate = Some(TokenBucket::new(rate_per_sec, burst));
        self
    }

    /// Start an operation: a token if a rate is set, then counted in flight
    /// until the [`Op`] drops.
    pub async fn op(&self) -> Op<'_> {
        if let Some(rate) = &self.rate {
            let waited = take_all(&[rate]).await;
            record_limiter_wait(&self.store, "op", waited);
        }
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        Op(self)
    }

    fn idle(&self) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0 && self.rate.as_ref().is_none_or(|r| r.idle())
    }

    /// Background work: wait until no operation is in flight and the rate
    /// (if any) has spare tokens. Takes nothing; the caller's own operations
    /// then go through [`Self::op`] as usual.
    pub async fn wait_idle(&self) {
        let started = Instant::now();
        loop {
            let quiet = self.quiet.notified();
            if self.idle() {
                break;
            }
            tokio::select! {
                _ = quiet => {}
                _ = tokio::time::sleep(IDLE_POLL) => {}
            }
        }
        record_limiter_wait(&self.store, "idle", started.elapsed());
    }
}

impl std::fmt::Debug for StoreLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreLimiter").field("store", &self.store).finish()
    }
}

impl std::fmt::Debug for LimiterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LimiterRegistry").field("chats", &self.chats.lock().unwrap().len()).finish()
    }
}

/// One [`ChatLimiter`] per (bot, chat), sharing one [`BotLimiter`] per bot.
pub struct LimiterRegistry {
    cfg: ChatLimiterConfig,
    bots: Mutex<HashMap<String, Arc<BotLimiter>>>,
    chats: Mutex<HashMap<(String, String), Arc<ChatLimiter>>>,
}

impl LimiterRegistry {
    pub fn new(cfg: ChatLimiterConfig) -> Self {
        Self { cfg, bots: Mutex::default(), chats: Mutex::default() }
    }

    /// The limiter of `chat` for the bot identified by `bot` (its id, not
    /// the secret token).
    pub fn chat(&self, bot: &str, chat: &str) -> Arc<ChatLimiter> {
        let mut chats = self.chats.lock().unwrap();
        if let Some(l) = chats.get(&(bot.to_string(), chat.to_string())) {
            return l.clone();
        }
        let bot_limiter = self
            .bots
            .lock()
            .unwrap()
            .entry(bot.to_string())
            .or_insert_with(|| Arc::new(BotLimiter::new(self.cfg.bot_rate_per_sec, self.cfg.bot_burst)))
            .clone();
        let l = Arc::new(ChatLimiter::for_bot(self.cfg.clone(), bot_limiter, chat));
        chats.insert((bot.to_string(), chat.to_string()), l.clone());
        l
    }
}

/// A rate limit the server asked for: `info` when it is long enough for a
/// person to notice (a stalled upload, a slow boot), `debug` otherwise.
fn cool_down(chat: &str, budget: &'static str, retry_after: Duration) {
    if retry_after >= Duration::from_secs(5) {
        tracing::info!(chat, budget, wait_secs = retry_after.as_secs_f64(), "rate limited: waiting");
    } else {
        debug!(chat, budget, ?retry_after, "rate limited");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn fast_send() -> ChatLimiter {
        ChatLimiter::new(ChatLimiterConfig {
            send_rate_per_sec: 100.0,
            send_burst: 2.0,
            get_file_rate_per_sec: 1000.0,
            get_file_burst: 100.0,
            delete_rate_per_sec: 1000.0,
            delete_burst: 100.0,
            upload_concurrency: 4,
            download_concurrency: 4,
            ..ChatLimiterConfig::default()
        })
    }

    #[tokio::test]
    async fn chats_share_their_bot_and_nothing_else() {
        let reg = LimiterRegistry::new(ChatLimiterConfig {
            bot_rate_per_sec: 0.01,
            bot_burst: 2.0,
            send_rate_per_sec: 0.01,
            send_burst: 1.0,
            ..ChatLimiterConfig::default()
        });
        let (a, b, a_again) = (reg.chat("bot1", "-1"), reg.chat("bot1", "-2"), reg.chat("bot1", "-1"));
        assert!(Arc::ptr_eq(&a, &a_again), "one limiter per chat");
        assert!(Arc::ptr_eq(a.bot(), b.bot()), "one bot limiter per bot");
        assert!(!Arc::ptr_eq(a.bot(), reg.chat("bot2", "-1").bot()));

        a.acquire(LimitBudget::Send).await;
        assert!(a.peek_wait(LimitBudget::Send) > Duration::from_secs(1), "chat a's send bucket is empty");
        assert_eq!(b.peek_wait(LimitBudget::Send), Duration::ZERO, "chat b's is not");
        b.acquire(LimitBudget::Send).await;
        // Two calls took the bot's burst of 2: every chat of the bot now waits.
        assert!(reg.chat("bot1", "-3").peek_wait(LimitBudget::Info) > Duration::from_secs(1));
        assert_eq!(reg.chat("bot2", "-3").peek_wait(LimitBudget::Info), Duration::ZERO);
    }

    #[tokio::test]
    async fn a_get_file_429_holds_the_bot_a_send_429_only_the_chat() {
        let reg = LimiterRegistry::new(ChatLimiterConfig::default());
        let (a, b) = (reg.chat("bot", "-1"), reg.chat("bot", "-2"));
        a.penalize_budget(LimitBudget::Send, Duration::from_secs(20));
        assert!(a.peek_wait(LimitBudget::Send) > Duration::from_secs(10));
        assert_eq!(b.peek_wait(LimitBudget::Send), Duration::ZERO);
        a.cool(Penalty::ClassAndBot(LimitBudget::GetFile), false, Duration::from_secs(20));
        assert!(b.peek_wait(LimitBudget::Delete) > Duration::from_secs(10), "getFile is not chat-scoped");
    }

    #[tokio::test]
    async fn waiting_for_the_bot_holds_no_chat_token() {
        let lim = Arc::new(ChatLimiter::new(ChatLimiterConfig {
            bot_rate_per_sec: 10.0,
            bot_burst: 1.0,
            send_rate_per_sec: 0.01,
            send_burst: 1.0,
            ..ChatLimiterConfig::default()
        }));
        lim.acquire(LimitBudget::Info).await; // the bot's only token
        let send = {
            let lim = lim.clone();
            tokio::spawn(async move { lim.acquire(LimitBudget::Send).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!send.is_finished(), "the send waits for the bot");
        assert_eq!(lim.send.peek_wait(), Duration::ZERO, "and has not taken the chat's send token");
        send.await.unwrap();
        assert!(lim.peek_wait(LimitBudget::Send) > Duration::from_secs(1), "now it has");
    }

    #[tokio::test]
    async fn waiting_for_the_chat_holds_no_bot_token() {
        let reg = LimiterRegistry::new(ChatLimiterConfig {
            bot_rate_per_sec: 0.01,
            bot_burst: 2.0,
            send_rate_per_sec: 10.0,
            send_burst: 1.0,
            ..ChatLimiterConfig::default()
        });
        let (a, b) = (reg.chat("bot", "-1"), reg.chat("bot", "-2"));
        a.acquire(LimitBudget::Send).await; // a's send token, and one of the bot's two
        let again = {
            let a = a.clone();
            tokio::spawn(async move { a.acquire(LimitBudget::Send).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!again.is_finished(), "a's second send waits for its chat");
        let t0 = Instant::now();
        b.acquire(LimitBudget::Send).await;
        assert!(t0.elapsed() < Duration::from_millis(50), "the bot's last token went to chat b");
        again.abort();
    }

    #[tokio::test]
    async fn send_bucket_independent_of_get_file() {
        let lim = ChatLimiter::new(ChatLimiterConfig {
            send_rate_per_sec: 0.01,
            send_burst: 1.0,
            get_file_rate_per_sec: 1000.0,
            get_file_burst: 100.0,
            ..ChatLimiterConfig::default()
        });
        lim.acquire(LimitBudget::Send).await;
        // getFile still has tokens even though send is exhausted.
        let t0 = Instant::now();
        lim.acquire(LimitBudget::GetFile).await;
        assert!(t0.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn penalize_send_does_not_block_get_file() {
        let lim = Arc::new(fast_send());
        lim.penalize_budget(LimitBudget::Send, Duration::from_millis(200));
        let t0 = Instant::now();
        lim.acquire(LimitBudget::GetFile).await;
        assert!(t0.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn upload_semaphore_caps_concurrency() {
        let lim = Arc::new(ChatLimiter::new(ChatLimiterConfig {
            upload_concurrency: 1,
            ..ChatLimiterConfig::default()
        }));
        let p1 = lim.acquire_upload().await;
        let lim2 = lim.clone();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _p = lim2.acquire_upload().await;
            let _ = tx.send(());
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(rx.try_recv().is_err());
        drop(p1);
        rx.await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn peek_wait_does_not_capture_tokens() {
        let lim = ChatLimiter::new(ChatLimiterConfig {
            send_rate_per_sec: 0.01,
            send_burst: 1.0,
            ..ChatLimiterConfig::default()
        });
        assert_eq!(lim.peek_wait(LimitBudget::Send), Duration::ZERO);
        lim.acquire(LimitBudget::Send).await;
        let peeked = lim.peek_wait(LimitBudget::Send);
        assert!(peeked > Duration::from_millis(50));
        // Second peek must not change the estimate (no capture).
        let peeked2 = lim.peek_wait(LimitBudget::Send);
        assert!((peeked2.as_secs_f64() - peeked.as_secs_f64()).abs() < 0.05);
        assert_eq!(lim.peek_wait(LimitBudget::GetFile), Duration::ZERO);
    }

    #[tokio::test]
    async fn background_waits_for_a_foreground_caller_and_spare_tokens() {
        let l = Arc::new(ChatLimiter::new(ChatLimiterConfig {
            delete_rate_per_sec: 20.0,
            delete_burst: 2.0,
            ..ChatLimiterConfig::default()
        }));
        l.wait_idle(LimitBudget::Delete).await; // a fresh budget is idle
        l.acquire(LimitBudget::Delete).await;
        l.acquire(LimitBudget::Delete).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let fg = {
            let (l, order) = (l.clone(), order.clone());
            tokio::spawn(async move {
                l.acquire(LimitBudget::Delete).await; // waits ~50 ms for a token
                order.lock().unwrap().push("foreground");
            })
        };
        tokio::task::yield_now().await;
        l.wait_idle(LimitBudget::Delete).await;
        order.lock().unwrap().push("background");
        fg.await.unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["foreground", "background"]);
    }

    #[tokio::test]
    async fn store_background_waits_until_no_operation_is_in_flight() {
        let l = Arc::new(StoreLimiter::new("disk"));
        l.wait_idle().await;
        let op = l.op().await;
        let bg = {
            let l = l.clone();
            tokio::spawn(async move { l.wait_idle().await })
        };
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!bg.is_finished(), "background ran while an operation was in flight");
        let t0 = Instant::now();
        drop(op);
        bg.await.unwrap();
        assert!(t0.elapsed() < IDLE_POLL, "the end of the last operation wakes background work");
    }
}
