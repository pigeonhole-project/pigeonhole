//! Rate limits a store paces its calls with (docs/shared-limiter.md). A
//! chat service (Telegram, Discord) limits on two levels, plus explicit
//! cool-downs from 429s:
//!
//! - **Bot** ([`BotLimiter`]): every call of one bot token, across all its
//!   chats (Telegram: about 30 per second).
//! - **Class** ([`ChatLimiter`]): per chat and per [`Class`] of call (sends,
//!   reads, deletes…). The service names its classes and their rates
//!   ([`ChatLimiterConfig`]); this crate knows no service. A call of a
//!   class also takes a bot token ([`Spend`]).
//! - **429**: `retry_after` freezes the [`Level`] that was hit, so that the
//!   other callers wait too ([`run`] does it, and the retrying).
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

mod pool;
mod run;
pub use pool::Pool;
pub use run::{run, Call, Failed, Retry, Verdict};
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
    MetricDoc::histogram(
        "pigeonhole_limiter_retry_wait_seconds",
        &["service", "method"],
        "From a rate limit to the next attempt of that call on the same bot (the wait plus the tokens after it)",
    ),
    MetricDoc::counter(
        "pigeonhole_pool_calls_total",
        &["chat", "member"],
        "Operations a pool of bots ran, by the member (0 = the primary) they ran on",
    ),
    MetricDoc::counter(
        "pigeonhole_pool_moves_total",
        &["chat"],
        "Operations a pool moved to another bot after a rate limit (it was not applied)",
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

fn record_retry_wait(service: &str, method: &str, d: Duration) {
    metrics::histogram!(
        "pigeonhole_limiter_retry_wait_seconds",
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

/// A class of calls with a bucket of its own in each chat: sends, reads,
/// deletes… The service names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Class(pub &'static str);

impl std::fmt::Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// A token bucket's rate: `per_sec` tokens a second, `burst` at most.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    pub per_sec: f64,
    pub burst: f64,
}

impl Rate {
    pub const fn new(per_sec: f64, burst: f64) -> Self {
        Self { per_sec, burst }
    }
}

/// What a call takes from its limiter, before each attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spend {
    /// Nothing (a CDN download: not an API call).
    Nothing,
    /// A token of the bot's.
    Bot,
    /// A token of the class's bucket in the chat, and one of the bot's.
    Class(Class),
}

/// What a rate limit cools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// The call's class in its chat (the bot's bucket for a call of no class).
    Class,
    /// Every call of the bot, in every chat.
    Bot,
}

#[derive(Debug, Clone)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    cool_down_until: Option<Instant>,
    rate_per_sec: f64,
    capacity: f64,
}

impl BucketState {
    fn new(rate: Rate) -> Self {
        let rate_per_sec = rate.per_sec.max(0.01);
        let capacity = rate.burst.max(1.0);
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
        self.clone().wait(Instant::now())
    }
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
    fn new(rate: Rate) -> Self {
        Self {
            state: Mutex::new(BucketState::new(rate)),
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
        let until = st.cool_down_until.map_or(until, |prev| prev.max(until));
        st.cool_down_until = Some(until);
        // Empty, and filling only from the end of the cool-down: a full
        // burst right after a long flood wait is what earns the next one.
        st.tokens = 0.0;
        st.last_refill = until;
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
    pub fn new(rate: Rate) -> Self {
        Self { calls: TokenBucket::new(rate) }
    }

    fn idle(&self) -> bool {
        self.calls.idle()
    }

    /// A 429 not tied to one chat: every chat of the bot waits.
    fn penalize(&self, retry_after: Duration) {
        cool_down("", "bot", retry_after);
        self.calls.penalize(retry_after);
    }
}

/// Budgets for one chat; each call also takes a token from its bot.
pub struct ChatLimiter {
    /// The chat / channel id, a metrics label.
    chat: String,
    bot: Arc<BotLimiter>,
    classes: Vec<(Class, TokenBucket)>,
    upload_sem: Semaphore,
    download_sem: Semaphore,
}

/// A service's limits: its bot's rate and each class's per chat.
#[derive(Debug, Clone)]
pub struct ChatLimiterConfig {
    /// Every call of one bot token.
    pub bot: Rate,
    pub classes: Vec<(Class, Rate)>,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
}

impl ChatLimiterConfig {
    pub fn rate(&self, class: Class) -> Option<Rate> {
        self.classes.iter().find(|(c, _)| *c == class).map(|(_, r)| *r)
    }

    /// `class` at `rate` (added if the service has no such class).
    pub fn with(mut self, class: Class, rate: Rate) -> Self {
        match self.classes.iter_mut().find(|(c, _)| *c == class) {
            Some((_, r)) => *r = rate,
            None => self.classes.push((class, rate)),
        }
        self
    }

    /// The bot and every class at `rate` (tests: limits that never bind).
    pub fn with_all(mut self, rate: Rate) -> Self {
        self.bot = rate;
        for (_, r) in &mut self.classes {
            *r = rate;
        }
        self
    }
}

impl ChatLimiter {
    /// A chat with a bot of its own (tests, single-chat tools).
    pub fn new(cfg: ChatLimiterConfig) -> Self {
        let bot = Arc::new(BotLimiter::new(cfg.bot));
        Self::for_bot(cfg, bot, "")
    }

    /// Chat `chat` of `bot`, sharing its bot-level budget with the bot's other chats.
    pub fn for_bot(cfg: ChatLimiterConfig, bot: Arc<BotLimiter>, chat: &str) -> Self {
        Self {
            chat: chat.to_owned(),
            bot,
            classes: cfg.classes.iter().map(|(c, r)| (*c, TokenBucket::new(*r))).collect(),
            upload_sem: Semaphore::new(cfg.upload_concurrency.max(1)),
            download_sem: Semaphore::new(cfg.download_concurrency.max(1)),
        }
    }

    pub fn bot(&self) -> &Arc<BotLimiter> {
        &self.bot
    }

    fn bucket(&self, class: Class) -> &TokenBucket {
        self.classes
            .iter()
            .find(|(c, _)| *c == class)
            .map(|(_, b)| b)
            .unwrap_or_else(|| panic!("class {class} is not in this limiter's config"))
    }

    /// Take what `spend` names: a class's token and the bot's at once.
    pub async fn acquire(&self, spend: Spend) {
        let (waited, op) = match spend {
            Spend::Nothing => return,
            Spend::Bot => (take_all(&[&self.bot.calls]).await, "bot"),
            Spend::Class(c) => (take_all(&[self.bucket(c), &self.bot.calls]).await, c.0),
        };
        record_limiter_wait(&self.chat, op, waited);
    }

    /// A rate limit on a call that spends `spend`: cool `level`.
    pub fn cool(&self, spend: Spend, level: Level, retry_after: Duration) {
        match (level, spend) {
            (Level::Bot, _) | (Level::Class, Spend::Bot) => self.bot.penalize(retry_after),
            (Level::Class, Spend::Class(c)) => {
                cool_down(&self.chat, c.0, retry_after);
                self.bucket(c).penalize(retry_after);
            }
            (Level::Class, Spend::Nothing) => {}
        }
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

    /// Background work: wait until `class` and the bot's have spare tokens
    /// and nobody waits for them. Takes no token; under steady foreground
    /// load it waits as long as the load lasts.
    pub async fn wait_idle(&self, class: Class) {
        let started = Instant::now();
        while !(self.bucket(class).idle() && self.bot.idle()) {
            tokio::time::sleep(IDLE_POLL).await;
        }
        record_limiter_wait(&self.chat, "idle", started.elapsed());
    }

    /// Estimated wait before `spend` could be taken (captures nothing).
    pub fn peek(&self, spend: Spend) -> Duration {
        match spend {
            Spend::Nothing => Duration::ZERO,
            Spend::Bot => self.bot.calls.peek_wait(),
            Spend::Class(c) => self.peek_wait(c),
        }
    }

    /// Estimated wait before the next token for `class` (does not capture a token).
    pub fn peek_wait(&self, class: Class) -> Duration {
        self.bucket(class).peek_wait().max(self.bot.calls.peek_wait())
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
    pub fn with_rate(mut self, rate: Rate) -> Self {
        self.rate = Some(TokenBucket::new(rate));
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
            .or_insert_with(|| Arc::new(BotLimiter::new(self.cfg.bot)))
            .clone();
        let l = Arc::new(ChatLimiter::for_bot(self.cfg.clone(), bot_limiter, chat));
        chats.insert((bot.to_string(), chat.to_string()), l.clone());
        l
    }
}

/// A rate limit the server asked for: `info` when it is long enough for a
/// person to notice (a stalled upload, a slow boot), `debug` otherwise.
fn cool_down(chat: &str, budget: &str, retry_after: Duration) {
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

    const SEND: Class = Class("send");
    const GET_FILE: Class = Class("get_file");
    const DELETE: Class = Class("delete");
    const INFO: Class = Class("info");

    /// Telegram-like limits with four classes.
    fn cfg() -> ChatLimiterConfig {
        ChatLimiterConfig {
            bot: Rate::new(25.0, 30.0),
            classes: vec![
                (SEND, Rate::new(0.3, 3.0)),
                (GET_FILE, Rate::new(15.0, 30.0)),
                (DELETE, Rate::new(1.0, 5.0)),
                (INFO, Rate::new(2.0, 5.0)),
            ],
            upload_concurrency: 2,
            download_concurrency: 8,
        }
    }

    fn fast_send() -> ChatLimiter {
        ChatLimiter::new(ChatLimiterConfig { upload_concurrency: 4, download_concurrency: 4, ..cfg().with(SEND, Rate::new(100.0, 2.0)).with(GET_FILE, Rate::new(1000.0, 100.0)).with(DELETE, Rate::new(1000.0, 100.0)) })
    }

    #[tokio::test]
    async fn chats_share_their_bot_and_nothing_else() {
        let reg = LimiterRegistry::new(ChatLimiterConfig { bot: Rate::new(0.01, 2.0), ..cfg().with(SEND, Rate::new(0.01, 1.0)) });
        let (a, b, a_again) = (reg.chat("bot1", "-1"), reg.chat("bot1", "-2"), reg.chat("bot1", "-1"));
        assert!(Arc::ptr_eq(&a, &a_again), "one limiter per chat");
        assert!(Arc::ptr_eq(a.bot(), b.bot()), "one bot limiter per bot");
        assert!(!Arc::ptr_eq(a.bot(), reg.chat("bot2", "-1").bot()));

        a.acquire(Spend::Class(SEND)).await;
        assert!(a.peek_wait(SEND) > Duration::from_secs(1), "chat a's send bucket is empty");
        assert_eq!(b.peek_wait(SEND), Duration::ZERO, "chat b's is not");
        b.acquire(Spend::Class(SEND)).await;
        // Two calls took the bot's burst of 2: every chat of the bot now waits.
        assert!(reg.chat("bot1", "-3").peek_wait(INFO) > Duration::from_secs(1));
        assert_eq!(reg.chat("bot2", "-3").peek_wait(INFO), Duration::ZERO);
    }

    #[tokio::test]
    async fn a_bot_level_429_holds_every_chat_a_class_429_only_its_own() {
        let reg = LimiterRegistry::new(cfg());
        let (a, b) = (reg.chat("bot", "-1"), reg.chat("bot", "-2"));
        a.cool(Spend::Class(SEND), Level::Class, Duration::from_secs(20));
        assert!(a.peek_wait(SEND) > Duration::from_secs(10));
        assert_eq!(b.peek_wait(SEND), Duration::ZERO);
        a.cool(Spend::Bot, Level::Bot, Duration::from_secs(20));
        assert!(b.peek_wait(DELETE) > Duration::from_secs(10), "the bot's level holds every chat");
    }

    #[tokio::test]
    async fn waiting_for_the_bot_holds_no_chat_token() {
        let lim = Arc::new(ChatLimiter::new(ChatLimiterConfig { bot: Rate::new(10.0, 1.0), ..cfg().with(SEND, Rate::new(0.01, 1.0)) }));
        lim.acquire(Spend::Class(INFO)).await; // the bot's only token
        let send = {
            let lim = lim.clone();
            tokio::spawn(async move { lim.acquire(Spend::Class(SEND)).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!send.is_finished(), "the send waits for the bot");
        assert_eq!(lim.bucket(SEND).peek_wait(), Duration::ZERO, "and has not taken the chat's send token");
        send.await.unwrap();
        assert!(lim.peek_wait(SEND) > Duration::from_secs(1), "now it has");
    }

    #[tokio::test]
    async fn waiting_for_the_chat_holds_no_bot_token() {
        let reg = LimiterRegistry::new(ChatLimiterConfig { bot: Rate::new(0.01, 2.0), ..cfg().with(SEND, Rate::new(10.0, 1.0)) });
        let (a, b) = (reg.chat("bot", "-1"), reg.chat("bot", "-2"));
        a.acquire(Spend::Class(SEND)).await; // a's send token, and one of the bot's two
        let again = {
            let a = a.clone();
            tokio::spawn(async move { a.acquire(Spend::Class(SEND)).await })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!again.is_finished(), "a's second send waits for its chat");
        let t0 = Instant::now();
        b.acquire(Spend::Class(SEND)).await;
        assert!(t0.elapsed() < Duration::from_millis(50), "the bot's last token went to chat b");
        again.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_cooled_bucket_starts_empty_when_the_cool_down_ends() {
        let lim = ChatLimiter::new(cfg().with(SEND, Rate::new(1.0, 3.0)));
        lim.cool(Spend::Class(SEND), Level::Class, Duration::from_secs(35));
        tokio::time::sleep(Duration::from_secs(35)).await;
        // Not a burst of three at once: one token a second from here.
        let t0 = Instant::now();
        for _ in 0..3 {
            lim.acquire(Spend::Class(SEND)).await;
        }
        assert!(t0.elapsed() >= Duration::from_millis(2900), "{:?}", t0.elapsed());
    }

    #[tokio::test]
    async fn send_bucket_independent_of_get_file() {
        let lim = ChatLimiter::new(cfg().with(SEND, Rate::new(0.01, 1.0)).with(GET_FILE, Rate::new(1000.0, 100.0)));
        lim.acquire(Spend::Class(SEND)).await;
        // getFile still has tokens even though send is exhausted.
        let t0 = Instant::now();
        lim.acquire(Spend::Class(GET_FILE)).await;
        assert!(t0.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn penalize_send_does_not_block_get_file() {
        let lim = Arc::new(fast_send());
        lim.cool(Spend::Class(SEND), Level::Class, Duration::from_millis(200));
        let t0 = Instant::now();
        lim.acquire(Spend::Class(GET_FILE)).await;
        assert!(t0.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn upload_semaphore_caps_concurrency() {
        let lim = Arc::new(ChatLimiter::new(ChatLimiterConfig { upload_concurrency: 1, ..cfg() }));
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
        let lim = ChatLimiter::new(cfg().with(SEND, Rate::new(0.01, 1.0)));
        assert_eq!(lim.peek_wait(SEND), Duration::ZERO);
        lim.acquire(Spend::Class(SEND)).await;
        let peeked = lim.peek_wait(SEND);
        assert!(peeked > Duration::from_millis(50));
        // Second peek must not change the estimate (no capture).
        let peeked2 = lim.peek_wait(SEND);
        assert!((peeked2.as_secs_f64() - peeked.as_secs_f64()).abs() < 0.05);
        assert_eq!(lim.peek_wait(GET_FILE), Duration::ZERO);
    }

    #[tokio::test]
    async fn background_waits_for_a_foreground_caller_and_spare_tokens() {
        let l = Arc::new(ChatLimiter::new(cfg().with(DELETE, Rate::new(20.0, 2.0))));
        l.wait_idle(DELETE).await; // a fresh budget is idle
        l.acquire(Spend::Class(DELETE)).await;
        l.acquire(Spend::Class(DELETE)).await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let fg = {
            let (l, order) = (l.clone(), order.clone());
            tokio::spawn(async move {
                l.acquire(Spend::Class(DELETE)).await; // waits ~50 ms for a token
                order.lock().unwrap().push("foreground");
            })
        };
        tokio::task::yield_now().await;
        l.wait_idle(DELETE).await;
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
