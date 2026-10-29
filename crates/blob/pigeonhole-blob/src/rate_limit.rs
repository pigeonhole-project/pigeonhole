//! Telegram rate limits on two levels, plus explicit cool-downs from 429s.
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, SemaphorePermit};
use tracing::debug;

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
    notify: Notify,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            state: Mutex::new(BucketState::new(rate_per_sec, burst)),
            notify: Notify::new(),
        }
    }

    async fn acquire(&self) -> Duration {
        let started = Instant::now();
        loop {
            let action = {
                let mut st = self.state.lock().unwrap();
                let now = Instant::now();
                if let Some(until) = st.cool_down_until {
                    if now < until {
                        Err(until.saturating_duration_since(now))
                    } else {
                        st.cool_down_until = None;
                        st.refill(now);
                        if st.tokens >= 1.0 {
                            st.tokens -= 1.0;
                            Ok(())
                        } else {
                            Err(st.time_until_token())
                        }
                    }
                } else {
                    st.refill(now);
                    if st.tokens >= 1.0 {
                        st.tokens -= 1.0;
                        Ok(())
                    } else {
                        Err(st.time_until_token())
                    }
                }
            };
            match action {
                Ok(()) => return started.elapsed(),
                Err(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = self.notify.notified() => {}
                    }
                }
            }
        }
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
        drop(st);
        self.notify.notify_waiters();
    }
}

/// Every call of one bot, across its chats.
pub struct BotLimiter {
    calls: TokenBucket,
}

impl BotLimiter {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self { calls: TokenBucket::new(rate_per_sec, burst) }
    }

    async fn acquire(&self) -> Duration {
        self.calls.acquire().await
    }

    /// A 429 not tied to one chat: every chat of the bot waits.
    pub fn penalize(&self, retry_after: Duration) {
        cool_down("bot", retry_after);
        self.calls.penalize(retry_after);
    }
}

/// Budgets for one chat; each call also takes a token from its bot.
pub struct ChatLimiter {
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
        Self::for_bot(cfg, bot)
    }

    /// A chat of `bot`, sharing its bot-level budget with the bot's other chats.
    pub fn for_bot(cfg: ChatLimiterConfig, bot: Arc<BotLimiter>) -> Self {
        Self {
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

    /// Chat bucket first, then the bot's.
    async fn take(&self, bucket: &TokenBucket, name: &'static str) {
        let waited = bucket.acquire().await + self.bot.acquire().await;
        crate::metrics::record_limiter_wait(name, waited);
    }

    /// `sendDocument` / `sendMessage` / pin / unpin.
    pub async fn acquire_send(&self) {
        self.take(&self.send, "send").await
    }

    /// `getChat`, `setChatDescription`.
    pub async fn acquire_info(&self) {
        self.take(&self.info, "info").await
    }

    pub fn penalize_info(&self, retry_after: Duration) {
        cool_down("info", retry_after);
        self.info.penalize(retry_after);
    }

    pub fn penalize_send(&self, retry_after: Duration) {
        cool_down("send", retry_after);
        self.send.penalize(retry_after);
    }

    /// `getFile` only (not the CDN byte stream).
    pub async fn acquire_get_file(&self) {
        self.take(&self.get_file, "get_file").await
    }

    /// `getFile` is not tied to a chat: its 429 holds the whole bot.
    pub fn penalize_get_file(&self, retry_after: Duration) {
        cool_down("getFile", retry_after);
        self.get_file.penalize(retry_after);
        self.bot.penalize(retry_after);
    }

    /// `deleteMessage`.
    pub async fn acquire_delete(&self) {
        self.take(&self.delete, "delete").await
    }

    pub fn penalize_delete(&self, retry_after: Duration) {
        cool_down("delete", retry_after);
        self.delete.penalize(retry_after);
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

    /// Estimated wait before the next token for `budget` (does not capture a token).
    pub fn peek_wait(&self, budget: LimitBudget) -> Duration {
        match budget {
            LimitBudget::Send => self.send.peek_wait(),
            LimitBudget::GetFile => self.get_file.peek_wait(),
            LimitBudget::Delete => self.delete.peek_wait(),
            LimitBudget::Info => self.info.peek_wait(),
        }
        .max(self.bot.calls.peek_wait())
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
        let l = Arc::new(ChatLimiter::for_bot(self.cfg.clone(), bot_limiter));
        chats.insert((bot.to_string(), chat.to_string()), l.clone());
        l
    }
}

/// A rate limit the server asked for: `info` when it is long enough for a
/// person to notice (a stalled upload, a slow boot), `debug` otherwise.
fn cool_down(budget: &'static str, retry_after: Duration) {
    if retry_after >= Duration::from_secs(5) {
        tracing::info!(budget, wait_secs = retry_after.as_secs_f64(), "rate limited: waiting");
    } else {
        debug!(budget, ?retry_after, "rate limited");
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

        a.acquire_send().await;
        assert!(a.peek_wait(LimitBudget::Send) > Duration::from_secs(1), "chat a's send bucket is empty");
        assert_eq!(b.peek_wait(LimitBudget::Send), Duration::ZERO, "chat b's is not");
        b.acquire_send().await;
        // Two calls took the bot's burst of 2: every chat of the bot now waits.
        assert!(reg.chat("bot1", "-3").peek_wait(LimitBudget::Info) > Duration::from_secs(1));
        assert_eq!(reg.chat("bot2", "-3").peek_wait(LimitBudget::Info), Duration::ZERO);
    }

    #[tokio::test]
    async fn a_get_file_429_holds_the_bot_a_send_429_only_the_chat() {
        let reg = LimiterRegistry::new(ChatLimiterConfig::default());
        let (a, b) = (reg.chat("bot", "-1"), reg.chat("bot", "-2"));
        a.penalize_send(Duration::from_secs(20));
        assert!(a.peek_wait(LimitBudget::Send) > Duration::from_secs(10));
        assert_eq!(b.peek_wait(LimitBudget::Send), Duration::ZERO);
        a.penalize_get_file(Duration::from_secs(20));
        assert!(b.peek_wait(LimitBudget::Delete) > Duration::from_secs(10), "getFile is not chat-scoped");
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
        lim.acquire_send().await;
        // getFile still has tokens even though send is exhausted.
        let t0 = Instant::now();
        lim.acquire_get_file().await;
        assert!(t0.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn penalize_send_does_not_block_get_file() {
        let lim = Arc::new(fast_send());
        lim.penalize_send(Duration::from_millis(200));
        let t0 = Instant::now();
        lim.acquire_get_file().await;
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
        lim.acquire_send().await;
        let peeked = lim.peek_wait(LimitBudget::Send);
        assert!(peeked > Duration::from_millis(50));
        // Second peek must not change the estimate (no capture).
        let peeked2 = lim.peek_wait(LimitBudget::Send);
        assert!((peeked2.as_secs_f64() - peeked.as_secs_f64()).abs() < 0.05);
        assert_eq!(lim.peek_wait(LimitBudget::GetFile), Duration::ZERO);
    }
}
