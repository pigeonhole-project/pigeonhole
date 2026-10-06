//! Per-chat Telegram rate limits: separate budgets for send, getFile, and delete.
//!
//! Chat sends (~20 msg/min on channels) must not share a bucket with `getFile`
//! (much higher) or `deleteMessage` (purge / GC). File CDN byte streams are not
//! API-metered — only capped by a download connection semaphore.

use std::sync::Mutex;
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

    async fn acquire(&self) {
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
                Ok(()) => return,
                Err(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = self.notify.notified() => {}
                    }
                }
            }
        }
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

/// Budgets for one Telegram chat / bot process.
pub struct ChatLimiter {
    send: TokenBucket,
    get_file: TokenBucket,
    delete: TokenBucket,
    upload_sem: Semaphore,
    download_sem: Semaphore,
}

#[derive(Debug, Clone)]
pub struct ChatLimiterConfig {
    pub send_rate_per_sec: f64,
    pub send_burst: f64,
    pub get_file_rate_per_sec: f64,
    pub get_file_burst: f64,
    pub delete_rate_per_sec: f64,
    pub delete_burst: f64,
    pub upload_concurrency: usize,
    pub download_concurrency: usize,
}

impl Default for ChatLimiterConfig {
    fn default() -> Self {
        Self {
            send_rate_per_sec: 0.5,
            send_burst: 3.0,
            get_file_rate_per_sec: 15.0,
            get_file_burst: 30.0,
            delete_rate_per_sec: 1.0,
            delete_burst: 5.0,
            upload_concurrency: 2,
            download_concurrency: 8,
        }
    }
}

impl ChatLimiter {
    pub fn new(cfg: ChatLimiterConfig) -> Self {
        Self {
            send: TokenBucket::new(cfg.send_rate_per_sec, cfg.send_burst),
            get_file: TokenBucket::new(cfg.get_file_rate_per_sec, cfg.get_file_burst),
            delete: TokenBucket::new(cfg.delete_rate_per_sec, cfg.delete_burst),
            upload_sem: Semaphore::new(cfg.upload_concurrency.max(1)),
            download_sem: Semaphore::new(cfg.download_concurrency.max(1)),
        }
    }

    /// `sendDocument` / `sendMessage` / pin / unpin.
    pub async fn acquire_send(&self) {
        self.send.acquire().await;
    }

    pub fn penalize_send(&self, retry_after: Duration) {
        debug!(?retry_after, "telegram send cool-down from 429");
        self.send.penalize(retry_after);
    }

    /// `getFile` only (not the CDN byte stream).
    pub async fn acquire_get_file(&self) {
        self.get_file.acquire().await;
    }

    pub fn penalize_get_file(&self, retry_after: Duration) {
        debug!(?retry_after, "telegram getFile cool-down from 429");
        self.get_file.penalize(retry_after);
    }

    /// `deleteMessage`.
    pub async fn acquire_delete(&self) {
        self.delete.acquire().await;
    }

    pub fn penalize_delete(&self, retry_after: Duration) {
        debug!(?retry_after, "telegram delete cool-down from 429");
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
        })
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
}
