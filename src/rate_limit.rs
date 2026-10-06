//! Shared per-chat Telegram rate limit: token bucket + upload concurrency.
//!
//! Without a process-wide limiter, parallel UploadPart / aws-cli multipart
//! workers each sleep the same `retry_after` after a 429 and then stampede
//! again. [`ChatLimiter`] serializes budget across all callers in this process.

use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore, SemaphorePermit};
use tracing::debug;

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    /// Absolute time until which no tokens may be taken (set from 429).
    cool_down_until: Option<Instant>,
    rate_per_sec: f64,
    capacity: f64,
}

impl BucketState {
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

enum AcquireAction {
    Ready,
    Wait(Duration),
}

/// Process-wide budget for one Telegram chat.
pub struct ChatLimiter {
    state: Mutex<BucketState>,
    notify: Notify,
    upload_sem: Semaphore,
}

impl ChatLimiter {
    pub fn new(rate_per_sec: f64, burst: f64, upload_concurrency: usize) -> Self {
        let rate_per_sec = rate_per_sec.max(0.01);
        let capacity = burst.max(1.0);
        let upload_concurrency = upload_concurrency.max(1);
        Self {
            state: Mutex::new(BucketState {
                tokens: capacity,
                last_refill: Instant::now(),
                cool_down_until: None,
                rate_per_sec,
                capacity,
            }),
            notify: Notify::new(),
            upload_sem: Semaphore::new(upload_concurrency),
        }
    }

    /// Wait until one API request token is available (honours 429 cool-down).
    pub async fn acquire(&self) {
        loop {
            let action = {
                let mut st = self.state.lock().unwrap();
                let now = Instant::now();
                if let Some(until) = st.cool_down_until {
                    if now < until {
                        AcquireAction::Wait(until.saturating_duration_since(now))
                    } else {
                        st.cool_down_until = None;
                        st.refill(now);
                        if st.tokens >= 1.0 {
                            st.tokens -= 1.0;
                            AcquireAction::Ready
                        } else {
                            AcquireAction::Wait(st.time_until_token())
                        }
                    }
                } else {
                    st.refill(now);
                    if st.tokens >= 1.0 {
                        st.tokens -= 1.0;
                        AcquireAction::Ready
                    } else {
                        AcquireAction::Wait(st.time_until_token())
                    }
                }
            };
            match action {
                AcquireAction::Ready => return,
                AcquireAction::Wait(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = self.notify.notified() => {}
                    }
                }
            }
        }
    }

    /// Slot for an in-flight upload (`sendDocument`). Hold until the call finishes.
    pub async fn acquire_upload(&self) -> SemaphorePermit<'_> {
        self.upload_sem
            .acquire()
            .await
            .expect("upload semaphore closed")
    }

    /// On HTTP 429: freeze the whole bucket for `retry_after` and wake waiters
    /// so they pick up the longer cool-down instead of stampeding together.
    pub fn penalize(&self, retry_after: Duration) {
        let retry_after = retry_after.max(Duration::from_secs(1));
        let mut st = self.state.lock().unwrap();
        let until = Instant::now() + retry_after;
        st.cool_down_until = Some(match st.cool_down_until {
            Some(prev) => prev.max(until),
            None => until,
        });
        st.tokens = 0.0;
        debug!(?retry_after, "telegram limiter cool-down from 429");
        drop(st);
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn acquire_consumes_burst_then_waits() {
        let lim = ChatLimiter::new(100.0, 2.0, 4);
        lim.acquire().await;
        lim.acquire().await;
        // Third token needs refill; with rate 100/s wait is tiny.
        let t0 = Instant::now();
        lim.acquire().await;
        assert!(t0.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn penalize_blocks_all_waiters() {
        let lim = Arc::new(ChatLimiter::new(10.0, 5.0, 4));
        lim.penalize(Duration::from_millis(80));
        let t0 = Instant::now();
        lim.acquire().await;
        assert!(t0.elapsed() >= Duration::from_millis(60));
    }

    #[tokio::test]
    async fn upload_semaphore_caps_concurrency() {
        let lim = Arc::new(ChatLimiter::new(1000.0, 100.0, 1));
        let p1 = lim.acquire_upload().await;
        let lim2 = lim.clone();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _p = lim2.acquire_upload().await;
            let _ = tx.send(());
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(rx.try_recv().is_err(), "second upload should wait");
        drop(p1);
        rx.await.unwrap();
        handle.await.unwrap();
    }
}
