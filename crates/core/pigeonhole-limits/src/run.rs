//! One call to a chat service, start to end: take its tokens before each
//! attempt, ask the service what a failure means ([`Verdict`]), cool the
//! buckets the call names on a rate limit, and retry within a [`Retry`]
//! (docs/shared-limiter.md, § The API). The services bring the call and its
//! `classify`; nothing here knows Telegram or Discord.

use crate::{record_throttle, ChatLimiter, LimitBudget};
use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// What a call spends and which buckets its rate limit cools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Call {
    /// `telegram`, `discord`: a metrics label.
    pub service: &'static str,
    /// The method (`messages.sendMedia`, `create message`).
    pub what: &'static str,
    /// The chat bucket each attempt takes a token from (with one of the
    /// bot's); `None` takes nothing.
    pub budget: Option<LimitBudget>,
    /// What a rate limit on it cools.
    pub penalty: Penalty,
}

/// The buckets a rate limit cools.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Penalty {
    None,
    /// The call's own bucket in its chat.
    Class(LimitBudget),
    /// Every call of the bot.
    Bot,
    /// Both.
    ClassAndBot(LimitBudget),
}

/// What a failed attempt means, as the service's `classify` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Rate limited: the call was not applied; try again after `after`.
    /// `whole_bot` when the service says the limit is the bot's, whatever
    /// the call (Discord's `global`).
    Wait { after: Duration, whole_bot: bool },
    /// Worth another attempt after a backoff (a lost connection, a 5xx).
    Transient,
    /// Hand the error up as is.
    Fatal,
}

/// How long a call keeps trying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    /// Nothing is waited past this, from the first attempt: a rate limit
    /// that does not fit ends the call as [`Failed::RateLimited`].
    pub budget: Duration,
    /// Attempts in all.
    pub attempts: u32,
    /// The first backoff after a transient failure, doubled up to `max_backoff`.
    pub backoff: Duration,
    pub max_backoff: Duration,
}

/// How a call failed for good.
#[derive(Debug)]
pub enum Failed<E> {
    /// Still rate limited: the last wait did not fit, or attempts ran out on one.
    RateLimited { retry_after: Duration, last: E },
    /// Still failing transiently when the budget or the attempts ran out.
    Unavailable { last: E, after: Duration },
    /// The service refused the call ([`Verdict::Fatal`]).
    Fatal(E),
}

impl ChatLimiter {
    pub(crate) fn cool(&self, penalty: Penalty, whole_bot: bool, after: Duration) {
        match penalty {
            Penalty::None => {}
            Penalty::Class(b) => self.penalize_budget(b, after),
            Penalty::Bot => self.bot().penalize(after),
            Penalty::ClassAndBot(b) => {
                self.penalize_budget(b, after);
                self.bot().penalize(after);
            }
        }
        if whole_bot && !matches!(penalty, Penalty::Bot | Penalty::ClassAndBot(_)) {
            self.bot().penalize(after);
        }
    }
}

/// Run `attempt` until it succeeds or fails for good. Before each attempt it
/// takes the call's tokens from `limiter` (none without one); a
/// [`Verdict::Wait`] cools the call's [`Penalty`] and is waited out.
pub async fn run<T, E, Fut>(
    limiter: Option<&ChatLimiter>,
    call: &Call,
    retry: &Retry,
    classify: impl Fn(&E) -> Verdict,
    mut attempt: impl FnMut() -> Fut,
) -> Result<T, Failed<E>>
where
    E: std::fmt::Display,
    Fut: Future<Output = Result<T, E>>,
{
    let started = tokio::time::Instant::now();
    let deadline = started + retry.budget;
    let mut backoff = retry.backoff;
    let mut tries = 0;
    loop {
        if let (Some(limiter), Some(budget)) = (limiter, call.budget) {
            limiter.acquire(budget).await;
        }
        tries += 1;
        let err = match attempt().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        let last_try = tries >= retry.attempts;
        let wait = match classify(&err) {
            Verdict::Fatal => return Err(Failed::Fatal(err)),
            Verdict::Wait { after, whole_bot } => {
                record_throttle(call.service, call.what, after);
                if let Some(limiter) = limiter {
                    limiter.cool(call.penalty, whole_bot, after);
                }
                if last_try || tokio::time::Instant::now() + after >= deadline {
                    return Err(Failed::RateLimited { retry_after: after, last: err });
                }
                after
            }
            Verdict::Transient => {
                let wait = backoff;
                backoff = (backoff * 2).min(retry.max_backoff);
                if last_try || tokio::time::Instant::now() + wait >= deadline {
                    return Err(Failed::Unavailable { last: err, after: started.elapsed() });
                }
                warn!(service = call.service, what = call.what, ?wait, error = %err, "call failed; retrying");
                wait
            }
        };
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChatLimiterConfig;
    use std::sync::atomic::{AtomicU32, Ordering};

    const SEND: Call = Call {
        service: "test",
        what: "send",
        budget: Some(LimitBudget::Send),
        penalty: Penalty::Class(LimitBudget::Send),
    };
    const RETRY: Retry = Retry {
        budget: Duration::from_secs(60),
        attempts: u32::MAX,
        backoff: Duration::from_millis(500),
        max_backoff: Duration::from_secs(16),
    };

    #[derive(Debug, PartialEq)]
    enum E {
        Flood(u64),
        Lost,
        No,
    }

    impl std::fmt::Display for E {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    fn classify(e: &E) -> Verdict {
        match e {
            E::Flood(s) => Verdict::Wait { after: Duration::from_secs(*s), whole_bot: *s > 100 },
            E::Lost => Verdict::Transient,
            E::No => Verdict::Fatal,
        }
    }

    fn loose() -> ChatLimiter {
        ChatLimiter::new(ChatLimiterConfig {
            bot_rate_per_sec: 1000.0,
            bot_burst: 1000.0,
            send_rate_per_sec: 1000.0,
            send_burst: 1000.0,
            ..ChatLimiterConfig::default()
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limit_cools_the_calls_bucket_and_is_waited_out() {
        let lim = loose();
        let tries = AtomicU32::new(0);
        let t0 = tokio::time::Instant::now();
        let v = run(Some(&lim), &SEND, &RETRY, classify, || {
            let n = tries.fetch_add(1, Ordering::SeqCst);
            async move { if n == 0 { Err(E::Flood(5)) } else { Ok(9) } }
        })
        .await
        .unwrap();
        assert_eq!((v, tries.load(Ordering::SeqCst)), (9, 2));
        assert!(t0.elapsed() >= Duration::from_secs(5), "the retry waited for the cooled send bucket");
        lim.cool(SEND.penalty, false, Duration::from_secs(5));
        assert!(lim.peek_wait(LimitBudget::Send) > Duration::from_secs(3), "the call's penalty is its send bucket");
        assert_eq!(lim.peek_wait(LimitBudget::Delete), Duration::ZERO, "and only it");
    }

    #[tokio::test(start_paused = true)]
    async fn the_bot_is_cooled_when_the_service_says_so() {
        let lim = loose();
        let r = run(Some(&lim), &SEND, &RETRY, classify, || async { Err::<(), _>(E::Flood(500)) }).await;
        assert!(matches!(r, Err(Failed::RateLimited { retry_after, .. }) if retry_after == Duration::from_secs(500)));
        assert!(lim.peek_wait(LimitBudget::Delete) > Duration::from_secs(100), "every call of the bot waits");
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_back_off_until_the_attempts_run_out() {
        let tries = AtomicU32::new(0);
        let retry = Retry { attempts: 3, ..RETRY };
        let r = run(None, &SEND, &retry, classify, || {
            tries.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(E::Lost) }
        })
        .await;
        assert!(matches!(r, Err(Failed::Unavailable { last: E::Lost, .. })));
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_not_retried() {
        let tries = AtomicU32::new(0);
        let r = run(None, &SEND, &RETRY, classify, || {
            tries.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(E::No) }
        })
        .await;
        assert!(matches!(r, Err(Failed::Fatal(E::No))));
        assert_eq!(tries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn every_attempt_takes_its_tokens() {
        let lim = ChatLimiter::new(ChatLimiterConfig { send_rate_per_sec: 0.001, send_burst: 2.0, ..ChatLimiterConfig::default() });
        let tries = AtomicU32::new(0);
        let _ = run(Some(&lim), &SEND, &Retry { attempts: 2, ..RETRY }, classify, || {
            tries.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(E::Lost) }
        })
        .await;
        assert_eq!(tries.load(Ordering::SeqCst), 2);
        assert!(lim.peek_wait(LimitBudget::Send) > Duration::from_secs(50), "two attempts took the burst of two");
    }
}
