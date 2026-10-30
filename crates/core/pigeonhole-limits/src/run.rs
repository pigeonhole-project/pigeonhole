//! One call to a chat service, start to end: take its tokens before each
//! attempt, ask the service what a failure means ([`Verdict`]), cool the
//! buckets the call names on a rate limit, and retry within a [`Retry`]
//! (docs/shared-limiter.md, § The API). The services bring the call and its
//! `classify`; nothing here knows Telegram or Discord.

use crate::{record_throttle, ChatLimiter, Level, Spend};
use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// What a call spends and what its rate limit cools by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Call {
    /// `telegram`, `discord`: a metrics label.
    pub service: &'static str,
    /// The method (`messages.sendMedia`, `create message`).
    pub what: &'static str,
    /// Taken before each attempt.
    pub spend: Spend,
    /// What a rate limit on the call cools, unless the service names a
    /// level itself ([`Verdict::Wait`]); `None` cools nothing.
    pub cools: Option<Level>,
}

/// What a failed attempt means, as the service's `classify` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Rate limited: the call was not applied; try again after `after`.
    /// `level` when the service says what the limit is (Discord's `global`:
    /// the bot), else the call's [`Call::cools`].
    Wait { after: Duration, level: Option<Level> },
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
    pub deadline: Duration,
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
    /// Still failing transiently when the deadline or the attempts ran out.
    Unavailable { last: E, after: Duration },
    /// The service refused the call ([`Verdict::Fatal`]).
    Fatal(E),
}

/// Run `attempt` until it succeeds or fails for good. Before each attempt it
/// takes the call's [`Spend`] from `limiter` (nothing without one); a
/// [`Verdict::Wait`] cools its level and is waited out.
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
    let deadline = started + retry.deadline;
    let mut backoff = retry.backoff;
    let mut tries = 0;
    loop {
        if let Some(limiter) = limiter {
            limiter.acquire(call.spend).await;
        }
        tries += 1;
        let err = match attempt().await {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        let last_try = tries >= retry.attempts;
        let wait = match classify(&err) {
            Verdict::Fatal => return Err(Failed::Fatal(err)),
            Verdict::Wait { after, level } => {
                record_throttle(call.service, call.what, after);
                if let (Some(limiter), Some(level)) = (limiter, level.or(call.cools)) {
                    limiter.cool(call.spend, level, after);
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
    use crate::{Class, ChatLimiterConfig, Rate};
    use std::sync::atomic::{AtomicU32, Ordering};

    const SEND: Class = Class("send");
    const DELETE: Class = Class("delete");
    const CALL: Call = Call { service: "test", what: "send", spend: Spend::Class(SEND), cools: Some(Level::Class) };
    const RETRY: Retry = Retry {
        deadline: Duration::from_secs(60),
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

    /// A flood of more than 100 s is the bot's (like Discord's `global`).
    fn classify(e: &E) -> Verdict {
        match e {
            E::Flood(s) => Verdict::Wait { after: Duration::from_secs(*s), level: (*s > 100).then_some(Level::Bot) },
            E::Lost => Verdict::Transient,
            E::No => Verdict::Fatal,
        }
    }

    fn limits(rate: Rate) -> ChatLimiterConfig {
        ChatLimiterConfig {
            bot: Rate::new(1000.0, 1000.0),
            classes: vec![(SEND, rate), (DELETE, Rate::new(1000.0, 1000.0))],
            upload_concurrency: 1,
            download_concurrency: 1,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limit_cools_the_calls_level_and_is_waited_out() {
        let lim = ChatLimiter::new(limits(Rate::new(1000.0, 1000.0)));
        let tries = AtomicU32::new(0);
        let t0 = tokio::time::Instant::now();
        let v = run(Some(&lim), &CALL, &RETRY, classify, || {
            let n = tries.fetch_add(1, Ordering::SeqCst);
            async move { if n == 0 { Err(E::Flood(5)) } else { Ok(9) } }
        })
        .await
        .unwrap();
        assert_eq!((v, tries.load(Ordering::SeqCst)), (9, 2));
        assert!(t0.elapsed() >= Duration::from_secs(5), "the retry waited for the cooled send bucket");
        let failed = run(Some(&lim), &Call { what: "again", ..CALL }, &Retry { attempts: 1, ..RETRY }, classify, || async {
            Err::<(), _>(E::Flood(5))
        })
        .await;
        assert!(matches!(failed, Err(Failed::RateLimited { .. })));
        assert!(lim.peek_wait(SEND) > Duration::from_secs(3), "the call's level is its class");
        assert_eq!(lim.peek_wait(DELETE), Duration::ZERO, "and only it");
    }

    #[tokio::test(start_paused = true)]
    async fn the_bot_is_cooled_when_the_service_says_so() {
        let lim = ChatLimiter::new(limits(Rate::new(1000.0, 1000.0)));
        let r = run(Some(&lim), &CALL, &RETRY, classify, || async { Err::<(), _>(E::Flood(500)) }).await;
        assert!(matches!(r, Err(Failed::RateLimited { retry_after, .. }) if retry_after == Duration::from_secs(500)));
        assert!(lim.peek_wait(DELETE) > Duration::from_secs(100), "every call of the bot waits");
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_back_off_until_the_attempts_run_out() {
        let tries = AtomicU32::new(0);
        let retry = Retry { attempts: 3, ..RETRY };
        let r = run(None, &CALL, &retry, classify, || {
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
        let r = run(None, &CALL, &RETRY, classify, || {
            tries.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(E::No) }
        })
        .await;
        assert!(matches!(r, Err(Failed::Fatal(E::No))));
        assert_eq!(tries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn every_attempt_takes_its_tokens() {
        let lim = ChatLimiter::new(limits(Rate::new(0.001, 2.0)));
        let tries = AtomicU32::new(0);
        let _ = run(Some(&lim), &CALL, &Retry { attempts: 2, ..RETRY }, classify, || {
            tries.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(E::Lost) }
        })
        .await;
        assert_eq!(tries.load(Ordering::SeqCst), 2);
        assert!(lim.peek_wait(SEND) > Duration::from_secs(50), "two attempts took the burst of two");
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_of_no_class_still_takes_the_bots_token() {
        let lim = ChatLimiter::new(ChatLimiterConfig { bot: Rate::new(0.001, 1.0), ..limits(Rate::new(1000.0, 1000.0)) });
        let bot_only = Call { spend: Spend::Bot, cools: Some(Level::Bot), ..CALL };
        run(Some(&lim), &bot_only, &RETRY, classify, || async { Ok::<_, E>(()) }).await.unwrap();
        assert!(lim.peek_wait(SEND) > Duration::from_secs(50), "the bot's only token is gone");
        let cdn = Call { spend: Spend::Nothing, cools: None, ..CALL };
        let t0 = tokio::time::Instant::now();
        run(Some(&lim), &cdn, &RETRY, classify, || async { Ok::<_, E>(()) }).await.unwrap();
        assert_eq!(t0.elapsed(), Duration::ZERO, "a CDN read takes nothing");
    }
}
