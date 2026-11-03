//! What a store over a chat service (Telegram, Discord) builds its clients
//! and its pool of bots from: one channel through several bots
//! ([`Pooled`], docs/shared-limiter.md § Choosing the account), a client's
//! failure for good as the store's error types ([`Failed::into_error`]),
//! and a store's cost hint from its limiter ([`cost`]).

use crate::{ChatLimiter, Class, Failed, Pool, Retry, Spend};
use pigeonhole_storage_api::{CostHint, OpKind, RateLimited, Unavailable};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// A service client of one channel through one bot, as a pool member.
pub trait Member: Clone {
    /// The same channel as a member of a pool of several: a rate limit
    /// ends a call at once (unless its outcome is unknown) instead of being
    /// waited out, so that the pool may move it to another bot.
    fn in_pool(&self) -> Self;
    /// The bot's limiter in this channel.
    fn limiter(&self) -> &Arc<ChatLimiter>;
}

impl Retry {
    /// The same retries in a pool: a rate limit gives way
    /// ([`Retry::give_way`]).
    pub const fn giving_way(self) -> Self {
        Self { give_way: true, ..self }
    }
}

/// One channel through several bots (the first is the primary): an
/// operation runs on the member whose budget for it is ready soonest and
/// moves to another when the service answers a rate limit
/// ([`RateLimited`]: the call was not applied).
pub struct Pooled<C> {
    members: Vec<C>,
    pool: Pool,
    deadline: Duration,
}

impl<C> Pooled<C> {
    /// `members` with their limiters, the primary first; an operation keeps
    /// moving and waiting for at most `deadline`. A member must give way on
    /// a rate limit, unless it is alone (no one to move to).
    pub fn new(chat: &str, members: Vec<(C, Arc<ChatLimiter>)>, deadline: Duration) -> Self {
        let (members, limiters) = members.into_iter().unzip();
        Self { members, pool: Pool::new(chat, limiters), deadline }
    }

    /// How long an operation keeps moving and waiting before it gives up.
    pub fn with_deadline(self, deadline: Duration) -> Self {
        Self { deadline, ..self }
    }

    /// `members` as [`Self::new`] takes them: each in a pool
    /// ([`Member::in_pool`]) unless it is alone, with its limiter.
    pub fn members<M: Member>(members: Vec<M>) -> Vec<(M, Arc<ChatLimiter>)> {
        let alone = members.len() == 1;
        members.into_iter().map(|m| (if alone { m.clone() } else { m.in_pool() }, m.limiter().clone())).collect()
    }

    /// The primary's limiter.
    pub fn limiter(&self) -> &Arc<ChatLimiter> {
        self.pool.member(0)
    }

    pub fn member(&self, i: usize) -> &C {
        &self.members[i]
    }

    pub fn primary(&self) -> &C {
        &self.members[0]
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Run `op` (given a member's index) on the member whose `spend` is
    /// ready soonest, moving on a rate limit.
    pub async fn run<T, Fut>(&self, spend: Spend, op: impl FnMut(usize) -> Fut) -> anyhow::Result<T>
    where
        Fut: Future<Output = anyhow::Result<T>>,
    {
        self.pool.run(spend, self.deadline, rate_limited, op).await
    }

    /// Run `op` on `member` only (what only that bot can do), waiting a
    /// rate limit out.
    pub async fn run_on<T, Fut>(&self, member: usize, op: impl FnMut(usize) -> Fut) -> anyhow::Result<T>
    where
        Fut: Future<Output = anyhow::Result<T>>,
    {
        self.pool.run_on(member, self.deadline, rate_limited, op).await
    }
}

/// The wait of an error that was a rate limit given way.
pub fn rate_limited(e: &anyhow::Error) -> Option<Duration> {
    e.downcast_ref::<RateLimited>().map(|r| r.retry_after)
}

impl<E: std::fmt::Display> Failed<E> {
    /// The failure as a store reports it, for call `what`: still rate
    /// limited → [`RateLimited`] (not applied: a pool may move it); an
    /// unknown outcome → [`Unavailable`], with the last error and how long
    /// it was tried; a refusal → `fatal(err)`.
    pub fn into_error(self, what: &'static str, fatal: impl FnOnce(E) -> anyhow::Error) -> anyhow::Error {
        match self {
            Failed::RateLimited { retry_after, .. } => RateLimited { method: what, retry_after }.into(),
            Failed::Unavailable { last, elapsed } => anyhow::Error::new(Unavailable(what.to_string()))
                .context(format!("{what}: {last} (still failing after {elapsed:?})")),
            Failed::Fatal(last) => fatal(last),
        }
    }
}

/// The classes a store's operations spend.
#[derive(Debug, Clone, Copy)]
pub struct Classes {
    pub send: Class,
    pub read: Class,
    pub delete: Class,
}

/// What `op` would wait for on `limiter`: a put its send, a read whose
/// location is cached (`cached`) nothing, another read its read, a delete its delete.
pub fn cost(limiter: &ChatLimiter, classes: Classes, op: OpKind, cached: bool) -> CostHint {
    let wait = match op {
        OpKind::Put => limiter.peek_wait(classes.send),
        OpKind::Get if cached => Duration::ZERO,
        OpKind::Get => limiter.peek_wait(classes.read),
        OpKind::Delete => limiter.peek_wait(classes.delete),
    };
    CostHint { wait_secs: wait.as_secs_f64() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChatLimiterConfig, Rate};

    #[test]
    fn failures_map_to_the_stores_error_types() {
        let e = Failed::<String>::RateLimited { retry_after: Duration::from_secs(3), last: "429".into() }
            .into_error("send", |_| unreachable!());
        assert_eq!(rate_limited(&e), Some(Duration::from_secs(3)));
        let e = Failed::Unavailable { last: "reset".to_string(), elapsed: Duration::from_secs(9) }
            .into_error("send", |_| unreachable!());
        assert!(e.downcast_ref::<Unavailable>().is_some() && rate_limited(&e).is_none());
        assert!(format!("{e:#}").contains("reset"), "{e:#}");
        let e = Failed::Fatal("400".to_string()).into_error("send", |last| anyhow::anyhow!("send: {last}"));
        assert_eq!(e.to_string(), "send: 400");
    }

    #[tokio::test(start_paused = true)]
    async fn a_cached_read_costs_nothing() {
        const SEND: Class = Class("send");
        let limiter = ChatLimiter::new(ChatLimiterConfig {
            bot: Rate::new(100.0, 100.0),
            classes: vec![(SEND, Rate::new(1.0, 1.0))],
            upload_concurrency: 1,
            download_concurrency: 1,
        });
        let classes = Classes { send: SEND, read: SEND, delete: SEND };
        limiter.acquire(Spend::Class(SEND)).await;
        assert!(cost(&limiter, classes, OpKind::Put, false).wait_secs > 0.5);
        assert_eq!(cost(&limiter, classes, OpKind::Get, true).wait_secs, 0.0);
    }
}
