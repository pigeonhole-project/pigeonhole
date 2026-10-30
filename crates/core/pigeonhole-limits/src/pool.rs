//! Several bots for one chat (docs/shared-limiter.md, § Choosing the
//! account: a pool). An operation runs on the member whose budget for it is
//! ready soonest. When it comes back rate limited (not applied: the member
//! gave way, see [`Retry::give_way`](crate::Retry::give_way)), it moves to
//! another member if one is ready before the wait is over, and else waits
//! it out and runs on the same member again.

use crate::{ChatLimiter, Spend};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// The bots of one chat, each with its limiter. Member 0 is the primary.
pub struct Pool {
    /// The chat / channel id, a metrics label.
    chat: String,
    members: Vec<Arc<ChatLimiter>>,
    /// When each member was last picked (a tick), so that ties spread.
    last_picked: Mutex<Vec<u64>>,
    ticks: AtomicU64,
}

impl Pool {
    /// `members` (at least one), the primary first.
    pub fn new(chat: &str, members: Vec<Arc<ChatLimiter>>) -> Self {
        assert!(!members.is_empty(), "a pool of no bots");
        let n = members.len();
        Self { chat: chat.to_owned(), members, last_picked: Mutex::new(vec![0; n]), ticks: AtomicU64::new(0) }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn member(&self, i: usize) -> &Arc<ChatLimiter> {
        &self.members[i]
    }

    /// The member whose `spend` is ready soonest (ties: the one picked least
    /// recently), other than `except`, and how soon. Made once: a call that
    /// waits for it does not switch when another frees up.
    fn pick(&self, spend: Spend, except: Option<usize>) -> Option<(usize, Duration)> {
        let last = self.last_picked.lock().unwrap();
        (0..self.members.len())
            .filter(|&i| Some(i) != except)
            .map(|i| (i, self.members[i].peek(spend)))
            .min_by_key(|&(i, wait)| (wait, last[i]))
    }

    fn picked(&self, i: usize) {
        self.last_picked.lock().unwrap()[i] = self.ticks.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::counter!("pigeonhole_pool_calls_total", "chat" => self.chat.clone(), "member" => i.to_string())
            .increment(1);
    }

    /// Run `op` on a member until it succeeds, fails for good, or stays rate
    /// limited past `deadline`. `rate_limited` tells an error that was a rate
    /// limit (the operation was not applied) and its wait; any other error
    /// ends the operation.
    pub async fn run<T, E, Fut>(
        &self,
        spend: Spend,
        deadline: Duration,
        rate_limited: impl Fn(&E) -> Option<Duration>,
        mut op: impl FnMut(usize) -> Fut,
    ) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, E>>,
    {
        let give_up = Instant::now() + deadline;
        let (mut member, _) = self.pick(spend, None).expect("a pool has members");
        loop {
            self.picked(member);
            let err = match op(member).await {
                Ok(v) => return Ok(v),
                Err(e) => e,
            };
            let Some(after) = rate_limited(&err) else { return Err(err) };
            if let Some((other, ready)) = self.pick(spend, Some(member)) {
                if ready < after {
                    metrics::counter!("pigeonhole_pool_moves_total", "chat" => self.chat.clone()).increment(1);
                    tracing::debug!(chat = %self.chat, from = member, to = other, ?after, "rate limited: moving to another bot");
                    member = other;
                    continue;
                }
            }
            if Instant::now() + after >= give_up {
                return Err(err);
            }
            tokio::time::sleep(after).await;
        }
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("chat", &self.chat).field("members", &self.members.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChatLimiterConfig, Class, Level, LimiterRegistry, Rate};

    const SEND: Class = Class("send");

    fn registry() -> LimiterRegistry {
        LimiterRegistry::new(ChatLimiterConfig {
            bot: Rate::new(1000.0, 1000.0),
            classes: vec![(SEND, Rate::new(0.3, 3.0))],
            upload_concurrency: 1,
            download_concurrency: 1,
        })
    }

    /// A member's answer: `Err(secs)` = rate limited for that long.
    async fn attempt(pool: &Pool, member: usize, flood: Option<u64>) -> Result<usize, u64> {
        pool.member(member).acquire(Spend::Class(SEND)).await;
        match flood {
            Some(secs) => {
                pool.member(member).cool(Spend::Class(SEND), Level::Class, Duration::from_secs(secs));
                Err(secs)
            }
            None => Ok(member),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_operation_moves_to_a_free_bot() {
        let reg = registry();
        let pool = Pool::new("-1", vec![reg.chat("a", "-1"), reg.chat("b", "-1")]);
        let t0 = Instant::now();
        let ran_on = pool
            .run(Spend::Class(SEND), Duration::from_secs(300), |e: &u64| Some(Duration::from_secs(*e)), |m| {
                let flood = (m == 0).then_some(40);
                attempt(&pool, m, flood)
            })
            .await
            .unwrap();
        assert_eq!(ran_on, 1, "the second bot ran it");
        assert!(t0.elapsed() < Duration::from_secs(1), "without waiting out the 40 s: {:?}", t0.elapsed());
        assert!(pool.member(0).peek_wait(SEND) > Duration::from_secs(30), "the first bot stays cooled");
        // The next operation picks the free bot without trying the cooled one.
        let next = pool
            .run(Spend::Class(SEND), Duration::from_secs(300), |e: &u64| Some(Duration::from_secs(*e)), |m| attempt(&pool, m, None))
            .await
            .unwrap();
        assert_eq!(next, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn with_every_bot_cooled_longer_it_waits_on_the_same_one() {
        let reg = registry();
        let pool = Pool::new("-1", vec![reg.chat("a", "-1"), reg.chat("b", "-1")]);
        pool.member(1).cool(Spend::Class(SEND), Level::Class, Duration::from_secs(100));
        let mut tries = Vec::new();
        let t0 = Instant::now();
        let ran_on = pool
            .run(Spend::Class(SEND), Duration::from_secs(300), |e: &u64| Some(Duration::from_secs(*e)), |m| {
                tries.push(m);
                let flood = (tries.len() == 1).then_some(10);
                attempt(&pool, m, flood)
            })
            .await
            .unwrap();
        assert_eq!((ran_on, tries), (0, vec![0, 0]), "b is cooled for longer than a's 10 s");
        assert!(t0.elapsed() >= Duration::from_secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn ties_spread_over_the_members() {
        let reg = LimiterRegistry::new(ChatLimiterConfig {
            bot: Rate::new(1000.0, 1000.0),
            classes: vec![(SEND, Rate::new(1000.0, 1000.0))],
            upload_concurrency: 1,
            download_concurrency: 1,
        });
        let pool = Pool::new("-1", vec![reg.chat("a", "-1"), reg.chat("b", "-1"), reg.chat("c", "-1")]);
        let mut ran = Vec::new();
        for _ in 0..6 {
            ran.push(pool.run(Spend::Class(SEND), Duration::from_secs(1), |_: &u64| None, |m| async move { Ok::<_, u64>(m) }).await.unwrap());
        }
        assert_eq!(ran, vec![0, 1, 2, 0, 1, 2]);
    }
}
