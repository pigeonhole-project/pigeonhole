//! A rate-limited answer and a retry loop shared by every chat transport
//! (Bot API 429, MTProto `FLOOD_WAIT_X`) and the adapters over them.

use anyhow::anyhow;
use std::future::Future;
use std::time::Duration;

/// The service refused a call for now (HTTP 429, `FLOOD_WAIT_X`): the call
/// was not applied; it may be repeated after `retry_after`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimited {
    pub method: &'static str,
    pub retry_after: Duration,
}

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: Too Many Requests, retry after {:?}", self.method, self.retry_after)
    }
}

impl std::error::Error for RateLimited {}

/// What a failed call may do again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    /// Only a rate limit (the call was not applied), within the budget.
    RateLimits,
    /// Rate limits and any other failure (idempotent calls), within the budget.
    Any,
}

/// Run `call` until it succeeds, retrying as `retry` allows within `budget`
/// (each attempt is bounded by what is left of it).
pub async fn retrying<T, Fut>(
    what: &str,
    retry: Retry,
    budget: Duration,
    mut call: impl FnMut() -> Fut,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = Duration::from_secs(1);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let err = match tokio::time::timeout(left, call()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) => e,
            Err(_) => return Err(anyhow!("{what}: no answer within {budget:?}")),
        };
        let wait = match (err.downcast_ref::<RateLimited>(), retry) {
            (Some(limited), _) => limited.retry_after + Duration::from_secs(1),
            (None, Retry::Any) => {
                let wait = backoff;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                wait
            }
            (None, Retry::RateLimits) => return Err(err.context(what.to_string())),
        };
        if tokio::time::Instant::now() + wait >= deadline {
            return Err(err.context(format!("{what}: still failing after {budget:?}")));
        }
        tracing::warn!(what, wait = ?wait, error = %format!("{err:#}"), "chat call failed; retrying");
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const BUDGET: Duration = Duration::from_secs(300);
    const DEADLINE: Duration = Duration::from_secs(30);

    fn limited() -> anyhow::Error {
        RateLimited { method: "pinChatMessage", retry_after: Duration::from_secs(20) }.into()
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limits_are_waited_out_and_other_errors_follow_the_policy() {
        let tries = AtomicU32::new(0);
        let call = || async {
            match tries.fetch_add(1, Ordering::SeqCst) {
                0 | 1 => Err(limited()),
                _ => Ok(7),
            }
        };
        assert_eq!(retrying("pin", Retry::RateLimits, BUDGET, call).await.unwrap(), 7);
        assert_eq!(tries.load(Ordering::SeqCst), 3);

        // A send may not outlive its deadline: two 20 s waits do not fit in 30 s.
        let tries = AtomicU32::new(0);
        let call = || async {
            tries.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(limited())
        };
        let err = retrying("send", Retry::RateLimits, DEADLINE, call).await.unwrap_err();
        assert!(format!("{err:#}").contains("still failing"), "{err:#}");
        assert_eq!(tries.load(Ordering::SeqCst), 2);

        // A network error: retried only where any retry is allowed.
        let tries = AtomicU32::new(0);
        let call = || async {
            if tries.fetch_add(1, Ordering::SeqCst) == 0 { Err(anyhow!("connection reset")) } else { Ok(()) }
        };
        retrying("getChat", Retry::Any, BUDGET, call).await.unwrap();
        let call = || async { Err::<(), _>(anyhow!("connection reset")) };
        assert!(retrying("setChatDescription", Retry::RateLimits, BUDGET, call).await.is_err());
    }
}
