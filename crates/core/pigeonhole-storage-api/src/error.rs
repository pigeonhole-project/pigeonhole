//! Failures a store reports by type, so the layers above act on them without
//! reading messages: a missing blob is repaired from another replica, a rate
//! limit or an outage routes reads around the store for a while. Any other
//! error is just a failure.

use std::time::Duration;

/// The blob is not in the store (deleted, or never there).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFound(pub String);

impl std::fmt::Display for NotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: not found", self.0)
    }
}

impl std::error::Error for NotFound {}

/// The store cannot serve for now (a 5xx, the connection is down); the same
/// call may work later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unavailable(pub String);

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: store unavailable", self.0)
    }
}

impl std::error::Error for Unavailable {}

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

/// What kind of failure an error is, by the types above anywhere in it: as
/// its source, or as context a store put around the cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    NotFound,
    RateLimited,
    Unavailable,
    /// The caller gave up waiting for an answer.
    Timeout,
    Other,
}

impl Failure {
    pub fn of(e: &anyhow::Error) -> Self {
        fn has<T: std::error::Error + Send + Sync + 'static>(e: &anyhow::Error) -> bool {
            // `downcast_ref` finds a context layer of type `T` as well;
            // `chain` finds `T` as the source of a foreign error.
            e.downcast_ref::<T>().is_some() || e.chain().any(|c| c.is::<T>())
        }
        if has::<NotFound>(e) {
            Failure::NotFound
        } else if has::<RateLimited>(e) {
            Failure::RateLimited
        } else if has::<Unavailable>(e) {
            Failure::Unavailable
        } else {
            Failure::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[test]
    fn kinds_survive_context_and_messages_do_not_count() {
        let gone: anyhow::Result<()> = Err(NotFound("blob 7".into()).into());
        let e = gone.context("read chunk 3").unwrap_err();
        assert_eq!(Failure::of(&e), Failure::NotFound);
        let e = anyhow::Error::new(Unavailable("get".into())).context("replica a");
        assert_eq!(Failure::of(&e), Failure::Unavailable);
        let e: anyhow::Error = RateLimited { method: "send", retry_after: Duration::from_secs(1) }.into();
        assert_eq!(Failure::of(&e), Failure::RateLimited);
        // A store marks a foreign cause by context.
        let e = anyhow::anyhow!("connection reset").context(Unavailable("send".into())).context("put part");
        assert_eq!(Failure::of(&e), Failure::Unavailable);
        // Words and numbers in a message are not a kind.
        for msg in ["blob 404 not found", "503 service unavailable", "read 1500 bytes: missing"] {
            assert_eq!(Failure::of(&anyhow::anyhow!("{msg}")), Failure::Other, "{msg}");
        }
    }
}
