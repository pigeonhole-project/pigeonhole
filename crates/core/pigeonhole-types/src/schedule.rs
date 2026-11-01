//! A background loop whose knobs change while it runs: how often it passes
//! (`None`: off), what a pass does, and "pass now".
//!
//! A [`Control`] holds the knobs; its loop waits on the [`Schedule`] for the
//! next pass. A change takes effect at once: a shorter period that is already
//! over starts a pass, `None` parks the loop until it is turned back on. The
//! period counts from the end of the previous pass.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::time::Instant;

/// The knobs of an expiry loop: it passes `every` (`None`: only when
/// asked), dropping what was not used for `ttl` (`None`: nothing expires, no
/// passes at all).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry {
    pub every: Option<Duration>,
    pub ttl: Option<Duration>,
}

impl Expiry {
    /// When it passes: never without a TTL.
    pub fn period(&self) -> Option<Duration> {
        self.ttl.and(self.every)
    }
}

/// The knobs of one background loop, and the way to ask it for a pass.
#[derive(Debug)]
pub struct Control<T> {
    tx: watch::Sender<T>,
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    now: Notify,
    /// The end of the last pass (the loop's start before the first).
    since: Mutex<Instant>,
}

impl<T: Clone + PartialEq> Control<T> {
    pub fn new(knobs: T) -> Self {
        let shared = Arc::new(Shared { now: Notify::new(), since: Mutex::new(Instant::now()) });
        Self { tx: watch::channel(knobs).0, shared }
    }

    /// What the loop waits on (one loop per control).
    pub fn schedule(&self) -> Schedule<T> {
        Schedule { rx: self.tx.subscribe(), shared: self.shared.clone(), closed: false }
    }

    /// New knobs; whether they differ from the current ones.
    pub fn set(&self, knobs: T) -> bool {
        self.tx.send_if_modified(|k| {
            let changed = *k != knobs;
            *k = knobs.clone();
            changed
        })
    }

    pub fn get(&self) -> T {
        self.tx.borrow().clone()
    }

    /// Pass once as soon as the loop can, even when it is off.
    pub fn run_now(&self) {
        self.shared.now.notify_one();
    }

    /// When the next pass is due by `period` (`None`: off).
    pub fn due(&self, period: impl Fn(&T) -> Option<Duration>) -> Option<Instant> {
        period(&self.tx.borrow()).map(|p| *self.shared.since.lock().unwrap() + p)
    }
}

/// The loop's side of a [`Control`].
#[derive(Debug)]
pub struct Schedule<T> {
    rx: watch::Receiver<T>,
    shared: Arc<Shared>,
    /// The control is gone: the knobs stay as they are.
    closed: bool,
}

impl<T: Clone + PartialEq> Schedule<T> {
    /// Knobs nothing changes (one-off passes, tests).
    pub fn fixed(knobs: T) -> Self {
        Control::new(knobs).schedule()
    }
}

impl<T: Clone> Schedule<T> {
    pub fn knobs(&self) -> T {
        self.rx.borrow().clone()
    }

    /// Wait until the next pass is due and return its knobs: `period` of
    /// them after the previous pass ended, or at once when asked to.
    pub async fn next(&mut self, period: impl Fn(&T) -> Option<Duration>) -> T {
        loop {
            let due = period(&self.rx.borrow_and_update()).map(|p| *self.shared.since.lock().unwrap() + p);
            let sleep = async move {
                match due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = sleep => break,
                _ = self.shared.now.notified() => break,
                changed = self.rx.changed(), if !self.closed => self.closed = changed.is_err(),
            }
        }
        self.rx.borrow().clone()
    }

    /// The pass just ended: the next period counts from now.
    pub fn done(&self) {
        *self.shared.since.lock().unwrap() = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Duration = Duration::from_secs(1);

    /// Whether `schedule` passes within `within` (virtual time).
    async fn passes(schedule: &mut Schedule<Option<Duration>>, within: Duration) -> bool {
        tokio::time::timeout(within, schedule.next(|p| *p)).await.is_ok()
    }

    #[tokio::test(start_paused = true)]
    async fn passes_every_period_from_the_end_of_the_last() {
        let control = Control::new(Some(10 * S));
        let mut s = control.schedule();
        assert!(!passes(&mut s, 9 * S).await);
        assert!(passes(&mut s, 2 * S).await);
        s.done();
        assert!(!passes(&mut s, 9 * S).await);
        assert!(passes(&mut s, 2 * S).await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_takes_effect_at_once() {
        let control = Control::new(Some(60 * S));
        let mut s = control.schedule();
        assert!(!passes(&mut s, 5 * S).await);
        // 5 s since the start: a 3 s period is over already.
        assert!(control.set(Some(3 * S)));
        assert!(passes(&mut s, Duration::from_millis(1)).await);
        assert!(!control.set(Some(3 * S)), "the same knobs are no change");
    }

    #[tokio::test(start_paused = true)]
    async fn off_parks_until_turned_on_or_asked() {
        let control = Control::new(None);
        let mut s = control.schedule();
        assert!(!passes(&mut s, 3600 * S).await);
        control.run_now();
        assert!(passes(&mut s, Duration::from_millis(1)).await, "a pass asked for runs while off");
        s.done();
        assert!(!passes(&mut s, 3600 * S).await);
        control.set(Some(S));
        assert!(passes(&mut s, 2 * S).await);
        assert_eq!(control.due(|p| *p), Some(*control.shared.since.lock().unwrap() + S));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_knobs_keep_their_period() {
        let mut s = Schedule::fixed(Some(10 * S));
        assert!(!passes(&mut s, 9 * S).await);
        assert!(passes(&mut s, 2 * S).await);
        let mut off = Schedule::<Option<Duration>>::fixed(None);
        assert!(!passes(&mut off, 3600 * S).await);
    }
}
