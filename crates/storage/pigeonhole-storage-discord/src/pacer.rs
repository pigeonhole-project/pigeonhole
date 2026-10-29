//! Pacing for Discord's thread creation: 50 per 300 s in a fixed window,
//! shared by every channel of the bot (live, docs/discord-log.md). Going
//! over answers 429 with a wait of minutes, which froze the log; so threads
//! are spent at an even pace instead (GCRA): at most `BURST` at once, then
//! one per `INTERVAL`, never more than 50 in any 300 s. When an answer says
//! the window is used up, the pace waits for it to reset.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;

/// 45 per 300 s plus a burst of 5: ≤ 50 in any window of 300 s.
const INTERVAL: Duration = Duration::from_millis(300_000 / 45);
const BURST: u32 = 5;

pub struct Pacer {
    /// Theoretical arrival time of the next request (GCRA).
    tat: Mutex<Option<Instant>>,
    interval: Duration,
    burst: u32,
}

impl Pacer {
    pub fn new(interval: Duration, burst: u32) -> Self {
        Self { tat: Mutex::new(None), interval, burst: burst.max(1) }
    }

    /// Wait for a slot.
    pub async fn wait(&self) {
        let wait = {
            let mut tat = self.tat.lock().unwrap();
            let now = Instant::now();
            let t = tat.map_or(now, |t| t.max(now));
            let tolerance = self.interval * (self.burst - 1);
            let wait = (t.checked_sub(tolerance).unwrap_or(now)).saturating_duration_since(now);
            *tat = Some(t + self.interval);
            wait
        };
        if !wait.is_zero() {
            tracing::debug!(wait_ms = wait.as_millis() as u64, "pacing thread creation");
            tokio::time::sleep(wait).await;
        }
    }

    /// The server's view after a request: none left until `reset_after`.
    pub fn observe(&self, remaining: Option<u64>, reset_after: Option<f64>) {
        if let (Some(0), Some(secs)) = (remaining, reset_after) {
            let until = Instant::now() + Duration::from_secs_f64(secs);
            let mut tat = self.tat.lock().unwrap();
            // The next request goes out once the window resets.
            let floor = until + self.interval * (self.burst - 1);
            *tat = Some(tat.map_or(floor, |t| t.max(floor)));
        }
    }
}

/// The thread pacer of the bot whose application id is `app_id` (one per
/// bot, whichever store or channel asks).
pub fn threads_of(app_id: &str) -> Arc<Pacer> {
    static PACERS: OnceLock<Mutex<HashMap<String, Arc<Pacer>>>> = OnceLock::new();
    let mut pacers = PACERS.get_or_init(Default::default).lock().unwrap();
    pacers.entry(app_id.to_string()).or_insert_with(|| Arc::new(Pacer::new(INTERVAL, BURST))).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn bursts_then_paces_and_never_exceeds_the_window() {
        let p = Pacer::new(INTERVAL, BURST);
        let t0 = Instant::now();
        let mut at = Vec::new();
        for _ in 0..60 {
            p.wait().await;
            at.push(t0.elapsed());
        }
        assert!(at[..BURST as usize].iter().all(|d| d.is_zero()), "the burst goes at once");
        assert!(at[BURST as usize] >= INTERVAL - Duration::from_millis(1));
        for (i, start) in at.iter().enumerate() {
            let in_window = at[i..].iter().filter(|d| **d < *start + Duration::from_secs(300)).count();
            assert!(in_window <= 50, "{in_window} in the window from {start:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_used_up_window_holds_until_it_resets() {
        let p = Pacer::new(INTERVAL, BURST);
        p.observe(Some(0), Some(120.0));
        let t0 = Instant::now();
        p.wait().await;
        assert!(t0.elapsed() >= Duration::from_secs(120));
    }
}
