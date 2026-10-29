//! Scenarios that run under the madsim simulator (`RUSTFLAGS="--cfg madsim"`,
//! docs/simulation.md) and, as plain tests, on tokio. Under the simulator a
//! seed fixes the whole run: task order, virtual time, randomness;
//! `MADSIM_TEST_SEED` replays one, `MADSIM_TEST_NUM` runs many,
//! `MADSIM_TEST_CHECK_DETERMINISM=1` runs each twice and compares.

use pigeonhole_meta::{Outcome, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A random number from the simulator (or a fixed sequence on tokio).
pub fn random_u64() -> u64 {
    #[cfg(madsim)]
    {
        use madsim::rand::Rng;
        madsim::rand::thread_rng().gen()
    }
    #[cfg(not(madsim))]
    {
        use std::sync::atomic::{AtomicU64, Ordering};
        static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
        let mut x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        x ^ (x >> 33)
    }
}

/// A random duration in `[0, max_ms)` milliseconds.
pub fn random_ms(max_ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(random_u64() % max_ms.max(1))
}

/// A toy namespace: every committed command id with its writer.
#[derive(Default, Serialize, Deserialize)]
pub struct Journal {
    pub entries: BTreeMap<u64, String>,
}

#[derive(Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
    pub writer: String,
}

impl StateMachine for Journal {
    const NAME: &'static str = "journal";
    const VERSION: u32 = 1;
    type Command = Record;
    type Reply = ();
    type Reject = pigeonhole_meta::Never;

    fn apply(&mut self, r: Record) -> anyhow::Result<Outcome<Self>> {
        self.entries.insert(r.id, r.writer);
        Ok(Ok(()))
    }
}
