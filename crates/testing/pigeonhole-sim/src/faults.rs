//! [`Faulty<B>`]: any store with faults injected (docs/fault-injection.md).
//!
//! Faults the store contracts allow: a delay, a failure before the
//! operation did anything, a failure after it did everything (an ambiguous
//! put), a put that lands late after its caller got an error, a rate limit.
//! One contract violation, off unless a plan asks for it: a damaged copy
//! (`FaultPlan::corrupt`), which reads must detect.
//! A run's faults come from its seed (the simulator's, under madsim), each
//! one drawn from `(seed, operation, the operation's counter)`, so the n-th
//! put of a store meets the same fault whatever the interleaving.
//! [`Faulty::quiesce`] turns them off for the checks at the end.

use crate::random_u64;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_blob::{
    After, BackendLimits, BlobBackend, BoxByteStream, ByteRange, Chain, CostHint, InstanceInfo, Linked, MakeAfter,
    OpKind, RateLimited, Sweepable,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often each fault happens (probabilities per operation) and how long
/// delays are.
#[derive(Debug, Clone)]
pub struct FaultPlan {
    pub delay: f64,
    pub max_delay: Duration,
    pub fail_before: f64,
    pub fail_after: f64,
    pub land_late: f64,
    pub late_by: Duration,
    pub rate_limit: f64,
    /// A contract violation, off in the plans above: this share of blobs
    /// reads back with a byte flipped, every time (a damaged copy).
    pub corrupt: f64,
}

impl FaultPlan {
    /// No faults.
    pub fn none() -> Self {
        Self {
            delay: 0.0,
            max_delay: Duration::ZERO,
            fail_before: 0.0,
            fail_after: 0.0,
            land_late: 0.0,
            late_by: Duration::ZERO,
            rate_limit: 0.0,
            corrupt: 0.0,
        }
    }

    /// A few percent of every kind.
    pub fn mild() -> Self {
        Self {
            delay: 0.3,
            max_delay: Duration::from_millis(20),
            fail_before: 0.03,
            fail_after: 0.03,
            land_late: 0.02,
            late_by: Duration::from_millis(200),
            rate_limit: 0.02,
            corrupt: 0.0,
        }
    }
}

impl FaultPlan {
    /// Every fault often: a third of the operations meet one.
    pub fn harsh() -> Self {
        Self {
            delay: 0.5,
            max_delay: Duration::from_millis(50),
            fail_before: 0.08,
            fail_after: 0.08,
            land_late: 0.08,
            late_by: Duration::from_millis(40),
            rate_limit: 0.05,
            corrupt: 0.0,
        }
    }
}

/// What a fault did, for the failure message and for assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    Delay,
    FailBefore,
    FailAfter,
    LandLate,
    RateLimit,
    Corrupt,
}

/// One draw: the fault for an operation, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Draw {
    None,
    Fault(Fault),
}

pub struct Faulty<B> {
    inner: Arc<B>,
    plan: FaultPlan,
    seed: u64,
    counters: [AtomicU64; 8],
    quiet: AtomicBool,
    log: Mutex<Vec<(&'static str, Fault)>>,
}

/// splitmix64: a counter-based draw.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

impl<B> Faulty<B> {
    /// `inner` with faults from `plan`, seeded from the run's seed.
    pub fn new(inner: Arc<B>, plan: FaultPlan) -> Self {
        Self::with_seed(inner, plan, random_u64())
    }

    pub fn with_seed(inner: Arc<B>, plan: FaultPlan, seed: u64) -> Self {
        Self { inner, plan, seed, counters: Default::default(), quiet: AtomicBool::new(false), log: Mutex::default() }
    }

    pub fn inner(&self) -> &Arc<B> {
        &self.inner
    }

    /// No more faults from now on.
    pub fn quiesce(&self) {
        self.quiet.store(true, Ordering::SeqCst);
    }

    /// Every fault injected so far.
    pub fn faults(&self) -> Vec<(&'static str, Fault)> {
        self.log.lock().unwrap().clone()
    }

    fn uniform(&self, op: usize, salt: u64) -> f64 {
        let n = self.counters[op].load(Ordering::SeqCst);
        (mix(self.seed ^ mix((op as u64) << 56 ^ n ^ salt << 40)) >> 11) as f64 / (1u64 << 53) as f64
    }

    /// The fault for the next operation `op` (an index per kind), counted.
    fn draw(&self, op: usize, name: &'static str, allowed: &[Fault]) -> (Draw, Duration) {
        let delay_draw = self.uniform(op, 1);
        let fault_draw = self.uniform(op, 2);
        let len_draw = self.uniform(op, 3);
        self.counters[op].fetch_add(1, Ordering::SeqCst);
        if self.quiet.load(Ordering::SeqCst) {
            return (Draw::None, Duration::ZERO);
        }
        let delay = if delay_draw < self.plan.delay { self.plan.max_delay.mul_f64(len_draw) } else { Duration::ZERO };
        let mut acc = 0.0;
        for &f in allowed {
            acc += match f {
                Fault::Delay => 0.0,
                Fault::FailBefore => self.plan.fail_before,
                Fault::FailAfter => self.plan.fail_after,
                Fault::LandLate => self.plan.land_late,
                Fault::RateLimit => self.plan.rate_limit,
                Fault::Corrupt => 0.0,
            };
            if fault_draw < acc {
                self.log.lock().unwrap().push((name, f));
                return (Draw::Fault(f), delay);
            }
        }
        if !delay.is_zero() {
            self.log.lock().unwrap().push((name, Fault::Delay));
        }
        (Draw::None, delay)
    }

    async fn before(&self, op: usize, name: &'static str, allowed: &[Fault]) -> Result<Draw> {
        let (draw, delay) = self.draw(op, name, allowed);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match draw {
            Draw::Fault(Fault::FailBefore) => Err(anyhow!("injected: {name} failed before doing anything")),
            Draw::Fault(Fault::RateLimit) => {
                Err(RateLimited { method: name, retry_after: Duration::from_millis(100) }.into())
            }
            other => Ok(other),
        }
    }
}

const PUT: usize = 0;
const GET: usize = 1;
const DELETE: usize = 2;
const LIST: usize = 3;
const PUT_AFTER: usize = 4;
const TAIL: usize = 5;
const SUCCESSOR: usize = 6;
const PRUNE: usize = 7;

/// Reads and deletes fail or wait; a failed delete may have deleted.
const READ_FAULTS: &[Fault] = &[Fault::FailBefore, Fault::RateLimit];
/// Writes may also be applied and still fail, or land later.
const WRITE_FAULTS: &[Fault] = &[Fault::FailBefore, Fault::FailAfter, Fault::LandLate, Fault::RateLimit];

#[async_trait]
impl<B: BlobBackend> BlobBackend for Faulty<B> {
    type Id = B::Id;
    type Key = B::Key;

    fn instance(&self) -> &InstanceInfo {
        self.inner.instance()
    }

    fn limits(&self) -> &BackendLimits {
        self.inner.limits()
    }

    fn key(id: &Self::Id) -> Self::Key {
        B::key(id)
    }

    fn cost(&self, op: OpKind, id: Option<&Self::Id>) -> CostHint {
        self.inner.cost(op, id)
    }

    async fn put(&self, data: Bytes, name: &str) -> Result<Self::Id> {
        match self.before(PUT, "put", WRITE_FAULTS).await? {
            Draw::Fault(Fault::FailAfter) => {
                self.inner.put(data, name).await?;
                Err(anyhow!("injected: put applied, then failed"))
            }
            Draw::Fault(Fault::LandLate) => {
                let (inner, name, late_by) = (self.inner.clone(), name.to_string(), self.plan.late_by);
                tokio::spawn(async move {
                    tokio::time::sleep(late_by).await;
                    let _ = inner.put(data, &name).await;
                });
                Err(anyhow!("injected: put failed, lands later"))
            }
            _ => self.inner.put(data, name).await,
        }
    }

    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        self.before(GET, "get", READ_FAULTS).await?;
        let stream = self.inner.get(id, range).await?;
        // The same blob is damaged on every read (a bad copy, not a flake).
        let key = pigeonhole_blob::OrderedKey::to_bytes(&B::key(id));
        let h = key.iter().fold(self.seed, |h, b| mix(h ^ *b as u64));
        if self.plan.corrupt > 0.0 && ((h >> 11) as f64 / (1u64 << 53) as f64) < self.plan.corrupt {
            let mut data = pigeonhole_blob::collect_stream(stream).await?.to_vec();
            let at = (h as usize) % data.len().max(1);
            if let Some(b) = data.get_mut(at) {
                *b ^= 0x5a;
            }
            self.log.lock().unwrap().push(("get", Fault::Corrupt));
            return Ok(pigeonhole_blob::bytes_stream(Bytes::from(data)));
        }
        Ok(stream)
    }

    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        match self.before(DELETE, "delete", &[Fault::FailBefore, Fault::FailAfter, Fault::RateLimit]).await? {
            Draw::Fault(Fault::FailAfter) => {
                self.inner.delete(keys).await?;
                Err(anyhow!("injected: delete applied, then failed"))
            }
            _ => self.inner.delete(keys).await,
        }
    }
}

#[async_trait]
impl<B: Sweepable> Sweepable for Faulty<B> {
    async fn candidates(&self, after: Option<Self::Key>, upto: Self::Key, limit: usize) -> Result<Vec<Self::Key>> {
        self.before(LIST, "candidates", READ_FAULTS).await?;
        self.inner.candidates(after, upto, limit).await
    }
}

#[async_trait]
impl<B: Chain> Chain for Faulty<B> {
    async fn put_after(&self, prev: Option<&Self::Id>, label: &str, data: Bytes) -> Result<After<Self::Id>> {
        match self.before(PUT_AFTER, "put_after", WRITE_FAULTS).await? {
            Draw::Fault(Fault::FailAfter) => {
                self.inner.put_after(prev, label, data).await?;
                Err(anyhow!("injected: put_after applied, then failed"))
            }
            Draw::Fault(Fault::LandLate) => {
                let (inner, prev, label, late_by) = (self.inner.clone(), prev.cloned(), label.to_string(), self.plan.late_by);
                tokio::spawn(async move {
                    tokio::time::sleep(late_by).await;
                    let _ = inner.put_after(prev.as_ref(), &label, data).await;
                });
                Err(anyhow!("injected: put_after failed, lands later"))
            }
            _ => self.inner.put_after(prev, label, data).await,
        }
    }

    async fn tail(&self) -> Result<Option<Linked<Self::Id>>> {
        self.before(TAIL, "tail", READ_FAULTS).await?;
        self.inner.tail().await
    }

    async fn successor(&self, id: &Self::Id) -> Result<Option<Linked<Self::Id>>> {
        self.before(SUCCESSOR, "successor", READ_FAULTS).await?;
        self.inner.successor(id).await
    }

    async fn prune(&self, below: &Self::Id) -> Result<()> {
        self.before(PRUNE, "prune", READ_FAULTS).await?;
        self.inner.prune(below).await
    }

    /// Faults around the store's own (atomic or not) way; no late landing
    /// (`make` borrows the caller's state).
    async fn put_after_tail(&self, label: &str, make: &MakeAfter<'_, Self::Id>) -> Result<Option<Self::Id>> {
        let allowed = [Fault::FailBefore, Fault::FailAfter, Fault::RateLimit];
        match self.before(PUT_AFTER, "put_after_tail", &allowed).await? {
            Draw::Fault(Fault::FailAfter) => {
                self.inner.put_after_tail(label, make).await?;
                Err(anyhow!("injected: put_after_tail applied, then failed"))
            }
            _ => self.inner.put_after_tail(label, make).await,
        }
    }
}
