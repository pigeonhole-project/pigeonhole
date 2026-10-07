//! Multi-instance replication: fan-out writes and failover reads (stage D.2).

use crate::backend::{bytes_stream, BoxByteStream};
use crate::erase::DynBlobBackend;
use crate::part_packer::{EncodedBlock, PartPacker, PartUploaded};
use crate::typed::{BlobLocator, CostHint, OpKind};
use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use pigeonhole_types::{ByteRange, RangeSupport};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::debug;

/// Stable instance id (config name), e.g. `tg-main`.
pub type InstanceId = String;

/// One blob part of a chunk replica.
#[derive(Debug, Clone)]
pub struct PartLayout {
    pub first_block: u32,
    pub block_count: u32,
    pub locator: BlobLocator,
    /// Stored length of each block in this part (enables Range GET).
    pub block_stored_lens: Vec<u32>,
}

impl From<PartUploaded> for PartLayout {
    fn from(p: PartUploaded) -> Self {
        Self {
            first_block: p.first_block,
            block_count: p.block_count,
            locator: p.locator,
            block_stored_lens: p.block_stored_lens,
        }
    }
}

/// Layout of one successful replica (all parts uploaded).
#[derive(Debug, Clone)]
pub struct ReplicaLayout {
    pub instance: InstanceId,
    pub parts: Vec<PartLayout>,
}

/// Chooses replica order for reads.
pub trait ReplicaSelector: Send + Sync {
    /// Return indices into `replicas` ordered best-first.
    fn rank(&self, replicas: &[ReplicaLayout], costs: &[CostHint]) -> Vec<usize>;

    /// Observe a successful op on `instance`.
    fn record_success(&self, instance: &str);

    /// Observe a failure; `http_status` when known (429/5xx).
    fn record_failure(&self, instance: &str, http_status: Option<u16>, timed_out: bool);
}

/// Circuit-breaking cheapest-first selector.
pub struct CheapestFirst {
    inflight_weight: f64,
    close_epsilon: f64,
    inner: Mutex<BreakerState>,
    rng: AtomicU64,
}

struct BreakerState {
    unhealthy: HashMap<String, Unhealthy>,
}

#[derive(Clone, Debug)]
struct Unhealthy {
    failures: u32,
    open_until: u64,
}

impl CheapestFirst {
    pub fn new() -> Self {
        Self {
            inflight_weight: 0.05,
            close_epsilon: 0.05,
            inner: Mutex::new(BreakerState {
                unhealthy: HashMap::new(),
            }),
            rng: AtomicU64::new(0xC0FFEE),
        }
    }

    pub fn with_weights(inflight_weight: f64, close_epsilon: f64) -> Self {
        Self {
            inflight_weight,
            close_epsilon,
            ..Self::new()
        }
    }

    fn tick() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn next_rand(&self) -> u64 {
        let mut x = self.rng.load(Ordering::Relaxed);
        if x == 0 {
            x = 0xDEADBEEF;
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng.store(x, Ordering::Relaxed);
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn is_unhealthy(&self, instance: &str) -> bool {
        let g = self.inner.lock().unwrap();
        match g.unhealthy.get(instance) {
            Some(u) if u.open_until > Self::tick() => true,
            _ => false,
        }
    }
}

impl Default for CheapestFirst {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaSelector for CheapestFirst {
    fn rank(&self, replicas: &[ReplicaLayout], costs: &[CostHint]) -> Vec<usize> {
        debug_assert_eq!(replicas.len(), costs.len());
        let mut idxs: Vec<usize> = (0..replicas.len()).collect();
        let weight = self.inflight_weight;
        let eps = self.close_epsilon;
        idxs.sort_by(|&a, &b| {
            let ua = self.is_unhealthy(&replicas[a].instance);
            let ub = self.is_unhealthy(&replicas[b].instance);
            match (ua, ub) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => {
                    let sa = costs[a].score(weight);
                    let sb = costs[b].score(weight);
                    if (sa - sb).abs() <= eps {
                        let ra = self.next_rand();
                        let rb = self.next_rand();
                        ra.cmp(&rb)
                    } else {
                        sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
                    }
                }
            }
        });
        idxs
    }

    fn record_success(&self, instance: &str) {
        let mut g = self.inner.lock().unwrap();
        g.unhealthy.remove(instance);
    }

    fn record_failure(&self, instance: &str, http_status: Option<u16>, timed_out: bool) {
        let trip = timed_out
            || http_status == Some(429)
            || http_status.is_some_and(|s| (500..600).contains(&s));
        if !trip {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let entry = g
            .unhealthy
            .entry(instance.to_string())
            .or_insert(Unhealthy {
                failures: 0,
                open_until: 0,
            });
        entry.failures = entry.failures.saturating_add(1);
        let backoff_ms = 1000u64
            .saturating_mul(1u64 << entry.failures.min(5).saturating_sub(1))
            .min(30_000);
        entry.open_until = Self::tick().saturating_add(backoff_ms);
    }
}

/// Fan-out write / failover read across a placement group.
pub struct Replicated {
    members: Vec<Arc<dyn DynBlobBackend>>,
    by_id: HashMap<InstanceId, Arc<dyn DynBlobBackend>>,
    write_quorum: usize,
    selector: Arc<dyn ReplicaSelector>,
    member_timeout: Duration,
}

impl Replicated {
    pub fn new(
        members: Vec<Arc<dyn DynBlobBackend>>,
        write_quorum: usize,
        selector: Arc<dyn ReplicaSelector>,
    ) -> Result<Self> {
        if members.is_empty() {
            bail!("Replicated requires at least one member");
        }
        if write_quorum == 0 || write_quorum > members.len() {
            bail!(
                "write_quorum {write_quorum} out of range for {} members",
                members.len()
            );
        }
        let mut by_id = HashMap::new();
        for m in &members {
            let id = m.instance().id.clone();
            if by_id.insert(id.clone(), m.clone()).is_some() {
                bail!("duplicate Replicated member instance id {id:?}");
            }
        }
        Ok(Self {
            members,
            by_id,
            write_quorum,
            selector,
            member_timeout: Duration::from_secs(60),
        })
    }

    pub fn with_member_timeout(mut self, timeout: Duration) -> Self {
        self.member_timeout = timeout;
        self
    }

    pub fn members(&self) -> &[Arc<dyn DynBlobBackend>] {
        &self.members
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn selector(&self) -> &Arc<dyn ReplicaSelector> {
        &self.selector
    }

    pub fn max_blob_size(&self) -> usize {
        self.members
            .iter()
            .map(|m| m.limits().max_blob_size)
            .min()
            .unwrap_or(0)
    }

    pub fn chunk_writer(&self) -> ChunkReplicaWriter {
        ChunkReplicaWriter {
            writers: self
                .members
                .iter()
                .map(|m| MemberWriter {
                    instance: m.instance().id.clone(),
                    packer: Some(PartPacker::new(m.clone())),
                    parts: Vec::new(),
                    failed: false,
                })
                .collect(),
            write_quorum: self.write_quorum,
            timeout: self.member_timeout,
        }
    }

    /// Stream stored bytes for `blocks` (half-open block index range), with failover.
    pub async fn read(
        &self,
        replicas: &[ReplicaLayout],
        blocks: Range<u32>,
    ) -> Result<BoxByteStream> {
        if blocks.start >= blocks.end {
            return Ok(bytes_stream(Bytes::new()));
        }
        if replicas.is_empty() {
            bail!("read requires at least one replica layout");
        }

        let costs: Vec<CostHint> = replicas
            .iter()
            .map(|r| {
                self.by_id
                    .get(&r.instance)
                    .map(|b| b.cost(OpKind::Get, None))
                    .unwrap_or_else(CostHint::free)
            })
            .collect();
        let order = self.selector.rank(replicas, &costs);

        let state = ReadState {
            backends: self.by_id.clone(),
            selector: self.selector.clone(),
            timeout: self.member_timeout,
            replicas: replicas.to_vec(),
            order,
            tried: Vec::new(),
            reranked: false,
            next_block: blocks.start,
            end_block: blocks.end,
            active: None,
        };
        Ok(Box::pin(stream::unfold(state, |mut st| async move {
            match st.pull().await {
                Ok(None) => None,
                Ok(Some(bytes)) => Some((Ok(bytes), st)),
                Err(e) => Some((Err(e), st)),
            }
        })))
    }
}

struct MemberWriter {
    instance: InstanceId,
    /// `None` while the packer is borrowed by an in-flight task.
    packer: Option<PartPacker>,
    parts: Vec<PartLayout>,
    failed: bool,
}

/// Parallel per-member packers for one chunk.
pub struct ChunkReplicaWriter {
    writers: Vec<MemberWriter>,
    write_quorum: usize,
    timeout: Duration,
}

impl ChunkReplicaWriter {
    /// Fan-out one encoded block to all live members (`Bytes` clone is refcounted).
    pub async fn push(&mut self, block: EncodedBlock) -> Result<()> {
        let timeout = self.timeout;
        let mut tasks = Vec::new();
        for (i, w) in self.writers.iter_mut().enumerate() {
            if w.failed {
                continue;
            }
            let Some(mut packer) = w.packer.take() else {
                continue;
            };
            let blk = EncodedBlock {
                stored: block.stored.clone(),
                logical_len: block.logical_len,
                codec: block.codec.clone(),
            };
            tasks.push(async move {
                let result = tokio::time::timeout(timeout, packer.push(blk)).await;
                (i, packer, result)
            });
        }

        for (i, packer, result) in futures::future::join_all(tasks).await {
            let w = &mut self.writers[i];
            w.packer = Some(packer);
            match result {
                Ok(Ok(Some(part))) => w.parts.push(part.into()),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    debug!(instance = %w.instance, error = %e, "member push failed");
                    w.failed = true;
                }
                Err(_) => {
                    debug!(instance = %w.instance, "member push timed out");
                    w.failed = true;
                }
            }
        }
        Ok(())
    }

    /// Flush remaining parts; return layouts only for members that uploaded all parts.
    pub async fn finish(mut self) -> Result<Vec<ReplicaLayout>> {
        let timeout = self.timeout;
        let mut tasks = Vec::new();
        for (i, w) in self.writers.iter_mut().enumerate() {
            if w.failed {
                continue;
            }
            let Some(packer) = w.packer.take() else {
                continue;
            };
            tasks.push(async move {
                let result = tokio::time::timeout(timeout, packer.finish()).await;
                (i, result)
            });
        }

        for (i, result) in futures::future::join_all(tasks).await {
            let w = &mut self.writers[i];
            match result {
                Ok(Ok(Some(part))) => w.parts.push(part.into()),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    debug!(instance = %w.instance, error = %e, "member finish failed");
                    w.failed = true;
                }
                Err(_) => {
                    debug!(instance = %w.instance, "member finish timed out");
                    w.failed = true;
                }
            }
        }

        let mut ok = Vec::new();
        for w in self.writers {
            if !w.failed {
                ok.push(ReplicaLayout {
                    instance: w.instance,
                    parts: w.parts,
                });
            }
        }
        if ok.len() < self.write_quorum {
            bail!(
                "write quorum not met: {} successful replicas, need {}",
                ok.len(),
                self.write_quorum
            );
        }
        Ok(ok)
    }
}

// --- failover read ---

struct ReadState {
    backends: HashMap<InstanceId, Arc<dyn DynBlobBackend>>,
    selector: Arc<dyn ReplicaSelector>,
    timeout: Duration,
    replicas: Vec<ReplicaLayout>,
    order: Vec<usize>,
    tried: Vec<usize>,
    /// One re-rank after a full failed pass (circuit half-open), then give up.
    reranked: bool,
    next_block: u32,
    end_block: u32,
    active: Option<ActivePart>,
}

struct ActivePart {
    instance: InstanceId,
    /// Absolute block range this download covers.
    blocks_left: Range<u32>,
    stream: BoxByteStream,
    /// Expected stored length for `blocks_left` (`u64::MAX` = read until EOF).
    expected_len: u64,
}

impl ReadState {
    async fn pull(&mut self) -> Result<Option<Bytes>> {
        loop {
            if self.next_block >= self.end_block {
                return Ok(None);
            }

            if self.active.is_none() && !self.open_next_part().await? {
                bail!(
                    "no healthy replica for blocks {}..{}",
                    self.next_block,
                    self.end_block
                );
            }

            // Buffer a whole part segment before yielding so a mid-stream failure
            // can restart from `next_block` without duplicating bytes to the caller.
            let mut active = self.active.take().expect("active part");
            let mut buf = BytesMut::new();
            let mut failed = false;
            loop {
                let done = active.expected_len != u64::MAX
                    && (buf.len() as u64) >= active.expected_len;
                if done {
                    break;
                }
                match tokio::time::timeout(self.timeout, active.stream.next()).await {
                    Ok(Some(Ok(chunk))) => {
                        if !chunk.is_empty() {
                            buf.extend_from_slice(&chunk);
                        }
                    }
                    Ok(Some(Err(e))) => {
                        let (status, timed_out) = classify_err(&e);
                        self.selector
                            .record_failure(&active.instance, status, timed_out);
                        debug!(
                            instance = %active.instance,
                            next_block = self.next_block,
                            error = %e,
                            "replica stream failed; failing over"
                        );
                        failed = true;
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        self.selector
                            .record_failure(&active.instance, None, true);
                        failed = true;
                        break;
                    }
                }
            }

            if failed {
                // Keep next_block; try another replica (including mid-stream resume).
                continue;
            }

            if active.expected_len != u64::MAX && (buf.len() as u64) < active.expected_len {
                self.selector
                    .record_failure(&active.instance, Some(500), false);
                continue;
            }

            if active.expected_len != u64::MAX && (buf.len() as u64) > active.expected_len {
                buf.truncate(active.expected_len as usize);
            }

            self.next_block = active.blocks_left.end;
            self.selector.record_success(&active.instance);
            self.tried.clear();
            self.reranked = false;
            if buf.is_empty() {
                continue;
            }
            return Ok(Some(buf.freeze()));
        }
    }

    async fn open_next_part(&mut self) -> Result<bool> {
        if self.tried.len() >= self.order.len() {
            if self.reranked {
                return Ok(false);
            }
            let costs: Vec<CostHint> = self
                .replicas
                .iter()
                .map(|r| {
                    self.backends
                        .get(&r.instance)
                        .map(|b| b.cost(OpKind::Get, None))
                        .unwrap_or_else(CostHint::free)
                })
                .collect();
            self.order = self.selector.rank(&self.replicas, &costs);
            self.tried.clear();
            self.reranked = true;
        }

        for &ri in &self.order.clone() {
            if self.tried.contains(&ri) {
                continue;
            }
            self.tried.push(ri);
            let layout = &self.replicas[ri];
            let Some(backend) = self.backends.get(&layout.instance) else {
                continue;
            };
            let Some((part, byte_range, block_range, expected_len)) =
                plan_part_read(layout, self.next_block, self.end_block)
            else {
                continue;
            };

            let range = match backend.limits().supports_range {
                RangeSupport::None => None,
                RangeSupport::BestEffort => byte_range,
            };

            let get = backend.get(&part.locator, range);
            match tokio::time::timeout(self.timeout, get).await {
                Ok(Ok(stream)) => {
                    self.active = Some(ActivePart {
                        instance: layout.instance.clone(),
                        blocks_left: block_range,
                        stream,
                        expected_len,
                    });
                    return Ok(true);
                }
                Ok(Err(e)) => {
                    let (status, timed_out) = classify_err(&e);
                    self.selector
                        .record_failure(&layout.instance, status, timed_out);
                    debug!(
                        instance = %layout.instance,
                        error = %e,
                        "replica get failed"
                    );
                }
                Err(_) => {
                    self.selector
                        .record_failure(&layout.instance, None, true);
                    debug!(instance = %layout.instance, "replica get timed out");
                }
            }
        }
        Ok(false)
    }
}

/// Plan a GET covering `[next, end)` starting at `next` within one part.
fn plan_part_read(
    layout: &ReplicaLayout,
    next: u32,
    end: u32,
) -> Option<(&PartLayout, Option<ByteRange>, Range<u32>, u64)> {
    for part in &layout.parts {
        let part_end = part.first_block + part.block_count;
        if next >= part_end || end <= part.first_block {
            continue;
        }
        if next < part.first_block {
            // Gap / misaligned metadata.
            return None;
        }
        let from = next;
        let to = end.min(part_end);
        let local_from = (from - part.first_block) as usize;
        let local_to = (to - part.first_block) as usize;
        if part.block_stored_lens.len() < local_to {
            // Missing lens: download whole part from block 0.
            let total: u64 = part.block_stored_lens.iter().map(|&l| l as u64).sum();
            let total = if total == 0 {
                // Unknown size — stream until EOF; expected_len = u64::MAX sentinel handled by EOF.
                u64::MAX
            } else {
                total
            };
            return Some((part, None, part.first_block..part_end, total));
        }
        let mut off = 0u64;
        for len in &part.block_stored_lens[..local_from] {
            off += u64::from(*len);
        }
        let mut need = 0u64;
        for len in &part.block_stored_lens[local_from..local_to] {
            need += u64::from(*len);
        }
        let range = Some(off..(off + need));
        return Some((part, range, from..to, need));
    }
    None
}

fn classify_err(e: &anyhow::Error) -> (Option<u16>, bool) {
    let msg = format!("{e:#}").to_ascii_lowercase();
    if msg.contains("timed out") || msg.contains("timeout") {
        return (None, true);
    }
    if msg.contains("429") {
        return (Some(429), false);
    }
    for code in [500u16, 502, 503, 504] {
        if msg.contains(&code.to_string()) {
            return (Some(code), false);
        }
    }
    (None, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erase::{erase, SharedBackend};
    use crate::typed::{
        CostHint, InstanceInfo, InstanceKind, InstanceRole, OpKind, BlobBackend,
    };
    use crate::{collect_stream, slice_range};
    use async_trait::async_trait;
    use pigeonhole_types::BackendLimits;
    use std::sync::atomic::AtomicU32;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Id {
        key: u64,
    }

    struct MockMem {
        info: InstanceInfo,
        limits: BackendLimits,
        next: AtomicU64,
        data: StdMutex<HashMap<u64, Bytes>>,
        /// Fail the N-th get after emitting `fail_after_bytes`.
        fail_get_after: StdMutex<Option<(u32, usize)>>,
        get_count: AtomicU32,
        put_fail_after: StdMutex<Option<u32>>,
        put_count: AtomicU32,
        unavailable: std::sync::atomic::AtomicBool,
        cost_wait: std::sync::atomic::AtomicU64, // wait_secs * 1000 as int
    }

    impl MockMem {
        fn new(id: &str, max_blob_size: usize) -> Arc<Self> {
            Arc::new(Self {
                info: InstanceInfo {
                    id: id.into(),
                    kind: InstanceKind::Memory,
                    fingerprint: format!("memory:{id}"),
                    location: format!("memory:{id}"),
                    role: InstanceRole::ReadWrite,
                },
                limits: BackendLimits {
                    max_blob_size,
                    supports_range: RangeSupport::BestEffort,
                    can_list: false,
                },
                next: AtomicU64::new(1),
                data: StdMutex::new(HashMap::new()),
                fail_get_after: StdMutex::new(None),
                get_count: AtomicU32::new(0),
                put_fail_after: StdMutex::new(None),
                put_count: AtomicU32::new(0),
                unavailable: std::sync::atomic::AtomicBool::new(false),
                cost_wait: std::sync::atomic::AtomicU64::new(0),
            })
        }

        fn erase(self: &Arc<Self>) -> SharedBackend {
            Arc::new(erase(Arc::clone(self)))
        }

        fn set_cost_wait(self: &Arc<Self>, secs: f64) {
            self.cost_wait
                .store((secs * 1000.0) as u64, Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl BlobBackend for Arc<MockMem> {
        type Id = Id;
        type Key = u64;
        fn instance(&self) -> &InstanceInfo {
            &self.info
        }
        fn limits(&self) -> &BackendLimits {
            &self.limits
        }
        fn key(id: &Self::Id) -> Self::Key {
            id.key
        }
        fn cost(&self, _: OpKind, _: Option<&Self::Id>) -> CostHint {
            CostHint {
                wait_secs: self.cost_wait.load(Ordering::Relaxed) as f64 / 1000.0,
                latency_ewma_secs: 0.0,
                inflight: 0,
            }
        }
        async fn put(&self, data: Bytes) -> Result<Self::Id> {
            if self.unavailable.load(Ordering::Relaxed) {
                bail!("503 service unavailable");
            }
            let n = self.put_count.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(after) = *self.put_fail_after.lock().unwrap() {
                if n > after {
                    bail!("503 put failed");
                }
            }
            if data.len() > self.limits.max_blob_size {
                bail!("oversize");
            }
            let key = self.next.fetch_add(1, Ordering::Relaxed);
            self.data.lock().unwrap().insert(key, data);
            Ok(Id { key })
        }
        async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
            if self.unavailable.load(Ordering::Relaxed) {
                bail!("503 service unavailable");
            }
            let data = self
                .data
                .lock()
                .unwrap()
                .get(&id.key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing"))?;
            let sliced = slice_range(data, range)?;
            let n = self.get_count.fetch_add(1, Ordering::Relaxed) + 1;
            let fail = *self.fail_get_after.lock().unwrap();
            if let Some((after_get, after_bytes)) = fail {
                if n == after_get {
                    let n = after_bytes.min(sliced.len());
                    let head = sliced.slice(0..n);
                    return Ok(Box::pin(stream::iter([
                        Ok(head),
                        Err(anyhow::anyhow!("503 mid-stream break")),
                    ])));
                }
            }
            Ok(bytes_stream(sliced))
        }
        async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
            let mut g = self.data.lock().unwrap();
            for k in keys {
                g.remove(k);
            }
            Ok(())
        }
    }

    fn block(fill: u8, n: usize) -> EncodedBlock {
        EncodedBlock {
            stored: Bytes::from(vec![fill; n]),
            logical_len: n as u32,
            codec: "raw".into(),
        }
    }

    async fn write_blocks(
        rep: &Replicated,
        blocks: &[EncodedBlock],
    ) -> Result<Vec<ReplicaLayout>> {
        let mut w = rep.chunk_writer();
        for b in blocks {
            w.push(b.clone()).await?;
        }
        w.finish().await
    }

    #[tokio::test]
    async fn different_limits_different_part_counts() {
        const MIB: usize = 1024 * 1024;
        let a = MockMem::new("a-19", 19 * MIB);
        let b = MockMem::new("b-10", 10 * MIB);
        let rep = Replicated::new(
            vec![a.erase(), b.erase()],
            2,
            Arc::new(CheapestFirst::new()),
        )
        .unwrap();

        let blocks = vec![
            block(1, 6 * MIB),
            block(2, 6 * MIB),
            block(3, 6 * MIB),
        ];
        let layouts = write_blocks(&rep, &blocks).await.unwrap();
        assert_eq!(layouts.len(), 2);
        let la = layouts.iter().find(|l| l.instance == "a-19").unwrap();
        let lb = layouts.iter().find(|l| l.instance == "b-10").unwrap();
        assert_eq!(la.parts.len(), 1, "19 MiB should pack 3×6 MiB into one part");
        assert_eq!(lb.parts.len(), 3, "10 MiB should need one part per block");

        let expect: Bytes = blocks.iter().flat_map(|b| b.stored.iter().copied()).collect();
        for layout in &layouts {
            let got = collect_stream(rep.read(std::slice::from_ref(layout), 0..3).await.unwrap())
                .await
                .unwrap();
            assert_eq!(got, expect, "mismatch on {}", layout.instance);
        }
        let got_both = collect_stream(rep.read(&layouts, 0..3).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got_both, expect);
    }

    #[tokio::test]
    async fn unavailable_replica_failover() {
        let a = MockMem::new("a", 1024);
        let b = MockMem::new("b", 1024);
        let rep = Replicated::new(
            vec![a.erase(), b.erase()],
            2,
            Arc::new(CheapestFirst::new()),
        )
        .unwrap();
        let blocks = vec![block(9, 100), block(8, 100)];
        let layouts = write_blocks(&rep, &blocks).await.unwrap();
        a.unavailable.store(true, Ordering::Relaxed);

        let expect: Bytes = blocks.iter().flat_map(|b| b.stored.iter().copied()).collect();
        let got = collect_stream(rep.read(&layouts, 0..2).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got, expect);
    }

    #[tokio::test]
    async fn mid_stream_break_continues() {
        let a = MockMem::new("a", 10_000);
        let b = MockMem::new("b", 10_000);
        a.set_cost_wait(0.0);
        b.set_cost_wait(10.0); // prefer a
        let rep = Replicated::new(
            vec![a.erase(), b.erase()],
            2,
            Arc::new(CheapestFirst::new()),
        )
        .unwrap();
        let blocks = vec![block(1, 500), block(2, 500)];
        let layouts = write_blocks(&rep, &blocks).await.unwrap();

        *a.fail_get_after.lock().unwrap() = Some((1, 100));

        let expect: Bytes = blocks.iter().flat_map(|b| b.stored.iter().copied()).collect();
        let got = collect_stream(rep.read(&layouts, 0..2).await.unwrap())
            .await
            .unwrap();
        assert_eq!(got, expect);
    }

    #[tokio::test]
    async fn partial_member_absent_from_finish() {
        let a = MockMem::new("a", 10_000);
        let b = MockMem::new("b", 10_000);
        *b.put_fail_after.lock().unwrap() = Some(1); // fail after first put
        let rep = Replicated::new(
            vec![a.erase(), b.erase()],
            1, // quorum 1 — a alone is enough
            Arc::new(CheapestFirst::new()),
        )
        .unwrap();
        // Two blocks that each seal their own part on both (limit 10k, block 6k).
        let mut w = rep.chunk_writer();
        w.push(block(1, 6000)).await.unwrap();
        w.push(block(2, 6000)).await.unwrap(); // b fails on 2nd put
        let layouts = w.finish().await.unwrap();
        assert_eq!(layouts.len(), 1);
        assert_eq!(layouts[0].instance, "a");
    }

    #[tokio::test]
    async fn quorum_error_when_too_few_replicas() {
        let a = MockMem::new("a", 10_000);
        let b = MockMem::new("b", 10_000);
        b.unavailable.store(true, Ordering::Relaxed);
        let rep = Replicated::new(
            vec![a.erase(), b.erase()],
            2,
            Arc::new(CheapestFirst::new()),
        )
        .unwrap();
        let mut w = rep.chunk_writer();
        w.push(block(1, 100)).await.unwrap();
        let err = w.finish().await.unwrap_err();
        assert!(
            err.to_string().contains("write quorum"),
            "{err}"
        );
    }
}
