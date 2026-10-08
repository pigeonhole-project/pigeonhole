//! Typed blob backend model.
//!
//! Storage crates implement [`BlobBackend`] with concrete `Id` / `Key`;
//! erasure lives behind [`crate::DynBlobBackend`].

use crate::BoxByteStream;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use pigeonhole_types::{BackendLimits, ByteRange};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;

/// Order-preserving key for sweep / delete-by-range.
pub trait OrderedKey: Copy + Ord + Send + Sync + Debug + 'static {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(b: &[u8]) -> Result<Self>;
}

impl OrderedKey for u64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn from_bytes(b: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = b
            .try_into()
            .map_err(|_| anyhow::anyhow!("u64 key: expected 8 bytes"))?;
        Ok(u64::from_be_bytes(arr))
    }
}

impl OrderedKey for i64 {
    fn to_bytes(&self) -> Vec<u8> {
        // Order-preserving for signed: flip sign bit then big-endian.
        let u = (*self as u64) ^ (1u64 << 63);
        u.to_be_bytes().to_vec()
    }
    fn from_bytes(b: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = b
            .try_into()
            .map_err(|_| anyhow::anyhow!("i64 key: expected 8 bytes"))?;
        let u = u64::from_be_bytes(arr) ^ (1u64 << 63);
        Ok(u as i64)
    }
}

/// Backend kind string as stored in config / `instances` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceKind {
    Telegram,
    Discord,
    Memory,
}

impl InstanceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Discord => "discord",
            Self::Memory => "memory",
        }
    }
}

/// Runtime role of an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstanceRole {
    ReadWrite,
    ReadOnly,
    Retired,
}

impl InstanceRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read-write",
            Self::ReadOnly => "read-only",
            Self::Retired => "retired",
        }
    }
}

/// Stable identity of a configured backend instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// Stable config name, e.g. `tg-main`.
    pub id: String,
    pub kind: InstanceKind,
    /// Opaque fingerprint string, e.g. `tg:{bot_id}:{scope}`.
    pub fingerprint: String,
    /// Location key for uniqueness of writers, e.g. `tg:chat:-100…`.
    pub location: String,
    pub role: InstanceRole,
}

/// Operation kind for cost peek (no token capture).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Put,
    Get,
    Delete,
    List,
}

/// Hint for replica selection / scheduling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostHint {
    /// Estimated wait before the op can start (limiter), seconds.
    pub wait_secs: f64,
    /// EWMA of past latency, seconds (0 if unknown).
    pub latency_ewma_secs: f64,
    /// In-flight ops on this instance.
    pub inflight: u32,
}

impl CostHint {
    pub fn free() -> Self {
        Self {
            wait_secs: 0.0,
            latency_ewma_secs: 0.0,
            inflight: 0,
        }
    }

    pub fn score(&self, inflight_weight: f64) -> f64 {
        self.wait_secs + self.latency_ewma_secs + f64::from(self.inflight) * inflight_weight
    }
}

/// Type-erased stored identity: order key + postcard/bincode locator bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLocator {
    pub key: Vec<u8>,
    pub locator: Vec<u8>,
}

/// Typed backend with associated Id/Key (stage 1.1).
#[async_trait]
pub trait BlobBackend: Send + Sync + 'static {
    type Id: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static;
    type Key: OrderedKey;

    fn instance(&self) -> &InstanceInfo;
    fn limits(&self) -> &BackendLimits;
    fn key(id: &Self::Id) -> Self::Key;

    /// Peek cost without capturing a limiter token.
    fn cost(&self, op: OpKind, id: Option<&Self::Id>) -> CostHint;

    async fn put(&self, data: Bytes) -> Result<Self::Id>;
    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream>;
    /// Batch delete; not-found is ok.
    async fn delete(&self, keys: &[Self::Key]) -> Result<()>;
}

/// Backends that can list keys in order for the sweeper.
#[async_trait]
pub trait Sweepable: BlobBackend {
    async fn candidates(
        &self,
        after: Option<Self::Key>,
        upto: Self::Key,
        limit: usize,
    ) -> Result<Vec<Self::Key>>;
}

/// Bootstrap pin as opaque bytes (superblock JSON).
#[async_trait]
pub trait TypedBootstrapPointer: Send + Sync {
    async fn read(&self) -> Result<Option<Bytes>>;
    async fn swap(&self, new: Bytes) -> Result<()>;
    /// Sort key of the pinned bootstrap message in this instance's key space.
    ///
    /// Memory pins that are not real backend messages return `None`.
    async fn pin_key(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// Share one backend instance as both [`SharedBackend`](crate::SharedBackend) member and pin.
#[async_trait]
impl<B: BlobBackend> BlobBackend for std::sync::Arc<B> {
    type Id = B::Id;
    type Key = B::Key;

    fn instance(&self) -> &InstanceInfo {
        (**self).instance()
    }
    fn limits(&self) -> &BackendLimits {
        (**self).limits()
    }
    fn key(id: &Self::Id) -> Self::Key {
        B::key(id)
    }
    fn cost(&self, op: OpKind, id: Option<&Self::Id>) -> CostHint {
        (**self).cost(op, id)
    }
    async fn put(&self, data: Bytes) -> Result<Self::Id> {
        (**self).put(data).await
    }
    async fn get(&self, id: &Self::Id, range: Option<ByteRange>) -> Result<BoxByteStream> {
        (**self).get(id, range).await
    }
    async fn delete(&self, keys: &[Self::Key]) -> Result<()> {
        (**self).delete(keys).await
    }
}

#[async_trait]
impl<B: Sweepable> Sweepable for std::sync::Arc<B> {
    async fn candidates(
        &self,
        after: Option<Self::Key>,
        upto: Self::Key,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        (**self).candidates(after, upto, limit).await
    }
}

#[async_trait]
impl<B: TypedBootstrapPointer + ?Sized> TypedBootstrapPointer for std::sync::Arc<B> {
    async fn read(&self) -> Result<Option<Bytes>> {
        (**self).read().await
    }
    async fn swap(&self, new: Bytes) -> Result<()> {
        (**self).swap(new).await
    }
    async fn pin_key(&self) -> Result<Option<Vec<u8>>> {
        (**self).pin_key().await
    }
}

/// Encode a typed id into [`BlobLocator`].
pub fn store_id<B: BlobBackend>(id: &B::Id) -> Result<BlobLocator> {
    let key = B::key(id).to_bytes();
    let locator = serde_json::to_vec(id)?;
    Ok(BlobLocator { key, locator })
}

/// Decode a typed id from locator bytes.
pub fn load_id<B: BlobBackend>(stored: &BlobLocator) -> Result<B::Id> {
    let id: B::Id = serde_json::from_slice(&stored.locator)?;
    let expect = B::key(&id).to_bytes();
    if expect != stored.key {
        bail!("BlobLocator key mismatch for decoded locator");
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_keys_preserve_order() {
        let mut vals = [ -3i64, 0, 1, -1, i64::MIN, i64::MAX, 42];
        let mut encoded: Vec<_> = vals.iter().map(|v| (*v, v.to_bytes())).collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        vals.sort();
        let decoded: Vec<i64> = encoded
            .iter()
            .map(|(_, b)| i64::from_bytes(b).unwrap())
            .collect();
        assert_eq!(decoded, vals.to_vec());
    }

    #[test]
    fn u64_roundtrip() {
        for v in [0u64, 1, 255, u64::MAX / 2, u64::MAX] {
            assert_eq!(u64::from_bytes(&v.to_bytes()).unwrap(), v);
        }
    }
}
