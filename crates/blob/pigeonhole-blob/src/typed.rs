//! Typed blob backend model: the traits live in `pigeonhole-storage-api`;
//! here only the type-erased stored identity.

pub use pigeonhole_storage_api::{
    BlobBackend, BlobId, CostHint, InstanceInfo, InstanceRole, OpKind, OrderedKey,
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Type-erased stored identity: order key + postcard/bincode locator bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLocator {
    pub key: Vec<u8>,
    pub locator: Vec<u8>,
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

