//! What a store holds, for showing it (the admin's view of a local disk):
//! its files and the records in them, read without changing anything.
//!
//! Disk-shaped, yet here and not in `pigeonhole-storage-disk`: the admin
//! crate shows it and may not depend on a storage crate (the layering in
//! scripts/check_deps.py); the disk store fills it in, the admin reads it.

/// One file of a store and its records, in the order they were written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentView {
    /// The file's name in the store's directory (`seg-0000000001`).
    pub name: String,
    pub size: u64,
    pub records: Vec<RecordView>,
}

/// One record: a blob, or a member of the store's chain (a log's batch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordView {
    /// The blob's key in its store.
    pub key: u64,
    /// The name it was put under.
    pub label: String,
    pub size: u64,
    pub dead: bool,
    /// A member of the chain, not a plain blob.
    pub chained: bool,
    /// The data, when the reader asked for it (and it is not dead).
    pub data: Option<Vec<u8>>,
}
