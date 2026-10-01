//! Applied-state storage independent from the consensus log.
//!
//! Every batch atomically stores arbitrary namespaced cells and its applied
//! watermark. The caller decides which committed application state the cells
//! represent. Both backends use synchronous durable commits.

mod bloom;
mod codec;
mod durable;
mod lsm;
mod merge;
mod redb_store;
mod table;

pub use lsm::{FaultPoint, LsmOptions, LsmStore};
pub use redb_store::RedbStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io;

pub const MAX_KEY_BYTES: usize = 4096;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_RECORDS: usize = 65_536;
pub const MAX_INSTALL_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_INSTALL_RECORDS: usize = 1_000_000;
pub const MAX_APPLY_RANGE: u64 = 256;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Mutation {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CommitOutcome {
    Applied,
    Replay,
}

/// Counters since this handle opened; bytes are submitted engine writes, not
/// physical device writes. Failed/partial OS writes are not credited as full.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Statistics {
    pub commits: u64,
    pub logical_bytes: u64,
    pub wal_bytes: u64,
    pub flush_table_bytes: u64,
    pub compaction_bytes: u64,
    pub metadata_bytes: u64,
    pub redb_write_bytes: u64,
    pub content_syncs: u64,
    pub directory_syncs: u64,
    pub compactions: u64,
    pub compaction_input_bytes: u64,
    pub bloom_probes: u64,
    pub bloom_positive: u64,
    pub bloom_false_positive: u64,
    pub point_read_block_bytes: u64,
    pub disk_bytes: u64,
    pub tables_per_level: [usize; 3],
    pub pending_compaction: bool,
}

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub trait StateStore: Send + Sync {
    fn applied_index(&self) -> u64;
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
    fn scan(&mut self) -> io::Result<Rows>;
    fn commit(&mut self, index: u64, changes: &[Mutation]) -> io::Result<CommitOutcome> {
        self.commit_range(index, index, changes)
    }
    /// Atomically persist the final cells for a contiguous committed range.
    /// The caller must evaluate every entry in order before coalescing cells.
    fn commit_range(
        &mut self,
        first: u64,
        index: u64,
        changes: &[Mutation],
    ) -> io::Result<CommitOutcome>;
    /// Replace all state at a verified snapshot boundary; caller must fence
    /// application/reads and prove that the supplied boundary is authoritative.
    fn install(&mut self, index: u64, cells: &[Mutation]) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    /// At most one bounded maintenance job, when a configured trigger is met.
    fn maintain(&mut self) -> io::Result<bool> {
        Ok(false)
    }
    fn statistics(&self) -> io::Result<Statistics>;
}

pub(crate) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub(crate) fn rejected(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub(crate) fn validate_changes(changes: &[Mutation]) -> io::Result<u64> {
    validate_mutations(changes, MAX_BATCH_RECORDS, MAX_BATCH_BYTES)
}

pub(crate) fn validate_range(first: u64, index: u64) -> io::Result<()> {
    if first == 0 || index < first || index - first >= MAX_APPLY_RANGE {
        return Err(rejected("invalid or oversized applied range"));
    }
    Ok(())
}

pub(crate) fn validate_install(cells: &[Mutation]) -> io::Result<u64> {
    validate_mutations(cells, MAX_INSTALL_RECORDS, MAX_INSTALL_BYTES)
}

fn validate_mutations(
    changes: &[Mutation],
    max_records: usize,
    max_bytes: usize,
) -> io::Result<u64> {
    if changes.len() > max_records {
        return Err(rejected("too many batch records"));
    }
    let mut bytes = 0usize;
    let mut keys = BTreeSet::new();
    for change in changes {
        if change.key.is_empty()
            || change.key.len() > MAX_KEY_BYTES
            || change
                .value
                .as_ref()
                .is_some_and(|v| v.len() > MAX_VALUE_BYTES)
            || !keys.insert(&change.key)
        {
            return Err(rejected("invalid key/value bounds or duplicate batch key"));
        }
        bytes = bytes
            .checked_add(change.key.len() + change.value.as_ref().map_or(0, Vec::len))
            .ok_or_else(|| rejected("batch size overflow"))?;
        if bytes > max_bytes {
            return Err(rejected("batch byte limit exceeded"));
        }
    }
    Ok(bytes as u64)
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
}

impl Record {
    fn size(&self) -> usize {
        16 + self.key.len() + self.value.as_ref().map_or(0, Vec::len)
    }
}
