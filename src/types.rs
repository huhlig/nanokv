//
// Copyright 2025-2026 Hans W. Uhlig. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// TODO(MVCC): Add imports for MVCC types
use crate::txn::TransactionId;
use crate::wal::LogSequenceNumber;

/// Logical version used by MVCC-capable engines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(u64);

impl Version {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexId({})", self.0)
    }
}

// TODO(MVCC): Version chain structure for storing multiple versions of a value
// This enables MVCC by maintaining a linked list of value versions.
// Each version tracks:
// - The value data
// - Which transaction created it
// - When it was committed (None if uncommitted)
// - Link to previous (older) version
//
// Usage: Tables store the head of the version chain for each key.
// When reading, traverse the chain to find the first visible version
// based on the transaction's snapshot LSN and active transaction list.
//
// Garbage collection: Periodically remove versions older than the
// minimum visible LSN across all active snapshots.
#[derive(Clone, Debug)]
pub struct VersionChain {
    /// Current value data
    pub value: Vec<u8>,
    /// Transaction that created this version
    pub created_by: TransactionId,
    /// LSN when this version was committed (None if uncommitted)
    pub commit_lsn: Option<LogSequenceNumber>,
    /// Previous version (older), if any
    pub prev_version: Option<Box<VersionChain>>,
}

impl VersionChain {
    /// Create a new version chain entry
    pub fn new(value: Vec<u8>, created_by: TransactionId) -> Self {
        Self {
            value,
            created_by,
            commit_lsn: None,
            prev_version: None,
        }
    }

    /// Mark this version as committed at the given LSN
    pub fn commit(&mut self, lsn: LogSequenceNumber) {
        self.commit_lsn = Some(lsn);
    }

    /// Add a new version to the front of the chain
    pub fn prepend(self, value: Vec<u8>, created_by: TransactionId) -> Self {
        Self {
            value,
            created_by,
            commit_lsn: None,
            prev_version: Some(Box::new(self)),
        }
    }

    // TODO(MVCC): Implement visibility check
    // pub fn find_visible_version(&self, snapshot_lsn: LogSequenceNumber, active_txns: &[TransactionId]) -> Option<&[u8]>

    // TODO(MVCC): Implement garbage collection
    // pub fn vacuum(&mut self, min_visible_lsn: LogSequenceNumber) -> usize
}
impl From<u64> for Version {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Defines whether a bound is inclusive, exclusive, or unbounded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bound<T> {
    Included(T),
    Excluded(T),
    Unbounded,
}

/// Common scan bounds for ordered tables and ordered indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanBounds {
    /// Scan the full ordered keyspace.
    All,
    /// Scan keys beginning with the supplied prefix.
    Prefix(KeyBuf),
    /// Scan a bounded range.
    Range {
        start: Bound<KeyBuf>,
        end: Bound<KeyBuf>,
    },
}

/// Durability policy for a write transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Useful for ephemeral in-memory or test engines.
    MemoryOnly,
    /// Write to WAL but do not force the file to stable storage immediately.
    WalOnly,
    /// Flush dirty buffers before reporting commit.
    FlushOnCommit,
    /// Force durable sync before reporting commit.
    SyncOnCommit,
}

/// Transaction isolation level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    SnapshotIsolation,
}

/// Memory pressure level for adaptive eviction.
///
/// Used by memory-aware components to respond to system memory pressure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemoryPressure {
    /// Normal operation, no pressure.
    None,
    /// Mild pressure, consider opportunistic eviction.
    Low,
    /// Moderate pressure, actively evict to stay within budget.
    Medium,
    /// High pressure, aggressively evict to avoid OOM.
    High,
    /// Critical pressure, emergency eviction required.
    Critical,
}

/// Consistency guarantees provided by a storage component.
///
/// This struct documents the ACID properties and crash recovery semantics
/// of a table or database implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsistencyGuarantees {
    /// Operations are atomic (all-or-nothing).
    pub atomicity: bool,
    /// Consistency checks are enforced (constraints, invariants).
    pub consistency: bool,
    /// Transaction isolation level.
    pub isolation: IsolationLevel,
    /// Durability guarantees for committed transactions.
    pub durability: Durability,
    /// Data survives process crashes and can be recovered.
    pub crash_safe: bool,
    /// Supports point-in-time recovery to any committed LSN.
    pub point_in_time_recovery: bool,
}

/// Table/index mutation type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationKind {
    Insert,
    Update,
    Upsert,
    Delete,
    RangeDelete,
}

/// Owned key buffer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBuf(pub Vec<u8>);

impl AsRef<[u8]> for KeyBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Owned value buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueBuf(pub Vec<u8>);

impl AsRef<[u8]> for ValueBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A key-value entry returned by owned iterators or batch operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: KeyBuf,
    pub value: ValueBuf,
}

/// Key encoding strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyEncoding {
    RawBytes,
    LexicographicTuple,
    BigEndianInteger,
    Utf8,
    TimestampMicros,
    Custom(u32),
}

/// Compression algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionKind {
    None,
    Lz4,
    Zstd,
    Snappy,
    Custom(u32),
}

/// Encryption algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncryptionKind {
    None,
    AesGcm,
    ChaCha20Poly1305,
    Custom(u32),
}
