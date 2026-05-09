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

//! Table capability traits.
//!
//! This module defines traits for different table capabilities:
//! - Point lookups
//! - Ordered scans
//! - Mutations
//! - Batch operations
//! - Memory management
//! - Maintenance operations

use crate::pager::PhysicalLocation;
use crate::table::TableResult;
use crate::txn::TransactionId;
use crate::types::{
    CompressionKind, EncryptionKind, KeyBuf, KeyEncoding, MemoryPressure, ScanBounds, ValueBuf,
};
use crate::wal::LogSequenceNumber;
use std::borrow::Cow;

/// Logical table identifier assigned by the catalog.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TableId(u64);

impl TableId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableId({})", self.0)
    }
}

impl From<u64> for TableId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Options for creating a table.
#[derive(Clone, Debug)]
pub struct TableOptions {
    pub engine: TableEngineKind,
    pub key_encoding: KeyEncoding,
    pub compression: Option<CompressionKind>,
    pub encryption: Option<EncryptionKind>,
    pub page_size: Option<usize>,
    pub format_version: u32,
}

/// Table metadata from the catalog.
#[derive(Clone, Debug)]
pub struct TableInfo {
    pub id: TableId,
    pub name: String,
    pub options: TableOptions,
    pub root: Option<PhysicalLocation>,
    pub created_lsn: LogSequenceNumber,
}

/// Physical table implementation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableEngineKind {
    BTree,
    BPlusTree,
    LsmTree,
    Art,
    Hash,
    Memory,
    AppendLog,
    ColumnarSegment,
    Custom(u32),
}

// =============================================================================
// Table capability traits
// =============================================================================

/// Point lookup capability.
// TODO(MVCC): Add snapshot parameter for MVCC visibility
// Current signature doesn't support reading at a specific snapshot.
// Should be:
// fn get(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>>;
//
// This allows tables to:
// 1. Traverse version chains to find the visible version
// 2. Support snapshot isolation
// 3. Enable time-travel queries
pub trait PointLookup {
    fn get(&self, key: &[u8]) -> TableResult<Option<ValueBuf>>;

    fn contains(&self, key: &[u8]) -> TableResult<bool> {
        Ok(self.get(key)?.is_some())
    }
}

/// Ordered scan capability.
pub trait OrderedScan {
    type Cursor<'a>: TableCursor
    where
        Self: 'a;

    fn scan(&self, bounds: ScanBounds) -> TableResult<Self::Cursor<'_>>;
}

/// Prefix scan capability.
pub trait PrefixScan: OrderedScan {
    fn scan_prefix(&self, prefix: &[u8]) -> TableResult<Self::Cursor<'_>> {
        self.scan(ScanBounds::Prefix(KeyBuf(prefix.to_vec())))
    }
}

/// Mutation capability.
pub trait MutableTable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<()>;

    fn delete(&mut self, key: &[u8]) -> TableResult<bool>;

    fn range_delete(&mut self, bounds: ScanBounds) -> TableResult<u64>;
}

/// Batch operation capability.
pub trait BatchOps {
    fn batch_get(&self, keys: &[&[u8]]) -> TableResult<Vec<Option<ValueBuf>>>;

    fn apply_batch(&mut self, batch: WriteBatch<'_>) -> TableResult<BatchReport>;
}

/// Flush capability.
pub trait Flushable {
    fn flush(&mut self) -> TableResult<()>;
}

/// Memory awareness for adaptive resource management.
pub trait MemoryAware {
    /// Get current memory usage in bytes.
    fn memory_usage(&self) -> usize;

    /// Get the configured memory budget in bytes.
    fn memory_budget(&self) -> usize;

    /// Check if the component can evict data to free memory.
    fn can_evict(&self) -> bool {
        false
    }
}

/// Cache eviction capability for memory management.
pub trait EvictableCache: MemoryAware {
    /// Evict data to reach the target memory usage.
    fn evict(&mut self, target_bytes: usize) -> TableResult<usize>;

    /// Get the eviction priority for a specific key.
    fn eviction_priority(&self, key: &[u8]) -> Option<u64> {
        let _ = key;
        None
    }

    /// Respond to memory pressure.
    fn on_memory_pressure(&mut self, pressure: MemoryPressure) -> TableResult<()> {
        let current = self.memory_usage();
        let budget = self.memory_budget();

        let target = match pressure {
            MemoryPressure::None => return Ok(()),
            MemoryPressure::Low => (budget as f64 * 0.90) as usize,
            MemoryPressure::Medium => (budget as f64 * 0.75) as usize,
            MemoryPressure::High => (budget as f64 * 0.50) as usize,
            MemoryPressure::Critical => (budget as f64 * 0.25) as usize,
        };

        if current > target {
            self.evict(target)?;
        }

        Ok(())
    }
}

/// Format migration capability for schema evolution.
pub trait Migratable {
    /// Get the current format version.
    fn format_version(&self) -> u32;

    /// Check if migration from the given version is supported.
    fn can_migrate_from(&self, from_version: u32) -> bool;

    /// Estimate the cost of migration in arbitrary units.
    /// Returns 0 if no migration needed, u64::MAX if migration is impossible,
    /// or a finite cost estimate if migration is possible.
    fn migration_cost(&self, from_version: u32) -> u64 {
        if from_version == self.format_version() {
            // No migration needed
            0
        } else if self.can_migrate_from(from_version) {
            // Migration is possible - return cost based on version difference
            // Larger version gaps typically require more work
            let version_delta = self.format_version().abs_diff(from_version);
            // Base cost of 100 per version step
            100u64.saturating_mul(version_delta as u64)
        } else {
            // Migration is not possible
            u64::MAX
        }
    }

    /// Perform migration from the specified version.
    fn migrate(&mut self, from_version: u32) -> TableResult<()>;
}

/// Marker trait for a full ordered key-value table.
pub trait OrderedKvTable: PointLookup + OrderedScan + MutableTable + BatchOps + Flushable {}

impl<T> OrderedKvTable for T where T: PointLookup + OrderedScan + MutableTable + BatchOps + Flushable
{}

/// Physical table engine.
pub trait TableEngine {
    type Reader<'a>: TableReader
    where
        Self: 'a;

    type Writer<'a>: TableWriter
    where
        Self: 'a;

    fn table_id(&self) -> TableId;

    fn name(&self) -> &str;

    fn kind(&self) -> TableEngineKind;

    fn capabilities(&self) -> TableCapabilities;

    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>>;

    fn writer(&self, tx_id: TransactionId) -> TableResult<Self::Writer<'_>>;

    fn stats(&self) -> TableResult<TableStatistics>;
}

/// Read view over a table engine.
pub trait TableReader: PointLookup + OrderedScan {
    fn snapshot_lsn(&self) -> LogSequenceNumber;

    fn approximate_len(&self) -> TableResult<Option<u64>>;
}

/// Write view over a table engine.
pub trait TableWriter: MutableTable + BatchOps + Flushable {
    fn tx_id(&self) -> TransactionId;
}

/// Declared table capabilities.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableCapabilities {
    pub ordered: bool,
    pub point_lookup: bool,
    pub prefix_scan: bool,
    pub reverse_scan: bool,
    pub range_delete: bool,
    pub merge_operator: bool,
    pub mvcc_native: bool,
    pub append_optimized: bool,
    pub memory_resident: bool,
    pub disk_resident: bool,
    pub supports_compression: bool,
    pub supports_encryption: bool,
}

// =============================================================================
// Maintenance, statistics, verification, and repair
// =============================================================================

/// Maintenance operations common to tables, indexes, and storage files.
///
/// Note: For flushing data to disk, use the `Flushable` trait instead.
/// This trait focuses on higher-level maintenance operations like compaction,
/// checkpointing, and vacuuming.
pub trait Maintainable {
    fn compact(&mut self, options: CompactionOptions) -> TableResult<CompactionReport>;

    fn checkpoint(&mut self) -> TableResult<CheckpointInfo>;

    fn vacuum(&mut self, options: VacuumOptions) -> TableResult<VacuumReport>;
}

/// Statistics provider for query planning and diagnostics.
pub trait StatisticsProvider {
    fn statistics(&self) -> TableResult<TableStatistics>;

    fn refresh_statistics(&mut self, budget: WorkBudget) -> TableResult<()>;
}

/// Consistency verification and repair.
pub trait ConsistencyVerifier {
    fn verify(&self, scope: VerifyScope) -> TableResult<VerificationReport>;

    fn repair(&mut self, plan: RepairPlan) -> TableResult<RepairReport>;
}

// =============================================================================
// Supporting types
// =============================================================================

/// Table-level cursor trait.
///
/// This trait is implemented by table engine cursors. The transaction-level
/// `Cursor` struct (in `txn::cursor`) boxes a `TableCursor` implementation.
///
/// These methods are critical for:
/// - Efficient range queries (seek)
/// - Reverse iteration (seek_for_prev, last)
/// - Boundary access (first, last)
/// - MVCC visibility (snapshot_lsn)
pub trait TableCursor {
    fn valid(&self) -> bool;
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> Option<&[u8]>;
    fn next(&mut self) -> TableResult<()>;
    fn prev(&mut self) -> TableResult<()>;
    fn seek(&mut self, key: &[u8]) -> TableResult<()>;
    fn seek_for_prev(&mut self, key: &[u8]) -> TableResult<()>;
    fn first(&mut self) -> TableResult<()>;
    fn last(&mut self) -> TableResult<()>;
    fn snapshot_lsn(&self) -> LogSequenceNumber;
}

/// A batch of table mutations.
#[derive(Clone, Debug)]
pub struct WriteBatch<'a> {
    pub mutations: Vec<Mutation<'a>>,
}

#[derive(Clone, Debug)]
pub enum Mutation<'a> {
    Put {
        key: Cow<'a, [u8]>,
        value: Cow<'a, [u8]>,
    },
    Delete {
        key: Cow<'a, [u8]>,
    },
    RangeDelete {
        bounds: ScanBounds,
    },
    Merge {
        key: Cow<'a, [u8]>,
        operand: Cow<'a, [u8]>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchReport {
    pub attempted: u64,
    pub applied: u64,
    pub deleted: u64,
    pub bytes_written: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub total_size_bytes: Option<u64>,
    pub key_stats: Option<KeyStatistics>,
    pub value_stats: Option<ValueStatistics>,
    pub histogram: Option<Histogram>,
    pub last_updated_lsn: Option<LogSequenceNumber>,
}

#[derive(Clone, Debug, Default)]
pub struct KeyStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub distinct_count: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ValueStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub null_count: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Debug)]
pub struct HistogramBucket {
    pub lower: KeyBuf,
    pub upper: KeyBuf,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct CompactionOptions {
    pub full: bool,
    pub max_pages: Option<u64>,
    pub target_level: Option<u32>,
}

#[derive(Clone, Debug, Default)]
pub struct CompactionReport {
    pub pages_read: u64,
    pub pages_written: u64,
    pub bytes_reclaimed: u64,
    pub output_lsn: Option<LogSequenceNumber>,
}

#[derive(Clone, Debug, Default)]
pub struct CheckpointInfo {
    pub checkpoint_lsn: LogSequenceNumber,
    pub durable_lsn: LogSequenceNumber,
    pub pages_flushed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct VacuumOptions {
    pub aggressive: bool,
    pub max_pages: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct VacuumReport {
    pub pages_freed: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkBudget {
    pub max_pages: Option<u64>,
    pub max_millis: Option<u64>,
    pub max_items: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyScope {
    Catalog,
    Table(crate::table::TableId),
    Index(crate::index::IndexId),
    Page(crate::pager::PageId),
    FullDatabase,
}

#[derive(Clone, Debug, Default)]
pub struct VerificationReport {
    pub checked_items: u64,
    pub errors: Vec<ConsistencyError>,
    pub warnings: Vec<ConsistencyWarning>,
}

#[derive(Clone, Debug)]
pub struct ConsistencyError {
    pub error_type: ConsistencyErrorType,
    pub location: String,
    pub description: String,
    pub severity: Severity,
}

#[derive(Clone, Debug)]
pub struct ConsistencyWarning {
    pub location: String,
    pub description: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsistencyErrorType {
    InvalidPointer,
    CorruptedPage,
    CorruptedIndex,
    OrphanedPage,
    MissingPage,
    ChecksumMismatch,
    WalMismatch,
    CatalogMismatch,
    IndexTableMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Clone, Debug)]
pub struct RepairPlan {
    pub actions: Vec<RepairAction>,
}

#[derive(Clone, Debug)]
pub enum RepairAction {
    RebuildIndex(crate::index::IndexId),
    ReclaimOrphanedPages,
    RestoreFromWal {
        through: LogSequenceNumber,
    },
    DropCorruptedObject {
        table: Option<crate::table::TableId>,
        index: Option<crate::index::IndexId>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct RepairReport {
    pub actions_attempted: u64,
    pub actions_succeeded: u64,
    pub unrepaired_errors: Vec<ConsistencyError>,
}
