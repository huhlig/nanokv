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

use crate::pager::PagerError;
use crate::wal::WalError;

/// Table Result Type
pub type TableResult<T> = Result<T, TableError>;

/// Table error types
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// Key not found
    #[error("Key not found: {key}")]
    KeyNotFound {
        key: String,
    },

    /// Invalid key format
    #[error("Invalid key '{key}': {reason}")]
    InvalidKey {
        key: String,
        reason: String,
    },

    /// Invalid value format
    #[error("Invalid value for key '{key}': {reason}")]
    InvalidValue {
        key: String,
        reason: String,
    },

    /// Table is full or out of space
    #[error("Table full: current_size={current_size}, max_size={max_size}, details={details}")]
    TableFull {
        current_size: usize,
        max_size: usize,
        details: String,
    },

    /// Corruption detected
    #[error("Corruption detected at {location}: type={corruption_type}, details={details}")]
    Corruption {
        location: String,
        corruption_type: String,
        details: String,
    },

    /// Checksum mismatch
    #[error("Checksum mismatch at {location}: expected {expected:x}, got {actual:x}")]
    ChecksumMismatch {
        location: String,
        expected: u64,
        actual: u64,
    },

    /// Invalid format version
    #[error("Invalid format version: {0}")]
    InvalidFormatVersion(u32),

    /// Migration not supported
    #[error("Migration from version {from} to {to} not supported")]
    MigrationNotSupported { from: u32, to: u32 },

    /// Migration failed
    #[error("Migration failed: {0}")]
    MigrationFailed(String),

    /// Compaction error
    #[error("Compaction failed at level {level}: input_files={input_files:?}, error={error}")]
    CompactionFailed {
        level: u32,
        input_files: Vec<String>,
        error: String,
    },

    /// Vacuum error
    #[error("Vacuum failed: reclaimed={reclaimed_bytes} bytes, error={error}")]
    VacuumFailed {
        reclaimed_bytes: usize,
        error: String,
    },

    /// Checkpoint error
    #[error("Checkpoint failed at sequence {sequence}: error={error}")]
    CheckpointFailed {
        sequence: u64,
        error: String,
    },

    /// Flush error
    #[error("Flush failed: memtable_size={memtable_size}, target_level={target_level}, error={error}")]
    FlushFailed {
        memtable_size: usize,
        target_level: u32,
        error: String,
    },

    /// Eviction error
    #[error("Eviction failed: sstable_id={sstable_id}, level={level}, reason={reason}")]
    EvictionFailed {
        sstable_id: String,
        level: u32,
        reason: String,
    },

    /// Statistics refresh error
    #[error("Statistics refresh failed: {0}")]
    StatisticsRefreshFailed(String),

    /// Verification error
    #[error("Verification failed: {0}")]
    VerificationFailed(String),

    /// Repair error
    #[error("Repair failed: {0}")]
    RepairFailed(String),

    /// Batch operation error
    #[error("Batch operation failed: operation_count={operation_count}, failed_at={failed_at}, error={error}")]
    BatchFailed {
        operation_count: usize,
        failed_at: usize,
        error: String,
    },

    /// Cursor error
    #[error("Cursor error: position={position}, operation={operation}, error={error}")]
    CursorError {
        position: String,
        operation: String,
        error: String,
    },

    /// Invalid scan bounds
    #[error("Invalid scan bounds: start={start:?}, end={end:?}, reason={reason}")]
    InvalidScanBounds {
        start: Option<String>,
        end: Option<String>,
        reason: String,
    },

    /// Transaction conflict
    #[error("Transaction conflict: transaction_id={transaction_id}, conflict_type={conflict_type}, details={details}")]
    TransactionConflict {
        transaction_id: u64,
        conflict_type: String,
        details: String,
    },

    /// Snapshot not found
    #[error("Snapshot not found: snapshot_id={snapshot_id}, context={context}")]
    SnapshotNotFound {
        snapshot_id: u64,
        context: String,
    },

    /// Memory limit exceeded
    #[error("Memory limit exceeded: current={current}, limit={limit}")]
    MemoryLimitExceeded { current: usize, limit: usize },

    /// Memtable is full
    #[error("Memtable is full")]
    MemtableFull,

    /// Memtable is immutable
    #[error("Memtable is immutable")]
    MemtableImmutable,

    /// Memtable is not immutable
    #[error("Memtable is not immutable")]
    MemtableNotImmutable,

    /// Compaction already running
    #[error("Compaction already running")]
    CompactionAlreadyRunning,

    /// Compaction thread panicked
    #[error("Compaction thread panicked")]
    CompactionThreadPanic,

    /// I/O error from pager
    #[error("Pager error: {0}")]
    Pager(#[from] PagerError),

    /// WAL error
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Other Table Error
    #[error("Table error: {0}")]
    Other(String),
}

impl TableError {
    /// Create a KeyNotFound error with context
    pub fn key_not_found(key: impl Into<String>) -> Self {
        Self::KeyNotFound {
            key: key.into(),
        }
    }

    /// Create an InvalidKey error with context
    pub fn invalid_key(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidKey {
            key: key.into(),
            reason: reason.into(),
        }
    }

    /// Create an InvalidValue error with context
    pub fn invalid_value(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidValue {
            key: key.into(),
            reason: reason.into(),
        }
    }

    /// Create a TableFull error with context
    pub fn table_full(current_size: usize, max_size: usize, details: impl Into<String>) -> Self {
        Self::TableFull {
            current_size,
            max_size,
            details: details.into(),
        }
    }

    /// Create a Corruption error with context
    pub fn corruption(
        location: impl Into<String>,
        corruption_type: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::Corruption {
            location: location.into(),
            corruption_type: corruption_type.into(),
            details: details.into(),
        }
    }

    /// Create a CompactionFailed error with context
    pub fn compaction_failed(
        level: u32,
        input_files: Vec<String>,
        error: impl Into<String>,
    ) -> Self {
        Self::CompactionFailed {
            level,
            input_files,
            error: error.into(),
        }
    }

    /// Create a VacuumFailed error with context
    pub fn vacuum_failed(reclaimed_bytes: usize, error: impl Into<String>) -> Self {
        Self::VacuumFailed {
            reclaimed_bytes,
            error: error.into(),
        }
    }

    /// Create a CheckpointFailed error with context
    pub fn checkpoint_failed(sequence: u64, error: impl Into<String>) -> Self {
        Self::CheckpointFailed {
            sequence,
            error: error.into(),
        }
    }

    /// Create a FlushFailed error with context
    pub fn flush_failed(
        memtable_size: usize,
        target_level: u32,
        error: impl Into<String>,
    ) -> Self {
        Self::FlushFailed {
            memtable_size,
            target_level,
            error: error.into(),
        }
    }

    /// Create an EvictionFailed error with context
    pub fn eviction_failed(
        sstable_id: impl Into<String>,
        level: u32,
        reason: impl Into<String>,
    ) -> Self {
        Self::EvictionFailed {
            sstable_id: sstable_id.into(),
            level,
            reason: reason.into(),
        }
    }

    /// Create a BatchFailed error with context
    pub fn batch_failed(
        operation_count: usize,
        failed_at: usize,
        error: impl Into<String>,
    ) -> Self {
        Self::BatchFailed {
            operation_count,
            failed_at,
            error: error.into(),
        }
    }

    /// Create a CursorError with context
    pub fn cursor_error(
        position: impl Into<String>,
        operation: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self::CursorError {
            position: position.into(),
            operation: operation.into(),
            error: error.into(),
        }
    }

    /// Create an InvalidScanBounds error with context
    pub fn invalid_scan_bounds(
        start: Option<String>,
        end: Option<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::InvalidScanBounds {
            start,
            end,
            reason: reason.into(),
        }
    }

    /// Create a TransactionConflict error with context
    pub fn transaction_conflict(
        transaction_id: u64,
        conflict_type: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::TransactionConflict {
            transaction_id,
            conflict_type: conflict_type.into(),
            details: details.into(),
        }
    }

    /// Create a SnapshotNotFound error with context
    pub fn snapshot_not_found(snapshot_id: u64, context: impl Into<String>) -> Self {
        Self::SnapshotNotFound {
            snapshot_id,
            context: context.into(),
        }
    }
}

// Made with Bob
