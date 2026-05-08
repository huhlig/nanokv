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
    #[error("Key not found")]
    KeyNotFound,

    /// Invalid key format
    #[error("Invalid key: {0}")]
    InvalidKey(String),

    /// Invalid value format
    #[error("Invalid value: {0}")]
    InvalidValue(String),

    /// Table is full or out of space
    #[error("Table full: {0}")]
    TableFull(String),

    /// Corruption detected
    #[error("Corruption detected: {0}")]
    Corruption(String),

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
    #[error("Compaction failed: {0}")]
    CompactionFailed(String),

    /// Vacuum error
    #[error("Vacuum failed: {0}")]
    VacuumFailed(String),

    /// Checkpoint error
    #[error("Checkpoint failed: {0}")]
    CheckpointFailed(String),

    /// Flush error
    #[error("Flush failed: {0}")]
    FlushFailed(String),

    /// Eviction error
    #[error("Eviction failed: {0}")]
    EvictionFailed(String),

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
    #[error("Batch operation failed: {0}")]
    BatchFailed(String),

    /// Cursor error
    #[error("Cursor error: {0}")]
    CursorError(String),

    /// Invalid scan bounds
    #[error("Invalid scan bounds: {0}")]
    InvalidScanBounds(String),

    /// Transaction conflict
    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),

    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Memory limit exceeded
    #[error("Memory limit exceeded: current={current}, limit={limit}")]
    MemoryLimitExceeded { current: usize, limit: usize },

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

// Made with Bob
