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

//! Centralized error observability utilities.
//!
//! This module provides a shared classification and recording layer for NanoKV
//! errors so callers can emit consistent metrics and structured logs without
//! duplicating subsystem-specific logic.

use crate::blob::BlobError;
use crate::index::{IndexError, IndexSourceError};
use crate::pager::PagerError;
use crate::table::TableError;
use crate::txn::{CursorError, TransactionError};
use crate::vfs::FileSystemError;
use crate::wal::WalError;
use metrics::counter;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{Level, error, warn};

/// Structured classification for observed errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorClassification {
    /// Top-level subsystem that emitted the error.
    pub subsystem: &'static str,
    /// Coarse-grained category used for metrics and dashboards.
    pub category: &'static str,
    /// Stable variant identifier suitable for labels.
    pub variant: &'static str,
    /// Severity level for logging and alerting.
    pub severity: ErrorSeverity,
}

/// Severity assigned to an observed error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}

impl ErrorSeverity {
    /// Returns the string form used in metrics labels.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }

    fn tracing_level(self) -> Level {
        match self {
            Self::Warning => Level::WARN,
            Self::Error | Self::Critical => Level::ERROR,
        }
    }
}

/// Summary returned after an error observation is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorObservation {
    /// Error classification used for metrics and logs.
    pub classification: ErrorClassification,
    /// Unix timestamp at which the error was recorded.
    pub timestamp_secs: u64,
    /// Human-readable error message.
    pub message: String,
}

/// Trait implemented by error types that can be classified centrally.
pub trait ErrorTelemetry {
    /// Classify the error into subsystem/category/severity dimensions.
    fn classification(&self) -> ErrorClassification;
}


impl ErrorTelemetry for PagerError {
    fn classification(&self) -> ErrorClassification {
        match self {
            PagerError::VfsError(_) => classification("pager", "dependency", "vfs_error", ErrorSeverity::Error),
            PagerError::InvalidPageId(_) => classification("pager", "validation", "invalid_page_id", ErrorSeverity::Warning),
            PagerError::PageNotFound(_) => classification("pager", "not_found", "page_not_found", ErrorSeverity::Warning),
            PagerError::ChecksumMismatch(_) => classification("pager", "corruption", "checksum_mismatch", ErrorSeverity::Critical),
            PagerError::InvalidPageSize(_) => classification("pager", "validation", "invalid_page_size", ErrorSeverity::Error),
            PagerError::InvalidFileHeader { .. } => classification("pager", "corruption", "invalid_file_header", ErrorSeverity::Critical),
            PagerError::InvalidSuperblock { .. } => classification("pager", "corruption", "invalid_superblock", ErrorSeverity::Critical),
            PagerError::CompressionError { .. } => classification("pager", "encoding", "compression_error", ErrorSeverity::Error),
            PagerError::DecompressionError { .. } => classification("pager", "encoding", "decompression_error", ErrorSeverity::Error),
            PagerError::EncryptionError { .. } => classification("pager", "encryption", "encryption_error", ErrorSeverity::Error),
            PagerError::DecryptionError { .. } => classification("pager", "encryption", "decryption_error", ErrorSeverity::Critical),
            PagerError::MissingEncryptionKey => classification("pager", "configuration", "missing_encryption_key", ErrorSeverity::Error),
            PagerError::ConfigError(_) => classification("pager", "configuration", "config_error", ErrorSeverity::Error),
            PagerError::DatabaseFull => classification("pager", "resource_exhaustion", "database_full", ErrorSeverity::Error),
            PagerError::PageAlreadyAllocated(_) => classification("pager", "consistency", "page_already_allocated", ErrorSeverity::Error),
            PagerError::PageAlreadyFree(_) => classification("pager", "consistency", "page_already_free", ErrorSeverity::Error),
            PagerError::PagePinned(_) => classification("pager", "resource_busy", "page_pinned", ErrorSeverity::Warning),
            PagerError::InvalidPageType(_) => classification("pager", "validation", "invalid_page_type", ErrorSeverity::Error),
            PagerError::IoError(_) => classification("pager", "io", "io_error", ErrorSeverity::Error),
            PagerError::InternalError(_) => classification("pager", "internal", "internal_error", ErrorSeverity::Critical),
        }
    }
}

impl ErrorTelemetry for WalError {
    fn classification(&self) -> ErrorClassification {
        match self {
            WalError::VfsError(_) => classification("wal", "dependency", "vfs_error", ErrorSeverity::Error),
            WalError::InvalidRecord { .. } => classification("wal", "corruption", "invalid_record", ErrorSeverity::Error),
            WalError::ChecksumMismatch { .. } => classification("wal", "corruption", "checksum_mismatch", ErrorSeverity::Critical),
            WalError::CorruptedWal { .. } => classification("wal", "corruption", "corrupted_wal", ErrorSeverity::Critical),
            WalError::TransactionNotFound { .. } => classification("wal", "not_found", "transaction_not_found", ErrorSeverity::Warning),
            WalError::TransactionAlreadyExists { .. } => classification("wal", "consistency", "transaction_already_exists", ErrorSeverity::Warning),
            WalError::InvalidTransactionState { .. } => classification("wal", "validation", "invalid_transaction_state", ErrorSeverity::Error),
            WalError::WalFull { .. } => classification("wal", "resource_exhaustion", "wal_full", ErrorSeverity::Error),
            WalError::RecoveryError { .. } => classification("wal", "recovery", "recovery_error", ErrorSeverity::Critical),
            WalError::CheckpointError { .. } => classification("wal", "checkpoint", "checkpoint_error", ErrorSeverity::Error),
            WalError::IoError(_) => classification("wal", "io", "io_error", ErrorSeverity::Error),
            WalError::SerializationError { .. } => classification("wal", "encoding", "serialization_error", ErrorSeverity::Error),
            WalError::DeserializationError { .. } => classification("wal", "encoding", "deserialization_error", ErrorSeverity::Error),
            WalError::EncryptionError { .. } => classification("wal", "encryption", "encryption_error", ErrorSeverity::Error),
            WalError::DecryptionError { .. } => classification("wal", "encryption", "decryption_error", ErrorSeverity::Critical),
            WalError::MissingEncryptionKey { .. } => classification("wal", "configuration", "missing_encryption_key", ErrorSeverity::Error),
            WalError::InternalError(_) => classification("wal", "internal", "internal_error", ErrorSeverity::Critical),
        }
    }
}

impl ErrorTelemetry for TableError {
    fn classification(&self) -> ErrorClassification {
        match self {
            TableError::KeyNotFound { .. } => classification("table", "not_found", "key_not_found", ErrorSeverity::Warning),
            TableError::InvalidKey { .. } => classification("table", "validation", "invalid_key", ErrorSeverity::Warning),
            TableError::InvalidValue { .. } => classification("table", "validation", "invalid_value", ErrorSeverity::Warning),
            TableError::TableFull { .. } => classification("table", "resource_exhaustion", "table_full", ErrorSeverity::Error),
            TableError::Corruption { .. } => classification("table", "corruption", "corruption", ErrorSeverity::Critical),
            TableError::ChecksumMismatch { .. } => classification("table", "corruption", "checksum_mismatch", ErrorSeverity::Critical),
            TableError::InvalidFormatVersion(_) => classification("table", "validation", "invalid_format_version", ErrorSeverity::Error),
            TableError::MigrationNotSupported { .. } => classification("table", "unsupported", "migration_not_supported", ErrorSeverity::Warning),
            TableError::MigrationFailed(_) => classification("table", "migration", "migration_failed", ErrorSeverity::Error),
            TableError::CompactionFailed { .. } => classification("table", "compaction", "compaction_failed", ErrorSeverity::Error),
            TableError::VacuumFailed { .. } => classification("table", "maintenance", "vacuum_failed", ErrorSeverity::Error),
            TableError::CheckpointFailed { .. } => classification("table", "checkpoint", "checkpoint_failed", ErrorSeverity::Error),
            TableError::FlushFailed { .. } => classification("table", "flush", "flush_failed", ErrorSeverity::Error),
            TableError::EvictionFailed { .. } => classification("table", "eviction", "eviction_failed", ErrorSeverity::Error),
            TableError::StatisticsRefreshFailed(_) => classification("table", "maintenance", "statistics_refresh_failed", ErrorSeverity::Warning),
            TableError::VerificationFailed(_) => classification("table", "verification", "verification_failed", ErrorSeverity::Error),
            TableError::RepairFailed(_) => classification("table", "repair", "repair_failed", ErrorSeverity::Error),
            TableError::BatchFailed { .. } => classification("table", "batch", "batch_failed", ErrorSeverity::Error),
            TableError::CursorError { .. } => classification("table", "cursor", "cursor_error", ErrorSeverity::Warning),
            TableError::InvalidScanBounds { .. } => classification("table", "validation", "invalid_scan_bounds", ErrorSeverity::Warning),
            TableError::TransactionConflict { .. } => classification("table", "conflict", "transaction_conflict", ErrorSeverity::Warning),
            TableError::SnapshotNotFound { .. } => classification("table", "not_found", "snapshot_not_found", ErrorSeverity::Warning),
            TableError::MemoryLimitExceeded { .. } => classification("table", "resource_exhaustion", "memory_limit_exceeded", ErrorSeverity::Error),
            TableError::MemtableFull => classification("table", "resource_exhaustion", "memtable_full", ErrorSeverity::Warning),
            TableError::MemtableImmutable => classification("table", "state", "memtable_immutable", ErrorSeverity::Warning),
            TableError::MemtableNotImmutable => classification("table", "state", "memtable_not_immutable", ErrorSeverity::Warning),
            TableError::CompactionAlreadyRunning => classification("table", "state", "compaction_already_running", ErrorSeverity::Warning),
            TableError::CompactionThreadPanic => classification("table", "internal", "compaction_thread_panic", ErrorSeverity::Critical),
            TableError::Pager(_) => classification("table", "dependency", "pager_error", ErrorSeverity::Error),
            TableError::Wal(_) => classification("table", "dependency", "wal_error", ErrorSeverity::Error),
            TableError::Io(_) => classification("table", "io", "io_error", ErrorSeverity::Error),
            TableError::Other(_) => classification("table", "internal", "other", ErrorSeverity::Error),
        }
    }
}

impl ErrorTelemetry for TransactionError {
    fn classification(&self) -> ErrorClassification {
        match self {
            TransactionError::InvalidState { .. } => classification("transaction", "state", "invalid_state", ErrorSeverity::Warning),
            TransactionError::WriteWriteConflict { .. } => classification("transaction", "conflict", "write_write_conflict", ErrorSeverity::Warning),
            TransactionError::ReadWriteConflict { .. } => classification("transaction", "conflict", "read_write_conflict", ErrorSeverity::Warning),
            TransactionError::SerializationConflict { .. } => classification("transaction", "conflict", "serialization_conflict", ErrorSeverity::Warning),
            TransactionError::TransactionNotFound { .. } => classification("transaction", "not_found", "transaction_not_found", ErrorSeverity::Warning),
            TransactionError::Deadlock { .. } => classification("transaction", "deadlock", "deadlock", ErrorSeverity::Error),
            TransactionError::Other(_) => classification("transaction", "internal", "other", ErrorSeverity::Error),
        }
    }
}

impl ErrorTelemetry for CursorError {
    fn classification(&self) -> ErrorClassification {
        match self {
            CursorError::InvalidState { .. } => classification("cursor", "state", "invalid_state", ErrorSeverity::Warning),
            CursorError::InvalidPosition { .. } => classification("cursor", "validation", "invalid_position", ErrorSeverity::Warning),
            CursorError::Transaction(_) => classification("cursor", "dependency", "transaction_error", ErrorSeverity::Error),
            CursorError::Other(_) => classification("cursor", "internal", "other", ErrorSeverity::Error),
        }
    }
}

impl ErrorTelemetry for BlobError {
    fn classification(&self) -> ErrorClassification {
        match self {
            BlobError::NotFound { .. } => classification("blob", "not_found", "not_found", ErrorSeverity::Warning),
            BlobError::InvalidReference { .. } => classification("blob", "validation", "invalid_reference", ErrorSeverity::Warning),
            BlobError::StaleReference { .. } => classification("blob", "consistency", "stale_reference", ErrorSeverity::Error),
            BlobError::TooLarge { .. } => classification("blob", "resource_exhaustion", "too_large", ErrorSeverity::Warning),
            BlobError::Io(_) => classification("blob", "io", "io_error", ErrorSeverity::Error),
            BlobError::Pager { .. } => classification("blob", "dependency", "pager_error", ErrorSeverity::Error),
            BlobError::Corrupted { .. } => classification("blob", "corruption", "corrupted", ErrorSeverity::Critical),
            BlobError::Internal(_) => classification("blob", "internal", "internal", ErrorSeverity::Critical),
        }
    }
}

impl ErrorTelemetry for IndexError {
    fn classification(&self) -> ErrorClassification {
        match self {
            IndexError::KeyNotFound { .. } => classification("index", "not_found", "key_not_found", ErrorSeverity::Warning),
            IndexError::DuplicateKey { .. } => classification("index", "conflict", "duplicate_key", ErrorSeverity::Warning),
            IndexError::InvalidKey { .. } => classification("index", "validation", "invalid_key", ErrorSeverity::Warning),
            IndexError::Stale { .. } => classification("index", "consistency", "stale", ErrorSeverity::Warning),
            IndexError::Corrupted { .. } => classification("index", "corruption", "corrupted", ErrorSeverity::Critical),
            IndexError::OperationFailed { .. } => classification("index", "operation", "operation_failed", ErrorSeverity::Error),
            IndexError::CapacityExceeded { .. } => classification("index", "resource_exhaustion", "capacity_exceeded", ErrorSeverity::Error),
            IndexError::UnsupportedOperation { .. } => classification("index", "unsupported", "unsupported_operation", ErrorSeverity::Warning),
            IndexError::Io { .. } => classification("index", "io", "io_error", ErrorSeverity::Error),
            IndexError::Table { .. } => classification("index", "dependency", "table_error", ErrorSeverity::Error),
            IndexError::Internal { .. } => classification("index", "internal", "internal", ErrorSeverity::Critical),
        }
    }
}

impl ErrorTelemetry for IndexSourceError {
    fn classification(&self) -> ErrorClassification {
        match self {
            IndexSourceError::TableScan(_) => classification("index_source", "dependency", "table_scan", ErrorSeverity::Error),
            IndexSourceError::Io(_) => classification("index_source", "io", "io_error", ErrorSeverity::Error),
            IndexSourceError::InvalidData(_) => classification("index_source", "validation", "invalid_data", ErrorSeverity::Warning),
            IndexSourceError::Cancelled(_) => classification("index_source", "cancellation", "cancelled", ErrorSeverity::Warning),
            IndexSourceError::Other(_) => classification("index_source", "internal", "other", ErrorSeverity::Error),
        }
    }
}

impl ErrorTelemetry for FileSystemError {
    fn classification(&self) -> ErrorClassification {
        match self {
            FileSystemError::InvalidPath { .. } => classification("vfs", "validation", "invalid_path", ErrorSeverity::Warning),
            FileSystemError::PathExists { .. } => classification("vfs", "conflict", "path_exists", ErrorSeverity::Warning),
            FileSystemError::PathMissing { .. } => classification("vfs", "not_found", "path_missing", ErrorSeverity::Warning),
            FileSystemError::ParentMissing { .. } => classification("vfs", "not_found", "parent_missing", ErrorSeverity::Warning),
            FileSystemError::FileAlreadyLocked { .. } => classification("vfs", "resource_busy", "file_already_locked", ErrorSeverity::Warning),
            FileSystemError::PermissionDenied { .. } => classification("vfs", "permission", "permission_denied", ErrorSeverity::Error),
            FileSystemError::AlreadyLocked { .. } => classification("vfs", "resource_busy", "already_locked", ErrorSeverity::Warning),
            FileSystemError::InvalidOperation { .. } => classification("vfs", "validation", "invalid_operation", ErrorSeverity::Warning),
            FileSystemError::UnsupportedOperation { .. } => classification("vfs", "unsupported", "unsupported_operation", ErrorSeverity::Warning),
            FileSystemError::InternalError(_) => classification("vfs", "internal", "internal_error", ErrorSeverity::Critical),
            FileSystemError::IOError(_) => classification("vfs", "io", "io_error", ErrorSeverity::Error),
            FileSystemError::WrappedError(_) => classification("vfs", "dependency", "wrapped_error", ErrorSeverity::Error),
        }
    }
}

fn classification(
    subsystem: &'static str,
    category: &'static str,
    variant: &'static str,
    severity: ErrorSeverity,
) -> ErrorClassification {
    ErrorClassification {
        subsystem,
        category,
        variant,
        severity,
    }
}

/// Record metrics and emit a structured tracing event for an error.
#[must_use]
pub fn record_error<E>(error: &E) -> ErrorObservation
where
    E: ErrorTelemetry + std::fmt::Display,
{
    let classification = error.classification();
    let timestamp_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let message = error.to_string();

    counter!(
        "nanokv.error.total",
        "subsystem" => classification.subsystem,
        "category" => classification.category,
        "variant" => classification.variant,
        "severity" => classification.severity.as_str()
    )
    .increment(1);

    counter!(
        "nanokv.error.category.total",
        "category" => classification.category,
        "severity" => classification.severity.as_str()
    )
    .increment(1);

    match classification.severity.tracing_level() {
        Level::WARN => warn!(
            subsystem = classification.subsystem,
            category = classification.category,
            variant = classification.variant,
            severity = classification.severity.as_str(),
            timestamp_secs = timestamp_secs,
            error = %message,
            "nanokv error recorded"
        ),
        _ => error!(
            subsystem = classification.subsystem,
            category = classification.category,
            variant = classification.variant,
            severity = classification.severity.as_str(),
            timestamp_secs = timestamp_secs,
            error = %message,
            "nanokv error recorded"
        ),
    };

    ErrorObservation {
        classification,
        timestamp_secs,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobRef;
    use crate::error::NanoKvError;
    use crate::index::IndexId;
    use crate::pager::{CompressionType, EncryptionType, PageId};
    use crate::table::TableError;
    use crate::txn::TransactionId;
    use crate::types::ObjectId;
    use crate::wal::LogSequenceNumber;
    use tracing_test::traced_test;

    #[test]
    fn classifies_pager_corruption_as_critical() {
        let err = PagerError::ChecksumMismatch(PageId::from(7));
        let classification = err.classification();

        assert_eq!(classification.subsystem, "pager");
        assert_eq!(classification.category, "corruption");
        assert_eq!(classification.variant, "checksum_mismatch");
        assert_eq!(classification.severity, ErrorSeverity::Critical);
    }

    #[test]
    fn classifies_transaction_conflicts_as_warnings() {
        let err = TransactionError::write_write_conflict(
            ObjectId::from(9_u64),
            b"key".to_vec(),
            TransactionId::from(1),
            TransactionId::from(2),
        );
        let classification = err.classification();

        assert_eq!(classification.subsystem, "transaction");
        assert_eq!(classification.category, "conflict");
        assert_eq!(classification.severity, ErrorSeverity::Warning);
    }

    #[test]
    fn classifies_unified_error_by_wrapped_subsystem() {
        let err = NanoKvError::from(WalError::corrupted_wal(42, "truncated", "unexpected EOF"));
        let classification = err.classification();

        assert_eq!(classification.subsystem, "wal");
        assert_eq!(classification.category, "corruption");
        assert_eq!(classification.variant, "corrupted_wal");
        assert_eq!(classification.severity, ErrorSeverity::Critical);
    }

    #[test]
    fn records_error_observation_details() {
        let err = TableError::corruption("sstable-1", "checksum", "footer mismatch");
        let observation = record_error(&err);

        assert_eq!(observation.classification.subsystem, "table");
        assert_eq!(observation.classification.category, "corruption");
        assert!(!observation.message.is_empty());
        assert!(observation.timestamp_secs > 0);
    }

    #[test]
    #[traced_test]
    fn emits_structured_log_for_recorded_error() {
        let err = WalError::encryption_error(
            LogSequenceNumber::from(10),
            "AES256-GCM",
            "failed to encrypt record",
        );

        let observation = record_error(&err);

        assert_eq!(observation.classification.subsystem, "wal");
        assert!(logs_contain("nanokv error recorded"));
        assert!(logs_contain(r#"subsystem="wal""#));
        assert!(logs_contain(r#"variant="encryption_error""#));
    }

    #[test]
    fn classifies_remaining_subsystems() {
        let vfs = FileSystemError::path_missing("missing.db");
        let blob = BlobError::corrupted(
            BlobRef::new(PageId::from(11), 128, 0x1234),
            "page 11",
            "checksum",
            "mismatch",
        );
        let index = IndexError::corrupted(IndexId::from(5), "root", "checksum", "bad root");
        let cursor = CursorError::invalid_position("before first");
        let pager = PagerError::compression_error(PageId::from(1), CompressionType::Lz4, "failed");
        let pager_encryption =
            PagerError::encryption_error(PageId::from(2), EncryptionType::Aes256Gcm, "failed");

        assert_eq!(vfs.classification().subsystem, "vfs");
        assert_eq!(blob.classification().subsystem, "blob");
        assert_eq!(index.classification().subsystem, "index");
        assert_eq!(cursor.classification().subsystem, "cursor");
        assert_eq!(pager.classification().category, "encoding");
        assert_eq!(pager_encryption.classification().category, "encryption");
    }
}

// Made with Bob
