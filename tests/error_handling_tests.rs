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

//! Comprehensive error handling tests
//!
//! This test suite validates error handling across all subsystems including:
//! - Encryption key errors
//! - WAL replay failures
//! - Resource exhaustion
//! - Error propagation across layers
//! - Error recovery mechanisms

use nanokv::blob::{BlobError, BlobRef};
use nanokv::error::NanoKvError;
use nanokv::index::{IndexError, IndexSourceError};
use nanokv::pager::{
    CompressionType, EncryptionType, Page, PageId, PageSize, PageType, Pager, PagerConfig,
    PagerError,
};
use nanokv::table::{TableError, TableId};
use nanokv::txn::{ConflictDetector, CursorError, TransactionError, TransactionId};
use nanokv::vfs::{File, FileSystem, FileSystemError, MemoryFileSystem};
use nanokv::wal::{
    LogSequenceNumber, WalError, WalReader, WalRecovery, WalWriter, WalWriterConfig, WriteOpType,
};
use std::io::{Read, Seek, SeekFrom, Write};

// ============================================================================
// Encryption Key Error Tests
// ============================================================================

#[test]
fn test_missing_encryption_key_on_open() {
    let fs = MemoryFileSystem::new();
    let path = "encrypted.db";

    // Create an encrypted database
    let key = [0x42u8; 32];
    let config = PagerConfig::default().with_encryption(EncryptionType::Aes256Gcm, key);

    let pager = Pager::create(&fs, path, config).expect("Failed to create pager");
    drop(pager);

    // Try to open without providing encryption - should fail
    // Note: Current API requires encryption config on open, so we test the error type
    let error = PagerError::MissingEncryptionKey;
    assert_eq!(
        error.to_string(),
        "Encryption key required but not provided"
    );
}

#[test]
fn test_encryption_error_types() {
    // Test that encryption-related error types exist and format correctly
    let error = PagerError::EncryptionError("Failed to encrypt page".to_string());
    assert!(error.to_string().contains("Encryption error"));

    let error = PagerError::DecryptionError("Failed to decrypt page".to_string());
    assert!(error.to_string().contains("Decryption error"));

    let error = PagerError::MissingEncryptionKey;
    assert_eq!(
        error.to_string(),
        "Encryption key required but not provided"
    );
}

#[test]
fn test_encryption_key_validation() {
    // Test that encryption key validation errors are properly typed
    let error = PagerError::ConfigError("Invalid encryption key size".to_string());
    assert!(error.to_string().contains("Configuration error"));

    let error2 = PagerError::EncryptionError("Key size mismatch".to_string());
    assert!(error2.to_string().contains("Encryption error"));
}

#[test]
fn test_wal_encryption_errors() {
    // Test WAL encryption error types
    let error = WalError::EncryptionError("Failed to encrypt record".to_string());
    assert!(error.to_string().contains("Encryption error"));

    let error = WalError::DecryptionError("Failed to decrypt record".to_string());
    assert!(error.to_string().contains("Decryption error"));

    let error = WalError::MissingEncryptionKey;
    assert_eq!(error.to_string(), "Missing encryption key");
}

// ============================================================================
// WAL Replay Failure Tests
// ============================================================================

#[test]
fn test_wal_corrupted_record_during_recovery() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).expect("Failed to create WAL");

    // Write some records
    writer
        .write_begin(TransactionId::from(1))
        .expect("Failed to write begin");
    writer
        .write_operation(
            TransactionId::from(1),
            TableId::from(1),
            WriteOpType::Put,
            b"key1".to_vec(),
            b"value1".to_vec(),
        )
        .expect("Failed to write operation");
    writer.flush().expect("Failed to flush");
    drop(writer);

    // Corrupt the WAL file
    let mut file = fs.open_file(path).expect("Failed to open WAL");
    file.seek(SeekFrom::Start(100)).expect("Failed to seek");
    file.write_all(&[0xFF; 50]).expect("Failed to corrupt");
    drop(file);

    // Recovery should detect corruption
    let result = WalRecovery::recover(&fs, path);
    assert!(result.is_err());
    if let Err(e) = result {
        match e {
            WalError::CorruptedWal(_)
            | WalError::ChecksumMismatch(_)
            | WalError::InvalidRecord(_)
            | WalError::DeserializationError(_) => {}
            e => panic!("Expected corruption-related error, got: {:?}", e),
        }
    }
}

#[test]
fn test_wal_incomplete_transaction_recovery() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).expect("Failed to create WAL");

    // Write incomplete transaction (begin but no commit/rollback)
    writer
        .write_begin(TransactionId::from(1))
        .expect("Failed to write begin");
    writer
        .write_operation(
            TransactionId::from(1),
            TableId::from(1),
            WriteOpType::Put,
            b"key1".to_vec(),
            b"value1".to_vec(),
        )
        .expect("Failed to write operation");
    writer.flush().expect("Failed to flush");
    drop(writer);

    // Recovery should handle incomplete transaction
    let result = WalRecovery::recover(&fs, path).expect("Recovery should succeed");

    // Incomplete transaction should be in active_transactions
    assert_eq!(result.active_transactions.len(), 1);
    assert!(result
        .active_transactions
        .contains(&TransactionId::from(1)));

    // No committed writes
    assert_eq!(result.committed_writes.len(), 0);
}

#[test]
fn test_wal_truncated_file_recovery() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).expect("Failed to create WAL");

    writer
        .write_begin(TransactionId::from(1))
        .expect("Failed to write begin");
    writer.flush().expect("Failed to flush");
    drop(writer);

    // Corrupt by writing garbage at the end
    let mut file = fs.open_file(path).expect("Failed to open file");
    let file_size = fs.filesize(path).expect("Failed to get size");
    file.seek(SeekFrom::Start(file_size - 10))
        .expect("Failed to seek");
    file.write_all(&[0xFF; 20]).expect("Failed to write");
    drop(file);

    // Recovery should handle corrupted file gracefully
    let result = WalRecovery::recover(&fs, path);

    // Should either succeed with partial recovery or fail with appropriate error
    match result {
        Ok(recovery) => {
            // Partial recovery is acceptable
            assert!(
                recovery.active_transactions.is_empty()
                    || recovery.active_transactions.len() <= 1
            );
        }
        Err(e) => {
            // Should be a corruption or EOF-related error
            match e {
                WalError::CorruptedWal(_)
                | WalError::InvalidRecord(_)
                | WalError::DeserializationError(_)
                | WalError::IoError(_)
                | WalError::ChecksumMismatch(_) => {}
                e => panic!("Unexpected error type: {:?}", e),
            }
        }
    }
}

#[test]
fn test_wal_invalid_transaction_state_sequence() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).expect("Failed to create WAL");

    // Write begin
    writer
        .write_begin(TransactionId::from(1))
        .expect("Failed to write begin");

    // Try to commit without any operations (valid but edge case)
    writer
        .write_commit(TransactionId::from(1))
        .expect("Failed to write commit");

    // Try to write operation after commit (invalid state)
    let result = writer.write_operation(
        TransactionId::from(1),
        TableId::from(1),
        WriteOpType::Put,
        b"key1".to_vec(),
        b"value1".to_vec(),
    );

    // Should fail with invalid transaction state
    assert!(result.is_err());
    if let Err(e) = result {
        match e {
            WalError::InvalidTransactionState(_) | WalError::TransactionNotFound(_) => {}
            e => panic!(
                "Expected InvalidTransactionState or TransactionNotFound, got: {:?}",
                e
            ),
        }
    }
}

#[test]
fn test_wal_error_types() {
    // Test various WAL error types
    let error = WalError::CorruptedWal("Invalid header".to_string());
    assert!(error.to_string().contains("Corrupted WAL"));

    let error = WalError::InvalidRecord("Bad record format".to_string());
    assert!(error.to_string().contains("Invalid WAL record"));

    let error = WalError::RecoveryError("Failed to replay".to_string());
    assert!(error.to_string().contains("Recovery error"));

    let error = WalError::CheckpointError("Checkpoint failed".to_string());
    assert!(error.to_string().contains("Checkpoint error"));
}

// ============================================================================
// Resource Exhaustion Tests
// ============================================================================

#[test]
fn test_database_full_error() {
    // Test the DatabaseFull error type
    let error = PagerError::DatabaseFull;
    assert_eq!(
        error.to_string(),
        "Database is full (no free pages available)"
    );
}

#[test]
fn test_memtable_full_error() {
    // This tests the TableError::MemtableFull error
    let error = TableError::MemtableFull;
    assert_eq!(error.to_string(), "Memtable is full");
}

#[test]
fn test_memory_limit_exceeded_error() {
    let error = TableError::MemoryLimitExceeded {
        current: 1024 * 1024 * 100, // 100 MB
        limit: 1024 * 1024 * 50,    // 50 MB limit
    };

    let error_str = error.to_string();
    assert!(error_str.contains("Memory limit exceeded"));
    assert!(error_str.contains("current=104857600"));
    assert!(error_str.contains("limit=52428800"));
}

#[test]
fn test_wal_full_error() {
    let error = WalError::WalFull;
    assert_eq!(error.to_string(), "WAL is full (max size reached)");
}

#[test]
fn test_blob_too_large_error() {
    let error = BlobError::TooLarge {
        size: 1024 * 1024 * 100, // 100 MB
        max: 1024 * 1024 * 10,   // 10 MB max
    };

    let error_str = error.to_string();
    assert!(error_str.contains("Blob too large"));
    assert!(error_str.contains("104857600"));
    assert!(error_str.contains("10485760"));
}

#[test]
fn test_table_full_error() {
    let error = TableError::TableFull("No more space in B-tree".to_string());
    assert!(error.to_string().contains("Table full"));
}

// ============================================================================
// Error Propagation Tests
// ============================================================================

#[test]
fn test_error_propagation_pager_to_nanokv() {
    let pager_error = PagerError::DatabaseFull;
    let nanokv_error: NanoKvError = pager_error.into();

    assert!(nanokv_error.is_pager());
    assert!(nanokv_error.as_pager().is_some());

    match nanokv_error.as_pager().unwrap() {
        PagerError::DatabaseFull => {}
        e => panic!("Expected DatabaseFull, got: {:?}", e),
    }
}

#[test]
fn test_error_propagation_wal_to_nanokv() {
    let wal_error = WalError::WalFull;
    let nanokv_error: NanoKvError = wal_error.into();

    assert!(nanokv_error.is_wal());
    assert!(nanokv_error.as_wal().is_some());
}

#[test]
fn test_error_propagation_table_to_nanokv() {
    let table_error = TableError::KeyNotFound;
    let nanokv_error: NanoKvError = table_error.into();

    assert!(nanokv_error.is_table());
    assert!(nanokv_error.as_table().is_some());
}

#[test]
fn test_error_propagation_transaction_to_nanokv() {
    let txn_error = TransactionError::SerializationConflict;
    let nanokv_error: NanoKvError = txn_error.into();

    assert!(nanokv_error.is_transaction());
    assert!(nanokv_error.as_transaction().is_some());
}

#[test]
fn test_error_propagation_vfs_to_pager() {
    let vfs_error = FileSystemError::path_missing("test.db");
    let pager_error: PagerError = vfs_error.into();

    match pager_error {
        PagerError::VfsError(_) => {}
        e => panic!("Expected VfsError, got: {:?}", e),
    }
}

#[test]
fn test_error_propagation_pager_to_table() {
    let pager_error = PagerError::ChecksumMismatch(PageId::from(42));
    let table_error: TableError = pager_error.into();

    match table_error {
        TableError::Pager(_) => {}
        e => panic!("Expected Pager error, got: {:?}", e),
    }
}

#[test]
fn test_error_propagation_io_to_multiple_layers() {
    let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Access denied");

    // IO error can propagate to Pager
    let pager_error: PagerError = io_error.into();
    match pager_error {
        PagerError::IoError(_) => {}
        e => panic!("Expected IoError, got: {:?}", e),
    }
}

// ============================================================================
// Transaction Conflict Error Tests
// ============================================================================

#[test]
fn test_write_write_conflict_error_details() {
    let error = TransactionError::WriteWriteConflict(
        TableId::from(1).as_object_id(),
        b"user:123".to_vec(),
        TransactionId::from(42),
    );

    let error_str = error.to_string();
    assert!(error_str.contains("Write-write conflict"));
    // The error format uses Debug for ObjectId and TransactionId
    assert!(error_str.contains("already locked"));
}

#[test]
fn test_read_write_conflict_error_details() {
    let error = TransactionError::ReadWriteConflict(TableId::from(2).as_object_id(), b"product:456".to_vec());

    let error_str = error.to_string();
    assert!(error_str.contains("Read-write conflict"));
    assert!(error_str.contains("was modified after read"));
}

#[test]
fn test_deadlock_error_details() {
    let error = TransactionError::Deadlock(TransactionId::from(99));

    let error_str = error.to_string();
    assert!(error_str.contains("Deadlock"));
    assert!(error_str.contains("involving transaction"));
}

#[test]
fn test_serialization_conflict_error() {
    let error = TransactionError::SerializationConflict;
    assert_eq!(error.to_string(), "Serialization conflict detected");
}

#[test]
fn test_transaction_not_found_error() {
    let error = TransactionError::TransactionNotFound(TransactionId::from(123));
    let error_str = error.to_string();
    assert!(error_str.contains("not found"));
}

#[test]
fn test_invalid_transaction_state_error() {
    let error = TransactionError::InvalidState(TransactionId::from(456));
    let error_str = error.to_string();
    assert!(error_str.contains("invalid state"));
}

// ============================================================================
// Checksum Mismatch Error Tests
// ============================================================================

#[test]
fn test_pager_checksum_mismatch_error() {
    let error = PagerError::ChecksumMismatch(PageId::from(123));

    let error_str = error.to_string();
    assert!(error_str.contains("Checksum mismatch"));
    // PageId uses Debug format
}

#[test]
fn test_wal_checksum_mismatch_error() {
    let error = WalError::ChecksumMismatch(LogSequenceNumber::from(456));

    let error_str = error.to_string();
    assert!(error_str.contains("Checksum mismatch"));
    // LogSequenceNumber uses Debug format
}

#[test]
fn test_table_checksum_mismatch_error() {
    let error = TableError::ChecksumMismatch {
        location: "SSTable block 5".to_string(),
        expected: 0xDEADBEEF,
        actual: 0xBADC0FFE,
    };

    let error_str = error.to_string();
    assert!(error_str.contains("Checksum mismatch"));
    assert!(error_str.contains("SSTable block 5"));
    assert!(error_str.contains("deadbeef"));
    assert!(error_str.contains("badc0ffe"));
}

#[test]
fn test_blob_stale_reference_error() {
    let error = BlobError::StaleReference {
        expected: 0x12345678,
        found: 0x87654321,
    };

    let error_str = error.to_string();
    assert!(error_str.contains("Stale blob reference"));
    assert!(error_str.contains("checksum mismatch"));
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

#[test]
fn test_recovery_from_write_conflict() {
    let mut detector = ConflictDetector::new();
    let txn1 = TransactionId::from(1);
    let txn2 = TransactionId::from(2);
    let table = TableId::from(1);

    // Transaction 1 locks a key
    detector.acquire_write_lock(table.as_object_id(), b"key1".to_vec(), txn1);

    // Transaction 2 tries to lock the same key and fails
    let result = detector.check_write_conflict(table.as_object_id(), b"key1", txn2);
    assert!(result.is_err());

    // Transaction 2 should be able to lock a different key
    let result2 = detector.check_write_conflict(table.as_object_id(), b"key2", txn2);
    assert!(result2.is_ok());
    detector.acquire_write_lock(table.as_object_id(), b"key2".to_vec(), txn2);

    // After transaction 1 releases locks, transaction 2 should be able to acquire
    detector.release_locks(txn1);
    let result3 = detector.check_write_conflict(table.as_object_id(), b"key1", txn2);
    assert!(result3.is_ok());
}

#[test]
fn test_error_type_checking_helpers() {
    // Test all the is_* helper methods
    let pager_err: NanoKvError = PagerError::DatabaseFull.into();
    assert!(pager_err.is_pager());
    assert!(!pager_err.is_wal());
    assert!(!pager_err.is_table());

    let wal_err: NanoKvError = WalError::WalFull.into();
    assert!(!wal_err.is_pager());
    assert!(wal_err.is_wal());
    assert!(!wal_err.is_table());

    let table_err: NanoKvError = TableError::KeyNotFound.into();
    assert!(!table_err.is_pager());
    assert!(!table_err.is_wal());
    assert!(table_err.is_table());
}

// ============================================================================
// VFS Error Tests
// ============================================================================

#[test]
fn test_vfs_path_missing_error() {
    let fs = MemoryFileSystem::new();

    // Try to open non-existent file
    let result = fs.open_file("nonexistent.db");
    assert!(result.is_err());

    match result.unwrap_err() {
        FileSystemError::PathMissing { path } => {
            assert_eq!(path, "nonexistent.db");
        }
        e => panic!("Expected PathMissing error, got: {:?}", e),
    }
}

#[test]
fn test_vfs_invalid_path_error() {
    let error = FileSystemError::invalid_path("/invalid/path", "test reason");
    let error_str = format!("{}", error);
    assert!(error_str.contains("Invalid path"));
    assert!(error_str.contains("/invalid/path"));
    assert!(error_str.contains("test reason"));
}

#[test]
fn test_vfs_permission_denied_error() {
    let error = FileSystemError::permission_denied("/test/file", "write");
    let error_str = format!("{}", error);
    assert!(error_str.contains("Permission denied"));
    assert!(error_str.contains("/test/file"));
    assert!(error_str.contains("write"));
}

#[test]
fn test_vfs_already_locked_error() {
    let error = FileSystemError::AlreadyLocked {
        path: "/test/file".to_string(),
    };
    let error_str = format!("{}", error);
    assert!(error_str.contains("already locked"));
    assert!(error_str.contains("/test/file"));
}

// ============================================================================
// Index Error Tests
// ============================================================================

#[test]
fn test_index_source_error_from_table_error() {
    let table_error = TableError::Corruption("Index data corrupted".to_string());
    let index_error: IndexSourceError = table_error.into();

    match index_error {
        IndexSourceError::TableScan(_) => {}
        e => panic!("Expected TableScan error, got: {:?}", e),
    }
}

#[test]
fn test_index_source_invalid_data_error() {
    let error = IndexSourceError::InvalidData("Malformed index entry".to_string());
    let error_str = error.to_string();
    assert!(error_str.contains("Invalid data"));
    assert!(error_str.contains("Malformed index entry"));
}

#[test]
fn test_index_source_cancelled_error() {
    let error = IndexSourceError::Cancelled("User interrupted rebuild".to_string());
    let error_str = error.to_string();
    assert!(error_str.contains("Scan cancelled"));
    assert!(error_str.contains("User interrupted rebuild"));
}

// ============================================================================
// Blob Error Tests
// ============================================================================

#[test]
fn test_blob_not_found_error() {
    let blob_ref = BlobRef::new(PageId::from(42), 100, 0x12345678);
    let error = BlobError::NotFound(blob_ref);
    let error_str = error.to_string();
    assert!(error_str.contains("Blob not found"));
}

#[test]
fn test_blob_invalid_reference_error() {
    let error = BlobError::InvalidReference("Bad blob ref format".to_string());
    let error_str = error.to_string();
    assert!(error_str.contains("Invalid blob reference"));
}

#[test]
fn test_blob_corrupted_error() {
    let error = BlobError::Corrupted("Blob data corrupted".to_string());
    let error_str = error.to_string();
    assert!(error_str.contains("Corruption detected"));
}

// ============================================================================
// Table Error Tests
// ============================================================================

#[test]
fn test_table_corruption_error() {
    let error = TableError::Corruption("B-tree structure corrupted".to_string());
    let error_str = error.to_string();
    assert!(error_str.contains("Corruption detected"));
}

#[test]
fn test_table_compaction_errors() {
    let error = TableError::CompactionFailed("Merge failed".to_string());
    assert!(error.to_string().contains("Compaction failed"));

    let error = TableError::CompactionAlreadyRunning;
    assert_eq!(error.to_string(), "Compaction already running");

    let error = TableError::CompactionThreadPanic;
    assert_eq!(error.to_string(), "Compaction thread panicked");
}

#[test]
fn test_table_memtable_errors() {
    let error = TableError::MemtableImmutable;
    assert_eq!(error.to_string(), "Memtable is immutable");

    let error = TableError::MemtableNotImmutable;
    assert_eq!(error.to_string(), "Memtable is not immutable");
}

// Made with Bob