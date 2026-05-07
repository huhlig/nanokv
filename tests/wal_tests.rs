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

//! Integration tests for WAL (Write-Ahead Log)

use nanokv::vfs::{LocalFileSystem, MemoryFileSystem};
use nanokv::wal::{WalReader, WalRecovery, WalWriter, WalWriterConfig, WriteOpType};
use std::fs;
use tempfile::TempDir;

#[test]
fn test_wal_basic_transaction_flow() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Begin transaction
    let begin_lsn = writer.write_begin(1).unwrap();
    assert_eq!(begin_lsn, 1);

    // Write operations
    writer
        .write_operation(
            1,
            "users".to_string(),
            WriteOpType::Put,
            b"user:1".to_vec(),
            b"Alice".to_vec(),
        )
        .unwrap();

    writer
        .write_operation(
            1,
            "users".to_string(),
            WriteOpType::Put,
            b"user:2".to_vec(),
            b"Bob".to_vec(),
        )
        .unwrap();

    // Commit transaction
    writer.write_commit(1).unwrap();
    writer.flush().unwrap();

    // Verify recovery
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 2);
    assert!(result.active_transactions.is_empty());
}

#[test]
fn test_wal_rollback_transaction() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Begin and rollback
    writer.write_begin(1).unwrap();
    writer
        .write_operation(
            1,
            "users".to_string(),
            WriteOpType::Put,
            b"user:1".to_vec(),
            b"Alice".to_vec(),
        )
        .unwrap();
    writer.write_rollback(1).unwrap();
    writer.flush().unwrap();

    // Verify recovery - no committed writes
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 0);
    assert!(result.active_transactions.is_empty());
}

#[test]
fn test_wal_crash_recovery_active_transaction() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    {
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Begin transaction but don't commit (simulating crash)
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "users".to_string(),
                WriteOpType::Put,
                b"user:1".to_vec(),
                b"Alice".to_vec(),
            )
            .unwrap();
        writer.flush().unwrap();
        // Writer dropped here (simulating crash)
    }

    // Recover - should find active transaction
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 0);
    assert_eq!(result.active_transactions.len(), 1);
    assert!(result.active_transactions.contains(&1));
}

#[test]
fn test_wal_multiple_concurrent_transactions() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Start multiple transactions
    writer.write_begin(1).unwrap();
    writer.write_begin(2).unwrap();
    writer.write_begin(3).unwrap();

    // Write to different transactions
    writer
        .write_operation(
            1,
            "table1".to_string(),
            WriteOpType::Put,
            b"key1".to_vec(),
            b"value1".to_vec(),
        )
        .unwrap();

    writer
        .write_operation(
            2,
            "table2".to_string(),
            WriteOpType::Put,
            b"key2".to_vec(),
            b"value2".to_vec(),
        )
        .unwrap();

    writer
        .write_operation(
            3,
            "table3".to_string(),
            WriteOpType::Put,
            b"key3".to_vec(),
            b"value3".to_vec(),
        )
        .unwrap();

    // Commit in different order
    writer.write_commit(2).unwrap();
    writer.write_commit(1).unwrap();
    writer.write_rollback(3).unwrap();

    writer.flush().unwrap();

    // Verify recovery
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 2);
    assert!(result.active_transactions.is_empty());
}

#[test]
fn test_wal_checkpoint_functionality() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Transaction 1: complete before checkpoint
    writer.write_begin(1).unwrap();
    writer
        .write_operation(
            1,
            "data".to_string(),
            WriteOpType::Put,
            b"key1".to_vec(),
            b"value1".to_vec(),
        )
        .unwrap();
    writer.write_commit(1).unwrap();

    // Transaction 2: active during checkpoint
    writer.write_begin(2).unwrap();
    writer
        .write_operation(
            2,
            "data".to_string(),
            WriteOpType::Put,
            b"key2".to_vec(),
            b"value2".to_vec(),
        )
        .unwrap();

    // Checkpoint
    let checkpoint_lsn = writer.write_checkpoint().unwrap();
    assert!(checkpoint_lsn > 0);

    // Transaction 2: complete after checkpoint
    writer.write_commit(2).unwrap();

    // Transaction 3: after checkpoint
    writer.write_begin(3).unwrap();
    writer
        .write_operation(
            3,
            "data".to_string(),
            WriteOpType::Put,
            b"key3".to_vec(),
            b"value3".to_vec(),
        )
        .unwrap();
    writer.write_commit(3).unwrap();

    writer.flush().unwrap();

    // Verify recovery includes all committed transactions
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 3);
    assert!(result.last_checkpoint_lsn.is_some());
    assert_eq!(result.last_checkpoint_lsn.unwrap(), checkpoint_lsn);
}

#[test]
fn test_wal_delete_operations() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    writer.write_begin(1).unwrap();

    // Put operation
    writer
        .write_operation(
            1,
            "users".to_string(),
            WriteOpType::Put,
            b"user:1".to_vec(),
            b"Alice".to_vec(),
        )
        .unwrap();

    // Delete operation
    writer
        .write_operation(
            1,
            "users".to_string(),
            WriteOpType::Delete,
            b"user:2".to_vec(),
            vec![],
        )
        .unwrap();

    writer.write_commit(1).unwrap();
    writer.flush().unwrap();

    // Verify recovery
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 2);
    assert_eq!(result.committed_writes[0].op_type, WriteOpType::Put);
    assert_eq!(result.committed_writes[1].op_type, WriteOpType::Delete);
}

#[test]
fn test_wal_large_values() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Create a large value (1MB)
    let large_value = vec![0xAB; 1024 * 1024];

    writer.write_begin(1).unwrap();
    writer
        .write_operation(
            1,
            "blobs".to_string(),
            WriteOpType::Put,
            b"blob:1".to_vec(),
            large_value.clone(),
        )
        .unwrap();
    writer.write_commit(1).unwrap();
    writer.flush().unwrap();

    // Verify recovery
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 1);
    assert_eq!(result.committed_writes[0].value, large_value);
}

#[test]
fn test_wal_reader_sequential_read() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    // Write some records
    {
        let writer = WalWriter::create(&fs, path, config).unwrap();
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "test".to_string(),
                WriteOpType::Put,
                b"key".to_vec(),
                b"value".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();
        writer.flush().unwrap();
    }

    // Read records sequentially
    let mut reader = WalReader::open(&fs, path).unwrap();
    let mut count = 0;
    while let Some(_record) = reader.read_next().unwrap() {
        count += 1;
    }
    assert_eq!(count, 3); // BEGIN, WRITE, COMMIT
}

#[test]
fn test_wal_with_local_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");
    let wal_path_str = wal_path.to_str().unwrap();

    let fs = LocalFileSystem::new(temp_dir.path());
    let config = WalWriterConfig::default();

    // Write to WAL
    {
        let writer = WalWriter::create(&fs, wal_path_str, config).unwrap();
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "users".to_string(),
                WriteOpType::Put,
                b"user:1".to_vec(),
                b"Alice".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();
        writer.flush().unwrap();
    }

    // Verify file exists
    assert!(wal_path.exists());

    // Recover from file
    let result = WalRecovery::recover(&fs, wal_path_str).unwrap();
    assert_eq!(result.committed_writes.len(), 1);

    // Cleanup
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn test_wal_buffered_writes() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let mut config = WalWriterConfig::default();
    config.buffer_size = 1024; // Small buffer
    config.sync_on_write = false; // Don't sync immediately

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Write multiple transactions
    for i in 1..=10 {
        writer.write_begin(i).unwrap();
        writer
            .write_operation(
                i,
                "data".to_string(),
                WriteOpType::Put,
                format!("key{}", i).as_bytes().to_vec(),
                format!("value{}", i).as_bytes().to_vec(),
            )
            .unwrap();
        writer.write_commit(i).unwrap();
    }

    // Explicit flush
    writer.flush().unwrap();

    // Verify all transactions recovered
    let result = WalRecovery::recover(&fs, path).unwrap();
    assert_eq!(result.committed_writes.len(), 10);
}

#[test]
fn test_wal_truncate() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Write some data
    writer.write_begin(1).unwrap();
    writer
        .write_operation(
            1,
            "data".to_string(),
            WriteOpType::Put,
            b"key".to_vec(),
            b"value".to_vec(),
        )
        .unwrap();
    writer.write_commit(1).unwrap();
    writer.flush().unwrap();

    let size_before = writer.file_size();
    assert!(size_before > 0);

    // Truncate
    writer.truncate().unwrap();

    let size_after = writer.file_size();
    assert_eq!(size_after, 0);
}

#[test]
fn test_wal_error_handling() {
    let fs = MemoryFileSystem::new();
    let path = "test.wal";
    let config = WalWriterConfig::default();

    let writer = WalWriter::create(&fs, path, config).unwrap();

    // Try to commit non-existent transaction
    let result = writer.write_commit(999);
    assert!(result.is_err());

    // Try to write to non-existent transaction
    let result = writer.write_operation(
        999,
        "table".to_string(),
        WriteOpType::Put,
        b"key".to_vec(),
        b"value".to_vec(),
    );
    assert!(result.is_err());

    // Try to begin duplicate transaction
    writer.write_begin(1).unwrap();
    let result = writer.write_begin(1);
    assert!(result.is_err());
}

// Made with Bob
