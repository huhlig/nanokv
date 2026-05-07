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

//! WAL writer - Handles writing records to the WAL file

use crate::vfs::{File, FileSystem};
use crate::wal::{Lsn, RecordData, TransactionId, WalError, WalRecord, WalResult};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;

/// WAL writer configuration
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// Buffer size for batching writes (bytes)
    pub buffer_size: usize,
    /// Whether to sync after each write
    pub sync_on_write: bool,
    /// Maximum WAL file size (bytes)
    pub max_wal_size: u64,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024, // 64KB buffer
            sync_on_write: true,    // Sync by default for durability
            max_wal_size: 1024 * 1024 * 1024, // 1GB max
        }
    }
}

/// WAL writer state
struct WalWriterState {
    /// Current LSN (incremented for each record)
    current_lsn: Lsn,
    /// Current file offset
    current_offset: u64,
    /// Active transactions
    active_txns: HashSet<TransactionId>,
    /// Write buffer
    buffer: Vec<u8>,
}

/// WAL writer - Manages writing records to the WAL file
pub struct WalWriter<FS: FileSystem> {
    /// VFS file handle
    file: Arc<RwLock<FS::File>>,
    /// Configuration
    config: WalWriterConfig,
    /// Writer state
    state: Arc<RwLock<WalWriterState>>,
}

impl<FS: FileSystem> WalWriter<FS> {
    /// Create a new WAL writer
    pub fn create(fs: &FS, path: &str, config: WalWriterConfig) -> WalResult<Self> {
        let file = fs.create_file(path)?;
        
        let state = WalWriterState {
            current_lsn: 1, // Start from LSN 1
            current_offset: 0,
            active_txns: HashSet::new(),
            buffer: Vec::with_capacity(config.buffer_size),
        };

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Open an existing WAL file
    pub fn open(fs: &FS, path: &str, config: WalWriterConfig) -> WalResult<Self> {
        let mut file = fs.open_file(path)?;
        
        // Get file size to determine current offset
        let file_size = file.get_size()?;
        
        // TODO: Scan the file to find the last LSN and active transactions
        // For now, we'll start from LSN 1 and assume no active transactions
        let state = WalWriterState {
            current_lsn: 1,
            current_offset: file_size,
            active_txns: HashSet::new(),
            buffer: Vec::with_capacity(config.buffer_size),
        };

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Write a BEGIN record
    pub fn write_begin(&self, txn_id: TransactionId) -> WalResult<Lsn> {
        let mut state = self.state.write();

        // Check if transaction already exists
        if state.active_txns.contains(&txn_id) {
            return Err(WalError::TransactionAlreadyExists(txn_id));
        }

        // Create record
        let lsn = state.current_lsn;
        let record = WalRecord::new(lsn, RecordData::Begin { txn_id });

        // Write record
        self.write_record_internal(&mut state, record)?;

        // Track active transaction
        state.active_txns.insert(txn_id);

        Ok(lsn)
    }

    /// Write a WRITE record
    pub fn write_operation(
        &self,
        txn_id: TransactionId,
        table: String,
        op_type: crate::wal::WriteOpType,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> WalResult<Lsn> {
        let mut state = self.state.write();

        // Check if transaction exists
        if !state.active_txns.contains(&txn_id) {
            return Err(WalError::TransactionNotFound(txn_id));
        }

        // Create record
        let lsn = state.current_lsn;
        let record = WalRecord::new(
            lsn,
            RecordData::Write {
                txn_id,
                table,
                op_type,
                key,
                value,
            },
        );

        // Write record
        self.write_record_internal(&mut state, record)?;

        Ok(lsn)
    }

    /// Write a COMMIT record
    pub fn write_commit(&self, txn_id: TransactionId) -> WalResult<Lsn> {
        let mut state = self.state.write();

        // Check if transaction exists
        if !state.active_txns.contains(&txn_id) {
            return Err(WalError::TransactionNotFound(txn_id));
        }

        // Create record
        let lsn = state.current_lsn;
        let record = WalRecord::new(lsn, RecordData::Commit { txn_id });

        // Write record
        self.write_record_internal(&mut state, record)?;

        // Remove from active transactions
        state.active_txns.remove(&txn_id);

        Ok(lsn)
    }

    /// Write a ROLLBACK record
    pub fn write_rollback(&self, txn_id: TransactionId) -> WalResult<Lsn> {
        let mut state = self.state.write();

        // Check if transaction exists
        if !state.active_txns.contains(&txn_id) {
            return Err(WalError::TransactionNotFound(txn_id));
        }

        // Create record
        let lsn = state.current_lsn;
        let record = WalRecord::new(lsn, RecordData::Rollback { txn_id });

        // Write record
        self.write_record_internal(&mut state, record)?;

        // Remove from active transactions
        state.active_txns.remove(&txn_id);

        Ok(lsn)
    }

    /// Write a CHECKPOINT record
    pub fn write_checkpoint(&self) -> WalResult<Lsn> {
        let mut state = self.state.write();

        // Create record with current active transactions
        let lsn = state.current_lsn;
        let active_txns: Vec<TransactionId> = state.active_txns.iter().copied().collect();
        let record = WalRecord::new(lsn, RecordData::Checkpoint { lsn, active_txns });

        // Write record
        self.write_record_internal(&mut state, record)?;

        Ok(lsn)
    }

    /// Internal method to write a record
    fn write_record_internal(
        &self,
        state: &mut WalWriterState,
        record: WalRecord,
    ) -> WalResult<()> {
        // Serialize record
        let bytes = record.to_bytes()?;

        // Check if WAL is full
        if state.current_offset + bytes.len() as u64 > self.config.max_wal_size {
            return Err(WalError::WalFull);
        }

        // Add to buffer
        state.buffer.extend_from_slice(&bytes);

        // Flush if buffer is full or sync is enabled
        if state.buffer.len() >= self.config.buffer_size || self.config.sync_on_write {
            self.flush_internal(state)?;
        }

        // Update state
        state.current_lsn += 1;
        state.current_offset += bytes.len() as u64;

        Ok(())
    }

    /// Flush buffered writes to disk
    pub fn flush(&self) -> WalResult<()> {
        let mut state = self.state.write();
        self.flush_internal(&mut state)
    }

    /// Internal flush implementation
    fn flush_internal(&self, state: &mut WalWriterState) -> WalResult<()> {
        if state.buffer.is_empty() {
            return Ok(());
        }

        let mut file = self.file.write();

        // Write buffer to file
        file.write_all(&state.buffer).map_err(WalError::IoError)?;

        // Sync if configured
        if self.config.sync_on_write {
            file.sync_data()?;
        }

        // Clear buffer
        state.buffer.clear();

        Ok(())
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> Lsn {
        self.state.read().current_lsn
    }

    /// Get active transactions
    pub fn active_transactions(&self) -> Vec<TransactionId> {
        self.state.read().active_txns.iter().copied().collect()
    }

    /// Get current file size
    pub fn file_size(&self) -> u64 {
        self.state.read().current_offset
    }

    /// Truncate the WAL file (used after checkpoint)
    pub fn truncate(&self) -> WalResult<()> {
        let mut state = self.state.write();
        let mut file = self.file.write();

        // Flush any pending writes
        if !state.buffer.is_empty() {
            file.write_all(&state.buffer).map_err(WalError::IoError)?;
            state.buffer.clear();
        }

        // Truncate file
        file.truncate()?;

        // Reset state
        state.current_offset = 0;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemoryFileSystem;
    use crate::wal::WriteOpType;

    #[test]
    fn test_create_wal_writer() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        assert_eq!(writer.current_lsn(), 1);
        assert_eq!(writer.file_size(), 0);
    }

    #[test]
    fn test_write_begin() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        let lsn = writer.write_begin(1).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(writer.current_lsn(), 2);
        assert_eq!(writer.active_transactions(), vec![1]);
    }

    #[test]
    fn test_write_operation() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        writer.write_begin(1).unwrap();
        let lsn = writer
            .write_operation(
                1,
                "test_table".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();

        assert_eq!(lsn, 2);
        assert_eq!(writer.current_lsn(), 3);
    }

    #[test]
    fn test_write_commit() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        writer.write_begin(1).unwrap();
        let lsn = writer.write_commit(1).unwrap();

        assert_eq!(lsn, 2);
        assert_eq!(writer.current_lsn(), 3);
        assert!(writer.active_transactions().is_empty());
    }

    #[test]
    fn test_write_rollback() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        writer.write_begin(1).unwrap();
        let lsn = writer.write_rollback(1).unwrap();

        assert_eq!(lsn, 2);
        assert_eq!(writer.current_lsn(), 3);
        assert!(writer.active_transactions().is_empty());
    }

    #[test]
    fn test_write_checkpoint() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        writer.write_begin(1).unwrap();
        writer.write_begin(2).unwrap();

        let lsn = writer.write_checkpoint().unwrap();
        assert_eq!(lsn, 3);

        let active = writer.active_transactions();
        assert_eq!(active.len(), 2);
        assert!(active.contains(&1));
        assert!(active.contains(&2));
    }

    #[test]
    fn test_transaction_not_found() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        let result = writer.write_commit(999);
        assert!(matches!(result, Err(WalError::TransactionNotFound(999))));
    }

    #[test]
    fn test_transaction_already_exists() {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "test.wal", config).unwrap();

        writer.write_begin(1).unwrap();
        let result = writer.write_begin(1);
        assert!(matches!(
            result,
            Err(WalError::TransactionAlreadyExists(1))
        ));
    }
}

// Made with Bob
