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

//! WAL recovery - Handles crash recovery and replay

use crate::vfs::FileSystem;
use crate::wal::{
    Lsn, RecordData, TransactionId, WalError, WalReader, WalRecord, WalResult, WriteOpType,
};
use std::collections::{HashMap, HashSet};

/// Transaction state during recovery
#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionState {
    /// Transaction has begun but not committed/rolled back
    Active,
    /// Transaction has been committed
    Committed,
    /// Transaction has been rolled back
    RolledBack,
}

/// Write operation recorded during recovery
#[derive(Debug, Clone)]
pub struct RecoveredWrite {
    /// Table name
    pub table: String,
    /// Operation type
    pub op_type: WriteOpType,
    /// Key
    pub key: Vec<u8>,
    /// Value (empty for delete)
    pub value: Vec<u8>,
}

/// Recovery result containing committed operations
#[derive(Debug)]
pub struct RecoveryResult {
    /// Last checkpoint LSN (if any)
    pub last_checkpoint_lsn: Option<Lsn>,
    /// Committed writes that need to be applied
    pub committed_writes: Vec<RecoveredWrite>,
    /// Active transactions at the end of recovery
    pub active_transactions: HashSet<TransactionId>,
    /// Total records processed
    pub records_processed: usize,
}

/// WAL recovery manager
pub struct WalRecovery {
    /// Transaction states
    transactions: HashMap<TransactionId, TransactionState>,
    /// Writes per transaction
    transaction_writes: HashMap<TransactionId, Vec<RecoveredWrite>>,
    /// Last checkpoint LSN
    last_checkpoint_lsn: Option<Lsn>,
    /// Records processed
    records_processed: usize,
}

impl WalRecovery {
    /// Create a new recovery manager
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
            transaction_writes: HashMap::new(),
            last_checkpoint_lsn: None,
            records_processed: 0,
        }
    }

    /// Perform recovery from a WAL file
    pub fn recover<FS: FileSystem>(fs: &FS, wal_path: &str) -> WalResult<RecoveryResult> {
        Self::recover_with_key(fs, wal_path, None)
    }

    /// Perform recovery from a WAL file with an optional encryption key
    pub fn recover_with_key<FS: FileSystem>(
        fs: &FS,
        wal_path: &str,
        encryption_key: Option<[u8; 32]>,
    ) -> WalResult<RecoveryResult> {
        let mut recovery = Self::new();
        let mut reader = WalReader::open(fs, wal_path, encryption_key)?;

        // Read and process all records
        while let Some(record) = reader.read_next()? {
            recovery.process_record(record)?;
        }

        // Build recovery result
        Ok(recovery.build_result())
    }

    /// Process a single WAL record
    fn process_record(&mut self, record: WalRecord) -> WalResult<()> {
        self.records_processed += 1;

        match record.data {
            RecordData::Begin { txn_id } => {
                self.process_begin(txn_id)?;
            }
            RecordData::Write {
                txn_id,
                table,
                op_type,
                key,
                value,
            } => {
                self.process_write(txn_id, table, op_type, key, value)?;
            }
            RecordData::Commit { txn_id } => {
                self.process_commit(txn_id)?;
            }
            RecordData::Rollback { txn_id } => {
                self.process_rollback(txn_id)?;
            }
            RecordData::Checkpoint { lsn, active_txns } => {
                self.process_checkpoint(lsn, active_txns)?;
            }
        }

        Ok(())
    }

    /// Process a BEGIN record
    fn process_begin(&mut self, txn_id: TransactionId) -> WalResult<()> {
        // Mark transaction as active
        self.transactions.insert(txn_id, TransactionState::Active);
        self.transaction_writes.insert(txn_id, Vec::new());
        Ok(())
    }

    /// Process a WRITE record
    fn process_write(
        &mut self,
        txn_id: TransactionId,
        table: String,
        op_type: WriteOpType,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> WalResult<()> {
        // Check if transaction exists
        if !self.transactions.contains_key(&txn_id) {
            return Err(WalError::RecoveryError(format!(
                "Write for unknown transaction {}",
                txn_id
            )));
        }

        // Add write to transaction
        let writes = self.transaction_writes.get_mut(&txn_id).unwrap();
        writes.push(RecoveredWrite {
            table,
            op_type,
            key,
            value,
        });

        Ok(())
    }

    /// Process a COMMIT record
    fn process_commit(&mut self, txn_id: TransactionId) -> WalResult<()> {
        // Check if transaction exists
        if !self.transactions.contains_key(&txn_id) {
            return Err(WalError::RecoveryError(format!(
                "Commit for unknown transaction {}",
                txn_id
            )));
        }

        // Mark transaction as committed
        self.transactions.insert(txn_id, TransactionState::Committed);
        Ok(())
    }

    /// Process a ROLLBACK record
    fn process_rollback(&mut self, txn_id: TransactionId) -> WalResult<()> {
        // Check if transaction exists
        if !self.transactions.contains_key(&txn_id) {
            return Err(WalError::RecoveryError(format!(
                "Rollback for unknown transaction {}",
                txn_id
            )));
        }

        // Mark transaction as rolled back and discard writes
        self.transactions
            .insert(txn_id, TransactionState::RolledBack);
        self.transaction_writes.remove(&txn_id);
        Ok(())
    }

    /// Process a CHECKPOINT record
    fn process_checkpoint(
        &mut self,
        lsn: Lsn,
        active_txns: Vec<TransactionId>,
    ) -> WalResult<()> {
        self.last_checkpoint_lsn = Some(lsn);

        // Note: We don't remove any transaction state or writes here.
        // The checkpoint just marks a point in time. All committed transactions
        // before and after the checkpoint should still be recovered.
        // We only use the checkpoint to know which transactions were active at that point.

        Ok(())
    }

    /// Build the recovery result
    fn build_result(self) -> RecoveryResult {
        let mut committed_writes = Vec::new();
        let mut active_transactions = HashSet::new();

        // Collect committed writes and active transactions
        for (txn_id, state) in self.transactions {
            match state {
                TransactionState::Committed => {
                    if let Some(writes) = self.transaction_writes.get(&txn_id) {
                        committed_writes.extend(writes.clone());
                    }
                }
                TransactionState::Active => {
                    active_transactions.insert(txn_id);
                }
                TransactionState::RolledBack => {
                    // Ignore rolled back transactions
                }
            }
        }

        RecoveryResult {
            last_checkpoint_lsn: self.last_checkpoint_lsn,
            committed_writes,
            active_transactions,
            records_processed: self.records_processed,
        }
    }
}

impl Default for WalRecovery {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemoryFileSystem;
    use crate::wal::{WalWriter, WalWriterConfig};

    #[test]
    fn test_recovery_committed_transaction() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Write a committed transaction
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();
        writer.flush().unwrap();

        // Recover
        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 1);
        assert_eq!(result.committed_writes[0].table, "table1");
        assert_eq!(result.committed_writes[0].key, b"key1");
        assert_eq!(result.committed_writes[0].value, b"value1");
        assert!(result.active_transactions.is_empty());
        assert_eq!(result.records_processed, 3);
    }

    #[test]
    fn test_recovery_rolled_back_transaction() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Write a rolled back transaction
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.write_rollback(1).unwrap();
        writer.flush().unwrap();

        // Recover
        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 0);
        assert!(result.active_transactions.is_empty());
        assert_eq!(result.records_processed, 3);
    }

    #[test]
    fn test_recovery_active_transaction() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Write an incomplete transaction
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.flush().unwrap();

        // Recover
        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 0);
        assert_eq!(result.active_transactions.len(), 1);
        assert!(result.active_transactions.contains(&1));
        assert_eq!(result.records_processed, 2);
    }

    #[test]
    fn test_recovery_multiple_transactions() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Transaction 1: committed
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();

        // Transaction 2: rolled back
        writer.write_begin(2).unwrap();
        writer
            .write_operation(
                2,
                "table2".to_string(),
                WriteOpType::Put,
                b"key2".to_vec(),
                b"value2".to_vec(),
            )
            .unwrap();
        writer.write_rollback(2).unwrap();

        // Transaction 3: active
        writer.write_begin(3).unwrap();
        writer
            .write_operation(
                3,
                "table3".to_string(),
                WriteOpType::Put,
                b"key3".to_vec(),
                b"value3".to_vec(),
            )
            .unwrap();

        writer.flush().unwrap();

        // Recover
        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 1);
        assert_eq!(result.committed_writes[0].table, "table1");
        assert_eq!(result.active_transactions.len(), 1);
        assert!(result.active_transactions.contains(&3));
        assert_eq!(result.records_processed, 8);
    }

    #[test]
    fn test_recovery_with_checkpoint() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        // Transaction 1: committed before checkpoint
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();

        // Transaction 2: active at checkpoint
        writer.write_begin(2).unwrap();
        writer
            .write_operation(
                2,
                "table2".to_string(),
                WriteOpType::Put,
                b"key2".to_vec(),
                b"value2".to_vec(),
            )
            .unwrap();

        // Checkpoint
        writer.write_checkpoint().unwrap();

        // Transaction 2: committed after checkpoint
        writer.write_commit(2).unwrap();

        writer.flush().unwrap();

        // Recover
        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 2);
        assert!(result.active_transactions.is_empty());
        assert!(result.last_checkpoint_lsn.is_some());
    }

    #[test]
    fn test_recovery_delete_operation() {
        let fs = MemoryFileSystem::new();
        let path = "test.wal";
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, path, config).unwrap();

        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Delete,
                b"key1".to_vec(),
                vec![],
            )
            .unwrap();
        writer.write_commit(1).unwrap();
        writer.flush().unwrap();

        let result = WalRecovery::recover(&fs, path).unwrap();

        assert_eq!(result.committed_writes.len(), 1);
        assert_eq!(result.committed_writes[0].op_type, WriteOpType::Delete);
        assert_eq!(result.committed_writes[0].key, b"key1");
        assert!(result.committed_writes[0].value.is_empty());
    }
}

// Made with Bob
