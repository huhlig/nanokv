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

use crate::table::TableId;
use crate::txn::{TransactionError, TransactionResult};
use crate::types::{IsolationLevel, ScanBounds, ValueBuf};
use crate::wal::LogSequenceNumber;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;

/// Transaction ID type
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        TransactionId(value)
    }
}

impl PartialEq<u64> for TransactionId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<TransactionId> for u64 {
    fn eq(&self, other: &TransactionId) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransactionId({})", self.0)
    }
}

/// Result of a successful commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitInfo {
    pub tx_id: TransactionId,
    pub commit_lsn: LogSequenceNumber,
    pub durable_lsn: Option<LogSequenceNumber>,
}

// TODO(MVCC): Transaction state machine for ACID properties
// Tracks the lifecycle of a transaction:
// - Active: Transaction is open and can perform operations
// - Preparing: Transaction is preparing to commit (2PC first phase)
// - Committed: Transaction has been committed
// - Aborted: Transaction has been rolled back
//
// State transitions:
// Active -> Preparing -> Committed
// Active -> Aborted
// Preparing -> Aborted (if commit fails)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// Transaction struct for managing database transactions.
pub struct Transaction {
    // TODO(MVCC): Implement transaction state tracking
    // Core transaction identity and isolation
    txn_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
    isolation: IsolationLevel,
    state: TransactionState,

    // TODO(MVCC): Implement read/write set tracking for conflict detection
    // For Serializable isolation: track all keys read to detect read-write conflicts
    // Only populated when isolation == Serializable
    read_set: HashSet<(TableId, Vec<u8>)>,

    // Track all writes for commit/rollback
    // (TableId, Key) -> Option<Value> mapping of all mutations in this transaction
    // None represents a delete, Some(value) represents a put
    write_set: HashMap<(TableId, Vec<u8>), Option<Vec<u8>>>,
}

impl Transaction {
    /// Create a new transaction with the given ID, snapshot LSN, and isolation level.
    pub fn new(
        txn_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            txn_id,
            snapshot_lsn,
            isolation,
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
        }
    }

    /// Record a read operation for conflict detection.
    ///
    /// Called by get() to track reads for Serializable isolation.
    /// Only tracks reads when isolation level is Serializable.
    pub fn record_read(&mut self, table_id: TableId, key: Vec<u8>) {
        if self.isolation == IsolationLevel::Serializable {
            self.read_set.insert((table_id, key));
        }
    }

    /// Record a write operation for commit/rollback.
    ///
    /// Called by put() to track writes for commit/rollback.
    pub fn record_write(&mut self, table_id: TableId, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.insert((table_id, key), Some(value));
    }

    /// Record a delete operation for commit/rollback.
    ///
    /// Called by delete() to track deletes for commit/rollback.
    pub fn record_delete(&mut self, table_id: TableId, key: Vec<u8>) {
        self.write_set.insert((table_id, key), None);
    }

    /// Prepare the transaction for commit (two-phase commit).
    ///
    /// Transitions from Active to Preparing state.
    pub fn prepare(&mut self) -> TransactionResult<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.txn_id));
        }
        self.state = TransactionState::Preparing;
        Ok(())
    }

    /// Check if the transaction is still active.
    ///
    /// Returns true if the transaction is in Active state.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Get the transaction ID.
    pub fn id(&self) -> TransactionId {
        self.txn_id
    }

    /// Get the isolation level of this transaction.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    /// Get the snapshot LSN at which this transaction reads.
    pub fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    /// Get a value from a table.
    ///
    /// This method first checks the transaction's write set for uncommitted changes,
    /// then would delegate to the underlying table engine for committed data.
    ///
    /// # Implementation Note
    ///
    /// Currently returns values from the write set only. Full implementation requires
    /// integration with table engines to read committed data at the snapshot LSN.
    pub fn get(&self, table: TableId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        // Check write set first for uncommitted changes
        let write_key = (table, key.to_vec());
        if let Some(value_opt) = self.write_set.get(&write_key) {
            // Found in write set - return the value or None if deleted
            return Ok(value_opt.as_ref().map(|v| ValueBuf(v.clone())));
        }

        // Record read for serializable isolation
        if self.isolation == IsolationLevel::Serializable {
            // Note: We cast away const here because record_read needs &mut self
            // In a full implementation, this would use interior mutability (RefCell/Mutex)
            // For now, we skip recording reads in get() - they should be recorded by the caller
        }

        // TODO: Delegate to table engine to read committed data at snapshot_lsn
        // For now, return None to indicate key not found in write set
        Ok(None)
    }

    /// Put a key-value pair into a table.
    ///
    /// Records the write in the transaction's write set. The change is not visible
    /// to other transactions until commit.
    pub fn put(&mut self, table: TableId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        // Record the write in the write set
        self.record_write(table, key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Delete a key from a table.
    ///
    /// Records the deletion in the transaction's write set. Returns true if the key
    /// existed (either in the write set or would exist in the underlying table).
    pub fn delete(&mut self, table: TableId, key: &[u8]) -> TransactionResult<bool> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        let write_key = (table, key.to_vec());
        
        // Check if key exists in write set
        let existed = self.write_set.contains_key(&write_key);
        
        // Record the deletion
        self.record_delete(table, key.to_vec());
        
        // TODO: In full implementation, also check if key exists in underlying table
        // For now, return true if it was in the write set
        Ok(existed)
    }

    /// Delete a range of keys from a table.
    ///
    /// # Implementation Note
    ///
    /// Currently not implemented. Full implementation requires:
    /// 1. Scanning the table at snapshot_lsn to find matching keys
    /// 2. Recording each deletion in the write set
    /// 3. Handling the interaction between range bounds and existing writes
    pub fn range_delete(&mut self, table: TableId, bounds: ScanBounds) -> TransactionResult<u64> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        // TODO: Implement range delete
        // This requires:
        // 1. Access to table engine to scan for keys in range
        // 2. Recording each deletion in write set
        // 3. Counting deleted keys
        let _ = (table, bounds);
        Err(TransactionError::Other(
            "range_delete not yet implemented - requires table engine integration".to_string(),
        ))
    }

    /// Commit the transaction.
    ///
    /// Transitions to Committed state and returns commit information.
    ///
    /// # Implementation Note
    ///
    /// Currently only validates state and returns mock commit info. Full implementation
    /// requires:
    /// 1. Conflict detection with other transactions
    /// 2. Writing changes to WAL
    /// 3. Applying write set to table engines
    /// 4. Releasing locks
    pub fn commit(mut self) -> TransactionResult<CommitInfo> {
        // Validate state - must be Active or Preparing
        if self.state != TransactionState::Active && self.state != TransactionState::Preparing {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        // Transition to Committed state
        self.state = TransactionState::Committed;

        // TODO: Full implementation needs to:
        // 1. Acquire write locks for all keys in write_set
        // 2. Perform conflict detection (check for write-write conflicts)
        // 3. Write commit record to WAL
        // 4. Apply write_set to table engines
        // 5. Release locks
        
        // For now, return mock commit info
        // In real implementation, commit_lsn would come from WAL
        Ok(CommitInfo {
            tx_id: self.txn_id,
            commit_lsn: self.snapshot_lsn, // Mock: use snapshot LSN
            durable_lsn: None, // Not yet durable
        })
    }

    /// Rollback the transaction.
    ///
    /// Transitions to Aborted state and discards all changes.
    pub fn rollback(mut self) -> TransactionResult<()> {
        // Can rollback from Active or Preparing state
        if self.state != TransactionState::Active && self.state != TransactionState::Preparing {
            return Err(TransactionError::InvalidState(self.txn_id));
        }

        // Transition to Aborted state
        self.state = TransactionState::Aborted;

        // Write set is automatically dropped when self is consumed
        // TODO: In full implementation, also need to:
        // 1. Release any locks held
        // 2. Write abort record to WAL (optional, for diagnostics)
        
        Ok(())
    }
}
