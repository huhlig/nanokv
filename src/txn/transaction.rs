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
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
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
    read_set: HashSet<Vec<u8>>,

    // Track all writes for commit/rollback
    // Key -> Value mapping of all mutations in this transaction
    write_set: HashMap<Vec<u8>, Vec<u8>>,
}

impl Transaction {
    /// TODO(MVCC): Implement transaction constructor
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

    /// TODO(MVCC): Implement read tracking for conflict detection
    /// Called by get() to track reads for Serializable isolation
    pub fn record_read(&mut self, key: Vec<u8>) {
        if self.isolation == IsolationLevel::Serializable {
            self.read_set.insert(key);
        }
    }

    /// TODO(MVCC): Implement write tracking
    /// Called by put() to track writes for commit/rollback
    pub fn record_write(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.insert(key, value);
    }

    /// TODO(MVCC): Implement state machine transitions
    pub fn prepare(&mut self) -> TransactionResult<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.txn_id));
        }
        self.state = TransactionState::Preparing;
        Ok(())
    }

    /// TODO(MVCC): Check if transaction is still active
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Get the transaction ID.
    pub fn id(&self) -> TransactionId {
        todo!("Return the transaction ID")
    }

    /// Get the isolation level of this transaction.
    pub fn isolation_level(&self) -> IsolationLevel {
        todo!("Return the isolation level")
    }

    /// Get the snapshot LSN at which this transaction reads.
    pub fn snapshot_lsn(&self) -> LogSequenceNumber {
        todo!("Return the snapshot LSN")
    }

    /// Get a value from a table.
    pub fn get(&self, table: TableId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        todo!("Perform point lookup in the specified table")
    }

    /// Put a key-value pair into a table.
    pub fn put(&mut self, table: TableId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
        todo!("Insert or update a key-value pair in the specified table")
    }

    /// Delete a key from a table.
    pub fn delete(&mut self, table: TableId, key: &[u8]) -> TransactionResult<bool> {
        todo!("Delete a key from the specified table, return true if it existed")
    }

    /// Delete a range of keys from a table.
    pub fn range_delete(&mut self, table: TableId, bounds: ScanBounds) -> TransactionResult<u64> {
        todo!("Delete all keys in the specified range, return count of deleted keys")
    }

    /// Commit the transaction.
    pub fn commit(self) -> TransactionResult<CommitInfo> {
        todo!("Commit all changes made in this transaction")
    }

    /// Rollback the transaction.
    pub fn rollback(self) -> TransactionResult<()> {
        todo!("Rollback all changes made in this transaction")
    }
}


