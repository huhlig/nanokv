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

use crate::txn::{ConflictDetector, TransactionError, TransactionResult};
use crate::types::{IsolationLevel, ObjectId, ScanBounds, ValueBuf};
use crate::wal::LogSequenceNumber;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

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

/// Transaction trait for supporting multiple storage engines.
///
/// Different storage engines have different transaction semantics:
/// - In-memory BTree: Transactions are trivially rolled back by discarding changes
/// - LSM Tree: Requires WAL coordination for durability
/// - Paged BTree: Requires page-level undo/redo logging
///
/// This trait provides a uniform interface for transaction operations across engines.
///
/// # Design Note: Indexes as Specialty Tables
///
/// Both tables and indexes are treated uniformly at the transaction layer using ObjectId.
/// The transaction layer does NOT automatically maintain indexes - that responsibility
/// belongs to the API consumer (e.g., the Database layer or query engine).
///
/// This design:
/// - Keeps the transaction layer simple and focused on ACID properties
/// - Allows flexible index maintenance strategies (synchronous, deferred, async)
/// - Enables custom index types without transaction layer changes
/// - Makes index updates explicit and visible in the transaction's write set
pub trait TransactionOps {
    /// Get the transaction ID.
    fn id(&self) -> TransactionId;

    /// Get the isolation level of this transaction.
    fn isolation_level(&self) -> IsolationLevel;

    /// Get the snapshot LSN at which this transaction reads.
    fn snapshot_lsn(&self) -> LogSequenceNumber;

    /// Get a value from an object (table or index).
    ///
    /// Both tables and indexes are treated uniformly using ObjectId.
    /// The transaction layer does not distinguish between them.
    fn get(&self, object: ObjectId, key: &[u8]) -> TransactionResult<Option<ValueBuf>>;

    /// Put a key-value pair into an object (table or index).
    ///
    /// For indexes, the caller is responsible for maintaining consistency
    /// with the parent table. The transaction layer does not automatically
    /// update indexes when table data changes.
    fn put(&mut self, object: ObjectId, key: &[u8], value: &[u8]) -> TransactionResult<()>;

    /// Delete a key from an object (table or index).
    ///
    /// For indexes, the caller is responsible for maintaining consistency
    /// with the parent table.
    fn delete(&mut self, object: ObjectId, key: &[u8]) -> TransactionResult<bool>;

    /// Delete a range of keys from an object (table or index).
    fn range_delete(&mut self, object: ObjectId, bounds: ScanBounds) -> TransactionResult<u64>;

    /// Commit the transaction.
    fn commit(self) -> TransactionResult<CommitInfo>;

    /// Rollback the transaction.
    fn rollback(self) -> TransactionResult<()>;
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

impl TransactionState {
    fn as_str(&self) -> &'static str {
        match self {
            TransactionState::Active => "Active",
            TransactionState::Preparing => "Preparing",
            TransactionState::Committed => "Committed",
            TransactionState::Aborted => "Aborted",
        }
    }
}

/// Transaction struct for managing database transactions.
pub struct Transaction {
    // Core transaction identity and isolation
    txn_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
    isolation: IsolationLevel,
    state: TransactionState,

    // For Serializable isolation: track all keys read to detect read-write conflicts
    // Only populated when isolation == Serializable
    read_set: HashSet<(ObjectId, Vec<u8>)>,

    // Track all writes for commit/rollback
    // (ObjectId, Key) -> Option<Value> mapping of all mutations in this transaction
    // None represents a delete (tombstone), Some(value) represents a put
    write_set: HashMap<(ObjectId, Vec<u8>), Option<Vec<u8>>>,

    // Shared conflict detector for coordinating with other transactions
    conflict_detector: Arc<Mutex<ConflictDetector>>,
}

impl Transaction {
    /// Create a new transaction with the given ID, snapshot LSN, isolation level, and conflict detector.
    pub fn new(
        txn_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
        isolation: IsolationLevel,
        conflict_detector: Arc<Mutex<ConflictDetector>>,
    ) -> Self {
        Self {
            txn_id,
            snapshot_lsn,
            isolation,
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            conflict_detector,
        }
    }

    /// Record a read operation for conflict detection.
    ///
    /// Called by get() to track reads for Serializable isolation.
    /// Only tracks reads when isolation level is Serializable.
    pub fn record_read(&mut self, object_id: ObjectId, key: Vec<u8>) {
        if self.isolation == IsolationLevel::Serializable {
            self.read_set.insert((object_id, key));
        }
    }

    /// Record a write operation for commit/rollback.
    ///
    /// Called by put() to track writes for commit/rollback.
    pub fn record_write(&mut self, object_id: ObjectId, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.insert((object_id, key), Some(value));
    }

    /// Record a delete operation for commit/rollback.
    ///
    /// Called by delete() to track deletes for commit/rollback.
    pub fn record_delete(&mut self, object_id: ObjectId, key: Vec<u8>) {
        self.write_set.insert((object_id, key), None);
    }

    /// Prepare the transaction for commit (two-phase commit).
    ///
    /// Transitions from Active to Preparing state.
    pub fn prepare(&mut self) -> TransactionResult<()> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "prepare",
            ));
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

    /// Get a value from an object (table or index).
    ///
    /// This method first checks the transaction's write set for uncommitted changes,
    /// then would delegate to the underlying storage engine for committed data.
    ///
    /// Both tables and indexes are treated uniformly using ObjectId.
    ///
    /// # Implementation Note
    ///
    /// Currently returns values from the write set only. Full implementation requires
    /// integration with storage engines to read committed data at the snapshot LSN.
    pub fn get(&self, object: ObjectId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "put",
            ));
        }

        let object_id = object;

        // Check write set first for uncommitted changes
        let write_key = (object_id, key.to_vec());
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

    /// Put a key-value pair into an object (table or index).
    ///
    /// Records the write in the transaction's write set. The change is not visible
    /// to other transactions until commit.
    ///
    /// For indexes, the caller is responsible for maintaining consistency with
    /// the parent table. The transaction layer does not automatically update indexes.
    pub fn put(&mut self, object: ObjectId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "delete",
            ));
        }

        let object_id = object;

        // Check for write-write conflicts and acquire lock
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.check_write_conflict(object_id, key, self.txn_id)?;
        detector.acquire_write_lock(object_id, key.to_vec(), self.txn_id);
        drop(detector);

        // Record the write in the write set
        self.record_write(object_id, key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Delete a key from an object (table or index).
    ///
    /// Records the deletion in the transaction's write set. Returns true if the key
    /// existed (either in the write set or would exist in the underlying storage).
    ///
    /// For indexes, the caller is responsible for maintaining consistency with
    /// the parent table.
    pub fn delete(&mut self, object: ObjectId, key: &[u8]) -> TransactionResult<bool> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "get",
            ));
        }

        let object_id = object;

        // Check for write-write conflicts and acquire lock
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.check_write_conflict(object_id, key, self.txn_id)?;
        detector.acquire_write_lock(object_id, key.to_vec(), self.txn_id);
        drop(detector);

        let write_key = (object_id, key.to_vec());
        
        // Check if key exists in write set
        let existed = self.write_set.contains_key(&write_key);
        
        // Record the deletion
        self.record_delete(object_id, key.to_vec());
        
        // TODO: In full implementation, also check if key exists in underlying table
        // For now, return true if it was in the write set
        Ok(existed)
    }

    /// Delete a range of keys from an object (table or index).
    ///
    /// # Implementation Note
    ///
    /// Currently not implemented. Full implementation requires:
    /// 1. Scanning the object at snapshot_lsn to find matching keys
    /// 2. Recording each deletion in the write set
    /// 3. Handling the interaction between range bounds and existing writes
    pub fn range_delete(&mut self, object: ObjectId, bounds: ScanBounds) -> TransactionResult<u64> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "scan",
            ));
        }

        // TODO: Implement range delete
        // This requires:
        // 1. Access to storage engine to scan for keys in range
        // 2. Recording each deletion in write set
        // 3. Counting deleted keys
        let _ = (object, bounds);
        Err(TransactionError::Other(
            "range_delete not yet implemented - requires storage engine integration".to_string(),
        ))
    }

    /// Commit the transaction.
    ///
    /// Transitions to Committed state and returns commit information.
    ///
    /// # Implementation Note
    ///
    /// Currently validates state, performs conflict detection, and releases locks.
    /// Full implementation requires:
    /// 1. Writing changes to WAL
    /// 2. Applying write set to table engines
    pub fn commit(mut self) -> TransactionResult<CommitInfo> {
        // Validate state - must be Active or Preparing
        if self.state != TransactionState::Active && self.state != TransactionState::Preparing {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "commit",
            ));
        }

        // For Serializable isolation, check for read-write conflicts
        if self.isolation == IsolationLevel::Serializable {
            let detector = self.conflict_detector.lock().unwrap();
            detector.check_read_write_conflicts(&self.read_set, self.txn_id)?;
        }

        // Transition to Committed state
        self.state = TransactionState::Committed;

        // TODO: Full implementation needs to:
        // 1. Write commit record to WAL
        // 2. Apply write_set to table engines
        
        // Release all locks held by this transaction
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.release_locks(self.txn_id);
        drop(detector);

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
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "rollback",
            ));
        }

        // Transition to Aborted state
        self.state = TransactionState::Aborted;

        // Release all locks held by this transaction
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.release_locks(self.txn_id);
        drop(detector);

        // Write set is automatically dropped when self is consumed
        // TODO: In full implementation, also need to:
        // 1. Write abort record to WAL (optional, for diagnostics)
        
        Ok(())
    }
}

/// Implement the TransactionOps trait for Transaction.
impl TransactionOps for Transaction {
    fn id(&self) -> TransactionId {
        self.txn_id
    }

    fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    fn get(&self, object: ObjectId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        Transaction::get(self, object, key)
    }

    fn put(&mut self, object: ObjectId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
        Transaction::put(self, object, key, value)
    }

    fn delete(&mut self, object: ObjectId, key: &[u8]) -> TransactionResult<bool> {
        Transaction::delete(self, object, key)
    }

    fn range_delete(&mut self, object: ObjectId, bounds: ScanBounds) -> TransactionResult<u64> {
        Transaction::range_delete(self, object, bounds)
    }

    fn commit(self) -> TransactionResult<CommitInfo> {
        Transaction::commit(self)
    }

    fn rollback(self) -> TransactionResult<()> {
        Transaction::rollback(self)
    }
}
