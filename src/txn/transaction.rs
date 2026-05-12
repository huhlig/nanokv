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

use crate::table::{PointLookup, TableEngineRegistry};
use crate::txn::{ConflictDetector, TransactionError, TransactionResult};
use crate::types::{Durability, IsolationLevel, ObjectId, ScanBounds, ValueBuf};
use crate::vfs::FileSystem;
use crate::wal::{LogSequenceNumber, WalWriter, WriteOpType};
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::{Arc, Mutex, RwLock};

/// Transaction ID type
#[derive(
    Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize,
)]
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
pub struct Transaction<FS: FileSystem> {
    // Core transaction identity and isolation
    txn_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
    isolation: IsolationLevel,
    durability: Durability,
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

    // WAL writer for durability
    wal: Arc<WalWriter<FS>>,

    // Engine registry for reading/writing to actual storage engines
    engine_registry: Arc<TableEngineRegistry<FS>>,

    // Current LSN (shared with Database)
    current_lsn: Arc<RwLock<LogSequenceNumber>>,
}

impl<FS: FileSystem> Transaction<FS> {
    /// Create a new transaction with the given ID, snapshot LSN, isolation level, durability policy, and shared resources.
    pub fn new(
        txn_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
        isolation: IsolationLevel,
        durability: Durability,
        conflict_detector: Arc<Mutex<ConflictDetector>>,
        wal: Arc<WalWriter<FS>>,
        engine_registry: Arc<TableEngineRegistry<FS>>,
        current_lsn: Arc<RwLock<LogSequenceNumber>>,
    ) -> Self {
        // Write BEGIN record to WAL to register the transaction
        let _ = wal.write_begin(txn_id);

        Self {
            txn_id,
            snapshot_lsn,
            isolation,
            durability,
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            conflict_detector,
            wal,
            engine_registry,
            current_lsn,
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
    /// then reads from the underlying storage engine for committed data.
    ///
    /// Both tables and indexes are treated uniformly using ObjectId.
    pub fn get(&self, object: ObjectId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "get",
            ));
        }

        let object_id = object;

        // Check write set first for uncommitted changes
        let write_key = (object_id, key.to_vec());
        if let Some(value_opt) = self.write_set.get(&write_key) {
            // Found in write set - return the value or None if deleted
            return Ok(value_opt.as_ref().map(|v| ValueBuf(v.clone())));
        }

        // Read from committed storage via engine registry
        if let Some(engine) = self.engine_registry.get(object_id) {
            use crate::table::{PointLookup, Table, TableEngineInstance};

            // Get a reader for the snapshot and use it to read the value
            let result = match &engine {
                TableEngineInstance::PagedBTree(btree) => {
                    let reader = Table::reader(btree.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get BTree reader: {}", e))
                    })?;
                    reader
                        .get(key, self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("BTree get failed: {}", e)))?
                }
                TableEngineInstance::LsmTree(lsm) => {
                    let reader = Table::reader(lsm.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get LSM reader: {}", e))
                    })?;
                    reader
                        .get(key, self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("LSM get failed: {}", e)))?
                }
                TableEngineInstance::MemoryBTree(mem) => {
                    let reader = Table::reader(mem.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get Memory BTree reader: {}", e))
                    })?;
                    reader.get(key, self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Memory BTree get failed: {}", e))
                    })?
                }
                TableEngineInstance::MemoryBlob(blob) => {
                    let reader = Table::reader(blob.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get Memory Blob reader: {}", e))
                    })?;
                    reader.get(key, self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Memory Blob get failed: {}", e))
                    })?
                }
            };
            return Ok(result);
        }

        // Table not found in registry - key not found
        Ok(None)
    }

    /// Put a key-value pair into an object (table or index).
    ///
    /// Records the write in the transaction's write set and WAL. The change is not visible
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
                "put",
            ));
        }

        let object_id = object;

        // Check for write-write conflicts and acquire lock
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.check_write_conflict(object_id, key, self.txn_id)?;
        detector.acquire_write_lock(object_id, key.to_vec(), self.txn_id);
        drop(detector);

        // Write to WAL
        self.wal
            .write_operation(
                self.txn_id,
                object_id,
                WriteOpType::Put,
                key.to_vec(),
                value.to_vec(),
            )
            .map_err(|e| TransactionError::Other(format!("WAL write failed: {}", e)))?;

        // Record the write in the write set
        self.record_write(object_id, key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Delete a key from an object (table or index).
    ///
    /// Records the deletion in the transaction's write set and WAL. Returns true if the key
    /// existed (either in the write set or in the underlying storage).
    ///
    /// For indexes, the caller is responsible for maintaining consistency with
    /// the parent table.
    pub fn delete(&mut self, object: ObjectId, key: &[u8]) -> TransactionResult<bool> {
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

        let write_key = (object_id, key.to_vec());

        // Check if key exists in write set or storage
        let existed = if self.write_set.contains_key(&write_key) {
            true
        } else {
            // Check in engine registry
            if let Some(engine) = self.engine_registry.get(object_id) {
                use crate::table::{PointLookup, Table, TableEngineInstance};

                match &engine {
                    TableEngineInstance::PagedBTree(btree) => {
                        let reader =
                            Table::reader(btree.as_ref(), self.snapshot_lsn).map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get BTree reader: {}",
                                    e
                                ))
                            })?;
                        reader.contains(key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("BTree contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::LsmTree(lsm) => {
                        let reader =
                            Table::reader(lsm.as_ref(), self.snapshot_lsn).map_err(|e| {
                                TransactionError::Other(format!("Failed to get LSM reader: {}", e))
                            })?;
                        reader.contains(key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("LSM contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::MemoryBTree(mem) => {
                        let reader =
                            Table::reader(mem.as_ref(), self.snapshot_lsn).map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get Memory BTree reader: {}",
                                    e
                                ))
                            })?;
                        reader.contains(key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Memory BTree contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::MemoryBlob(blob) => {
                        let reader =
                            Table::reader(blob.as_ref(), self.snapshot_lsn).map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get Memory Blob reader: {}",
                                    e
                                ))
                            })?;
                        reader.contains(key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Memory Blob contains failed: {}", e))
                        })?
                    }
                }
            } else {
                false
            }
        };

        // Write to WAL
        self.wal
            .write_operation(
                self.txn_id,
                object_id,
                WriteOpType::Delete,
                key.to_vec(),
                vec![],
            )
            .map_err(|e| TransactionError::Other(format!("WAL write failed: {}", e)))?;

        // Record the deletion
        self.record_delete(object_id, key.to_vec());

        Ok(existed)
    }

    /// Delete a range of keys from an object (table or index).
    ///
    /// # Implementation Note
    ///
    /// Currently not implemented. Full implementation requires:
    /// 1. Scanning the object at snapshot_lsn to find matching keys
    /// 2. Recording each deletion in the write set and WAL
    /// 3. Handling the interaction between range bounds and existing writes
    pub fn range_delete(&mut self, object: ObjectId, bounds: ScanBounds) -> TransactionResult<u64> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "range_delete",
            ));
        }

        // TODO: Implement range delete
        // This requires:
        // 1. Access to storage engine to scan for keys in range
        // 2. Recording each deletion in write set and WAL
        // 3. Counting deleted keys
        let _ = (object, bounds);
        Err(TransactionError::Other(
            "range_delete not yet implemented - requires storage engine integration".to_string(),
        ))
    }

    /// Commit the transaction.
    ///
    /// Writes commit record to WAL, applies changes to storage engines, and releases locks.
    /// The durability policy controls how the commit is persisted:
    /// - MemoryOnly: No WAL writes (for in-memory tables only)
    /// - WalOnly: Write to WAL buffer but don't force sync
    /// - FlushOnCommit: Flush WAL buffer to OS but don't force disk sync
    /// - SyncOnCommit: Force sync to stable storage before returning
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

        // Write COMMIT record to WAL based on durability policy
        let commit_lsn = match self.durability {
            Durability::MemoryOnly => {
                // For memory-only durability, we still write to WAL for consistency
                // but we don't force any syncing
                self.wal
                    .write_commit(self.txn_id)
                    .map_err(|e| TransactionError::Other(format!("WAL commit failed: {}", e)))?
            }
            Durability::WalOnly => {
                // Write to WAL buffer but don't force sync
                self.wal
                    .write_commit(self.txn_id)
                    .map_err(|e| TransactionError::Other(format!("WAL commit failed: {}", e)))?
            }
            Durability::FlushOnCommit => {
                // Write commit record and flush buffer to OS
                let lsn = self
                    .wal
                    .write_commit(self.txn_id)
                    .map_err(|e| TransactionError::Other(format!("WAL commit failed: {}", e)))?;
                
                // Flush the WAL buffer to ensure data reaches the OS
                self.wal
                    .flush()
                    .map_err(|e| TransactionError::Other(format!("WAL flush failed: {}", e)))?;
                
                lsn
            }
            Durability::SyncOnCommit => {
                // Write commit record - this will automatically sync if sync_on_write is enabled
                // The write_commit method already handles syncing based on WAL config
                self.wal
                    .write_commit(self.txn_id)
                    .map_err(|e| TransactionError::Other(format!("WAL commit failed: {}", e)))?
            }
        };

        // Apply write set to storage engines
        use crate::table::{Flushable, MutableTable, Table, TableEngineInstance};

        for ((object_id, key), value_opt) in &self.write_set {
            if let Some(engine) = self.engine_registry.get(*object_id) {
                match &engine {
                    TableEngineInstance::PagedBTree(btree) => {
                        // Get a writer for this transaction
                        let mut writer = Table::writer(
                            btree.as_ref(),
                            self.txn_id,
                            self.snapshot_lsn,
                        )
                        .map_err(|e| {
                            TransactionError::Other(format!("Failed to get BTree writer: {}", e))
                        })?;

                        match value_opt {
                            Some(value) => {
                                MutableTable::put(&mut writer, key, value).map_err(|e| {
                                    TransactionError::Other(format!("BTree put failed: {}", e))
                                })?;
                            }
                            None => {
                                MutableTable::delete(&mut writer, key).map_err(|e| {
                                    TransactionError::Other(format!("BTree delete failed: {}", e))
                                })?;
                            }
                        }

                        // Flush the writer
                        Flushable::flush(&mut writer).map_err(|e| {
                            TransactionError::Other(format!("BTree flush failed: {}", e))
                        })?;
                    }
                    TableEngineInstance::LsmTree(lsm) => {
                        let mut writer = Table::writer(
                            lsm.as_ref(),
                            self.txn_id,
                            self.snapshot_lsn,
                        )
                        .map_err(|e| {
                            TransactionError::Other(format!("Failed to get LSM writer: {}", e))
                        })?;

                        match value_opt {
                            Some(value) => {
                                MutableTable::put(&mut writer, key, value).map_err(|e| {
                                    TransactionError::Other(format!("LSM put failed: {}", e))
                                })?;
                            }
                            None => {
                                MutableTable::delete(&mut writer, key).map_err(|e| {
                                    TransactionError::Other(format!("LSM delete failed: {}", e))
                                })?;
                            }
                        }

                        Flushable::flush(&mut writer).map_err(|e| {
                            TransactionError::Other(format!("LSM flush failed: {}", e))
                        })?;
                    }
                    TableEngineInstance::MemoryBTree(mem) => {
                        let mut writer =
                            Table::writer(mem.as_ref(), self.txn_id, self.snapshot_lsn).map_err(
                                |e| {
                                    TransactionError::Other(format!(
                                        "Failed to get Memory BTree writer: {}",
                                        e
                                    ))
                                },
                            )?;

                        match value_opt {
                            Some(value) => {
                                MutableTable::put(&mut writer, key, value).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory BTree put failed: {}",
                                        e
                                    ))
                                })?;
                            }
                            None => {
                                MutableTable::delete(&mut writer, key).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory BTree delete failed: {}",
                                        e
                                    ))
                                })?;
                            }
                        }

                        Flushable::flush(&mut writer).map_err(|e| {
                            TransactionError::Other(format!("Memory BTree flush failed: {}", e))
                        })?;

                        // Mark versions as committed so they become visible to readers
                        writer.commit_versions(commit_lsn).map_err(|e| {
                            TransactionError::Other(format!(
                                "Memory BTree commit_versions failed: {}",
                                e
                            ))
                        })?;
                    }
                    TableEngineInstance::MemoryBlob(blob) => {
                        let mut writer =
                            Table::writer(blob.as_ref(), self.txn_id, self.snapshot_lsn).map_err(
                                |e| {
                                    TransactionError::Other(format!(
                                        "Failed to get Memory Blob writer: {}",
                                        e
                                    ))
                                },
                            )?;

                        match value_opt {
                            Some(value) => {
                                MutableTable::put(&mut writer, key, value).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory Blob put failed: {}",
                                        e
                                    ))
                                })?;
                            }
                            None => {
                                MutableTable::delete(&mut writer, key).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory Blob delete failed: {}",
                                        e
                                    ))
                                })?;
                            }
                        }

                        Flushable::flush(&mut writer).map_err(|e| {
                            TransactionError::Other(format!("Memory Blob flush failed: {}", e))
                        })?;
                    }
                }
            }
            // If engine not found, skip (table may have been dropped)
        }

        // Update current LSN
        {
            let mut current_lsn = self.current_lsn.write().unwrap();
            *current_lsn = commit_lsn;
        }

        // Transition to Committed state
        self.state = TransactionState::Committed;

        // Release all locks held by this transaction
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.release_locks(self.txn_id);
        drop(detector);

        Ok(CommitInfo {
            tx_id: self.txn_id,
            commit_lsn,
            durable_lsn: Some(commit_lsn), // WAL is synced, so it's durable
        })
    }

    /// Rollback the transaction.
    ///
    /// Writes rollback record to WAL, discards all changes, and releases locks.
    pub fn rollback(mut self) -> TransactionResult<()> {
        // Can rollback from Active or Preparing state
        if self.state != TransactionState::Active && self.state != TransactionState::Preparing {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "rollback",
            ));
        }

        // Write ROLLBACK record to WAL
        self.wal
            .write_rollback(self.txn_id)
            .map_err(|e| TransactionError::Other(format!("WAL rollback failed: {}", e)))?;

        // Transition to Aborted state
        self.state = TransactionState::Aborted;

        // Release all locks held by this transaction
        let mut detector = self.conflict_detector.lock().unwrap();
        detector.release_locks(self.txn_id);
        drop(detector);

        // Write set is automatically dropped when self is consumed
        Ok(())
    }
}

/// Implement the TransactionOps trait for Transaction.
impl<FS: FileSystem> TransactionOps for Transaction<FS> {
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
