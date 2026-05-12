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

use crate::table::{
    ApproximateMembership, PointLookup, SearchableTable, SpecialtyTableCapabilities,
    SpecialtyTableStats, TableCursor, TableEngineRegistry, VerificationReport,
};
use crate::txn::{ConflictDetector, TransactionError, TransactionResult};
use crate::types::{Durability, IsolationLevel, TableId, ScanBounds, ValueBuf};
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
    fn get(&self, object: TableId, key: &[u8]) -> TransactionResult<Option<ValueBuf>>;

    /// Put a key-value pair into an object (table or index).
    ///
    /// For indexes, the caller is responsible for maintaining consistency
    /// with the parent table. The transaction layer does not automatically
    /// update indexes when table data changes.
    fn put(&mut self, object: TableId, key: &[u8], value: &[u8]) -> TransactionResult<()>;

    /// Delete a key from an object (table or index).
    ///
    /// For indexes, the caller is responsible for maintaining consistency
    /// with the parent table.
    fn delete(&mut self, object: TableId, key: &[u8]) -> TransactionResult<bool>;

    /// Delete a range of keys from an object (table or index).
    fn range_delete(&mut self, object: TableId, bounds: ScanBounds) -> TransactionResult<u64>;

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
    read_set: HashSet<(TableId, Vec<u8>)>,

    // Track all writes for commit/rollback
    // (ObjectId, Key) -> Option<Value> mapping of all mutations in this transaction
    // None represents a delete (tombstone), Some(value) represents a put
    write_set: HashMap<(TableId, Vec<u8>), Option<Vec<u8>>>,

    // Track specialty-table bloom inserts for commit/rollback visibility
    bloom_write_set: HashSet<(TableId, Vec<u8>)>,

    // Shared conflict detector for coordinating with other transactions
    conflict_detector: Arc<Mutex<ConflictDetector>>,

    // WAL writer for durability
    wal: Arc<WalWriter<FS>>,

    // Engine registry for reading/writing to actual storage engines
    engine_registry: Arc<TableEngineRegistry<FS>>,

    // Current LSN (shared with Database)
    current_lsn: Arc<RwLock<LogSequenceNumber>>,

    // Current table context for specialty table operations
    current_table_id: Option<TableId>,
    current_table_name: Option<String>,
}

impl<FS: FileSystem> Transaction<FS> {
    fn build(
        txn_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
        isolation: IsolationLevel,
        durability: Durability,
        conflict_detector: Arc<Mutex<ConflictDetector>>,
        wal: Arc<WalWriter<FS>>,
        engine_registry: Arc<TableEngineRegistry<FS>>,
        current_lsn: Arc<RwLock<LogSequenceNumber>>,
    ) -> Self {
        Self {
            txn_id,
            snapshot_lsn,
            isolation,
            durability,
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            bloom_write_set: HashSet::new(),
            conflict_detector,
            wal,
            engine_registry,
            current_lsn,
            current_table_id: None,
            current_table_name: None,
        }
    }

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

        Self::build(
            txn_id,
            snapshot_lsn,
            isolation,
            durability,
            conflict_detector,
            wal,
            engine_registry,
            current_lsn,
        )
    }

    /// Create a new read-only transaction without registering it as an active WAL writer transaction.
    pub fn new_read_only(
        txn_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
        isolation: IsolationLevel,
        conflict_detector: Arc<Mutex<ConflictDetector>>,
        wal: Arc<WalWriter<FS>>,
        engine_registry: Arc<TableEngineRegistry<FS>>,
        current_lsn: Arc<RwLock<LogSequenceNumber>>,
    ) -> Self {
        Self::build(
            txn_id,
            snapshot_lsn,
            isolation,
            Durability::WalOnly,
            conflict_detector,
            wal,
            engine_registry,
            current_lsn,
        )
    }

    /// Record a read operation for conflict detection.
    ///
    /// Called by get() to track reads for Serializable isolation.
    /// Only tracks reads when isolation level is Serializable.
    pub fn record_read(&mut self, object_id: TableId, key: Vec<u8>) {
        if self.isolation == IsolationLevel::Serializable {
            self.read_set.insert((object_id, key));
        }
    }

    /// Record a write operation for commit/rollback.
    ///
    /// Called by put() to track writes for commit/rollback.
    pub fn record_write(&mut self, object_id: TableId, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.insert((object_id, key), Some(value));
    }

    /// Record a delete operation for commit/rollback.
    ///
    /// Called by delete() to track deletes for commit/rollback.
    pub fn record_delete(&mut self, object_id: TableId, key: Vec<u8>) {
        self.write_set.insert((object_id, key), None);
    }

    /// Record a bloom filter insert for commit/rollback.
    pub fn record_bloom_insert(&mut self, object_id: TableId, key: Vec<u8>) {
        self.bloom_write_set.insert((object_id, key));
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

    /// Get the current table context for specialty table operations.
    pub fn current_table(&self) -> Option<(TableId, &str)> {
        self.current_table_id
            .zip(self.current_table_name.as_deref())
    }

    /// Set the current table context for specialty table operations.
    pub fn with_table(&mut self, table_id: TableId) -> &mut Self {
        self.current_table_id = Some(table_id);
        self.current_table_name = self
            .engine_registry
            .get(table_id)
            .map(|engine| engine.name().to_string());
        self
    }

    /// Clear the current table context for specialty table operations.
    pub fn clear_table_context(&mut self) {
        self.current_table_id = None;
        self.current_table_name = None;
    }

    /// Execute a scoped set of bloom filter operations using the transaction as
    /// an `ApproximateMembership` implementation.
    pub fn with_bloom<F, R>(&mut self, table_id: TableId, f: F) -> TransactionResult<R>
    where
        F: FnOnce(&mut dyn ApproximateMembership) -> crate::table::TableResult<R>,
    {
        self.with_table(table_id);
        let result = f(self);
        self.clear_table_context();
        result.map_err(|e| TransactionError::Other(e.to_string()))
    }

    /// Get a value from an object (table or index).
    ///
    /// This method first checks the transaction's write set for uncommitted changes,
    /// then reads from the underlying storage engine for committed data.
    ///
    /// Both tables and indexes are treated uniformly using ObjectId.
    pub fn get(&self, object: TableId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
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
            use crate::table::{PointLookup, SearchableTable, TableEngineInstance};

            // Get a reader for the snapshot and use trait-based dispatch
            let result = match &engine {
                TableEngineInstance::PagedBTree(btree) => {
                    let reader = SearchableTable::reader(btree.as_ref(), self.snapshot_lsn)
                        .map_err(|e| {
                            TransactionError::Other(format!("Failed to get BTree reader: {}", e))
                        })?;
                    PointLookup::get(&reader, key, self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("BTree get failed: {}", e)))?
                }
                TableEngineInstance::LsmTree(lsm) => {
                    let reader =
                        SearchableTable::reader(lsm.as_ref(), self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Failed to get LSM reader: {}", e))
                        })?;
                    PointLookup::get(&reader, key, self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("LSM get failed: {}", e)))?
                }
                TableEngineInstance::MemoryBTree(mem) => {
                    let reader =
                        SearchableTable::reader(mem.as_ref(), self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!(
                                "Failed to get Memory BTree reader: {}",
                                e
                            ))
                        })?;
                    PointLookup::get(&reader, key, self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Memory BTree get failed: {}", e))
                    })?
                }
                TableEngineInstance::MemoryHashTable(hash) => {
                    let reader = hash.reader(self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get Hash table reader: {}", e))
                    })?;
                    PointLookup::get(&reader, key, self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Hash table get failed: {}", e))
                    })?
                }
                TableEngineInstance::MemoryBlob(blob) => {
                    // Blob tables don't use reader/writer pattern, access directly
                    blob.get(key).map_err(|e| {
                        TransactionError::Other(format!("Memory Blob get failed: {}", e))
                    })?
                }
                TableEngineInstance::PagedBloomFilter(bloom) => {
                    // Bloom filters support approximate membership check
                    // Returns Some(empty) if probably present, None if definitely absent
                    if bloom.contains(key).map_err(|e| {
                        TransactionError::Other(format!("Bloom filter contains failed: {}", e))
                    })? {
                        Some(ValueBuf(vec![]))
                    } else {
                        None
                    }
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
    pub fn put(&mut self, object: TableId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
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
    pub fn delete(&mut self, object: TableId, key: &[u8]) -> TransactionResult<bool> {
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
                use crate::table::{PointLookup, SearchableTable, TableEngineInstance};

                match &engine {
                    TableEngineInstance::PagedBTree(btree) => {
                        let reader = SearchableTable::reader(btree.as_ref(), self.snapshot_lsn)
                            .map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get BTree reader: {}",
                                    e
                                ))
                            })?;
                        PointLookup::contains(&reader, key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("BTree contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::LsmTree(lsm) => {
                        let reader = SearchableTable::reader(lsm.as_ref(), self.snapshot_lsn)
                            .map_err(|e| {
                                TransactionError::Other(format!("Failed to get LSM reader: {}", e))
                            })?;
                        PointLookup::contains(&reader, key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("LSM contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::MemoryBTree(mem) => {
                        let reader = SearchableTable::reader(mem.as_ref(), self.snapshot_lsn)
                            .map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get Memory BTree reader: {}",
                                    e
                                ))
                            })?;
                        PointLookup::contains(&reader, key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Memory BTree contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::MemoryHashTable(hash) => {
                        let reader = hash.reader(self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Failed to get Hash table reader: {}", e))
                        })?;
                        PointLookup::contains(&reader, key, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Hash table contains failed: {}", e))
                        })?
                    }
                    TableEngineInstance::MemoryBlob(blob) => {
                        // Blob tables don't use reader pattern, check directly
                        blob.get(key)
                            .map_err(|e| {
                                TransactionError::Other(format!("Memory Blob get failed: {}", e))
                            })?
                            .is_some()
                    }
                    TableEngineInstance::PagedBloomFilter(bloom) => {
                        // Bloom filters support approximate membership check
                        bloom.contains(key)
                            .map_err(|e| {
                                TransactionError::Other(format!("Bloom filter contains failed: {}", e))
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
    /// Scans the object at the transaction snapshot, acquires per-key write locks,
    /// records delete operations in the WAL, and stores tombstones in the write set.
    /// Returns the number of keys matched by the snapshot-visible scan.
    pub fn range_delete(&mut self, object: TableId, bounds: ScanBounds) -> TransactionResult<u64> {
        // Check if transaction is still active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "range_delete",
            ));
        }

        let object_id = object;
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

        if let Some(engine) = self.engine_registry.get(object_id) {
            use crate::table::{OrderedScan, SearchableTable, TableEngineInstance};

            match &engine {
                TableEngineInstance::PagedBTree(btree) => {
                    let reader = SearchableTable::reader(btree.as_ref(), self.snapshot_lsn)
                        .map_err(|e| {
                            TransactionError::Other(format!("Failed to get BTree reader: {}", e))
                        })?;
                    let mut cursor = OrderedScan::scan(&reader, bounds.clone(), self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("BTree scan failed: {}", e)))?;

                    while cursor.valid() {
                        if let Some(key) = cursor.key() {
                            keys_to_delete.push(key.to_vec());
                        }
                        cursor.next().map_err(|e| {
                            TransactionError::Other(format!("BTree cursor next failed: {}", e))
                        })?;
                    }
                }
                TableEngineInstance::LsmTree(lsm) => {
                    let reader = SearchableTable::reader(lsm.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get LSM reader: {}", e))
                    })?;
                    let mut cursor = OrderedScan::scan(&reader, bounds.clone(), self.snapshot_lsn)
                        .map_err(|e| TransactionError::Other(format!("LSM scan failed: {}", e)))?;

                    while cursor.valid() {
                        if let Some(key) = cursor.key() {
                            keys_to_delete.push(key.to_vec());
                        }
                        cursor.next().map_err(|e| {
                            TransactionError::Other(format!("LSM cursor next failed: {}", e))
                        })?;
                    }
                }
                TableEngineInstance::MemoryBTree(mem) => {
                    let reader = SearchableTable::reader(mem.as_ref(), self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Failed to get Memory BTree reader: {}", e))
                    })?;
                    let mut cursor = OrderedScan::scan(&reader, bounds, self.snapshot_lsn).map_err(|e| {
                        TransactionError::Other(format!("Memory BTree scan failed: {}", e))
                    })?;

                    while cursor.valid() {
                        if let Some(key) = cursor.key() {
                            keys_to_delete.push(key.to_vec());
                        }
                        cursor.next().map_err(|e| {
                            TransactionError::Other(format!(
                                "Memory BTree cursor next failed: {}",
                                e
                            ))
                        })?;
                    }
                }
                TableEngineInstance::MemoryHashTable(_) => {
                    return Err(TransactionError::Other(
                        "range_delete is not supported for hash tables".to_string(),
                    ));
                }
                TableEngineInstance::MemoryBlob(_) => {
                    return Err(TransactionError::Other(
                        "range_delete is not supported for blob tables".to_string(),
                    ));
                }
                TableEngineInstance::PagedBloomFilter(_) => {
                    return Err(TransactionError::Other(
                        "range_delete is not supported for bloom filter tables".to_string(),
                    ));
                }
            }
        }

        for key in &keys_to_delete {
            let mut detector = self.conflict_detector.lock().unwrap();
            detector.check_write_conflict(object_id, key, self.txn_id)?;
            detector.acquire_write_lock(object_id, key.clone(), self.txn_id);
            drop(detector);

            self.wal
                .write_operation(
                    self.txn_id,
                    object_id,
                    WriteOpType::Delete,
                    key.clone(),
                    vec![],
                )
                .map_err(|e| TransactionError::Other(format!("WAL write failed: {}", e)))?;

            self.record_delete(object_id, key.clone());
        }

        Ok(keys_to_delete.len() as u64)
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
                        let mut writer =
                            SearchableTable::writer(btree.as_ref(), self.txn_id, self.snapshot_lsn)
                                .map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Failed to get BTree writer: {}",
                                        e
                                    ))
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
                        let mut writer =
                            SearchableTable::writer(lsm.as_ref(), self.txn_id, self.snapshot_lsn)
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
                            SearchableTable::writer(mem.as_ref(), self.txn_id, self.snapshot_lsn)
                                .map_err(|e| {
                                TransactionError::Other(format!(
                                    "Failed to get Memory BTree writer: {}",
                                    e
                                ))
                            })?;

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
                    TableEngineInstance::MemoryHashTable(hash) => {
                        let mut writer = hash.writer(self.txn_id, self.snapshot_lsn).map_err(|e| {
                            TransactionError::Other(format!("Failed to get Hash table writer: {}", e))
                        })?;

                        match value_opt {
                            Some(value) => {
                                MutableTable::put(&mut writer, key, value).map_err(|e| {
                                    TransactionError::Other(format!("Hash table put failed: {}", e))
                                })?;
                            }
                            None => {
                                MutableTable::delete(&mut writer, key).map_err(|e| {
                                    TransactionError::Other(format!("Hash table delete failed: {}", e))
                                })?;
                            }
                        }

                        Flushable::flush(&mut writer).map_err(|e| {
                            TransactionError::Other(format!("Hash table flush failed: {}", e))
                        })?;
                    }
                    TableEngineInstance::MemoryBlob(blob) => {
                        // Blob tables don't use writer pattern, access directly
                        match value_opt {
                            Some(value) => {
                                blob.put(key, value).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory Blob put failed: {}",
                                        e
                                    ))
                                })?;
                            }
                            None => {
                                blob.delete(key).map_err(|e| {
                                    TransactionError::Other(format!(
                                        "Memory Blob delete failed: {}",
                                        e
                                    ))
                                })?;
                            }
                        }
                        // No flush needed for blob tables (in-memory, no writer)
                    }
                    TableEngineInstance::PagedBloomFilter(_) => {
                        // Bloom filters don't support transactional put/delete operations
                        // They should be updated through their specialized ApproximateMembership API
                        return Err(TransactionError::Other(
                            "transactional put/delete is not supported for bloom filter tables; use ApproximateMembership API"
                                .to_string(),
                        ));
                    }
                }
            }
            // If engine not found, skip (table may have been dropped)
        }

        // Apply bloom filter inserts after generic KV writes.
        for (object_id, key) in &self.bloom_write_set {
            if let Some(engine) = self.engine_registry.get(*object_id) {
                match &engine {
                    crate::table::TableEngineInstance::PagedBloomFilter(bloom) => {
                        bloom
                            .insert(key)
                            .map_err(|e| TransactionError::Other(format!("Bloom insert failed: {}", e)))?;
                    }
                    _ => {
                        return Err(TransactionError::Other(
                            "table is not a bloom filter".to_string(),
                        ))
                    }
                }
            }
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

impl<FS: FileSystem> ApproximateMembership for Transaction<FS> {
    fn table_id(&self) -> TableId {
        self.current_table_id.unwrap_or(TableId::from(0))
    }

    fn name(&self) -> &str {
        self.current_table_name.as_deref().unwrap_or("unknown")
    }

    fn capabilities(&self) -> SpecialtyTableCapabilities {
        let Some(table_id) = self.current_table_id else {
            return SpecialtyTableCapabilities::default();
        };

        match self.engine_registry.get(table_id) {
            Some(crate::table::TableEngineInstance::PagedBloomFilter(bloom)) => {
                ApproximateMembership::capabilities(bloom.as_ref())
            }
            _ => SpecialtyTableCapabilities::default(),
        }
    }

    fn insert_key(&mut self, key: &[u8]) -> crate::table::TableResult<()> {
        if !self.is_active() {
            return Err(crate::table::TableError::Other(format!(
                "transaction {} is not active for insert_key",
                self.txn_id
            )));
        }

        let table_id = self.current_table_id.ok_or_else(|| {
            crate::table::TableError::Other(
                "no table context set for bloom operation".to_string(),
            )
        })?;

        self.wal
            .write_operation(
                self.txn_id,
                table_id,
                WriteOpType::BloomInsert,
                key.to_vec(),
                vec![],
            )
            .map_err(|e| crate::table::TableError::Other(format!("WAL write failed: {}", e)))?;

        self.record_bloom_insert(table_id, key.to_vec());
        Ok(())
    }

    fn might_contain(&self, key: &[u8]) -> crate::table::TableResult<bool> {
        if !self.is_active() {
            return Err(crate::table::TableError::Other(format!(
                "transaction {} is not active for might_contain",
                self.txn_id
            )));
        }

        let table_id = self.current_table_id.ok_or_else(|| {
            crate::table::TableError::Other(
                "no table context set for bloom operation".to_string(),
            )
        })?;

        if self.bloom_write_set.contains(&(table_id, key.to_vec())) {
            return Ok(true);
        }

        match self.engine_registry.get(table_id) {
            Some(crate::table::TableEngineInstance::PagedBloomFilter(bloom)) => {
                bloom.might_contain(key)
            }
            Some(_) => Err(crate::table::TableError::Other(
                "table is not a bloom filter".to_string(),
            )),
            None => Ok(false),
        }
    }

    fn false_positive_rate(&self) -> Option<f64> {
        let table_id = self.current_table_id?;
        match self.engine_registry.get(table_id) {
            Some(crate::table::TableEngineInstance::PagedBloomFilter(bloom)) => {
                Some(bloom.false_positive_rate())
            }
            _ => None,
        }
    }

    fn stats(&self) -> crate::table::TableResult<SpecialtyTableStats> {
        let table_id = self.current_table_id.ok_or_else(|| {
            crate::table::TableError::Other(
                "no table context set for bloom operation".to_string(),
            )
        })?;

        match self.engine_registry.get(table_id) {
            Some(crate::table::TableEngineInstance::PagedBloomFilter(bloom)) => bloom.stats(),
            Some(_) => Err(crate::table::TableError::Other(
                "table is not a bloom filter".to_string(),
            )),
            None => Err(crate::table::TableError::Other(
                "table not found".to_string(),
            )),
        }
    }

    fn verify(&self) -> crate::table::TableResult<VerificationReport> {
        let table_id = self.current_table_id.ok_or_else(|| {
            crate::table::TableError::Other(
                "no table context set for bloom operation".to_string(),
            )
        })?;

        match self.engine_registry.get(table_id) {
            Some(crate::table::TableEngineInstance::PagedBloomFilter(bloom)) => bloom.verify(),
            Some(_) => Err(crate::table::TableError::Other(
                "table is not a bloom filter".to_string(),
            )),
            None => Err(crate::table::TableError::Other(
                "table not found".to_string(),
            )),
        }
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

    fn get(&self, object: TableId, key: &[u8]) -> TransactionResult<Option<ValueBuf>> {
        Transaction::get(self, object, key)
    }

    fn put(&mut self, object: TableId, key: &[u8], value: &[u8]) -> TransactionResult<()> {
        Transaction::put(self, object, key, value)
    }

    fn delete(&mut self, object: TableId, key: &[u8]) -> TransactionResult<bool> {
        Transaction::delete(self, object, key)
    }

    fn range_delete(&mut self, object: TableId, bounds: ScanBounds) -> TransactionResult<u64> {
        Transaction::range_delete(self, object, bounds)
    }

    fn commit(self) -> TransactionResult<CommitInfo> {
        Transaction::commit(self)
    }

    fn rollback(self) -> TransactionResult<()> {
        Transaction::rollback(self)
    }
}
