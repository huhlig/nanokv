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

//! Top-level embedded database implementation.
//!
//! This module provides the main `Database` struct that owns the catalog, file allocation,
//! transaction manager, WAL, and registered table/index engines. ACID semantics are coordinated
//! at this layer.

use crate::index::{IndexId, IndexInfo, IndexOptions};
use crate::snap::{Snapshot, SnapshotId};
use crate::table::{TableId, TableInfo, TableOptions};
use crate::txn::{ConflictDetector, Transaction, TransactionId};
use crate::types::{ConsistencyGuarantees, Durability, IsolationLevel};
use crate::wal::LogSequenceNumber;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Top-level embedded database.
///
/// This struct owns the catalog, file allocation, transaction manager, WAL,
/// and registered table/index engines. ACID semantics should be coordinated at
/// this layer rather than by independently stacking transactional wrappers around
/// individual tables.
pub struct Database {
    // Transaction management
    /// Shared conflict detector for coordinating transactions
    conflict_detector: Arc<Mutex<ConflictDetector>>,
    
    /// Next transaction ID to allocate
    next_txn_id: Arc<Mutex<u64>>,
    
    /// Current LSN for snapshot isolation
    current_lsn: Arc<RwLock<LogSequenceNumber>>,
    
    // Catalog management
    /// Table catalog: maps table names to table IDs and metadata
    table_catalog: Arc<RwLock<HashMap<String, TableInfo>>>,
    
    /// Index catalog: maps index IDs to index metadata
    index_catalog: Arc<RwLock<HashMap<IndexId, IndexInfo>>>,
    
    // Storage layer (to be implemented)
    // TODO: Add fields for:
    // - WAL writer/reader
    // - Pager for page-level storage
    // - Table engines registry
    // - Snapshot manager
}

impl Database {
    /// Create a new database instance.
    pub fn new() -> Self {
        Self {
            conflict_detector: Arc::new(Mutex::new(ConflictDetector::new())),
            next_txn_id: Arc::new(Mutex::new(1)),
            current_lsn: Arc::new(RwLock::new(LogSequenceNumber::from(0))),
            table_catalog: Arc::new(RwLock::new(HashMap::new())),
            index_catalog: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Allocate a new transaction ID.
    fn allocate_txn_id(&self) -> TransactionId {
        let mut next_id = self.next_txn_id.lock().unwrap();
        let txn_id = TransactionId::from(*next_id);
        *next_id += 1;
        txn_id
    }

    /// Begin a read-only transaction using the latest stable snapshot.
    pub fn begin_read(&self) -> Result<Transaction, DatabaseError> {
        let txn_id = self.allocate_txn_id();
        let snapshot_lsn = *self.current_lsn.read().unwrap();
        Ok(Transaction::new(
            txn_id,
            snapshot_lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
        ))
    }

    /// Begin a write transaction with the requested durability policy.
    pub fn begin_write(&self, durability: Durability) -> Result<Transaction, DatabaseError> {
        let _ = durability; // TODO: Use durability policy
        let txn_id = self.allocate_txn_id();
        let snapshot_lsn = *self.current_lsn.read().unwrap();
        Ok(Transaction::new(
            txn_id,
            snapshot_lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
        ))
    }

    /// Begin a read-only transaction at a specific snapshot LSN.
    ///
    /// This is useful for reading from named snapshots or implementing
    /// time-travel queries. Returns an error if the LSN is not available
    /// (e.g., too old and already garbage collected).
    pub fn begin_read_at(&self, lsn: LogSequenceNumber) -> Result<Transaction, DatabaseError> {
        let txn_id = self.allocate_txn_id();
        // TODO: Validate that LSN is still available
        Ok(Transaction::new(
            txn_id,
            lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
        ))
    }

    /// Create a logical table using a chosen physical engine.
    ///
    /// This operation is transactional - the table becomes visible only after
    /// the current LSN advances (simulating a commit).
    pub fn create_table(
        &self,
        name: &str,
        options: TableOptions,
    ) -> Result<TableId, DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Check if table already exists
        if catalog.contains_key(name) {
            return Err(DatabaseError {
                message: format!("Table '{}' already exists", name),
            });
        }

        // Allocate new table ID
        let table_id = TableId::from(catalog.len() as u64 + 1);
        
        // Get current LSN for creation timestamp
        let created_lsn = *self.current_lsn.read().unwrap();
        
        // Create table info
        let table_info = TableInfo {
            id: table_id,
            name: name.to_string(),
            options,
            root: None, // No root page yet
            created_lsn,
        };
        
        // Add to catalog
        catalog.insert(name.to_string(), table_info);
        
        Ok(table_id)
    }

    /// Drop a logical table and its dependent indexes.
    ///
    /// This operation is transactional - the table becomes invisible only after
    /// the current LSN advances (simulating a commit).
    pub fn drop_table(&self, table: TableId) -> Result<(), DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Find and remove the table
        let table_name = catalog
            .iter()
            .find(|(_, info)| info.id == table)
            .map(|(name, _)| name.clone());
        
        if let Some(name) = table_name {
            catalog.remove(&name);
            
            // TODO: Also remove dependent indexes from index_catalog
            
            Ok(())
        } else {
            Err(DatabaseError {
                message: format!("Table {:?} not found", table),
            })
        }
    }

    /// Open an existing table by name.
    pub fn open_table(&self, name: &str) -> Result<Option<TableId>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.get(name).map(|info| info.id))
    }

    /// Return catalog-visible tables.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.values().cloned().collect())
    }

    /// Create an index over a table.
    ///
    /// This operation is transactional - the index becomes visible only after
    /// the current LSN advances (simulating a commit).
    pub fn create_index(
        &self,
        table: TableId,
        name: &str,
        options: IndexOptions,
    ) -> Result<IndexId, DatabaseError> {
        let mut catalog = self.index_catalog.write().unwrap();
        
        // Allocate new index ID
        let index_id = IndexId::from(catalog.len() as u64 + 1);
        
        // Get current LSN for creation timestamp
        let created_lsn = *self.current_lsn.read().unwrap();
        
        // Create index info
        let index_info = IndexInfo {
            id: index_id,
            table_id: table,
            name: name.to_string(),
            options,
            root: None, // No root page yet
            created_lsn,
            stale: false, // New index starts fresh
        };
        
        // Add to catalog
        catalog.insert(index_id, index_info);
        
        Ok(index_id)
    }

    /// Drop an index.
    ///
    /// This operation is transactional - the index becomes invisible only after
    /// the current LSN advances (simulating a commit).
    pub fn drop_index(&self, index: IndexId) -> Result<(), DatabaseError> {
        let mut catalog = self.index_catalog.write().unwrap();
        
        if catalog.remove(&index).is_some() {
            Ok(())
        } else {
            Err(DatabaseError {
                message: format!("Index {:?} not found", index),
            })
        }
    }

    /// Return catalog-visible indexes for a table.
    pub fn list_indexes(&self, table: TableId) -> Result<Vec<IndexInfo>, DatabaseError> {
        let catalog = self.index_catalog.read().unwrap();
        Ok(catalog
            .values()
            .filter(|info| info.table_id == table)
            .cloned()
            .collect())
    }

    /// Create a named snapshot at the current LSN.
    ///
    /// The snapshot pins necessary pages/segments to enable consistent reads
    /// at the snapshot LSN. Snapshots must be explicitly released to free
    /// resources.
    pub fn create_snapshot(&self, name: &str) -> Result<Snapshot, DatabaseError> {
        todo!("Create a named snapshot at the current LSN")
    }

    /// List all active snapshots.
    pub fn list_snapshots(&self) -> Result<Vec<Snapshot>, DatabaseError> {
        todo!("Return a list of all active snapshots")
    }

    /// Release a snapshot, allowing its resources to be reclaimed.
    ///
    /// After releasing, the snapshot LSN may no longer be available for reads.
    pub fn release_snapshot(&self, snapshot_id: SnapshotId) -> Result<(), DatabaseError> {
        todo!("Release the specified snapshot and reclaim its resources")
    }

    /// Get the consistency guarantees provided by this database.
    ///
    /// This documents the ACID properties, isolation levels, and crash
    /// recovery semantics. Query planners and applications can use this
    /// to make informed decisions about transaction boundaries and
    /// error handling.
    pub fn consistency_guarantees(&self) -> ConsistencyGuarantees {
        // Conservative default
        ConsistencyGuarantees {
            atomicity: true,
            consistency: true,
            isolation: IsolationLevel::ReadCommitted,
            durability: Durability::WalOnly,
            crash_safe: false,
            point_in_time_recovery: false,
        }
    }
}

/// Database error type.
#[derive(Debug)]
pub struct DatabaseError {
    pub message: String,
}

impl Default for DatabaseError {
    fn default() -> Self {
        Self {
            message: "Unknown database error".to_string(),
        }
    }
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database error: {}", self.message)
    }
}

impl std::error::Error for DatabaseError {}
