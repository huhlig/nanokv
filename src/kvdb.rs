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

use crate::snap::{Snapshot, SnapshotId};
use crate::table::{TableInfo, TableOptions, TableKind, IndexKind, IndexField, IndexConsistency};
use crate::txn::{ConflictDetector, Transaction, TransactionId};
use crate::types::ObjectId;
use crate::types::{ConsistencyGuarantees, Durability, IsolationLevel};
use crate::vfs::FileSystem;
use crate::wal::{LogSequenceNumber, WalWriter, WalWriterConfig, WriteOpType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Top-level embedded database.
///
/// This struct owns the catalog, file allocation, transaction manager, WAL,
/// and registered table/index engines. ACID semantics should be coordinated at
/// this layer rather than by independently stacking transactional wrappers around
/// individual tables.
pub struct Database<FS: FileSystem> {
    // Transaction management
    /// Shared conflict detector for coordinating transactions
    conflict_detector: Arc<Mutex<ConflictDetector>>,
    
    /// Next transaction ID to allocate
    next_txn_id: Arc<Mutex<u64>>,
    
    /// Current LSN for snapshot isolation
    current_lsn: Arc<RwLock<LogSequenceNumber>>,
    
    // Catalog management
    /// Unified catalog: maps table/index names to their metadata
    /// Both regular tables and indexes are stored here
    table_catalog: Arc<RwLock<HashMap<String, TableInfo>>>,
    
    // Storage layer
    /// Write-ahead log for durability
    wal: Arc<WalWriter<FS>>,
    
    /// Table engine storage (ObjectId -> in-memory representation)
    /// In a full implementation, this would be a registry of different engine types
    /// For now, we use a simple HashMap to store table data
    table_storage: Arc<RwLock<HashMap<ObjectId, HashMap<Vec<u8>, Vec<u8>>>>>,
}

impl<FS: FileSystem> Database<FS> {
    /// Create a new database instance with the given filesystem and WAL path.
    pub fn new(fs: &FS, wal_path: &str) -> Result<Self, DatabaseError> {
        let wal_config = WalWriterConfig::default();
        let wal = WalWriter::create(fs, wal_path, wal_config)
            .map_err(|e| DatabaseError {
                message: format!("Failed to create WAL: {}", e),
            })?;
        
        Ok(Self {
            conflict_detector: Arc::new(Mutex::new(ConflictDetector::new())),
            next_txn_id: Arc::new(Mutex::new(1)),
            current_lsn: Arc::new(RwLock::new(LogSequenceNumber::from(0))),
            table_catalog: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(wal),
            table_storage: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Open an existing database instance.
    pub fn open(fs: &FS, wal_path: &str) -> Result<Self, DatabaseError> {
        let wal_config = WalWriterConfig::default();
        let wal = WalWriter::open(fs, wal_path, wal_config)
            .map_err(|e| DatabaseError {
                message: format!("Failed to open WAL: {}", e),
            })?;
        
        // Get current LSN from WAL
        let current_lsn = wal.current_lsn();
        
        Ok(Self {
            conflict_detector: Arc::new(Mutex::new(ConflictDetector::new())),
            next_txn_id: Arc::new(Mutex::new(1)),
            current_lsn: Arc::new(RwLock::new(current_lsn)),
            table_catalog: Arc::new(RwLock::new(HashMap::new())),
            wal: Arc::new(wal),
            table_storage: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Allocate a new transaction ID.
    fn allocate_txn_id(&self) -> TransactionId {
        let mut next_id = self.next_txn_id.lock().unwrap();
        let txn_id = TransactionId::from(*next_id);
        *next_id += 1;
        txn_id
    }

    /// Begin a read-only transaction using the latest stable snapshot.
    pub fn begin_read(&self) -> Result<Transaction<FS>, DatabaseError> {
        let txn_id = self.allocate_txn_id();
        let snapshot_lsn = *self.current_lsn.read().unwrap();
        
        // Write BEGIN record to WAL
        self.wal.write_begin(txn_id)
            .map_err(|e| DatabaseError {
                message: format!("Failed to write BEGIN to WAL: {}", e),
            })?;
        
        Ok(Transaction::new(
            txn_id,
            snapshot_lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
            Arc::clone(&self.wal),
            Arc::clone(&self.table_storage),
            Arc::clone(&self.current_lsn),
        ))
    }

    /// Begin a write transaction with the requested durability policy.
    pub fn begin_write(&self, durability: Durability) -> Result<Transaction<FS>, DatabaseError> {
        let _ = durability; // TODO: Use durability policy
        let txn_id = self.allocate_txn_id();
        let snapshot_lsn = *self.current_lsn.read().unwrap();
        
        // Write BEGIN record to WAL
        self.wal.write_begin(txn_id)
            .map_err(|e| DatabaseError {
                message: format!("Failed to write BEGIN to WAL: {}", e),
            })?;
        
        Ok(Transaction::new(
            txn_id,
            snapshot_lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
            Arc::clone(&self.wal),
            Arc::clone(&self.table_storage),
            Arc::clone(&self.current_lsn),
        ))
    }

    /// Begin a read-only transaction at a specific snapshot LSN.
    ///
    /// This is useful for reading from named snapshots or implementing
    /// time-travel queries. Returns an error if the LSN is not available
    /// (e.g., too old and already garbage collected).
    pub fn begin_read_at(&self, lsn: LogSequenceNumber) -> Result<Transaction<FS>, DatabaseError> {
        let txn_id = self.allocate_txn_id();
        
        // Write BEGIN record to WAL
        self.wal.write_begin(txn_id)
            .map_err(|e| DatabaseError {
                message: format!("Failed to write BEGIN to WAL: {}", e),
            })?;
        
        // TODO: Validate that LSN is still available
        Ok(Transaction::new(
            txn_id,
            lsn,
            IsolationLevel::ReadCommitted,
            Arc::clone(&self.conflict_detector),
            Arc::clone(&self.wal),
            Arc::clone(&self.table_storage),
            Arc::clone(&self.current_lsn),
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
    ) -> Result<ObjectId, DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Check if table already exists
        if catalog.contains_key(name) {
            return Err(DatabaseError {
                message: format!("Table '{}' already exists", name),
            });
        }

        // Allocate new table ID
        let table_id = ObjectId::from(catalog.len() as u64 + 1);
        
        // Get current LSN for creation timestamp
        let created_lsn = *self.current_lsn.read().unwrap();
        
        // Create table info
        let table_info = TableInfo {
            id: table_id,
            name: name.to_string(),
            options,
            root: None, // No root page yet
            created_lsn,
            stale: false, // Only relevant for indexes
        };
        
        // Add to catalog
        catalog.insert(name.to_string(), table_info);
        
        Ok(table_id)
    }

    /// Drop a logical table and its dependent indexes.
    ///
    /// This operation is transactional - the table becomes invisible only after
    /// the current LSN advances (simulating a commit).
    pub fn drop_table(&self, table: ObjectId) -> Result<(), DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Find and remove the table
        let table_name = catalog
            .iter()
            .find(|(_, info)| info.id == table && matches!(info.options.kind, TableKind::Regular))
            .map(|(name, _)| name.clone());
        
        if let Some(name) = table_name {
            catalog.remove(&name);
            
            // Remove all dependent indexes from the unified catalog
            let index_names: Vec<String> = catalog
                .iter()
                .filter(|(_, info)| {
                    matches!(info.options.kind, TableKind::Index { parent_table, .. } if parent_table == table)
                })
                .map(|(name, _)| name.clone())
                .collect();
            
            for index_name in index_names {
                catalog.remove(&index_name);
            }
            
            Ok(())
        } else {
            Err(DatabaseError {
                message: format!("Table {:?} not found", table),
            })
        }
    }

    /// Open an existing table by name.
    pub fn open_table(&self, name: &str) -> Result<Option<ObjectId>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.get(name).map(|info| info.id))
    }

    /// Get table or index info by ObjectId.
    pub fn get_object_info(&self, id: ObjectId) -> Result<Option<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.values().find(|info| info.id == id).cloned())
    }

    /// Get table or index info by name.
    pub fn get_object_info_by_name(&self, name: &str) -> Result<Option<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.get(name).cloned())
    }

    /// Check if an ObjectId refers to a regular table.
    pub fn is_table(&self, id: ObjectId) -> Result<bool, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.values().any(|info| {
            info.id == id && matches!(info.options.kind, TableKind::Regular)
        }))
    }

    /// Check if an ObjectId refers to an index.
    pub fn is_index(&self, id: ObjectId) -> Result<bool, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.values().any(|info| {
            info.id == id && matches!(info.options.kind, TableKind::Index { .. })
        }))
    }

    /// Return catalog-visible tables (excludes indexes).
    pub fn list_tables(&self) -> Result<Vec<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog
            .values()
            .filter(|info| matches!(info.options.kind, TableKind::Regular))
            .cloned()
            .collect())
    }

    /// Return all catalog objects (both tables and indexes).
    pub fn list_all_objects(&self) -> Result<Vec<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog.values().cloned().collect())
    }

    /// Create an index over a table.
    ///
    /// This operation is transactional - the index becomes visible only after
    /// the current LSN advances (simulating a commit).
    pub fn create_index(
        &self,
        parent_table: ObjectId,
        name: &str,
        index_kind: IndexKind,
        fields: Vec<IndexField>,
        unique: bool,
        consistency: IndexConsistency,
    ) -> Result<ObjectId, DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Check if index already exists
        if catalog.contains_key(name) {
            return Err(DatabaseError {
                message: format!("Index '{}' already exists", name),
            });
        }
        
        // Allocate new index ID (same as table ID since they're unified)
        let index_id = ObjectId::from(catalog.len() as u64 + 1);
        
        // Get current LSN for creation timestamp
        let created_lsn = *self.current_lsn.read().unwrap();
        
        // Create table options for the index
        let options = TableOptions {
            engine: crate::table::TableEngineKind::BTree, // Default engine for indexes
            key_encoding: crate::types::KeyEncoding::RawBytes,
            compression: None,
            encryption: None,
            page_size: None,
            format_version: 1,
            kind: TableKind::Index {
                parent_table,
                index_kind,
            },
            index_fields: fields,
            unique,
            consistency: Some(consistency),
        };
        
        // Create table info for the index
        let index_info = TableInfo {
            id: index_id,
            name: name.to_string(),
            options,
            root: None, // No root page yet
            created_lsn,
            stale: false, // New index starts fresh
        };
        
        // Add to unified catalog
        catalog.insert(name.to_string(), index_info);
        
        Ok(index_id)
    }

    /// Drop an index.
    ///
    /// This operation is transactional - the index becomes invisible only after
    /// the current LSN advances (simulating a commit).
    pub fn drop_index(&self, index: ObjectId) -> Result<(), DatabaseError> {
        let mut catalog = self.table_catalog.write().unwrap();
        
        // Find and remove the index
        let index_name = catalog
            .iter()
            .find(|(_, info)| {
                info.id == index && matches!(info.options.kind, TableKind::Index { .. })
            })
            .map(|(name, _)| name.clone());
        
        if let Some(name) = index_name {
            catalog.remove(&name);
            Ok(())
        } else {
            Err(DatabaseError {
                message: format!("Index {:?} not found", index),
            })
        }
    }

    /// Return catalog-visible indexes for a table.
    pub fn list_indexes(&self, table: ObjectId) -> Result<Vec<TableInfo>, DatabaseError> {
        let catalog = self.table_catalog.read().unwrap();
        Ok(catalog
            .values()
            .filter(|info| {
                matches!(info.options.kind, TableKind::Index { parent_table, .. } if parent_table == table)
            })
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
