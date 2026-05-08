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

use crate::types::{ConsistencyGuarantees, Durability, IsolationLevel};
use crate::index::{IndexId, IndexInfo, IndexOptions};
use crate::snap::{Snapshot, SnapshotId};
use crate::table::{TableId, TableInfo, TableOptions};
use crate::txn::Transaction;
use crate::wal::LogSequenceNumber;

/// Top-level embedded database.
///
/// This struct owns the catalog, file allocation, transaction manager, WAL,
/// and registered table/index engines. ACID semantics should be coordinated at
/// this layer rather than by independently stacking transactional wrappers around
/// individual tables.
pub struct Database {
    // TODO: Add internal fields for catalog, WAL, page store, etc.
}

impl Database {
    /// Begin a read-only transaction using the latest stable snapshot.
    pub fn begin_read(&self) -> Result<Transaction, DatabaseError> {
        todo!("Create a read-only transaction at the latest stable snapshot LSN")
    }

    /// Begin a write transaction with the requested durability policy.
    pub fn begin_write(&self, durability: Durability) -> Result<Transaction, DatabaseError> {
        todo!("Create a write transaction with the specified durability policy")
    }

    /// Begin a read-only transaction at a specific snapshot LSN.
    ///
    /// This is useful for reading from named snapshots or implementing
    /// time-travel queries. Returns an error if the LSN is not available
    /// (e.g., too old and already garbage collected).
    pub fn begin_read_at(&self, lsn: LogSequenceNumber) -> Result<Transaction, DatabaseError> {
        todo!("Create a read-only transaction at the specified LSN")
    }

    /// Create a logical table using a chosen physical engine.
    pub fn create_table(
        &self,
        name: &str,
        options: TableOptions,
    ) -> Result<TableId, DatabaseError> {
        todo!("Create a new table with the specified name and options")
    }

    /// Drop a logical table and its dependent indexes.
    pub fn drop_table(&self, table: TableId) -> Result<(), DatabaseError> {
        todo!("Drop the table and all its dependent indexes")
    }

    /// Open an existing table by name.
    pub fn open_table(&self, name: &str) -> Result<Option<TableId>, DatabaseError> {
        todo!("Look up a table by name in the catalog")
    }

    /// Return catalog-visible tables.
    pub fn list_tables(&self) -> Result<Vec<TableInfo>, DatabaseError> {
        todo!("Return a list of all tables in the catalog")
    }

    /// Create an index over a table.
    pub fn create_index(
        &self,
        table: TableId,
        name: &str,
        options: IndexOptions,
    ) -> Result<IndexId, DatabaseError> {
        todo!("Create a new index on the specified table")
    }

    /// Drop an index.
    pub fn drop_index(&self, index: IndexId) -> Result<(), DatabaseError> {
        todo!("Drop the specified index")
    }

    /// Return catalog-visible indexes for a table.
    pub fn list_indexes(&self, table: TableId) -> Result<Vec<IndexInfo>, DatabaseError> {
        todo!("Return a list of all indexes for the specified table")
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
