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

//! Conflict detection for MVCC transactions
//!
//! This module provides conflict detection mechanisms for ensuring
//! transaction isolation. It tracks which transactions have accessed
//! which keys and detects conflicts based on the isolation level.

use crate::table::TableId;
use crate::txn::{TransactionError, TransactionId, TransactionResult};
use std::collections::{HashMap, HashSet};

/// TODO(MVCC): Implement conflict detection types
/// Types of conflicts that can occur between transactions:
/// - WriteWrite: Two transactions write to the same key
/// - ReadWrite: A transaction reads a key that another transaction writes
/// - Serialization: Complex conflict in serializable isolation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    WriteWrite,
    ReadWrite,
    Serialization,
}

// TODO(MVCC): Implement conflict detector
// Tracks which transactions have locked which keys for writing.
// Used to detect write-write conflicts before they occur.
//
// Usage:
// 1. Before writing a key, call check_write_conflict()
// 2. If no conflict, call acquire_write_lock()
// 3. On commit/abort, call release_locks()
pub struct ConflictDetector {
    // Maps (table_id, key) -> transaction ID that has write lock
    write_locks: HashMap<(TableId, Vec<u8>), TransactionId>,
}

impl ConflictDetector {
    pub fn new() -> Self {
        Self {
            write_locks: HashMap::new(),
        }
    }

    /// TODO(MVCC): Implement write conflict detection
    pub fn check_write_conflict(&self, table_id: TableId, key: &[u8], txn_id: TransactionId) -> TransactionResult<()> {
        let lock_key = (table_id, key.to_vec());
        if let Some(&other_txn) = self.write_locks.get(&lock_key) {
            if other_txn != txn_id {
                return Err(TransactionError::WriteWriteConflict(
                    table_id,
                    key.to_vec(),
                    other_txn,
                ));
            }
        }
        Ok(())
    }

    /// TODO(MVCC): Implement lock acquisition
    pub fn acquire_write_lock(&mut self, table_id: TableId, key: Vec<u8>, txn_id: TransactionId) {
        self.write_locks.insert((table_id, key), txn_id);
    }

    /// TODO(MVCC): Implement lock release
    pub fn release_locks(&mut self, txn_id: TransactionId) {
        self.write_locks.retain(|_, &mut holder| holder != txn_id);
    }

    /// TODO(MVCC): Implement read-write conflict detection
    /// For serializable isolation, check if any key in the read set
    /// has been written by another transaction
    pub fn check_read_write_conflicts(
        &self,
        read_set: &HashSet<(TableId, Vec<u8>)>,
        txn_id: TransactionId,
    ) -> TransactionResult<()> {
        for (table_id, key) in read_set {
            if let Some(&other_txn) = self.write_locks.get(&(*table_id, key.clone())) {
                if other_txn != txn_id {
                    return Err(TransactionError::ReadWriteConflict(
                        *table_id,
                        key.clone(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Default for ConflictDetector {
    fn default() -> Self {
        Self::new()
    }
}

// TODO(MVCC): Implement deadlock detection
// Tracks wait-for relationships between transactions to detect cycles.
// When a transaction waits for a lock held by another transaction,
// we add an edge to the wait-for graph. If a cycle is detected,
// we abort one of the transactions to break the deadlock.
//
pub struct DeadlockDetector {
    // wait_for_graph: HashMap<TransactionId, Vec<TransactionId>>,
}

impl DeadlockDetector {
    pub fn new() -> Self {
        todo!("Implement DeadlockDetector::new")
    }
    pub fn add_wait(&mut self, waiter: TransactionId, holder: TransactionId) {
        todo!("Implement DeadlockDetector::add_wait")
    }
    pub fn remove_wait(&mut self, waiter: TransactionId) {
        todo!("Implement DeadlockDetector::remove_wait")
    }
    pub fn detect_cycle(&self) -> Option<Vec<TransactionId>> {
        todo!("Implement DeadlockDetector::detect_cycle")
    }
}


