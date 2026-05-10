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

/// Tracks which transactions have locked which keys for writing.
/// Used to detect write-write conflicts before they occur.
///
/// # Usage
/// 1. Before writing a key, call check_write_conflict()
/// 2. If no conflict, call acquire_write_lock()
/// 3. On commit/abort, call release_locks()
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

    /// Check if a write would conflict with an existing write lock.
    ///
    /// Returns an error if another transaction holds a write lock on the key.
    pub fn check_write_conflict(
        &self,
        table_id: TableId,
        key: &[u8],
        txn_id: TransactionId,
    ) -> TransactionResult<()> {
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

    /// Acquire a write lock on a key for the given transaction.
    ///
    /// Should be called after check_write_conflict() succeeds.
    pub fn acquire_write_lock(&mut self, table_id: TableId, key: Vec<u8>, txn_id: TransactionId) {
        self.write_locks.insert((table_id, key), txn_id);
    }

    /// Release all write locks held by the given transaction.
    ///
    /// Called on commit or abort to free locks.
    pub fn release_locks(&mut self, txn_id: TransactionId) {
        self.write_locks.retain(|_, &mut holder| holder != txn_id);
    }

    /// Check for read-write conflicts in serializable isolation.
    ///
    /// For serializable isolation, check if any key in the read set
    /// has been written by another transaction.
    pub fn check_read_write_conflicts(
        &self,
        read_set: &HashSet<(TableId, Vec<u8>)>,
        txn_id: TransactionId,
    ) -> TransactionResult<()> {
        for (table_id, key) in read_set {
            if let Some(&other_txn) = self.write_locks.get(&(*table_id, key.clone())) {
                if other_txn != txn_id {
                    return Err(TransactionError::ReadWriteConflict(*table_id, key.clone()));
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

/// Tracks wait-for relationships between transactions to detect cycles.
///
/// When a transaction waits for a lock held by another transaction,
/// we add an edge to the wait-for graph. If a cycle is detected,
/// we abort one of the transactions to break the deadlock.
///
/// Uses a simple directed graph representation where an edge from A to B
/// means transaction A is waiting for transaction B to release a lock.
pub struct DeadlockDetector {
    /// Maps each waiting transaction to the set of transactions it's waiting for
    wait_for_graph: HashMap<TransactionId, Vec<TransactionId>>,
}

impl DeadlockDetector {
    /// Create a new deadlock detector with an empty wait-for graph.
    pub fn new() -> Self {
        Self {
            wait_for_graph: HashMap::new(),
        }
    }

    /// Add a wait-for edge: waiter is waiting for holder.
    ///
    /// This records that `waiter` is blocked waiting for a lock held by `holder`.
    pub fn add_wait(&mut self, waiter: TransactionId, holder: TransactionId) {
        self.wait_for_graph
            .entry(waiter)
            .or_insert_with(Vec::new)
            .push(holder);
    }

    /// Remove all wait-for edges originating from the given transaction.
    ///
    /// Called when a transaction completes (commits or aborts) or acquires
    /// the lock it was waiting for.
    pub fn remove_wait(&mut self, waiter: TransactionId) {
        self.wait_for_graph.remove(&waiter);
    }

    /// Detect if there's a cycle in the wait-for graph using DFS.
    ///
    /// Returns the cycle as a vector of transaction IDs if found, or None.
    /// Uses depth-first search with a recursion stack to detect back edges.
    pub fn detect_cycle(&self) -> Option<Vec<TransactionId>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        // Try DFS from each unvisited node
        for &txn_id in self.wait_for_graph.keys() {
            if !visited.contains(&txn_id) {
                if let Some(cycle) = self.dfs_detect_cycle(
                    txn_id,
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                ) {
                    return Some(cycle);
                }
            }
        }

        None
    }

    /// Helper function for cycle detection using DFS.
    fn dfs_detect_cycle(
        &self,
        current: TransactionId,
        visited: &mut HashSet<TransactionId>,
        rec_stack: &mut HashSet<TransactionId>,
        path: &mut Vec<TransactionId>,
    ) -> Option<Vec<TransactionId>> {
        visited.insert(current);
        rec_stack.insert(current);
        path.push(current);

        // Visit all neighbors (transactions this one is waiting for)
        if let Some(neighbors) = self.wait_for_graph.get(&current) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    // Recurse on unvisited neighbor
                    if let Some(cycle) = self.dfs_detect_cycle(neighbor, visited, rec_stack, path)
                    {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&neighbor) {
                    // Found a back edge - cycle detected!
                    // Extract the cycle from the path
                    let cycle_start = path.iter().position(|&id| id == neighbor).unwrap();
                    return Some(path[cycle_start..].to_vec());
                }
            }
        }

        // Backtrack
        path.pop();
        rec_stack.remove(&current);
        None
    }
}
