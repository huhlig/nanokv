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

//! Tests for transaction layer index operations.
//!
//! These tests verify that the transaction layer correctly handles index operations
//! as explicit operations on specialty tables, without automatic index maintenance.

use nanokv::index::IndexId;
use nanokv::table::TableId;
use nanokv::txn::{ConflictDetector, Transaction, TransactionId};
use nanokv::types::IsolationLevel;
use nanokv::wal::LogSequenceNumber;
use std::sync::{Arc, Mutex};

#[test]
fn test_index_put_and_get() {
    // Create a transaction
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));
    let mut txn = Transaction::new(
        TransactionId::from(1),
        LogSequenceNumber::from(100),
        IsolationLevel::ReadCommitted,
        conflict_detector,
    );

    // Create an index ID
    let index_id = IndexId::from(1000);

    // Put a value into the index
    let key = b"index_key_1";
    let value = b"index_value_1";
    txn.index_put(index_id, key, value).unwrap();

    // Get the value back from the index
    let result = txn.index_get(index_id, key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_ref(), value);
}

#[test]
fn test_index_delete() {
    // Create a transaction
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));
    let mut txn = Transaction::new(
        TransactionId::from(2),
        LogSequenceNumber::from(200),
        IsolationLevel::ReadCommitted,
        conflict_detector,
    );

    // Create an index ID
    let index_id = IndexId::from(2000);

    // Put a value into the index
    let key = b"index_key_2";
    let value = b"index_value_2";
    txn.index_put(index_id, key, value).unwrap();

    // Verify it exists
    assert!(txn.index_get(index_id, key).unwrap().is_some());

    // Delete the key
    let existed = txn.index_delete(index_id, key).unwrap();
    assert!(existed);

    // Verify it's gone
    assert!(txn.index_get(index_id, key).unwrap().is_none());
}

#[test]
fn test_index_and_table_operations_independent() {
    // Create a transaction
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));
    let mut txn = Transaction::new(
        TransactionId::from(3),
        LogSequenceNumber::from(300),
        IsolationLevel::ReadCommitted,
        conflict_detector,
    );

    // Create table and index IDs
    let table_id = TableId::from(100);
    let index_id = IndexId::from(3000);

    // Use the same key for both table and index
    let key = b"shared_key";
    let table_value = b"table_value";
    let index_value = b"index_value";

    // Put into table
    txn.put(table_id, key, table_value).unwrap();

    // Put into index (explicit operation, not automatic)
    txn.index_put(index_id, key, index_value).unwrap();

    // Verify both exist independently
    let table_result = txn.get(table_id, key).unwrap();
    assert_eq!(table_result.unwrap().as_ref(), table_value);

    let index_result = txn.index_get(index_id, key).unwrap();
    assert_eq!(index_result.unwrap().as_ref(), index_value);

    // Delete from table doesn't affect index
    txn.delete(table_id, key).unwrap();
    assert!(txn.get(table_id, key).unwrap().is_none());
    assert!(txn.index_get(index_id, key).unwrap().is_some());
}

#[test]
fn test_index_write_conflict_detection() {
    // Create shared conflict detector
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));

    // Create two transactions
    let mut txn1 = Transaction::new(
        TransactionId::from(4),
        LogSequenceNumber::from(400),
        IsolationLevel::ReadCommitted,
        conflict_detector.clone(),
    );

    let mut txn2 = Transaction::new(
        TransactionId::from(5),
        LogSequenceNumber::from(400),
        IsolationLevel::ReadCommitted,
        conflict_detector.clone(),
    );

    // Create an index ID
    let index_id = IndexId::from(4000);
    let key = b"conflict_key";

    // First transaction writes to index
    txn1.index_put(index_id, key, b"value1").unwrap();

    // Second transaction tries to write to same index key - should conflict
    let result = txn2.index_put(index_id, key, b"value2");
    assert!(result.is_err());
}

#[test]
fn test_index_operations_in_write_set() {
    // Create a transaction
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));
    let mut txn = Transaction::new(
        TransactionId::from(6),
        LogSequenceNumber::from(600),
        IsolationLevel::ReadCommitted,
        conflict_detector,
    );

    // Create table and index IDs
    let table_id = TableId::from(200);
    let index_id = IndexId::from(5000);

    // Perform multiple operations
    txn.put(table_id, b"table_key_1", b"table_value_1").unwrap();
    txn.index_put(index_id, b"index_key_1", b"index_value_1").unwrap();
    txn.put(table_id, b"table_key_2", b"table_value_2").unwrap();
    txn.index_put(index_id, b"index_key_2", b"index_value_2").unwrap();

    // All operations should be visible within the transaction
    assert!(txn.get(table_id, b"table_key_1").unwrap().is_some());
    assert!(txn.get(table_id, b"table_key_2").unwrap().is_some());
    assert!(txn.index_get(index_id, b"index_key_1").unwrap().is_some());
    assert!(txn.index_get(index_id, b"index_key_2").unwrap().is_some());
}

#[test]
fn test_no_automatic_index_maintenance() {
    // This test documents that index maintenance is NOT automatic
    let conflict_detector = Arc::new(Mutex::new(ConflictDetector::new()));
    let mut txn = Transaction::new(
        TransactionId::from(7),
        LogSequenceNumber::from(700),
        IsolationLevel::ReadCommitted,
        conflict_detector,
    );

    let table_id = TableId::from(300);
    let index_id = IndexId::from(6000);

    // Put a value into the table
    txn.put(table_id, b"key", b"value").unwrap();

    // The index is NOT automatically updated
    // This is the caller's responsibility
    assert!(txn.index_get(index_id, b"key").unwrap().is_none());

    // Caller must explicitly maintain the index
    txn.index_put(index_id, b"key", b"value").unwrap();
    assert!(txn.index_get(index_id, b"key").unwrap().is_some());
}

// Made with Bob
