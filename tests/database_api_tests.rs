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

//! Integration tests for Phase 4: Database & Table Handle APIs
//!
//! Tests the high-level Database API with:
//! - Table management (create, drop, list)
//! - CRUD operations (insert, update, upsert, get, delete)
//! - Table handle wrapper
//! - Index maintenance
//! - Error handling

use nanokv::kvdb::{Database, DatabaseError, DatabaseErrorKind};
use nanokv::table::{TableOptions, TableKind, TableEngineKind, IndexKind, IndexField, IndexConsistency};
use nanokv::types::{ObjectId, KeyEncoding, Durability};
use nanokv::vfs::MemoryFileSystem;
use std::sync::Arc;

/// Helper to create a test database
fn create_test_db() -> Database<MemoryFileSystem> {
    let fs = MemoryFileSystem::new();
    Database::new(&fs, "test.wal").expect("Failed to create database")
}

/// Helper to create default table options
fn default_table_options() -> TableOptions {
    TableOptions {
        engine: TableEngineKind::Memory,
        key_encoding: KeyEncoding::RawBytes,
        compression: None,
        encryption: None,
        page_size: None,
        format_version: 1,
        kind: TableKind::Regular,
        index_fields: vec![],
        unique: false,
        consistency: None,
    }
}

// =============================================================================
// Table Management Tests
// =============================================================================

#[test]
fn test_create_table() {
    let db = create_test_db();
    
    let table_id = db.create_table("users", default_table_options())
        .expect("Failed to create table");
    
    // Verify table exists
    assert!(db.is_table(table_id).unwrap());
    assert!(!db.is_index(table_id).unwrap());
    
    // Verify table info
    let info = db.get_object_info(table_id).unwrap().expect("Table not found");
    assert_eq!(info.name, "users");
    assert!(matches!(info.options.kind, TableKind::Regular));
}

#[test]
fn test_create_duplicate_table() {
    let db = create_test_db();
    
    db.create_table("users", default_table_options())
        .expect("Failed to create table");
    
    // Try to create duplicate
    let result = db.create_table("users", default_table_options());
    assert!(result.is_err());
    
    let err = result.unwrap_err();
    assert_eq!(err.kind, DatabaseErrorKind::TableAlreadyExists);
}

#[test]
fn test_drop_table() {
    let db = create_test_db();
    
    let table_id = db.create_table("users", default_table_options())
        .expect("Failed to create table");
    
    // Drop table
    db.drop_table(table_id).expect("Failed to drop table");
    
    // Verify table no longer exists
    assert!(!db.is_table(table_id).unwrap());
    assert!(db.get_object_info(table_id).unwrap().is_none());
}

#[test]
fn test_list_tables() {
    let db = create_test_db();
    
    // Create multiple tables
    db.create_table("users", default_table_options()).unwrap();
    db.create_table("posts", default_table_options()).unwrap();
    db.create_table("comments", default_table_options()).unwrap();
    
    // List tables
    let tables = db.list_tables().expect("Failed to list tables");
    assert_eq!(tables.len(), 3);
    
    let names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"posts".to_string()));
    assert!(names.contains(&"comments".to_string()));
}

#[test]
fn test_open_table() {
    let db = create_test_db();
    
    db.create_table("users", default_table_options()).unwrap();
    
    // Open by name
    let table_id = db.open_table("users")
        .expect("Failed to open table")
        .expect("Table not found");
    
    assert!(db.is_table(table_id).unwrap());
    
    // Try to open non-existent table
    let result = db.open_table("nonexistent");
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// =============================================================================
// CRUD Operations Tests
// =============================================================================

#[test]
fn test_insert() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert key-value pair
    db.insert(table_id, b"user1", b"Alice")
        .expect("Failed to insert");
    
    // Verify value
    let value = db.get(table_id, b"user1")
        .expect("Failed to get")
        .expect("Value not found");
    assert_eq!(value.as_ref(), b"Alice");
}

#[test]
fn test_insert_duplicate_key() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert first time
    db.insert(table_id, b"user1", b"Alice").unwrap();
    
    // Try to insert duplicate
    let result = db.insert(table_id, b"user1", b"Bob");
    assert!(result.is_err());
    
    let err = result.unwrap_err();
    assert_eq!(err.kind, DatabaseErrorKind::KeyAlreadyExists);
    
    // Verify original value unchanged
    let value = db.get(table_id, b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice");
}

#[test]
fn test_update() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert initial value
    db.insert(table_id, b"user1", b"Alice").unwrap();
    
    // Update value
    db.update(table_id, b"user1", b"Alice Smith")
        .expect("Failed to update");
    
    // Verify updated value
    let value = db.get(table_id, b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice Smith");
}

#[test]
fn test_update_nonexistent_key() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Try to update non-existent key
    let result = db.update(table_id, b"user1", b"Alice");
    assert!(result.is_err());
    
    let err = result.unwrap_err();
    assert_eq!(err.kind, DatabaseErrorKind::KeyNotFound);
}

#[test]
fn test_upsert_insert() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Upsert (insert)
    let is_update = db.upsert(table_id, b"user1", b"Alice")
        .expect("Failed to upsert");
    assert!(!is_update); // Was an insert
    
    // Verify value
    let value = db.get(table_id, b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice");
}

#[test]
fn test_upsert_update() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert initial value
    db.insert(table_id, b"user1", b"Alice").unwrap();
    
    // Upsert (update)
    let is_update = db.upsert(table_id, b"user1", b"Alice Smith")
        .expect("Failed to upsert");
    assert!(is_update); // Was an update
    
    // Verify updated value
    let value = db.get(table_id, b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice Smith");
}

#[test]
fn test_get() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert value
    db.insert(table_id, b"user1", b"Alice").unwrap();
    
    // Get existing key
    let value = db.get(table_id, b"user1")
        .expect("Failed to get")
        .expect("Value not found");
    assert_eq!(value.as_ref(), b"Alice");
    
    // Get non-existent key
    let value = db.get(table_id, b"user2").expect("Failed to get");
    assert!(value.is_none());
}

#[test]
fn test_delete() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert value
    db.insert(table_id, b"user1", b"Alice").unwrap();
    
    // Delete existing key
    let deleted = db.delete(table_id, b"user1")
        .expect("Failed to delete");
    assert!(deleted);
    
    // Verify key no longer exists
    let value = db.get(table_id, b"user1").unwrap();
    assert!(value.is_none());
    
    // Delete non-existent key
    let deleted = db.delete(table_id, b"user2")
        .expect("Failed to delete");
    assert!(!deleted);
}

// =============================================================================
// Table Handle Tests
// =============================================================================

#[test]
fn test_table_handle_crud() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Get table handle
    let table = db.table(table_id).expect("Failed to get table handle");
    
    // Insert via handle
    table.insert(b"user1", b"Alice").expect("Failed to insert");
    
    // Get via handle
    let value = table.get(b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice");
    
    // Update via handle
    table.update(b"user1", b"Alice Smith").expect("Failed to update");
    let value = table.get(b"user1").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"Alice Smith");
    
    // Upsert via handle
    let is_update = table.upsert(b"user2", b"Bob").unwrap();
    assert!(!is_update);
    
    // Contains via handle
    assert!(table.contains(b"user1").unwrap());
    assert!(table.contains(b"user2").unwrap());
    assert!(!table.contains(b"user3").unwrap());
    
    // Delete via handle
    let deleted = table.delete(b"user1").unwrap();
    assert!(deleted);
    assert!(!table.contains(b"user1").unwrap());
}

#[test]
fn test_table_handle_info() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    let table = db.table(table_id).unwrap();
    
    // Check ID
    assert_eq!(table.id(), table_id);
    
    // Check info
    let info = table.info().unwrap().unwrap();
    assert_eq!(info.name, "users");
    assert_eq!(info.id, table_id);
}

// =============================================================================
// Index Management Tests
// =============================================================================

#[test]
fn test_create_index() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Create index
    let index_id = db.create_index(
        table_id,
        "users_email_idx",
        IndexKind::DenseOrdered,
        vec![IndexField {
            name: "email".to_string(),
            encoding: KeyEncoding::Utf8,
            descending: false,
        }],
        true, // unique
        IndexConsistency::Synchronous,
    ).expect("Failed to create index");
    
    // Verify index exists
    assert!(db.is_index(index_id).unwrap());
    assert!(!db.is_table(index_id).unwrap());
    
    // Verify index info
    let info = db.get_object_info(index_id).unwrap().unwrap();
    assert_eq!(info.name, "users_email_idx");
    assert!(matches!(info.options.kind, TableKind::Index { .. }));
}

#[test]
fn test_list_indexes() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Create multiple indexes
    db.create_index(
        table_id,
        "users_email_idx",
        IndexKind::DenseOrdered,
        vec![],
        true,
        IndexConsistency::Synchronous,
    ).unwrap();
    
    db.create_index(
        table_id,
        "users_name_idx",
        IndexKind::DenseOrdered,
        vec![],
        false,
        IndexConsistency::Synchronous,
    ).unwrap();
    
    // List indexes
    let indexes = db.list_indexes(table_id).expect("Failed to list indexes");
    assert_eq!(indexes.len(), 2);
    
    let names: Vec<String> = indexes.iter().map(|i| i.name.clone()).collect();
    assert!(names.contains(&"users_email_idx".to_string()));
    assert!(names.contains(&"users_name_idx".to_string()));
}

#[test]
fn test_drop_index() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    let index_id = db.create_index(
        table_id,
        "users_email_idx",
        IndexKind::DenseOrdered,
        vec![],
        true,
        IndexConsistency::Synchronous,
    ).unwrap();
    
    // Drop index
    db.drop_index(index_id).expect("Failed to drop index");
    
    // Verify index no longer exists
    assert!(!db.is_index(index_id).unwrap());
    assert!(db.get_object_info(index_id).unwrap().is_none());
}

#[test]
fn test_drop_table_drops_indexes() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    let index_id = db.create_index(
        table_id,
        "users_email_idx",
        IndexKind::DenseOrdered,
        vec![],
        true,
        IndexConsistency::Synchronous,
    ).unwrap();
    
    // Drop table
    db.drop_table(table_id).expect("Failed to drop table");
    
    // Verify both table and index are gone
    assert!(!db.is_table(table_id).unwrap());
    assert!(!db.is_index(index_id).unwrap());
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_operation_on_nonexistent_table() {
    let db = create_test_db();
    let fake_table_id = ObjectId::from(999);
    
    // Try operations on non-existent table
    let result = db.insert(fake_table_id, b"key", b"value");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, DatabaseErrorKind::NotATable);
    
    let result = db.get(fake_table_id, b"key");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, DatabaseErrorKind::NotATable);
}

#[test]
fn test_operation_on_index_as_table() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    let index_id = db.create_index(
        table_id,
        "users_email_idx",
        IndexKind::DenseOrdered,
        vec![],
        true,
        IndexConsistency::Synchronous,
    ).unwrap();
    
    // Try to use index as table
    let result = db.insert(index_id, b"key", b"value");
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind, DatabaseErrorKind::NotATable);
}

// =============================================================================
// Multi-operation Tests
// =============================================================================

#[test]
fn test_multiple_tables() {
    let db = create_test_db();
    
    let users_id = db.create_table("users", default_table_options()).unwrap();
    let posts_id = db.create_table("posts", default_table_options()).unwrap();
    
    // Insert into both tables
    db.insert(users_id, b"user1", b"Alice").unwrap();
    db.insert(posts_id, b"post1", b"Hello World").unwrap();
    
    // Verify isolation
    assert!(db.get(users_id, b"user1").unwrap().is_some());
    assert!(db.get(users_id, b"post1").unwrap().is_none());
    assert!(db.get(posts_id, b"post1").unwrap().is_some());
    assert!(db.get(posts_id, b"user1").unwrap().is_none());
}

#[test]
fn test_crud_sequence() {
    let db = create_test_db();
    let table_id = db.create_table("users", default_table_options()).unwrap();
    
    // Insert multiple keys
    db.insert(table_id, b"user1", b"Alice").unwrap();
    db.insert(table_id, b"user2", b"Bob").unwrap();
    db.insert(table_id, b"user3", b"Charlie").unwrap();
    
    // Update one
    db.update(table_id, b"user2", b"Bob Smith").unwrap();
    
    // Delete one
    db.delete(table_id, b"user3").unwrap();
    
    // Verify final state
    assert_eq!(db.get(table_id, b"user1").unwrap().unwrap().as_ref(), b"Alice");
    assert_eq!(db.get(table_id, b"user2").unwrap().unwrap().as_ref(), b"Bob Smith");
    assert!(db.get(table_id, b"user3").unwrap().is_none());
}

// Made with Bob
