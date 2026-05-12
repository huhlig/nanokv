# Bloom Filter Transaction Integration Design

## Problem Statement

The transaction layer (`Transaction::get/put/delete/range_delete/commit`) currently assumes KV/searchable/mutable table semantics. However, `PagedBloomFilter` implements the `ApproximateMembership` trait with different APIs:
- `insert_key(key)` - add a key to the filter
- `might_contain(key)` - check approximate membership

This semantic mismatch means bloom filters cannot participate in standard transactional KV operations, leading to explicit unsupported match arms in `src/txn/transaction.rs`.

## Architecture Analysis

### Current Transaction Layer Design

The transaction layer uses a **monolithic match-based dispatch** pattern where it must know about every table type and handle their specific operations. This creates tight coupling and poor extensibility.

### Table Type Taxonomy

Tables in the system fall into distinct categories:

1. **Searchable KV Tables** (implement `SearchableTable` trait)
   - PagedBTree, MemoryBTree, LsmTree, MemoryHashTable
   - Support: reader/writer pattern, point lookups, ordered scans
   - Operations: get/put/delete/range_delete

2. **Blob Tables** (implement `Table` trait only)
   - MemoryBlob, PagedBlob, FileBlob
   - Support: direct get/put/delete on large binary objects
   - Operations: get/put/list/prefix/search/delete

3. **Specialty Tables** (implement specialty traits)
   - PagedBloomFilter (ApproximateMembership): insert_key/might_contain/remove_key
   - FullTextSearch: index_document/search/remove_document
   - VectorSearch: insert_vector/search_vector/remove_vector
   - GeoSpatial: insert_geometry/intersects/nearest/remove_geometry

   - TimeSeries: insert_point/query_range/remove_point
   - GraphAdjacency: add_edge/traverse/remove_edge


**Key Insight**: Each specialty table has fundamentally different semantics and operations.

## Recommended Solution: Transaction Implements Specialty Traits

### Core Concept

Instead of adding specialty-specific methods to Transaction or using match-based dispatch, **Transaction should implement the specialty table traits directly**. This provides:

1. **Unified Interface**: Users interact with Transaction using the same trait methods as the underlying tables
2. **Type Safety**: Trait bounds ensure operations are only available for appropriate table types
3. **Extensibility**: New specialty tables just need their trait implemented on Transaction
4. **Separation of Concerns**: Transaction handles ACID properties, tables handle storage

### Design

```rust
// Transaction implements specialty table traits
impl<FS: FileSystem> ApproximateMembership for Transaction<FS> {
    fn table_id(&self) -> ObjectId {
        // Return the current table context or error if not set
        self.current_table_id.ok_or(TransactionError::NoTableContext)
    }
    
    fn name(&self) -> &str {
        // Return name from current table context
        self.current_table_name.as_deref().unwrap_or("unknown")
    }
    
    fn capabilities(&self) -> SpecialtyTableCapabilities {
        // Delegate to underlying table
        if let Some(engine) = self.engine_registry.get(self.current_table_id?) {
            match &engine {
                TableEngineInstance::PagedBloomFilter(bloom) => bloom.capabilities(),
                _ => SpecialtyTableCapabilities::default(),
            }
        } else {
            SpecialtyTableCapabilities::default()
        }
    }
    
    fn insert_key(&mut self, key: &[u8]) -> TableResult<()> {
        // Check if transaction is active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "insert_key",
            ).into());
        }
        
        let object_id = self.current_table_id?;
        
        // Write to WAL
        self.wal.write_operation(
            self.txn_id,
            object_id,
            WriteOpType::BloomInsert,
            key.to_vec(),
            vec![],
        )?;
        
        // Record in write set
        self.record_bloom_insert(object_id, key.to_vec());
        
        Ok(())
    }
    
    fn might_contain(&self, key: &[u8]) -> TableResult<bool> {
        // Check if transaction is active
        if !self.is_active() {
            return Err(TransactionError::invalid_state(
                self.txn_id,
                self.state.as_str(),
                "might_contain",
            ).into());
        }
        
        let object_id = self.current_table_id?;
        
        // Check write set first for uncommitted inserts
        if self.bloom_write_set.contains(&(object_id, key.to_vec())) {
            return Ok(true);
        }
        
        // Query underlying bloom filter
        if let Some(engine) = self.engine_registry.get(object_id) {
            match &engine {
                TableEngineInstance::PagedBloomFilter(bloom) => {
                    bloom.might_contain(key)
                }
                _ => Err(TableError::Other(
                    "Table is not a bloom filter".to_string()
                )),
            }
        } else {
            Ok(false)
        }
    }
    
    fn false_positive_rate(&self) -> Option<f64> {
        // Delegate to underlying table
        if let Some(engine) = self.engine_registry.get(self.current_table_id.ok()?) {
            match &engine {
                TableEngineInstance::PagedBloomFilter(bloom) => bloom.false_positive_rate(),
                _ => None,
            }
        } else {
            None
        }
    }
    
    fn stats(&self) -> TableResult<SpecialtyTableStats> {
        // Delegate to underlying table
        let object_id = self.current_table_id?;
        if let Some(engine) = self.engine_registry.get(object_id) {
            match &engine {
                TableEngineInstance::PagedBloomFilter(bloom) => bloom.stats(),
                _ => Err(TableError::Other("Table is not a bloom filter".to_string())),
            }
        } else {
            Err(TableError::Other("Table not found".to_string()))
        }
    }
    
    fn verify(&self) -> TableResult<VerificationReport> {
        // Delegate to underlying table
        let object_id = self.current_table_id?;
        if let Some(engine) = self.engine_registry.get(object_id) {
            match &engine {
                TableEngineInstance::PagedBloomFilter(bloom) => bloom.verify(),
                _ => Err(TableError::Other("Table is not a bloom filter".to_string())),
            }
        } else {
            Err(TableError::Other("Table not found".to_string()))
        }
    }
}
```

### Table Context Management

To support trait implementations, Transaction needs to track which table is being operated on:

```rust
pub struct Transaction<FS: FileSystem> {
    // ... existing fields ...
    
    /// Current table context for specialty trait operations
    current_table_id: Option<ObjectId>,
    current_table_name: Option<String>,
    
    /// Bloom filter write set (table_id, key)
    bloom_write_set: HashSet<(ObjectId, Vec<u8>)>,
}

impl<FS: FileSystem> Transaction<FS> {
    /// Set the table context for specialty operations
    pub fn with_table(&mut self, object_id: ObjectId) -> &mut Self {
        self.current_table_id = Some(object_id);
        if let Some(engine) = self.engine_registry.get(object_id) {
            self.current_table_name = Some(engine.name().to_string());
        }
        self
    }
    
    /// Clear the table context
    pub fn clear_table_context(&mut self) {
        self.current_table_id = None;
        self.current_table_name = None;
    }
}
```

### Usage Pattern

```rust
// Using bloom filter within transaction
let mut txn = db.begin_transaction()?;

// Set table context
txn.with_table(bloom_table_id);

// Now Transaction implements ApproximateMembership
txn.insert_key(b"key1")?;
txn.insert_key(b"key2")?;

let exists = txn.might_contain(b"key1")?; // true
let maybe = txn.might_contain(b"key3")?;  // false or true (false positive)

// Can mix with KV operations on different tables
txn.put(kv_table_id, b"key", b"value")?;

txn.commit()?;
```

### Alternative: Scoped Table Operations

For better ergonomics, provide a scoped API:

```rust
impl<FS: FileSystem> Transaction<FS> {
    /// Execute operations on a bloom filter table
    pub fn with_bloom<F, R>(&mut self, object_id: ObjectId, f: F) -> TransactionResult<R>
    where
        F: FnOnce(&mut dyn ApproximateMembership) -> TableResult<R>,
    {
        self.with_table(object_id);
        let result = f(self);
        self.clear_table_context();
        result.map_err(Into::into)
    }
    
    /// Execute operations on a full-text search table
    pub fn with_fulltext<F, R>(&mut self, object_id: ObjectId, f: F) -> TransactionResult<R>
    where
        F: FnOnce(&mut dyn FullTextSearch) -> TableResult<R>,
    {
        self.with_table(object_id);
        let result = f(self);
        self.clear_table_context();
        result.map_err(Into::into)
    }
    
    // Similar for other specialty traits...
}
```

Usage:

```rust
let mut txn = db.begin_transaction()?;

// Scoped bloom filter operations
txn.with_bloom(bloom_table_id, |bloom| {
    bloom.insert_key(b"key1")?;
    bloom.insert_key(b"key2")?;
    let exists = bloom.might_contain(b"key1")?;
    Ok(exists)
})?;

// Scoped full-text operations
txn.with_fulltext(fts_table_id, |fts| {
    fts.index_document(b"doc1", &[TextField { 
        name: "title", 
        text: "Hello World", 
        boost: 1.0 
    }])?;
    Ok(())
})?;

txn.commit()?;
```

## Implementation Plan

### Step 1: Extend Transaction Structure

```rust
pub struct Transaction<FS: FileSystem> {
    // ... existing fields ...
    
    /// Current table context for specialty trait operations
    current_table_id: Option<ObjectId>,
    current_table_name: Option<String>,
    
    /// Specialty table write sets
    bloom_write_set: HashSet<(ObjectId, Vec<u8>)>,
    // Future: fulltext_write_set, vector_write_set, etc.
}
```

### Step 2: Add Table Context Methods

```rust
impl<FS: FileSystem> Transaction<FS> {
    pub fn with_table(&mut self, object_id: ObjectId) -> &mut Self;
    pub fn clear_table_context(&mut self);
    pub fn current_table(&self) -> Option<ObjectId>;
}
```

### Step 3: Implement ApproximateMembership Trait

```rust
impl<FS: FileSystem> ApproximateMembership for Transaction<FS> {
    fn table_id(&self) -> ObjectId;
    fn name(&self) -> &str;
    fn capabilities(&self) -> SpecialtyTableCapabilities;
    fn insert_key(&mut self, key: &[u8]) -> TableResult<()>;
    fn might_contain(&self, key: &[u8]) -> TableResult<bool>;
    fn false_positive_rate(&self) -> Option<f64>;
    fn stats(&self) -> TableResult<SpecialtyTableStats>;
    fn verify(&self) -> TableResult<VerificationReport>;
}
```

### Step 4: Update WAL for Bloom Operations

```rust
// In src/wal/record.rs
pub enum WriteOpType {
    Put,
    Delete,
    BloomInsert,  // New
    // Future: FullTextIndex, VectorInsert, etc.
}
```

### Step 5: Update Commit Logic

```rust
impl<FS: FileSystem> Transaction<FS> {
    pub fn commit(mut self) -> TransactionResult<CommitInfo> {
        // ... existing commit logic ...
        
        // Apply bloom filter inserts
        for (object_id, key) in &self.bloom_write_set {
            if let Some(engine) = self.engine_registry.get(*object_id) {
                match &engine {
                    TableEngineInstance::PagedBloomFilter(bloom) => {
                        bloom.insert_key(key)?;
                    }
                    _ => return Err(TransactionError::Other(
                        "Table is not a bloom filter".to_string()
                    )),
                }
            }
        }
        
        // ... rest of commit logic ...
    }
}
```

### Step 6: Add Scoped Helper Methods (Optional)

```rust
impl<FS: FileSystem> Transaction<FS> {
    pub fn with_bloom<F, R>(&mut self, object_id: ObjectId, f: F) -> TransactionResult<R>
    where F: FnOnce(&mut dyn ApproximateMembership) -> TableResult<R>;
    
    // Future: with_fulltext, with_vector, with_geo, etc.
}
```

### Step 7: Tests

```rust
#[test]
fn test_bloom_filter_transaction() {
    let mut txn = db.begin_transaction()?;
    
    txn.with_table(bloom_id);
    txn.insert_key(b"key1")?;
    assert!(txn.might_contain(b"key1")?);
    
    txn.commit()?;
}

#[test]
fn test_bloom_filter_rollback() {
    let mut txn = db.begin_transaction()?;
    
    txn.with_table(bloom_id);
    txn.insert_key(b"key1")?;
    
    txn.rollback()?;
    
    // Key should not be in bloom filter after rollback
    let txn2 = db.begin_transaction()?;
    txn2.with_table(bloom_id);
    // might_contain could still return true due to false positives,
    // but we can verify it's not in the committed state
}

#[test]
fn test_mixed_operations() {
    let mut txn = db.begin_transaction()?;
    
    // KV operations
    txn.put(kv_table_id, b"key", b"value")?;
    
    // Bloom operations
    txn.with_table(bloom_id);
    txn.insert_key(b"key1")?;
    
    // Both should commit atomically
    txn.commit()?;
}
```

## Benefits of This Approach

1. **Unified Interface**: Transaction implements the same traits as tables
2. **Type Safety**: Trait bounds ensure correct usage
3. **Extensibility**: Easy to add new specialty table support
4. **Separation of Concerns**: Transaction handles ACID, tables handle storage
5. **Familiar API**: Users use the same trait methods they know from tables
6. **Composability**: Can mix different table types in one transaction

## Bloom Filter Transaction Semantics

### Insert Operations
- **Durability**: Bloom inserts are written to WAL
- **Isolation**: Inserts are visible only after commit
- **Atomicity**: Inserts are applied atomically with other transaction operations
- **Consistency**: False positive rate may increase with inserts

### Query Operations
- **Snapshot Isolation**: Queries see committed inserts up to snapshot LSN
- **Write Set Visibility**: Queries see uncommitted inserts in current transaction
- **Semantics**: `might_contain()` returns true if key was inserted (committed or uncommitted) or if false positive

### Limitations
- **No Delete**: Bloom filters don't support deletion (by design)
- **No Range Operations**: Bloom filters don't support range queries
- **Approximate**: Results are probabilistic, not exact
- **Write-Only Transactions**: Bloom inserts don't participate in conflict detection

## Future Extensions

Once this pattern is established for bloom filters, it can be extended to other specialty tables:

```rust
impl<FS: FileSystem> FullTextSearch for Transaction<FS> { ... }
impl<FS: FileSystem> VectorSearch for Transaction<FS> { ... }
impl<FS: FileSystem> GeoSpatial for Transaction<FS> { ... }
impl<FS: FileSystem> TimeSeries for Transaction<FS> { ... }
impl<FS: FileSystem> GraphAdjacency for Transaction<FS> { ... }
```

Each implementation follows the same pattern:
1. Check transaction state
2. Write to WAL
3. Record in specialty write set
4. Apply on commit
5. Discard on rollback

## Conclusion

By having Transaction implement specialty table traits, we achieve:
- **Elegant design**: Transaction is a transactional wrapper around any table type
- **Type safety**: Trait bounds ensure correct operations
- **Extensibility**: New table types just need trait implementation
- **Consistency**: Same API whether using table directly or through transaction
- **Simplicity**: No need for match-based dispatch or specialty methods

This design explicitly acknowledges that specialty tables have different semantics, and provides appropriate transaction-layer integration by implementing their traits directly.