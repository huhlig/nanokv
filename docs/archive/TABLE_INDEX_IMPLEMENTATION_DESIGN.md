Weeks 10-12)
- **Week 10**: BTree index
  - Reuse BTreeTable for storage
  - Composite key handling
  - Index cursor
  
- **Week 11**: Hash index and Bloom filter
  - Hash bucket management
  - Bloom filter implementation
  - Integration with LSM
  
- **Week 12**: Testing and optimization
  - Integration tests
  - Performance benchmarks
  - Bug fixes

### Phase 5: LSMTable (Weeks 13-15)
- **Week 13**: Memtable and SSTable
  - Skip list memtable
  - SSTable format
  - Flush mechanism
  
- **Week 14**: Compaction
  - Size-tiered compaction
  - Leveled compaction (optional)
  - Background compaction thread
  
- **Week 15**: Integration and testing
  - WAL integration
  - MVCC support
  - Performance testing

### Phase 6: Specialized Indexes (Weeks 16-20)
- **Week 16**: Full-text index
  - Tokenizer
  - Inverted index
  - Query processing
  
- **Week 17**: Vector index (HNSW)
  - HNSW graph structure
  - Vector search
  - Distance metrics
  
- **Week 18**: Spatial index (R-Tree)
  - R-Tree nodes
  - Spatial queries
  - MBR calculations
  
- **Week 19**: Graph and time-series indexes
  - Graph adjacency lists
  - Time-series buckets
  - Specialized queries
  
- **Week 20**: Final integration and testing
  - End-to-end tests
  - Performance optimization
  - Documentation

---

## 8. Testing Strategy

### 8.1 Unit Tests

**BTreeTable Tests** (`tests/btree_table_tests.rs`):
```rust
#[test]
fn test_btree_insert_and_get() {
    let fs = LocalFileSystem::new();
    let pager = Pager::create(&fs, "test.db", PagerConfig::default()).unwrap();
    let mut table = BTreeTable::create(Arc::new(pager), TableId::from(1), "test".to_string()).unwrap();
    
    table.put(b"key1", b"value1", 0, 0).unwrap();
    assert_eq!(table.get(b"key1").unwrap(), Some(b"value1".to_vec()));
}

#[test]
fn test_btree_split() {
    // Test node splitting when full
}

#[test]
fn test_btree_mvcc() {
    // Test version chains and snapshot isolation
}
```

**LSMTable Tests** (`tests/lsm_table_tests.rs`):
```rust
#[test]
fn test_lsm_memtable_flush() {
    // Test memtable rotation and flush to SSTable
}

#[test]
fn test_lsm_compaction() {
    // Test compaction strategies
}

#[test]
fn test_lsm_bloom_filter() {
    // Test bloom filter effectiveness
}
```

**Transaction Tests** (`tests/transaction_tests.rs`):
```rust
#[test]
fn test_transaction_commit() {
    // Test successful commit
}

#[test]
fn test_transaction_rollback() {
    // Test rollback and cleanup
}

#[test]
fn test_concurrent_transactions() {
    // Test isolation between concurrent transactions
}

#[test]
fn test_deadlock_detection() {
    // Test deadlock detection and resolution
}
```

### 8.2 Integration Tests

**Table-Index Integration** (`tests/table_index_integration_tests.rs`):
```rust
#[test]
fn test_table_with_btree_index() {
    // Create table, add index, verify queries use index
}

#[test]
fn test_table_with_bloom_filter() {
    // Test LSM table with bloom filters
}

#[test]
fn test_full_text_search() {
    // Test full-text index with document insertion and search
}
```

### 8.3 Concurrency Tests

**MVCC Tests** (`tests/mvcc_tests.rs`):
```rust
#[test]
fn test_snapshot_isolation() {
    // Multiple readers at different snapshots
}

#[test]
fn test_write_write_conflict() {
    // Test conflict detection
}

#[test]
fn test_concurrent_reads_and_writes() {
    // Heavy concurrent load
}
```

### 8.4 Recovery Tests

**Crash Recovery** (`tests/recovery_tests.rs`):
```rust
#[test]
fn test_wal_recovery() {
    // Simulate crash, recover from WAL
}

#[test]
fn test_partial_transaction_recovery() {
    // Test recovery of incomplete transactions
}

#[test]
fn test_catalog_recovery() {
    // Test catalog reconstruction from metadata
}
```

### 8.5 Performance Benchmarks

**Benchmark Suite** (`benches/table_index_benchmarks.rs`):
```rust
fn bench_btree_insert(c: &mut Criterion) {
    c.bench_function("btree_insert_1000", |b| {
        b.iter(|| {
            // Insert 1000 keys
        });
    });
}

fn bench_lsm_write_throughput(c: &mut Criterion) {
    // Measure write throughput
}

fn bench_index_lookup(c: &mut Criterion) {
    // Measure index lookup performance
}

fn bench_full_text_search(c: &mut Criterion) {
    // Measure search performance
}
```

---

## 9. Open Questions and Decisions Needed

### 9.1 BTree Configuration

**Question**: What should be the default B-Tree order?
- **Option A**: Fixed order (e.g., 128) for simplicity
- **Option B**: Dynamic based on page size
- **Recommendation**: Dynamic based on page size for flexibility

**Question**: Should we support variable-length keys efficiently?
- **Option A**: Prefix compression for keys
- **Option B**: Simple length-prefixed encoding
- **Recommendation**: Start with simple encoding, add compression later

### 9.2 LSM Compaction Strategy

**Question**: Which compaction strategy should be default?
- **Option A**: Size-tiered (simpler, better for write-heavy)
- **Option B**: Leveled (better read performance, more complex)
- **Recommendation**: Size-tiered as default, leveled as option

**Question**: Should compaction be synchronous or asynchronous?
- **Option A**: Synchronous (simpler, blocks writes)
- **Option B**: Asynchronous background thread (complex, better performance)
- **Recommendation**: Asynchronous with configurable thread pool

### 9.3 MVCC Garbage Collection

**Question**: When should old versions be garbage collected?
- **Option A**: During compaction only
- **Option B**: Separate GC thread
- **Option C**: On-demand during reads
- **Recommendation**: During compaction for LSM, separate GC for BTree

**Question**: How long should versions be retained?
- **Option A**: Until no active snapshots reference them
- **Option B**: Configurable retention period
- **Recommendation**: Option A with optional minimum retention

### 9.4 Index Maintenance

**Question**: Should indexes be updated synchronously or asynchronously?
- **Option A**: Synchronous (consistent, slower writes)
- **Option B**: Asynchronous (faster writes, eventual consistency)
- **Recommendation**: Synchronous for critical indexes, async option for others

**Question**: How should we handle index corruption?
- **Option A**: Automatic rebuild on detection
- **Option B**: Manual rebuild required
- **Recommendation**: Automatic rebuild with user notification

### 9.5 Memory Management

**Question**: How should we limit memory usage?
- **Option A**: Global memory budget across all components
- **Option B**: Per-component budgets
- **Recommendation**: Global budget with per-component hints

**Question**: What eviction policy should we use?
- **Option A**: LRU (simple, predictable)
- **Option B**: ARC (adaptive, better hit rate)
- **Recommendation**: Start with LRU, add ARC as option

### 9.6 Performance vs Complexity Trade-offs

**Question**: Should we optimize for read or write performance?
- **Consideration**: BTree favors reads, LSM favors writes
- **Recommendation**: Provide both, let users choose based on workload

**Question**: How much complexity is acceptable for performance gains?
- **Consideration**: Advanced features like bloom filters, compression
- **Recommendation**: Implement incrementally, measure impact

---

## 10. Appendix: Code Examples

### 10.1 Complete BTree Insert Example

```rust
// src/table/btree/insert.rs
impl<FS: FileSystem> BTreeTable<FS> {
    pub fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        lsn: Lsn,
        tx_id: TxId,
    ) -> Result<(), Error> {
        // Find leaf page
        let leaf_page_id = self.find_leaf_page(key)?;
        let mut leaf_page = self.pager.read_page(leaf_page_id)?;
        let mut leaf_node = LeafNode::from_page(&leaf_page)?;
        
        // Check if key exists (for MVCC update)
        if let Some(cell_idx) = leaf_node.find_key(key) {
            // Update existing key with new version
            let cell = &mut leaf_node.cells[cell_idx];
            cell.version_chain.prepend_version(lsn, tx_id, value)?;
        } else {
            // Insert new key
            let cell = LeafCell {
                key: key.to_vec(),
                version_chain: VersionChain::new(lsn, tx_id, value),
            };
            
            // Check if page is full
            if leaf_node.is_full() {
                let (new_page_id, split_key) = self.split_leaf(leaf_page_id)?;
                
                // Decide which page to insert into
                if key < split_key.as_slice() {
                    leaf_node.insert_cell(cell)?;
                } else {
                    let mut new_page = self.pager.read_page(new_page_id)?;
                    let mut new_node = LeafNode::from_page(&new_page)?;
                    new_node.insert_cell(cell)?;
                    self.pager.write_page(new_page_id, &new_node.to_page())?;
                }
                
                // Update parent
                self.insert_into_parent(leaf_page_id, split_key, new_page_id)?;
            } else {
                leaf_node.insert_cell(cell)?;
            }
        }
        
        // Write updated page
        self.pager.write_page(leaf_page_id, &leaf_node.to_page())?;
        
        // Update metadata
        self.metadata.write().entry_count += 1;
        self.metadata.write().last_lsn = lsn;
        
        Ok(())
    }
}
```

### 10.2 Complete LSM Get Example

```rust
// src/table/lsm/get.rs
impl<FS: FileSystem> LSMTable<FS> {
    pub fn get(&self, key: &[u8], snapshot_lsn: Lsn) -> Result<Option<Vec<u8>>, Error> {
        // 1. Check memtable
        if let Some(value) = self.memtable.read().get(key, snapshot_lsn) {
            return Ok(value);
        }
        
        // 2. Check immutable memtables (newest first)
        for memtable in self.immutable_memtables.read().iter().rev() {
            if let Some(value) = memtable.get(key, snapshot_lsn) {
                return Ok(value);
            }
        }
        
        // 3. Check SSTables level by level
        let levels = self.levels.read();
        for level in levels.iter() {
            // Find SSTables that might contain the key
            let candidates: Vec<&SSTable<FS>> = level.sstables.iter()
                .filter(|sst| sst.might_contain_key(key))
                .collect();
            
            // Search in reverse order (newest first)
            for sstable in candidates.iter().rev() {
                // Check bloom filter first
                if !sstable.bloom_filter.might_contain(key) {
                    continue;
                }
                
                // Binary search in SSTable
                if let Some(value) = sstable.get(key, snapshot_lsn)? {
                    return Ok(value);
                }
            }
        }
        
        Ok(None)
    }
}
```

### 10.3 Complete Transaction Example

```rust
// Example usage
fn example_transaction() -> Result<(), Error> {
    let db = Database::open("mydb.db")?;
    
    // Begin transaction
    let mut tx = db.begin_write(Durability::SyncOnCommit)?;
    
    // Get table
    let table_id = db.open_table("users")?.unwrap();
    
    // Insert data
    tx.put(table_id, b"user:1", b"Alice")?;
    tx.put(table_id, b"user:2", b"Bob")?;
    
    // Query with cursor
    let mut cursor = tx.cursor(table_id, ScanBounds::All)?;
    cursor.first()?;
    while cursor.valid() {
        if let (Some(key), Some(value)) = (cursor.key(), cursor.value()) {
            println!("{:?} = {:?}", key, value);
        }
        cursor.next()?;
    }
    
    // Commit
    let commit_info = tx.commit()?;
    println!("Committed at LSN {}", commit_info.commit_lsn);
    
    Ok(())
}
```

---

## 11. References

### 11.1 Existing Documentation
- [TABLE_INDEX_ARCHITECTURE.md](docs/TABLE_INDEX_ARCHITECTURE.md) - High-level architecture
- [TABLE_INDEX_TRAITS.md](docs/TABLE_INDEX_TRAITS.md) - Trait design details
- [TABLE_INDEX_ISSUES.md](docs/TABLE_INDEX_ISSUES.md) - Implementation issues

### 11.2 Existing Code
- [`Pager`](src/pager/pagefile.rs:37-52) - Page management
- [`WAL`](src/wal.rs:1-100) - Write-ahead logging
- [`VFS`](src/vfs/filesystem.rs:21-49) - File system abstraction
- [`embedded_kv_traits.rs`](src/embedded_kv_traits.rs:1-1841) - Trait definitions

### 11.3 External References
- SQLite B-Tree implementation
- RocksDB LSM design
- PostgreSQL MVCC
- LevelDB compaction strategies

---

**Document Status**: Complete and ready for implementation

**Next Steps**:
1. Review and approve this design
2. Create implementation issues in beads
3. Begin Phase 1: Trait organization
4. Implement BTreeTable (Phase 2)
5. Continue through remaining phases
