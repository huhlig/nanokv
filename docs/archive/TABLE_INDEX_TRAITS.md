> LSMIterator<'table, S> {
    fn rebuild_heap(&mut self) -> Result<(), IteratorError> {
        self.heap.clear();
        for (level, iter) in self.iterators.iter_mut().enumerate() {
            if let Some(entry) = iter.peek_key().zip(iter.peek_value()) {
                self.heap.push(HeapEntry {
                    key: entry.0.to_vec(),
                    value: entry.1.to_vec(),
                    level,
                    sequence: 0,
                });
            }
        }
        Ok(())
    }
}
```

---

## 6. Transaction Integration

### 6.1 Transaction Context

```rust
/// Transaction context for ACID operations
pub struct TransactionContext {
    txn_id: TransactionId,
    isolation_level: IsolationLevel,
    read_version: Version,
    write_version: Version,
    wal_writer: Arc<RwLock<WalWriter<impl FileSystem>>>,
    locks: LockManager,
}

impl TransactionContext {
    pub fn begin(
        wal: Arc<RwLock<WalWriter<impl FileSystem>>>,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let txn_id = TransactionId::new();
        wal.write().write_begin(txn_id)?;
        
        Ok(Self {
            txn_id,
            isolation_level: isolation,
            read_version: Version::current(),
            write_version: Version::next(),
            wal_writer: wal,
            locks: LockManager::new(),
        })
    }
    
    pub fn commit(self) -> Result<()> {
        self.wal_writer.write().write_commit(self.txn_id)?;
        self.locks.release_all();
        Ok(())
    }
    
    pub fn rollback(self) -> Result<()> {
        self.wal_writer.write().write_rollback(self.txn_id)?;
        self.locks.release_all();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}
```

### 6.2 Transactional Table Wrapper

```rust
/// Wrapper that adds transaction support to any table
pub struct TransactionalTable<T: Table> {
    inner: T,
    txn_context: Option<TransactionContext>,
    undo_log: Vec<UndoRecord>,
}

impl<T: Table> TransactionalTable<T> {
    pub fn new(table: T) -> Self {
        Self {
            inner: table,
            txn_context: None,
            undo_log: Vec::new(),
        }
    }
    
    pub fn begin_transaction(&mut self, context: TransactionContext) {
        self.txn_context = Some(context);
        self.undo_log.clear();
    }
    
    pub fn commit(&mut self) -> Result<()> {
        if let Some(ctx) = self.txn_context.take() {
            ctx.commit()?;
            self.undo_log.clear();
        }
        Ok(())
    }
    
    pub fn rollback(&mut self) -> Result<()> {
        if let Some(ctx) = self.txn_context.take() {
            // Apply undo log in reverse
            for record in self.undo_log.drain(..).rev() {
                match record {
                    UndoRecord::Put { key, old_value } => {
                        if let Some(value) = old_value {
                            self.inner.put(&key, &value)?;
                        } else {
                            self.inner.delete(&key)?;
                        }
                    }
                    UndoRecord::Delete { key, old_value } => {
                        self.inner.put(&key, &old_value)?;
                    }
                }
            }
            ctx.rollback()?;
        }
        Ok(())
    }
}

impl<T: Table> Table for TransactionalTable<T> {
    type Error = T::Error;
    type Key<'a> = T::Key<'a> where Self: 'a;
    type Value<'a> = T::Value<'a> where Self: 'a;
    type Iter<'a> = T::Iter<'a> where Self: 'a;
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        // Record old value for undo
        if self.txn_context.is_some() {
            let old_value = self.inner.get(key)?;
            self.undo_log.push(UndoRecord::Put {
                key: key.to_vec(),
                old_value: old_value.map(|v| v.as_ref().to_vec()),
            });
        }
        
        self.inner.put(key, value)
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Self::Value<'_>>, Self::Error> {
        self.inner.get(key)
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool, Self::Error> {
        // Record old value for undo
        if self.txn_context.is_some() {
            if let Some(old_value) = self.inner.get(key)? {
                self.undo_log.push(UndoRecord::Delete {
                    key: key.to_vec(),
                    old_value: old_value.as_ref().to_vec(),
                });
            }
        }
        
        self.inner.delete(key)
    }
    
    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.inner.contains(key)
    }
    
    fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) 
        -> Result<Self::Iter<'_>, Self::Error> {
        self.inner.range(start, end)
    }
    
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

enum UndoRecord {
    Put { key: Vec<u8>, old_value: Option<Vec<u8>> },
    Delete { key: Vec<u8>, old_value: Vec<u8> },
}
```

---

## 7. Concurrency and MVCC

### 7.1 MVCC Implementation

```rust
/// Multi-version concurrency control
pub struct MVCCTable<T: Table> {
    inner: T,
    versions: Arc<RwLock<VersionMap>>,
    current_version: AtomicU64,
    gc_threshold: u64,
}

struct VersionMap {
    // key -> [(version, value)]
    data: HashMap<Vec<u8>, Vec<(Version, Option<Vec<u8>>)>>,
}

impl<T: Table> MVCCTable<T> {
    pub fn new(table: T) -> Self {
        Self {
            inner: table,
            versions: Arc::new(RwLock::new(VersionMap {
                data: HashMap::new(),
            })),
            current_version: AtomicU64::new(1),
            gc_threshold: 100,
        }
    }
    
    pub fn snapshot(&self) -> MVCCSnapshot {
        MVCCSnapshot {
            version: Version(self.current_version.load(Ordering::SeqCst)),
            versions: Arc::clone(&self.versions),
        }
    }
    
    pub fn gc_old_versions(&mut self, before: Version) -> Result<usize> {
        let mut versions = self.versions.write();
        let mut removed = 0;
        
        for (_key, version_list) in versions.data.iter_mut() {
            version_list.retain(|(v, _)| {
                if *v < before {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
        }
        
        Ok(removed)
    }
}

impl<T: Table> Table for MVCCTable<T> {
    type Error = T::Error;
    type Key<'a> = T::Key<'a> where Self: 'a;
    type Value<'a> = Vec<u8>;  // Always owned for MVCC
    type Iter<'a> = MVCCIterator<'a, T>;
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let version = Version(self.current_version.fetch_add(1, Ordering::SeqCst));
        
        // Store version
        let mut versions = self.versions.write();
        versions.data
            .entry(key.to_vec())
            .or_insert_with(Vec::new)
            .push((version, Some(value.to_vec())));
        
        // Write to underlying table
        self.inner.put(key, value)
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        // Get latest version
        let versions = self.versions.read();
        if let Some(version_list) = versions.data.get(key) {
            if let Some((_, value)) = version_list.last() {
                return Ok(value.clone());
            }
        }
        
        self.inner.get(key).map(|v| v.map(|v| v.as_ref().to_vec()))
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool, Self::Error> {
        let version = Version(self.current_version.fetch_add(1, Ordering::SeqCst));
        
        // Store tombstone
        let mut versions = self.versions.write();
        versions.data
            .entry(key.to_vec())
            .or_insert_with(Vec::new)
            .push((version, None));
        
        self.inner.delete(key)
    }
    
    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.get(key).map(|v| v.is_some())
    }
    
    fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) 
        -> Result<Self::Iter<'_>, Self::Error> {
        let snapshot = self.snapshot();
        Ok(MVCCIterator {
            inner: self.inner.range(start, end)?,
            snapshot,
        })
    }
    
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

pub struct MVCCSnapshot {
    version: Version,
    versions: Arc<RwLock<VersionMap>>,
}

impl MVCCSnapshot {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let versions = self.versions.read();
        if let Some(version_list) = versions.data.get(key) {
            // Find latest version <= snapshot version
            for (v, value) in version_list.iter().rev() {
                if *v <= self.version {
                    return value.clone();
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(u64);

impl Version {
    pub fn current() -> Self {
        Self(0)  // Placeholder
    }
    
    pub fn next() -> Self {
        Self(1)  // Placeholder
    }
}
```

---

## 8. Statistics and Query Planning

### 8.1 Statistics Collection

```rust
/// Statistics collector for query optimization
pub struct StatisticsCollector<T: Table> {
    table: T,
    stats: Arc<RwLock<TableStatistics>>,
    sample_rate: f64,
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub total_size: u64,
    pub key_stats: KeyStatistics,
    pub value_stats: ValueStatistics,
    pub histogram: Option<Histogram>,
    pub last_updated: Timestamp,
}

#[derive(Debug, Clone)]
pub struct KeyStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub distinct_count: u64,
}

#[derive(Debug, Clone)]
pub struct ValueStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub null_count: u64,
}

impl<T: Table> StatisticsCollector<T> {
    pub fn new(table: T, sample_rate: f64) -> Self {
        Self {
            table,
            stats: Arc::new(RwLock::new(TableStatistics::default())),
            sample_rate,
        }
    }
    
    pub fn collect_stats(&mut self) -> Result<()> {
        let mut row_count = 0u64;
        let mut total_size = 0u64;
        let mut key_sizes = Vec::new();
        let mut value_sizes = Vec::new();
        let mut distinct_keys = HashSet::new();
        
        // Sample the table
        let iter = self.table.range(Bound::Unbounded, Bound::Unbounded)?;
        for result in iter {
            let entry = result?;
            
            // Sample based on rate
            if rand::random::<f64>() > self.sample_rate {
                continue;
            }
            
            row_count += 1;
            key_sizes.push(entry.key.len());
            value_sizes.push(entry.value.len());
            total_size += entry.key.len() as u64 + entry.value.len() as u64;
            distinct_keys.insert(entry.key);
        }
        
        // Calculate statistics
        let key_stats = KeyStatistics {
            min_size: *key_sizes.iter().min().unwrap_or(&0),
            max_size: *key_sizes.iter().max().unwrap_or(&0),
            avg_size: key_sizes.iter().sum::<usize>() as f64 / key_sizes.len() as f64,
            distinct_count: distinct_keys.len() as u64,
        };
        
        let value_stats = ValueStatistics {
            min_size: *value_sizes.iter().min().unwrap_or(&0),
            max_size: *value_sizes.iter().max().unwrap_or(&0),
            avg_size: value_sizes.iter().sum::<usize>() as f64 / value_sizes.len() as f64,
            null_count: 0,
        };
        
        let mut stats = self.stats.write();
        stats.row_count = row_count;
        stats.total_size = total_size;
        stats.key_stats = key_stats;
        stats.value_stats = value_stats;
        stats.last_updated = Timestamp::now();
        
        Ok(())
    }
    
    pub fn get_stats(&self) -> TableStatistics {
        self.stats.read().clone()
    }
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self {
            row_count: 0,
            total_size: 0,
            key_stats: KeyStatistics {
                min_size: 0,
                max_size: 0,
                avg_size: 0.0,
                distinct_count: 0,
            },
            value_stats: ValueStatistics {
                min_size: 0,
                max_size: 0,
                avg_size: 0.0,
                null_count: 0,
            },
            histogram: None,
            last_updated: Timestamp::now(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        Self(0)  // Placeholder
    }
}
```

---

## 9. Error Recovery and Consistency

### 9.1 Consistency Verification

```rust
/// Consistency checker for tables and indexes
pub struct ConsistencyChecker<T: Table> {
    table: T,
}

impl<T: Table> ConsistencyChecker<T> {
    pub fn new(table: T) -> Self {
        Self { table }
    }
    
    pub fn verify(&self) -> Result<Vec<ConsistencyError>> {
        let mut errors = Vec::new();
        
        // Check 1: Verify all keys are ordered (for ordered tables)
        if let Ok(iter) = self.table.range(Bound::Unbounded, Bound::Unbounded) {
            let mut prev_key: Option<Vec<u8>> = None;
            for result in iter {
                match result {
                    Ok(entry) => {
                        if let Some(ref prev) = prev_key {
                            if entry.key < *prev {
                                errors.push(ConsistencyError {
                                    error_type: ConsistencyErrorType::InvalidPointer,
                                    location: format!("key ordering"),
                                    description: format!(
                                        "Key {:?} comes after {:?} but is smaller",
                                        entry.key, prev
                                    ),
                                    severity: Severity::Error,
                                });
                            }
                        }
                        prev_key = Some(entry.key);
                    }
                    Err(e) => {
                        errors.push(ConsistencyError {
                            error_type: ConsistencyErrorType::CorruptedIndex,
                            location: format!("iteration"),
                            description: format!("Iterator error: {}", e),
                            severity: Severity::Critical,
                        });
                    }
                }
            }
        }
        
        // Check 2: Verify checksums (if applicable)
        // Check 3: Verify no orphaned pages
        // Check 4: Verify index consistency
        
        Ok(errors)
    }
    
    pub fn repair(&mut self, error: &ConsistencyError) -> Result<()> {
        match error.error_type {
            ConsistencyErrorType::OrphanedPage => {
                // Attempt to reclaim orphaned pages
                Ok(())
            }
            ConsistencyErrorType::CorruptedIndex => {
                // Attempt to rebuild index
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Cannot repair error: {:?}", error)),
        }
    }
}
```

---

## 10. Performance Considerations

### 10.1 Zero-Cost Abstractions

**Key Principle**: Abstractions should compile down to the same code as hand-written implementations.

**Techniques**:
1. **Monomorphization**: Use generics instead of trait objects
2. **Inline hints**: Mark hot paths with `#[inline]`
3. **Associated types**: Avoid boxing when possible
4. **Const generics**: Compile-time specialization

```rust
// Good: Monomorphized, zero overhead
pub fn process_table<T: Table>(table: &T, key: &[u8]) -> Result<()> {
    table.get(key)?;
    Ok(())
}

// Bad: Dynamic dispatch, vtable overhead
pub fn process_table_dyn(table: &dyn Table, key: &[u8]) -> Result<()> {
    table.get(key)?;
    Ok(())
}

// Good: Const generic for compile-time optimization
pub struct FixedSizeKey<const N: usize>([u8; N]);

impl<const N: usize> FixedSizeKey<N> {
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
```

### 10.2 Memory Layout Optimization

```rust
/// Optimized node layout for cache efficiency
#[repr(C)]
pub struct BTreeNode {
    // Hot fields first (accessed on every operation)
    pub key_count: u16,
    pub is_leaf: bool,
    pub _padding: u8,
    
    // Warm fields (accessed frequently)
    pub parent: PageId,
    pub next_sibling: PageId,
    
    // Cold fields (accessed rarely)
    pub metadata: NodeMetadata,
    
    // Data arrays (cache-line aligned)
    pub keys: [Vec<u8>; MAX_KEYS],
    pub children: [PageId; MAX_CHILDREN],
}

// Ensure proper alignment
const _: () = assert!(std::mem::align_of::<BTreeNode>() == 64);
```

### 10.3 Batch Operations

```rust
/// Batch operations for better performance
pub trait BatchTable: Table {
    /// Batch insert (single write barrier)
    fn batch_put(&mut self, entries: &[(&[u8], &[u8])]) -> Result<Vec<Result<()>>>;
    
    /// Batch get (single read barrier)
    fn batch_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::Value<'_>>>>;
    
    /// Batch delete (single write barrier)
    fn batch_delete(&mut self, keys: &[&[u8]]) -> Result<Vec<Result<bool>>>;
}

impl<T: Table> BatchTable for T {
    fn batch_put(&mut self, entries: &[(&[u8], &[u8])]) -> Result<Vec<Result<()>>> {
        // Default implementation: sequential puts
        // Implementations can override for better performance
        entries.iter()
            .map(|(k, v)| self.put(k, v))
            .collect()
    }
    
    fn batch_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::Value<'_>>>> {
        keys.iter()
            .map(|k| self.get(k))
            .collect()
    }
    
    fn batch_delete(&mut self, keys: &[&[u8]]) -> Result<Vec<Result<bool>>> {
        keys.iter()
            .map(|k| self.delete(k))
            .collect()
    }
}
```

---

## 11. Integration with Existing Architecture

### 11.1 Pager Integration

```rust
/// Table implementation using existing Pager
pub struct PagedBTreeTable<FS: FileSystem> {
    pager: Arc<Pager<FS>>,
    root_page_id: PageId,
    config: BTreeConfig,
}

impl<FS: FileSystem> PagedBTreeTable<FS> {
    pub fn create(pager: Arc<Pager<FS>>, config: BTreeConfig) -> Result<Self> {
        // Allocate root page
        let root_page_id = pager.allocate_page(PageType::BTreeInternal)?;
        
        // Initialize root node
        let mut root_page = Page::new(
            root_page_id,
            PageType::BTreeInternal,
            pager.page_size().data_size(),
        );
        
        // Write empty root
        let root_node = BTreeNode::new_leaf();
        root_page.data_mut().extend_from_slice(&root_node.serialize()?);
        pager.write_page(&root_page)?;
        
        Ok(Self {
            pager,
            root_page_id,
            config,
        })
    }
    
    pub fn open(pager: Arc<Pager<FS>>, root_page_id: PageId) -> Result<Self> {
        // Read root page to get config
        let root_page = pager.read_page(root_page_id)?;
        let root_node = BTreeNode::deserialize(root_page.data())?;
        
        Ok(Self {
            pager,
            root_page_id,
            config: BTreeConfig::default(),
        })
    }
}

impl<FS: FileSystem> Table for PagedBTreeTable<FS> {
    type Error = PagerError;
    type Key<'a> = Vec<u8>;
    type Value<'a> = Vec<u8>;
    type Iter<'a> = BTreeIterator<'a, PageBackend<FS>>;
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), PagerError> {
        // Navigate to leaf
        let leaf_page_id = self.find_leaf(key)?;
        let mut leaf_page = self.pager.read_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize(leaf_page.data())?;
        
        // Insert into leaf
        leaf_node.insert(key, value)?;
        
        // Check if split needed
        if leaf_node.should_split() {
            self.split_leaf(leaf_page_id, &mut leaf_node)?;
        }
        
        // Write back
        leaf_page.data_mut().clear();
        leaf_page.data_mut().extend_from_slice(&leaf_node.serialize()?);
        self.pager.write_page(&leaf_page)?;
        
        Ok(())
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PagerError> {
        let leaf_page_id = self.find_leaf(key)?;
        let leaf_page = self.pager.read_page(leaf_page_id)?;
        let leaf_node = BTreeNode::deserialize(leaf_page.data())?;
        
        Ok(leaf_node.get(key).map(|v| v.to_vec()))
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool, PagerError> {
        let leaf_page_id = self.find_leaf(key)?;
        let mut leaf_page = self.pager.read_page(leaf_page_id)?;
        let mut leaf_node = BTreeNode::deserialize(leaf_page.data())?;
        
        let deleted = leaf_node.delete(key);
        
        if deleted {
            // Check if merge needed
            if leaf_node.should_merge() {
                self.merge_leaf(leaf_page_id, &mut leaf_node)?;
            }
            
            // Write back
            leaf_page.data_mut().clear();
            leaf_page.data_mut().extend_from_slice(&leaf_node.serialize()?);
            self.pager.write_page(&leaf_page)?;
        }
        
        Ok(deleted)
    }
    
    fn contains(&self, key: &[u8]) -> Result<bool, PagerError> {
        self.get(key).map(|v| v.is_some())
    }
    
    fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) 
        -> Result<Self::Iter<'_>, PagerError> {
        // Create iterator starting at start bound
        Ok(BTreeIterator::new(self, start, end))
    }
    
    fn flush(&mut self) -> Result<(), PagerError> {
        self.pager.sync()
    }
}
```

### 11.2 WAL Integration

```rust
/// WAL-backed table wrapper
pub struct WALTable<T: Table, FS: FileSystem> {
    inner: T,
    wal: Arc<RwLock<WalWriter<FS>>>,
    table_name: String,
}

impl<T: Table, FS: FileSystem> WALTable<T, FS> {
    pub fn new(table: T, wal: Arc<RwLock<WalWriter<FS>>>, name: String) -> Self {
        Self {
            inner: table,
            wal,
            table_name: name,
        }
    }
}

impl<T: Table, FS: FileSystem> Table for WALTable<T, FS> {
    type Error = T::Error;
    type Key<'a> = T::Key<'a> where Self: 'a;
    type Value<'a> = T::Value<'a> where Self: 'a;
    type Iter<'a> = T::Iter<'a> where Self: 'a;
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        // Write to WAL first
        let txn_id = TransactionId::new();
        self.wal.write().write_operation(
            txn_id,
            self.table_name.clone(),
            WriteOpType::Put,
            key.to_vec(),
            value.to_vec(),
        ).map_err(|_| /* convert error */)?;
        
        // Then write to table
        self.inner.put(key, value)
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<Self::Value<'_>>, Self::Error> {
        self.inner.get(key)
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool, Self::Error> {
        // Write to WAL first
        let txn_id = TransactionId::new();
        self.wal.write().write_operation(
            txn_id,
            self.table_name.clone(),
            WriteOpType::Delete,
            key.to_vec(),
            vec![],
        ).map_err(|_| /* convert error */)?;
        
        self.inner.delete(key)
    }
    
    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        self.inner.contains(key)
    }
    
    fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) 
        -> Result<Self::Iter<'_>, Self::Error> {
        self.inner.range(start, end)
    }
    
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.wal.write().flush().map_err(|_| /* convert error */)?;
        self.inner.flush()
    }
}
```

---

## 12. Example Implementations

### 12.1 Simple Memory Table

```rust
/// Simple in-memory table using BTreeMap
pub struct MemoryTable {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Table for MemoryTable {
    type Error = std::convert::Infallible;
    type Key<'a> = &'a [u8];
    type Value<'a> = &'a [u8];
    type Iter<'a> = MemoryTableIterator<'a>;
    
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Self::Error> {
        Ok(self.data.get(key).map(|v| v.as_slice()))
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(self.data.remove(key).is_some())
    }
    
    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(self.data.contains_key(key))
    }
    
    fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) 
        -> Result<Self::Iter<'_>, Self::Error> {
        Ok(MemoryTableIterator {
            iter: self.data.range((start, end)),
        })
    }
    
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub struct MemoryTableIterator<'a> {
    iter: std::collections::btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> TableIterator<'a> for MemoryTableIterator<'a> {
    fn snapshot_id(&self) -> SnapshotId {
        SnapshotId(0)
    }
    
    fn is_valid(&self) -> bool {
        true
    }
    
    fn seek(&mut self, _key: &[u8]) -> Result<(), IteratorError> {
        // Not supported for BTreeMap iterator
        Ok(())
    }
    
    fn seek_for_prev(&mut self, _key: &[u8]) -> Result<(), IteratorError> {
        Ok(())
    }
    
    fn peek_key(&self) -> Option<&[u8]> {
        None  // Not supported
    }
    
    fn peek_value(&self) -> Option<&[u8]> {
        None  // Not supported
    }
}

impl<'a> Iterator for MemoryTableIterator<'a> {
    type Item = Result<Entry, IteratorError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| Ok(Entry {
            key: k.clone(),
            value: v.clone(),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotId(u64);
```

### 12.2 Bloom Filter Index

```rust
/// Bloom filter for membership testing
pub struct BloomFilterIndex {
    bitmap: Vec<u64>,
    size_bits: usize,
    hash_count: usize,
}

impl BloomFilterIndex {
    pub fn new(size_bits: usize, hash_count: usize) -> Self {
        let words = (size_bits + 63) / 64;
        Self {
            bitmap: vec![0; words],
            size_bits,
            hash_count,
        }
    }
    
    fn hash(&self, key: &[u8], seed: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % self.size_bits
    }
    
    pub fn might_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let bit = self.hash(key, i);
            let word = bit / 64;
            let bit_in_word = bit % 64;
            
            if (self.bitmap[word] & (1u64 << bit_in_word)) == 0 {
                return false;
            }
        }
        true
    }
}

impl Index for BloomFilterIndex {
    type Error = std::convert::Infallible;
    type Query = Vec<u8>;
    type Result = bool;
    
    fn index_type(&self) -> IndexType {
        IndexType::Bloom(BloomConfig {
            size_bits: self.size_bits as u64,
            hash_functions: self.hash_count as u8,
            false_positive_rate: 0.01,
        })
    }
    
    fn insert(&mut self, key: &[u8], _location: &[u8]) -> Result<(), Self::Error> {
        for i in 0..self.hash_count {
            let bit = self.hash(key, i);
            let word = bit / 64;
            let bit_in_word = bit % 64;
            
            self.bitmap[word] |= 1u64 << bit_in_word;
        }
        Ok(())
    }
    
    fn remove(&mut self, _key: &[u8]) -> Result<bool, Self::Error> {
        // Bloom filters don't support removal
        Ok(false)
    }
    
    fn query(&self, query: &Self::Query) -> Result<Self::Result, Self::Error> {
        Ok(self.might_contain(query))
    }
    
    fn stats(&self) -> IndexStats {
        let set_bits = self.bitmap.iter()
            .map(|w| w.count_ones() as u64)
            .sum();
        
        IndexStats {
            entry_count: set_bits,
            size_bytes: self.bitmap.len() * 8,
            index_type: self.index_type(),
        }
    }
    
    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ApproximateIndex for BloomFilterIndex {
    fn might_contain(&self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(self.might_contain(key))
    }
    
    fn false_positive_rate(&self) -> f64 {
        let m = self.size_bits as f64;
        let k = self.hash_count as f64;
        let n = self.stats().entry_count as f64;
        
        (1.0 - (-k * n / m).exp()).powf(k)
    }
}
```

---

## 13. Migration Path

### 13.1 Phase 1: Core Traits (Week 1)

1. Define core trait hierarchy in `src/table/mod.rs` and `src/index/mod.rs`
2. Implement basic types (errors, configs, stats)
3. Add comprehensive documentation
4. Create trait tests

### 13.2 Phase 2: Memory Implementation (Week 2)

1. Implement `MemoryTable` using `BTreeMap`
2. Implement `MemoryTableIterator`
3. Add unit tests
4. Benchmark against raw `BTreeMap`

### 13.3 Phase 3: Pager Integration (Week 3)

1. Implement `PageBackend` storage abstraction
2. Create `PagedBTreeTable` using existing `Pager`
3. Integrate with WAL
4. Add integration tests

### 13.4 Phase 4: Advanced Features (Week 4)

1. Implement MVCC wrapper
2. Add transaction support
3. Implement statistics collection
4. Add consistency checking

### 13.5 Phase 5: Specialized Indexes (Weeks 5-8)

1. Bloom filter index
2. Full-text index
3. Vector index (HNSW)
4. Spatial index (R-Tree)

---

## 14. Conclusion

This trait hierarchy design provides:

1. **Flexibility**: Multiple storage backends, table types, and index types
2. **Performance**: Zero-cost abstractions, monomorphization, cache-friendly layouts
3. **Safety**: Explicit lifetimes, strong typing, error handling
4. **Composability**: Traits can be mixed and matched
5. **Maintainability**: Clear separation of concerns, well-documented

The design explicitly addresses hidden factors like:
- Iterator invalidation semantics
- MVCC and snapshot isolation
- Memory pressure handling
- Statistics collection for query optimization
- Error recovery and consistency
- Zero-copy vs owned data tradeoffs

Integration with existing VFS, Pager, and WAL layers is straightforward through the storage abstraction layer.

---

**End of Document**