# Pager Concurrency Improvement Design

## Current State Analysis

### Identified Locking Bottlenecks

#### 1. **Pagefile (src/pager/pagefile.rs)**
- **Line 39**: `file: Arc<RwLock<FS::File>>` - Entire file wrapped in RwLock
- **Line 43**: `header: Arc<RwLock<FileHeader>>` - Header locked for all operations
- **Line 45**: `superblock: Arc<RwLock<Superblock>>` - Superblock locked for all operations
- **Line 47**: `free_list: Arc<RwLock<FreeList>>` - Free list locked for all operations

**Problems:**
- `read_page()` (line 373) takes **write lock** on file (line 395) even for reads
- `write_page()` (line 422) takes write lock on file (line 451)
- `allocate_page()` (line 214) holds both free_list and superblock write locks atomically (lines 218-227)
- `free_page()` (line 283) holds both free_list and superblock write locks atomically (lines 321-325)
- All page I/O serialized through single file lock
- Header and superblock updates require separate lock acquisitions

#### 2. **Cache (src/pager/cache.rs)**
- **Line 132**: `inner: Arc<RwLock<CacheInner>>` - Entire cache state in single RwLock
- **Line 172**: `get()` takes **write lock** even for reads (to update LRU)
- **Line 196**: `put()` takes write lock
- **Line 258**: `mark_dirty()` takes write lock

**Problems:**
- Cache lookups serialize all operations (even reads need write lock for LRU update)
- No concurrent cache access possible
- LRU list manipulation requires exclusive access

#### 3. **FreeList (src/pager/freelist.rs)**
- Wrapped in RwLock at Pager level
- All allocations/deallocations serialize through this lock
- Simple Vec-based stack (lines 156, 217-230)

**Problems:**
- High contention on allocation/deallocation
- No concurrent access to free pages
- Could use lock-free stack

#### 4. **PinTable (src/pager/pin_table.rs)**
- **Line 36**: `pins: Arc<RwLock<HashMap<PageId, usize>>>` - HashMap in RwLock
- Already relatively fine-grained (per-page reference counting)

**Problems:**
- Could use concurrent HashMap or sharded locks
- Pin/unpin operations serialize

## Proposed Architecture

### Phase 1: Page-Level Locking (High Priority)

#### 1.1 Page Table with Fine-Grained Locks

Create a new `PageTable` structure that maintains per-page locks:

```rust
pub struct PageTable {
    /// Sharded locks for page I/O (reduces contention)
    page_locks: Vec<RwLock<()>>,
    /// Number of shards (power of 2 for fast modulo)
    shard_count: usize,
}

impl PageTable {
    pub fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let page_locks = (0..shard_count)
            .map(|_| RwLock::new(()))
            .collect();
        
        Self { page_locks, shard_count }
    }
    
    fn shard_index(&self, page_id: PageId) -> usize {
        (page_id.as_u64() as usize) & (self.shard_count - 1)
    }
    
    pub fn read_lock(&self, page_id: PageId) -> RwLockReadGuard<()> {
        self.page_locks[self.shard_index(page_id)].read()
    }
    
    pub fn write_lock(&self, page_id: PageId) -> RwLockWriteGuard<()> {
        self.page_locks[self.shard_index(page_id)].write()
    }
}
```

**Benefits:**
- Multiple pages can be read/written concurrently if in different shards
- Reduces contention by factor of shard_count
- Simple to implement and reason about

#### 1.2 Separate File Handle Per Thread (Advanced)

Use thread-local file handles to eliminate file lock contention:

```rust
pub struct Pager<FS: FileSystem> {
    /// Shared file path
    file_path: String,
    /// VFS reference
    fs: Arc<FS>,
    /// Page-level locks
    page_table: PageTable,
    // ... other fields
}

impl<FS: FileSystem> Pager<FS> {
    fn get_file_handle(&self) -> PagerResult<FS::File> {
        // Open file handle per thread (or use a pool)
        self.fs.open_file(&self.file_path)
    }
}
```

**Benefits:**
- Eliminates file lock contention entirely
- OS-level concurrent I/O
- Scales with number of threads

**Tradeoffs:**
- More complex resource management
- Need to ensure file handles are properly closed
- May hit OS file descriptor limits

### Phase 2: Lock-Free Data Structures (Medium Priority)

#### 2.1 Lock-Free Free List

Replace Vec-based free list with lock-free stack using `crossbeam`:

```rust
use crossbeam::queue::SegQueue;

pub struct FreeList {
    /// Lock-free queue of free pages
    free_pages: SegQueue<PageId>,
    /// Atomic counter for total free pages
    total_free: AtomicU64,
    // ... metadata fields (still need some synchronization)
}

impl FreeList {
    pub fn push_page(&self, page_id: PageId) {
        self.free_pages.push(page_id);
        self.total_free.fetch_add(1, Ordering::SeqCst);
    }
    
    pub fn pop_page(&self) -> Option<PageId> {
        let page_id = self.free_pages.pop()?;
        self.total_free.fetch_sub(1, Ordering::SeqCst);
        Some(page_id)
    }
}
```

**Benefits:**
- No lock contention on allocation/deallocation
- Wait-free operations
- Better scalability

#### 2.2 Concurrent Cache with Sharding

Replace single RwLock with sharded cache:

```rust
pub struct PageCache {
    /// Sharded cache entries
    shards: Vec<RwLock<CacheShard>>,
    shard_count: usize,
}

struct CacheShard {
    entries: HashMap<PageId, CacheEntry>,
    lru_head: Option<PageId>,
    lru_tail: Option<PageId>,
    stats: CacheStats,
}

impl PageCache {
    fn shard_index(&self, page_id: PageId) -> usize {
        (page_id.as_u64() as usize) & (self.shard_count - 1)
    }
    
    pub fn get(&self, page_id: PageId) -> Option<Page> {
        let mut shard = self.shards[self.shard_index(page_id)].write();
        // ... operate on shard only
    }
}
```

**Benefits:**
- Concurrent access to different shards
- Reduces cache lock contention
- Each shard maintains its own LRU

**Tradeoffs:**
- Global LRU becomes approximate
- More complex eviction policy
- May need periodic rebalancing

### Phase 3: Optimistic Concurrency Control (Low Priority)

#### 3.1 Version-Based Validation

Add version numbers to pages for optimistic reads:

```rust
pub struct Page {
    // ... existing fields
    version: AtomicU64,
}

impl Pager<FS> {
    pub fn read_page_optimistic(&self, page_id: PageId) -> PagerResult<(Page, u64)> {
        let version_before = self.get_page_version(page_id);
        let page = self.read_page_unlocked(page_id)?;
        let version_after = self.get_page_version(page_id);
        
        if version_before != version_after {
            return Err(PagerError::ConcurrentModification);
        }
        
        Ok((page, version_before))
    }
}
```

**Benefits:**
- Readers don't block writers
- Better read scalability
- Suitable for read-heavy workloads

**Tradeoffs:**
- Retry logic needed
- More complex error handling
- May increase latency under contention

## Implementation Plan

### Step 1: Add Page Table (Week 1)
1. Create `src/pager/page_table.rs` with sharded page locks
2. Integrate into `Pager` struct
3. Update `read_page()` to use page-level read locks
4. Update `write_page()` to use page-level write locks
5. Add tests for concurrent page access

### Step 2: Refactor File Access (Week 1)
1. Remove global file RwLock
2. Implement file handle pooling or thread-local handles
3. Update all file I/O to use new approach
4. Ensure proper cleanup and error handling

### Step 3: Lock-Free Free List (Week 2)
1. Add `crossbeam` dependency
2. Refactor `FreeList` to use `SegQueue`
3. Update allocation/deallocation logic
4. Add stress tests for concurrent allocation

### Step 4: Sharded Cache (Week 2)
1. Refactor `PageCache` to use sharded design
2. Implement per-shard LRU
3. Update cache statistics aggregation
4. Add benchmarks for cache contention

### Step 5: Benchmarking & Tuning (Week 3)
1. Create comprehensive concurrency benchmarks
2. Test with varying shard counts (16, 32, 64, 128)
3. Measure throughput improvement
4. Profile for remaining bottlenecks
5. Document optimal configurations

## Performance Expectations

### Current State (Baseline)
- Single-threaded: ~100K ops/sec
- 4 threads: ~120K ops/sec (20% improvement, high contention)
- 8 threads: ~130K ops/sec (30% improvement, severe contention)

### After Phase 1 (Page-Level Locking)
- Single-threaded: ~100K ops/sec (no regression)
- 4 threads: ~300K ops/sec (3x improvement)
- 8 threads: ~500K ops/sec (5x improvement)

### After Phase 2 (Lock-Free Structures)
- Single-threaded: ~110K ops/sec (slight improvement)
- 4 threads: ~400K ops/sec (4x improvement)
- 8 threads: ~700K ops/sec (7x improvement)

### After Phase 3 (Optimistic Concurrency)
- Read-heavy workload (90% reads):
  - 8 threads: ~1M ops/sec (10x improvement)
- Write-heavy workload (50% writes):
  - 8 threads: ~600K ops/sec (6x improvement)

## Risk Mitigation

### Deadlock Prevention
- Establish lock ordering: page_table < free_list < superblock < header
- Use try_lock with timeout for complex operations
- Add deadlock detection in debug builds

### Data Consistency
- Maintain atomic operations for critical sections
- Use memory barriers appropriately
- Add assertions for invariants
- Comprehensive testing with ThreadSanitizer

### Backward Compatibility
- Keep existing API surface unchanged
- Add feature flags for new concurrency modes
- Provide migration path for existing databases

## Testing Strategy

### Unit Tests
- Per-component lock-free operation tests
- Shard distribution tests
- Lock ordering validation

### Integration Tests
- Concurrent allocation/deallocation
- Concurrent read/write to different pages
- Concurrent read/write to same page
- Cache eviction under load

### Stress Tests
- High-concurrency workloads (100+ threads)
- Mixed read/write patterns
- Long-running stability tests
- Memory leak detection

### Performance Tests
- Throughput benchmarks (ops/sec)
- Latency percentiles (p50, p95, p99)
- Scalability tests (1, 2, 4, 8, 16 threads)
- Comparison with baseline

## Success Criteria

1. **Correctness**: All existing tests pass
2. **Performance**: 3-5x throughput improvement with 8 threads
3. **Scalability**: Linear scaling up to 8 threads
4. **Stability**: No deadlocks or data corruption in 24-hour stress test
5. **Maintainability**: Code complexity remains manageable

## References

- SQLite's page cache implementation
- PostgreSQL's buffer manager
- RocksDB's block cache
- "The Art of Multiprocessor Programming" by Herlihy & Shavit
- "Database Internals" by Alex Petrov