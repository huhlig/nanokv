# Pager Lock Ordering and Deadlock Prevention

## Overview

This document establishes the lock acquisition order for the pager module to prevent deadlocks. All code in the pager module MUST follow this ordering to ensure thread safety.

## Lock Hierarchy (Acquire in This Order)

The locks must be acquired in the following order to prevent deadlocks:

1. **pin_table** (PinTable::pins - RwLock)
2. **superblock** (Arc<RwLock<Superblock>>)
3. **header** (Arc<RwLock<FileHeader>>)
4. **page_table** (PageTable - sharded RwLocks)
5. **cache** (PageCache - sharded RwLocks)
6. **file** (Arc<RwLock<FS::File>>)

**Note**: `free_list` uses lock-free atomics and can be accessed at any time without ordering concerns.

## Rationale

### Why This Order?

1. **pin_table first**: Must be acquired before any page operations to prevent use-after-free
2. **superblock before page_table**: Superblock tracks global state (total pages, free count) that affects page allocation
3. **header before page_table**: Header contains metadata that may be needed before page operations
4. **page_table before cache**: Page-level locks protect individual pages, cache is a performance optimization
5. **cache before file**: Cache may trigger file writes on eviction
6. **file last**: File I/O is the most expensive operation and should be held for minimal time

### Lock Characteristics

- **pin_table**: Single RwLock protecting HashMap<PageId, usize>
- **superblock**: Single RwLock protecting database metadata
- **header**: Single RwLock protecting file header
- **page_table**: 64 sharded RwLocks (one per page shard)
- **cache**: 32 sharded RwLocks (one per cache shard)
- **file**: Single RwLock protecting VFS file handle
- **free_list**: Lock-free (AtomicU64 + SegQueue)

## Common Patterns

### Pattern 1: Page Allocation

```rust
// 1. Try lock-free free list first (no ordering needed)
if let Some(page_id) = self.free_list.pop_page() {
    // 2. Update superblock
    let mut superblock = self.superblock.write();
    superblock.mark_page_allocated();
    drop(superblock);
    
    // 3. Acquire page lock
    let _page_lock = self.page_table.write_lock(page_id);
    
    // 4. Write to file
    let mut file = self.file.write();
    // ... write operations
} else {
    // Allocate new page from superblock
    let mut superblock = self.superblock.write();
    let page_id = superblock.allocate_new_page();
    drop(superblock);
    
    let _page_lock = self.page_table.write_lock(page_id);
    let mut file = self.file.write();
    // ... write operations
}
```

### Pattern 2: Page Read

```rust
// 1. Pin the page
self.pin_table.pin(page_id);

// 2. Acquire page lock
let _page_lock = self.page_table.read_lock(page_id);

// 3. Read from file
let mut file = self.file.write(); // VFS requires &mut
file.read_at_offset(offset, &mut buffer)?;
drop(file);

// 4. Update cache (if enabled)
if let Some(cache) = &self.cache {
    cache.put(page.clone(), false);
}

// 5. Always unpin
self.pin_table.unpin(page_id);
```

### Pattern 3: Page Write

```rust
// 1. Acquire page lock
let _page_lock = self.page_table.write_lock(page_id);

// 2. Write to file
let mut file = self.file.write();
file.write_to_offset(offset, &buffer)?;
drop(file);

// 3. Update cache (if enabled)
if let Some(cache) = &self.cache {
    cache.put(page.clone(), dirty);
}
```

### Pattern 4: Page Free

```rust
// 1. Check if pinned (read-only check)
if self.pin_table.is_pinned(page_id) {
    return Err(PagerError::PagePinned(page_id));
}

// 2. Acquire page lock
let _page_lock = self.page_table.write_lock(page_id);

// 3. Read and verify page
let mut file = self.file.write();
// ... read and verify
drop(file);

// 4. Add to free list (lock-free)
self.free_list.push_page(page_id);

// 5. Update superblock
let mut superblock = self.superblock.write();
superblock.mark_page_freed();
```

## Critical Rules

### Rule 1: Never Hold Multiple Global Locks Simultaneously

❌ **WRONG**:
```rust
let superblock = self.superblock.write();
let header = self.header.write();  // DEADLOCK RISK!
// ... operations
```

✅ **CORRECT**:
```rust
let superblock_data = {
    let superblock = self.superblock.write();
    superblock.clone()
};
let header_data = {
    let header = self.header.write();
    header.clone()
};
// Use cloned data
```

### Rule 2: Always Release Locks Before Acquiring Lower-Priority Locks

❌ **WRONG**:
```rust
let _page_lock = self.page_table.write_lock(page_id);
let mut superblock = self.superblock.write();  // WRONG ORDER!
```

✅ **CORRECT**:
```rust
let mut superblock = self.superblock.write();
// ... operations
drop(superblock);
let _page_lock = self.page_table.write_lock(page_id);
```

### Rule 3: Pin Before Lock, Unpin After Unlock

✅ **CORRECT**:
```rust
self.pin_table.pin(page_id);
let _page_lock = self.page_table.read_lock(page_id);
// ... operations
drop(_page_lock);
self.pin_table.unpin(page_id);
```

### Rule 4: Use try_lock for Optional Locks

When lock ordering cannot be guaranteed, use try_lock:

```rust
if let Some(guard) = self.page_table.try_write_lock(page_id) {
    // Got the lock
} else {
    // Could not acquire, handle appropriately
}
```

## Debug Assertions

In debug builds, we track lock acquisition order:

```rust
#[cfg(debug_assertions)]
thread_local! {
    static LOCK_ORDER: RefCell<Vec<&'static str>> = RefCell::new(Vec::new());
}

#[cfg(debug_assertions)]
fn assert_lock_order(lock_name: &'static str, expected_level: usize) {
    LOCK_ORDER.with(|order| {
        let mut order = order.borrow_mut();
        if let Some(&last) = order.last() {
            let last_level = lock_level(last);
            assert!(
                expected_level >= last_level,
                "Lock ordering violation: tried to acquire {} (level {}) while holding {} (level {})",
                lock_name, expected_level, last, last_level
            );
        }
        order.push(lock_name);
    });
}
```

## Testing

All pager operations should be tested for:

1. **Correctness**: Operations produce correct results
2. **Concurrency**: Multiple threads can operate without deadlock
3. **Lock ordering**: Debug assertions catch violations
4. **Performance**: Lock contention is minimized

See `tests/pager_concurrency_tests.rs` for examples.

## Common Pitfalls

### Pitfall 1: Forgetting to Drop Locks

```rust
let superblock = self.superblock.write();
// ... long operation ...
let _page_lock = self.page_table.write_lock(page_id);  // Still holding superblock!
```

**Solution**: Explicitly drop locks or use scopes:
```rust
{
    let superblock = self.superblock.write();
    // ... operations
}  // Lock dropped here
let _page_lock = self.page_table.write_lock(page_id);
```

### Pitfall 2: Acquiring Locks in Callbacks

```rust
cache.get_or_insert(page_id, || {
    let mut file = self.file.write();  // May violate ordering!
    // ...
});
```

**Solution**: Acquire all necessary locks before callbacks.

### Pitfall 3: Recursive Lock Acquisition

```rust
fn operation_a(&self) {
    let _lock = self.superblock.write();
    self.operation_b();  // May try to acquire superblock again!
}
```

**Solution**: Use `parking_lot::RwLock` which panics on recursive acquisition, or refactor to avoid recursion.

## Version History

- **2026-05-12**: Initial version documenting lock hierarchy
- **Future**: Add lock-free alternatives where possible

## References

- [Deadlock Prevention Strategies](https://en.wikipedia.org/wiki/Deadlock_prevention_algorithms)
- [Lock Ordering in Rust](https://doc.rust-lang.org/nomicon/deadlock.html)
- parking_lot documentation: https://docs.rs/parking_lot/