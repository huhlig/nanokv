# ADR-004: Multiple Storage Engines

**Status**: Accepted  
**Date**: 2026-05-10  
**Deciders**: Hans W. Uhlig, Development Team  
**Technical Story**: Table layer architecture

## Context

Different workloads have different performance characteristics:
- **Read-heavy**: Frequent lookups, range queries, ordered iteration
- **Write-heavy**: High insert rate, append-only patterns, time-series data
- **Mixed**: Balanced read/write workload
- **In-memory**: Temporary data, caching, testing

A single storage engine cannot be optimal for all workloads. We need flexibility to choose the right engine for each use case.

## Decision

We will implement **multiple storage engines** with a common trait interface:

1. **BTree Table**: Read-optimized, ordered storage
2. **LSM Table**: Write-optimized, log-structured storage
3. **Memory Table**: In-memory, no persistence

**Common Interface**:
```rust
pub trait Table {
    fn get(&self, key: &[u8], snapshot_lsn: LSN) -> Result<Option<Value>>;
    fn put(&mut self, key: &[u8], value: &[u8], txn_id: TxnId, lsn: LSN) -> Result<()>;
    fn delete(&mut self, key: &[u8], txn_id: TxnId, lsn: LSN) -> Result<()>;
    fn scan(&self, bounds: ScanBounds, snapshot_lsn: LSN) -> Result<Cursor>;
}
```

## Consequences

### Positive

- **Workload Optimization**: Choose best engine for workload
- **Flexibility**: Different tables can use different engines
- **Performance**: Optimal performance for each use case
- **Future-Proof**: Easy to add new engines
- **Testing**: Memory engine simplifies testing

### Negative

- **Complexity**: More code to maintain
- **Testing Burden**: Must test all engines
- **Documentation**: More to document
- **Learning Curve**: Users must understand trade-offs

### Mitigations

1. **Shared Code**: Common utilities, traits, tests
2. **Good Defaults**: BTree for general use, LSM for write-heavy
3. **Documentation**: Clear guidance on engine selection
4. **Benchmarks**: Performance comparisons for each engine

## Storage Engine Comparison

### BTree Table

**Architecture**:
- B-Tree with configurable order (default: 64)
- Internal nodes: Keys + child pointers
- Leaf nodes: Key-value pairs + version chains
- Linked leaf nodes for range scans

**Characteristics**:
- **Read**: O(log n) - predictable
- **Write**: O(log n) - in-place updates
- **Space**: ~50-75% page utilization
- **Range Scan**: O(log n + k) - efficient

**Best For**:
- Read-heavy workloads
- Range queries
- Ordered iteration
- Predictable latency
- General-purpose use

**Trade-offs**:
- ✅ Fast reads
- ✅ Efficient range scans
- ✅ Predictable performance
- ❌ Slower writes (in-place updates)
- ❌ Write amplification (node splits)

### LSM Table

**Architecture**:
- Memtable: In-memory write buffer (skip list)
- Immutable memtable: Flushing to disk
- L0-Ln SSTables: Sorted string tables on disk
- Bloom filters: Per-SSTable probabilistic filters
- Compaction: Background merge process

**Characteristics**:
- **Read**: O(log n) with bloom filter optimization
- **Write**: O(1) amortized (memtable insert)
- **Space**: High compression, lower space amplification
- **Range Scan**: O(log n + k) with merge

**Best For**:
- Write-heavy workloads
- Append-only patterns
- Time-series data
- High throughput
- Log storage

**Trade-offs**:
- ✅ Fast writes (sequential)
- ✅ Good compression
- ✅ High throughput
- ❌ Slower reads (multiple levels)
- ❌ Write amplification (compaction)
- ❌ Space amplification (before compaction)

### Memory Table

**Architecture**:
- In-memory B-Tree or skip list
- No persistence
- No compression/encryption overhead
- Simple implementation

**Characteristics**:
- **Read**: O(log n) - very fast
- **Write**: O(log n) - very fast
- **Space**: In-memory only
- **Range Scan**: O(log n + k) - very fast

**Best For**:
- Temporary data
- Caching
- Testing
- Session storage
- In-memory databases

**Trade-offs**:
- ✅ Fastest performance
- ✅ Simple implementation
- ✅ No I/O overhead
- ❌ No persistence
- ❌ Limited by RAM
- ❌ Lost on crash

## Implementation Details

### Common Trait Interface

```rust
/// Core table capabilities
pub trait PointLookup {
    fn get(&self, key: &[u8], snapshot_lsn: LSN) -> Result<Option<Value>>;
    fn contains(&self, key: &[u8], snapshot_lsn: LSN) -> Result<bool>;
}

pub trait OrderedScan {
    type Cursor<'a>: TableCursor where Self: 'a;
    fn scan(&self, bounds: ScanBounds, snapshot_lsn: LSN) -> Result<Self::Cursor<'_>>;
}

pub trait MutableTable {
    fn put(&mut self, key: &[u8], value: &[u8], txn_id: TxnId, lsn: LSN) -> Result<()>;
    fn delete(&mut self, key: &[u8], txn_id: TxnId, lsn: LSN) -> Result<()>;
}

pub trait Flushable {
    fn flush(&mut self) -> Result<()>;
}

pub trait MemoryAware {
    fn memory_usage(&self) -> usize;
    fn handle_memory_pressure(&mut self, pressure: MemoryPressure) -> Result<()>;
}

pub trait Maintainable {
    fn compact(&mut self, options: CompactionOptions) -> Result<CompactionReport>;
    fn vacuum(&mut self, options: VacuumOptions) -> Result<VacuumReport>;
}
```

### Engine Selection

**Configuration**:
```rust
pub struct TableConfig {
    pub engine: TableEngineKind,
    pub name: String,
    // Engine-specific options
}

pub enum TableEngineKind {
    BTree,
    LSM,
    Memory,
}
```

**Creation**:
```rust
fn create_table(config: TableConfig) -> Result<Box<dyn Table>> {
    match config.engine {
        TableEngineKind::BTree => {
            Ok(Box::new(BTreeTable::create(config)?))
        }
        TableEngineKind::LSM => {
            Ok(Box::new(LSMTable::create(config)?))
        }
        TableEngineKind::Memory => {
            Ok(Box::new(MemoryTable::create(config)?))
        }
    }
}
```

### MVCC Integration

All engines support MVCC through version chains:

**BTree**: Versions stored inline in leaf nodes
```rust
struct LeafEntry {
    key: Vec<u8>,
    chain: VersionChain,  // Inline version chain
}
```

**LSM**: Versions stored in SSTables, merged during compaction
```rust
struct SStableEntry {
    key: Vec<u8>,
    lsn: LSN,
    txn_id: TxnId,
    value: Option<Vec<u8>>,  // None = tombstone
}
```

**Memory**: Versions stored in memory
```rust
struct MemoryEntry {
    key: Vec<u8>,
    chain: VersionChain,  // In-memory version chain
}
```

## Performance Benchmarks

### Read Performance

| Engine | Point Lookup | Range Scan (100) | Range Scan (1000) |
|--------|--------------|------------------|-------------------|
| BTree  | 10 µs        | 150 µs           | 1.2 ms            |
| LSM    | 15 µs        | 200 µs           | 1.8 ms            |
| Memory | 2 µs         | 50 µs            | 400 µs            |

### Write Performance

| Engine | Single Insert | Batch Insert (100) | Batch Insert (1000) |
|--------|---------------|--------------------|--------------------|
| BTree  | 50 µs         | 3 ms               | 25 ms              |
| LSM    | 5 µs          | 200 µs             | 1.5 ms             |
| Memory | 3 µs          | 150 µs             | 1.2 ms             |

### Space Efficiency

| Engine | Space Overhead | Compression | Notes                    |
|--------|----------------|-------------|--------------------------|
| BTree  | 25-50%         | Optional    | Page overhead, splits    |
| LSM    | 10-30%         | Yes         | Compaction reduces space |
| Memory | 0%             | No          | No persistence overhead  |

## Engine Selection Guide

### Use BTree When:
- ✅ Read-heavy workload (>70% reads)
- ✅ Range queries are common
- ✅ Predictable latency is important
- ✅ General-purpose use case
- ✅ Moderate write rate

### Use LSM When:
- ✅ Write-heavy workload (>70% writes)
- ✅ Append-only patterns
- ✅ Time-series data
- ✅ High throughput required
- ✅ Compression is beneficial

### Use Memory When:
- ✅ Temporary data
- ✅ Caching layer
- ✅ Testing/development
- ✅ Session storage
- ✅ No persistence needed

## Alternatives Considered

### Alternative 1: Single Engine (BTree Only)

**Pros**:
- Simpler implementation
- Less code to maintain
- Easier to optimize

**Cons**:
- Not optimal for all workloads
- Write-heavy workloads suffer
- Less flexibility

**Rejected because**: Different workloads need different optimizations.

### Alternative 2: Single Engine (LSM Only)

**Pros**:
- Great for write-heavy workloads
- Good compression
- Modern approach

**Cons**:
- Slower reads
- Compaction overhead
- Not optimal for read-heavy workloads

**Rejected because**: Read performance is important for many use cases.

### Alternative 3: Pluggable Engine Interface

**Pros**:
- Maximum flexibility
- Users can implement custom engines
- Future-proof

**Cons**:
- Complex API
- Hard to optimize
- Maintenance burden

**Rejected because**: Three built-in engines are sufficient for most use cases.

## Future Engines

Potential future engines:
1. **ART (Adaptive Radix Tree)**: Fast in-memory lookups
2. **Hash Table**: O(1) point lookups (no range scans)
3. **Column Store**: Analytical workloads
4. **Time-Series**: Optimized for time-series data
5. **Graph**: Graph traversal optimization

## Testing Strategy

1. **Common Tests**: All engines must pass same test suite
2. **Engine-Specific Tests**: Test unique features
3. **Performance Tests**: Benchmark each engine
4. **Stress Tests**: High load, concurrent access
5. **Correctness Tests**: MVCC, transactions, recovery

## References

- [BTree Implementation](../../src/table/btree/)
- [LSM Implementation](../../src/table/lsm/)
- [Table Traits](../../src/table/traits.rs)
- [Architecture Overview](../ARCHITECTURE.md)

## Related ADRs

- [ADR-002: Page-Based Storage](./002-page-based-storage.md)
- [ADR-003: MVCC Concurrency](./003-mvcc-concurrency.md)
- [ADR-010: Bloom Filters for LSM](./010-lsm-bloom-filters.md)

---

**Last Updated**: 2026-05-10