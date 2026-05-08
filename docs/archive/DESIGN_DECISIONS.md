# Design Decisions and Open Questions

**Date**: 2026-05-08  
**Status**: Decisions documented, stakeholder input needed

## Overview

This document outlines the key architectural decisions made during the design phase and identifies open questions requiring stakeholder input before implementation.

## 1. Foundational Decisions (Made)

### 1.1 Keep Existing Traits ✅

**Decision**: Use [`embedded_kv_traits.rs`](../src/embedded_kv_traits.rs) as-is without modifications.

**Rationale**:
- Traits are comprehensive and well-designed
- 1,841 lines represent significant design effort
- Capability-oriented approach is correct
- Zero-copy strategy is sound
- MVCC awareness is built-in

**Impact**: Accelerates implementation, ensures consistency.

### 1.2 Modular Trait Organization ✅

**Decision**: Organize traits into logical modules while keeping single-file structure.

**Structure**:
```
embedded_kv_traits.rs
├── Core Database (KvDatabase, KvTransaction)
├── Table Capabilities (PointLookup, OrderedScan, etc.)
├── Memory Management (MemoryAware, EvictableCache)
├── Index Traits (Index, OrderedIndex, HashIndex, etc.)
└── Maintenance (Maintainable, StatisticsProvider, etc.)
```

**Rationale**: Improves discoverability without breaking single-file design.

### 1.3 Dual Table Engine Strategy ✅

**Decision**: Implement both BTreeTable and LSMTable.

**Rationale**:
- Different workloads have different needs
- BTree: Read-optimized, predictable performance
- LSM: Write-optimized, better compression
- Users can choose based on workload

**Trade-off**: More implementation complexity vs flexibility.

### 1.4 MVCC Transaction Model ✅

**Decision**: Use multi-version concurrency control with snapshot isolation.

**Rationale**:
- Enables concurrent reads without blocking writes
- Standard approach in modern databases
- Traits already designed for MVCC
- Supports time-travel queries

**Trade-off**: Complexity and storage overhead vs concurrency.

### 1.5 Page-Based Architecture ✅

**Decision**: Continue using page-based storage from Pager.

**Rationale**:
- Infrastructure already exists and works well
- Standard approach (SQLite, PostgreSQL)
- Enables efficient caching
- Supports zero-copy access

**Constraint**: Node sizes must fit within pages.

## 2. BTree Configuration (Open Questions)

### 2.1 Node Order

**Question**: What should be the default B-Tree order?

**Options**:

**Option A: Fixed Order (e.g., 128)**
- ✅ Simple to implement
- ✅ Predictable behavior
- ❌ Not optimal for all page sizes
- ❌ Wastes space on small pages

**Option B: Dynamic Based on Page Size**
- ✅ Optimal space utilization
- ✅ Adapts to configuration
- ❌ More complex calculation
- ❌ Variable behavior

**Recommendation**: **Option B** - Dynamic based on page size

**Calculation**:
```rust
order = (page_size - header_size) / (key_size + pointer_size + overhead)
```

**Rationale**: Maximizes space utilization, adapts to different configurations.

### 2.2 Key Encoding Strategy

**Question**: How should we handle variable-length keys?

**Options**:

**Option A: Prefix Compression**
- ✅ Better space efficiency
- ✅ More keys per node
- ❌ Complex implementation
- ❌ Slower key comparison

**Option B: Simple Length-Prefixed**
- ✅ Simple implementation
- ✅ Fast key access
- ❌ Less space efficient
- ❌ Fewer keys per node

**Recommendation**: **Option B initially, Option A later**

**Rationale**: Start simple, add optimization when needed.

### 2.3 Split Strategy

**Question**: When should nodes split?

**Options**:

**Option A: Split at 100% Full**
- ✅ Maximum space utilization
- ❌ Frequent splits
- ❌ More write amplification

**Option B: Split at 75% Full**
- ✅ Fewer splits
- ✅ Room for growth
- ❌ Lower space utilization

**Recommendation**: **Option B** - Split at 75% full

**Rationale**: Reduces write amplification, improves performance.

## 3. LSM Configuration (Open Questions)

### 3.1 Compaction Strategy

**Question**: Which compaction strategy should be default?

**Options**:

**Option A: Size-Tiered Compaction**
- ✅ Simpler implementation
- ✅ Better for write-heavy workloads
- ✅ Lower write amplification
- ❌ Higher read amplification
- ❌ More space amplification

**Option B: Leveled Compaction**
- ✅ Better read performance
- ✅ Lower space amplification
- ❌ Higher write amplification
- ❌ More complex implementation

**Recommendation**: **Option A as default, Option B as option**

**Rationale**: Start with simpler approach, add leveled as optimization.

### 3.2 Compaction Timing

**Question**: Should compaction be synchronous or asynchronous?

**Options**:

**Option A: Synchronous**
- ✅ Simpler implementation
- ✅ Predictable behavior
- ❌ Blocks writes during compaction
- ❌ Latency spikes

**Option B: Asynchronous Background Thread**
- ✅ Better write throughput
- ✅ Lower latency
- ❌ More complex implementation
- ❌ Requires thread coordination

**Recommendation**: **Option B** - Asynchronous with configurable thread pool

**Rationale**: Better performance, worth the complexity.

### 3.3 Memtable Size

**Question**: What should be the default memtable size?

**Options**:
- 4 MB (small, frequent flushes)
- 16 MB (medium, balanced)
- 64 MB (large, fewer flushes)

**Recommendation**: **16 MB default, configurable**

**Rationale**: Balances memory usage and flush frequency.

## 4. MVCC and Garbage Collection (Open Questions)

### 4.1 Garbage Collection Timing

**Question**: When should old versions be garbage collected?

**Options**:

**Option A: During Compaction Only**
- ✅ No separate GC thread
- ✅ Amortized cost
- ❌ Delayed reclamation
- ❌ Not applicable to BTree

**Option B: Separate GC Thread**
- ✅ Timely reclamation
- ✅ Works for all table types
- ❌ Additional thread overhead
- ❌ More complex coordination

**Option C: On-Demand During Reads**
- ✅ No separate thread
- ✅ Opportunistic cleanup
- ❌ Unpredictable performance
- ❌ May miss versions

**Recommendation**: **Option A for LSM, Option B for BTree**

**Rationale**: Leverage compaction for LSM, use dedicated GC for BTree.

### 4.2 Version Retention Policy

**Question**: How long should versions be retained?

**Options**:

**Option A: Until No Active Snapshots**
- ✅ Minimal storage overhead
- ✅ Correct for MVCC
- ❌ Requires snapshot tracking
- ❌ No time-travel beyond snapshots

**Option B: Configurable Retention Period**
- ✅ Enables time-travel queries
- ✅ Predictable behavior
- ❌ Higher storage overhead
- ❌ May retain unnecessary versions

**Recommendation**: **Option A with optional minimum retention**

**Rationale**: Correct MVCC semantics with optional time-travel support.

### 4.3 Snapshot Management

**Question**: How should snapshots be managed?

**Decision**: Use LSN-based snapshots with reference counting.

**Implementation**:
```rust
struct Snapshot {
    lsn: Lsn,
    ref_count: AtomicUsize,
    created_at: Instant,
}
```

**Rationale**: Simple, efficient, enables GC when ref_count reaches 0.

## 5. Index Maintenance (Open Questions)

### 5.1 Update Strategy

**Question**: Should indexes be updated synchronously or asynchronously?

**Options**:

**Option A: Synchronous**
- ✅ Strong consistency
- ✅ Simpler implementation
- ❌ Slower writes
- ❌ Blocks on index updates

**Option B: Asynchronous**
- ✅ Faster writes
- ✅ Better throughput
- ❌ Eventual consistency
- ❌ More complex coordination

**Recommendation**: **Option A for critical indexes, Option B as option**

**Rationale**: Consistency by default, performance when needed.

### 5.2 Corruption Handling

**Question**: How should we handle index corruption?

**Options**:

**Option A: Automatic Rebuild**
- ✅ Self-healing
- ✅ Better user experience
- ❌ May hide underlying issues
- ❌ Rebuild overhead

**Option B: Manual Rebuild Required**
- ✅ User awareness
- ✅ Controlled timing
- ❌ Worse user experience
- ❌ Requires manual intervention

**Recommendation**: **Option A with user notification**

**Rationale**: Self-healing with transparency.

### 5.3 Index Selection

**Question**: Should index selection be automatic or manual?

**Decision**: Manual selection initially, automatic optimization later.

**Rationale**: Simpler to implement, users have control.

## 6. Memory Management (Open Questions)

### 6.1 Memory Budget Allocation

**Question**: How should memory be allocated across components?

**Options**:

**Option A: Global Budget**
- ✅ Simple configuration
- ✅ Flexible allocation
- ❌ Potential contention
- ❌ Requires coordination

**Option B: Per-Component Budgets**
- ✅ Predictable behavior
- ✅ No contention
- ❌ May waste memory
- ❌ More configuration

**Recommendation**: **Option A with per-component hints**

**Example**:
```rust
MemoryConfig {
    total_budget: 1_000_000_000,  // 1 GB
    cache_hint: 0.6,               // 60% for cache
    memtable_hint: 0.3,            // 30% for memtables
    other_hint: 0.1,               // 10% for other
}
```

**Rationale**: Flexibility with guidance.

### 6.2 Eviction Policy

**Question**: What eviction policy should we use?

**Options**:

**Option A: LRU (Least Recently Used)**
- ✅ Simple implementation
- ✅ Predictable behavior
- ✅ Good for most workloads
- ❌ Not adaptive

**Option B: ARC (Adaptive Replacement Cache)**
- ✅ Adaptive to workload
- ✅ Better hit rate
- ❌ More complex
- ❌ Higher overhead

**Recommendation**: **Option A initially, Option B as option**

**Rationale**: Start simple, add optimization when needed.

### 6.3 Memory Pressure Response

**Question**: How should components respond to memory pressure?

**Decision**: Use tiered pressure levels with progressive response.

**Levels**:
```rust
enum MemoryPressure {
    None,      // < 70% utilization
    Low,       // 70-80% - opportunistic eviction
    Medium,    // 80-90% - active eviction
    High,      // 90-95% - aggressive eviction
    Critical,  // > 95% - emergency measures
}
```

**Rationale**: Progressive response prevents thrashing.

## 7. Performance vs Complexity Trade-offs

### 7.1 Read vs Write Optimization

**Question**: Should we optimize for reads or writes?

**Decision**: Provide both BTree (read-optimized) and LSM (write-optimized).

**Rationale**: Different workloads need different optimizations.

### 7.2 Complexity Acceptance

**Question**: How much complexity is acceptable for performance gains?

**Guidelines**:
1. **Measure first** - Benchmark before optimizing
2. **Incremental complexity** - Add features gradually
3. **Document trade-offs** - Explain why complexity is needed
4. **Provide simple defaults** - Advanced features optional

**Example**: Bloom filters add complexity but provide measurable benefit.

## 8. Testing Strategy Decisions

### 8.1 Test Coverage Goals

**Decision**: Aim for 80%+ code coverage with focus on critical paths.

**Priority**:
1. **Critical paths** - Insert, get, delete, scan
2. **Error handling** - All error cases
3. **Concurrency** - Race conditions, deadlocks
4. **Recovery** - Crash scenarios

### 8.2 Performance Testing

**Decision**: Benchmark all major operations with realistic workloads.

**Benchmarks**:
- Point lookups (get)
- Range scans
- Inserts (single and batch)
- Updates
- Deletes
- Compaction overhead
- Memory usage

## 9. Documentation Standards

**Decision**: Comprehensive documentation for all public APIs.

**Requirements**:
- Module-level documentation
- Trait documentation with examples
- Implementation notes
- Performance characteristics
- Error conditions

## 10. Summary of Recommendations

### Implement Immediately
1. ✅ Dynamic BTree order based on page size
2. ✅ Simple length-prefixed key encoding
3. ✅ Size-tiered LSM compaction
4. ✅ Asynchronous compaction with thread pool
5. ✅ LSN-based snapshots with reference counting
6. ✅ Synchronous index updates by default
7. ✅ Global memory budget with component hints
8. ✅ LRU eviction policy

### Add Later (Phase 2+)
1. 🔄 Prefix compression for keys
2. 🔄 Leveled compaction option
3. 🔄 Asynchronous index updates
4. 🔄 ARC eviction policy
5. 🔄 Advanced query optimization

### Requires Stakeholder Input
1. ❓ Default memtable size (4/16/64 MB)
2. ❓ Version retention policy details
3. ❓ Memory budget allocation percentages
4. ❓ Compaction thread pool size
5. ❓ Performance vs correctness priorities

## Related Documents

- **[Problem Statement](PROBLEM_STATEMENT.md)** - Why this matters
- **[Key Findings](KEY_FINDINGS.md)** - Analysis results
- **[Critical Insights](CRITICAL_INSIGHTS.md)** - Hidden factors
- **[TABLE_INDEX_IMPLEMENTATION_DESIGN.md](TABLE_INDEX_IMPLEMENTATION_DESIGN.md)** - Detailed designs

---

**Next**: See [Critical Insights](CRITICAL_INSIGHTS.md) for hidden factors and "owl perspective".