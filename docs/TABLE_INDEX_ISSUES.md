# Table and Index Implementation Issues

**Created**: 2026-05-07  
**Epic**: nanokv-y8u

This document summarizes all beads issues created for the table and index architecture implementation.

---

## Epic

**nanokv-y8u**: Table and Index Architecture Implementation (Priority 1, Epic)
- Complete implementation of table and index architecture as defined in docs/TABLE_INDEX_ARCHITECTURE.md
- Foundation for higher-level database systems

---

## Foundation Issues (Priority 1)

### Page Layer
**nanokv-jud**: Extend PageType enum for specialized index types (Task)
- Add page types: HashBucket, ArtNode4/16/48/256, BloomFilter, InvertedIndex, RTreeNode, GraphAdjList, VectorIndex, TimeSeriesBucket, IndexMetadata
- Update from_u8() and to_u8() methods in src/pager/page.rs

### Table Layer
**nanokv-590**: Define core Table trait and TableConfig (Task)
- Create src/table/mod.rs with Table trait
- Define TableType enum (BTree, LSM, ART)
- Define TableConfig and TableStats structs

**nanokv-c6d**: Implement BTree Table (persistent) (Feature)
- Disk-backed B-Tree in src/table/btree.rs
- Node structures, serialization, insert/delete/scan operations
- Depends on: nanokv-590

### Index Layer
**nanokv-7f1**: Define core Index trait and IndexConfig (Task)
- Create src/index/mod.rs with Index trait
- Define IndexType enum (BTree, Hash, LSM, FullText, Vector, Spatial, Graph, TimeSeries, Bloom)
- Define IndexConfig, IndexStats, IndexQuery, IndexResult
- Depends on: nanokv-590

**nanokv-f49**: Implement BTree Index (Feature)
- Standard ordered index using BTreeTable
- Support exact lookups, range queries, prefix scans
- Depends on: nanokv-7f1

**nanokv-784**: Implement Hash Index (Feature)
- Hash-based exact lookups with O(1) performance
- Bucket management and collision handling
- Depends on: nanokv-7f1

**nanokv-vdw**: Implement Bloom Filter (Feature)
- Probabilistic membership testing
- Configurable false positive rate
- LSM table optimization
- Depends on: nanokv-7f1

---

## Advanced Table Types (Priority 2)

**nanokv-1lf**: Implement LSM Table (Feature)
- Write-optimized LSM tree storage
- MemTable, SSTable, level compaction
- Bloom filters per SSTable
- Depends on: nanokv-590

**nanokv-tq3**: Implement ART Table (memory-only) (Feature)
- Adaptive Radix Tree for fast in-memory lookups
- Node types: Node4, Node16, Node48, Node256, Leaf
- Path compression
- Depends on: nanokv-590

---

## Specialized Indexes (Priority 2)

**nanokv-pb2**: Implement Full-Text Index (Feature)
- Inverted index for text search
- Tokenizer, posting lists, document store
- Support AND/OR/phrase queries
- Depends on: nanokv-7f1

**nanokv-ejy**: Implement Vector Index (HNSW) (Feature)
- Approximate nearest neighbor search
- HNSW algorithm with configurable parameters
- Distance metrics: Euclidean, Cosine, DotProduct
- Depends on: nanokv-7f1

**nanokv-dat**: Implement Spatial Index (R-Tree) (Feature)
- R-Tree for spatial queries
- Support point, bounding box, radius, polygon queries
- MBR calculations and node splitting
- Depends on: nanokv-7f1

**nanokv-0bz**: Implement Graph Index (Feature)
- Graph traversal queries
- Adjacency list representation
- Neighbor queries, shortest path, BFS/DFS traversal
- Depends on: nanokv-7f1

**nanokv-yzr**: Implement Time Series Index (Feature)
- Time-based data queries
- Bucket management with specialized compression
- Support aggregations (sum, avg, min, max, count)
- Depends on: nanokv-7f1

---

## Advanced Patterns (Priority 2)

**nanokv-btu**: Implement Composite Index pattern (Feature)
- Combine multiple index types
- LSMWithBloom pattern for performance
- Query routing strategies
- Depends on: nanokv-7f1

---

## Testing (Priority 2)

**nanokv-ufp**: Add table and index integration tests (Task)
- Comprehensive integration tests in tests/table_index_tests.rs
- Test CRUD, scans, index operations, composite indexes
- Property-based tests and benchmarks
- Depends on: nanokv-7f1

---

## Implementation Priority

### Phase 1: Core Foundation (Weeks 4-6)
1. nanokv-jud - Extend PageType enum
2. nanokv-590 - Define Table trait
3. nanokv-c6d - Implement BTree Table
4. nanokv-7f1 - Define Index trait

### Phase 2: Core Indexes (Weeks 7-8)
1. nanokv-f49 - BTree Index
2. nanokv-784 - Hash Index
3. nanokv-vdw - Bloom Filter

### Phase 3: LSM Table (Weeks 9-10)
1. nanokv-1lf - LSM Table with bloom filters

### Phase 4: Specialized Indexes (Weeks 11-14)
1. nanokv-pb2 - Full-Text Index
2. nanokv-ejy - Vector Index (HNSW)
3. nanokv-dat - Spatial Index (R-Tree)
4. nanokv-0bz - Graph Index
5. nanokv-yzr - Time Series Index

### Phase 5: Advanced Features (Weeks 15-16)
1. nanokv-tq3 - ART Table (memory-only)
2. nanokv-btu - Composite Index pattern
3. nanokv-ufp - Integration tests

---

## Dependencies Graph

```
nanokv-y8u (Epic)
├── nanokv-jud (PageType extension)
├── nanokv-590 (Table trait)
│   ├── nanokv-c6d (BTree Table)
│   ├── nanokv-1lf (LSM Table)
│   ├── nanokv-tq3 (ART Table)
│   └── nanokv-7f1 (Index trait)
│       ├── nanokv-f49 (BTree Index)
│       ├── nanokv-784 (Hash Index)
│       ├── nanokv-vdw (Bloom Filter)
│       ├── nanokv-pb2 (Full-Text Index)
│       ├── nanokv-ejy (Vector Index)
│       ├── nanokv-dat (Spatial Index)
│       ├── nanokv-0bz (Graph Index)
│       ├── nanokv-yzr (Time Series Index)
│       ├── nanokv-btu (Composite Index)
│       └── nanokv-ufp (Integration tests)
```

---

## Quick Reference

To see ready work:
```bash
bd ready
```

To claim an issue:
```bash
bd update <id> --claim
```

To view issue details:
```bash
bd show <id>
```

---

**Total Issues Created**: 15
- 1 Epic
- 2 Foundation tasks
- 12 Feature implementations
- 1 Testing task

All issues are tracked in the beads system and linked to the architecture document at docs/TABLE_INDEX_ARCHITECTURE.md.