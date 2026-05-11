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
pub mod btree;
mod error;
pub mod lsm;
mod traits;

pub use self::btree::{MemoryBTree, PagedBTree};
pub use self::error::{TableError, TableResult};
pub use self::lsm::{
    BloomFilter, BloomFilterBuilder, CompactionConfig, CompactionStrategy, LevelConfig,
    LsmConfig, Memtable, MemtableConfig, MemtableType, SStableConfig,
};
pub use self::traits::{
    // Core table traits
    BatchOps, BatchReport, CheckpointInfo, CompactionOptions, CompactionReport, ConsistencyError,
    ConsistencyErrorType, ConsistencyVerifier, ConsistencyWarning, EvictableCache, Flushable,
    Histogram, HistogramBucket, IndexConsistency, IndexField, IndexMetadata, KeyStatistics,
    Maintainable, MemoryAware, Migratable, MutableTable, Mutation, OrderedKvTable, OrderedScan,
    PointLookup, PrefixScan, RepairAction, RepairPlan, RepairReport, Severity, StatisticsProvider,
    Table, TableCapabilities, TableCursor, TableEngine, TableEngineKind, TableInfo,
    TableOptions, TableReader, TableStatistics, TableWriter, VacuumOptions,
    VacuumReport, ValueStatistics, VerificationReport, VerifyScope, WorkBudget, WriteBatch,
    // Specialty table traits (formerly index traits)
    ApproximateMembership, CandidateSet, CostEstimate, DenseOrdered, EdgeCursor, EdgeRef,
    FullTextSearch, GeoHit, GeoPoint, GeoSpatial, GeometryRef, GraphAdjacency, HnswVector,
    IvfVector, PhysicalRange, Predicate, QueryBudget, QueryablePredicate, Rebuildable,
    RebuildBudget, RebuildProgress, ScoredDocument, SparseOrdered, SparseQuery,
    SpecialtyTableCapabilities, SpecialtyTableCursor, SpecialtyTableSource,
    SpecialtyTableSourceError, SpecialtyTableStats, TextField, TextQuery, TimePointRef,
    TimeSeries, TimeSeriesCursor, VectorHit, VectorMetric, VectorSearch, VectorSearchOptions,
};
