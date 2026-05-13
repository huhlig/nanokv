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
pub mod blob;
pub mod bloom;
pub mod btree;
mod error;
pub mod hash;
pub mod hnsw;
pub mod lsm;
pub mod rtree;
mod traits;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use crate::pager::{PageId, Pager};
use crate::table::lsm::LsmTree;
use crate::types::TableId;
use crate::vfs::FileSystem;
pub use self::blob::{FileBlob, MemoryBlob, PagedBlob};
pub use self::bloom::{BloomFilter, BloomFilterBuilder, PagedBloomFilter};
pub use self::btree::{MemoryBTree, PagedBTree};
pub use self::error::{TableError, TableResult};
pub use self::hash::{MemoryHashTable, MemoryHashTableReader, MemoryHashTableWriter};
pub use self::hnsw::{HnswConfig, PagedHnswVector};
pub use self::lsm::{
    CompactionConfig, CompactionStrategy, LevelConfig,
    LsmConfig, Memtable, MemtableConfig, MemtableType, SStableConfig,
};
pub use self::rtree::{PagedRTree, SpatialConfig, SplitStrategy};
pub use self::traits::{
    // Core table traits
    BatchOps, BatchReport, CheckpointInfo, CompactionOptions, CompactionReport, ConsistencyError,
    ConsistencyErrorType, ConsistencyVerifier, ConsistencyWarning, EvictableCache, Flushable,
    Histogram, HistogramBucket, KeyStatistics,
    Maintainable, MemoryAware, Migratable, MutableTable, Mutation, OrderedKvTable, OrderedScan,
    PointLookup, PrefixScan, RepairAction, RepairPlan, RepairReport, SearchableTable, Severity, SliceValueStream,
    StatisticsProvider, Table, TableCapabilities, TableCursor, TableEngine, TableEngineKind, TableInfo,
    TableOptions, TableReader, TableStatistics, TableWriter, VacuumOptions,
    VacuumReport, ValueStatistics, ValueStream, VerificationReport, VerifyScope, WorkBudget, WriteBatch,
    // Specialty table traits (formerly index traits)
    ApproximateMembership, CandidateSet, CostEstimate, DenseOrdered, EdgeCursor, EdgeRef,
    FullTextSearch, GeoHit, GeoPoint, GeoSpatial, GeometryRef, GraphAdjacency, HnswVector,
    IvfVector, PhysicalRange, Predicate, QueryBudget, QueryablePredicate, Rebuildable,
    RebuildBudget, RebuildProgress, ScoredDocument, SparseOrdered, SparseQuery,
    SpecialtyTableCapabilities, SpecialtyTableCursor, SpecialtyTableSource,
    SpecialtyTableSourceError, SpecialtyTableStats, TextField, TextQuery, TimePointRef,
    TimeSeries, TimeSeriesCursor, VectorHit, VectorMetric, VectorSearch, VectorSearchOptions,
};


// =============================================================================
// Table Engine Wrapper
// =============================================================================

/// Wrapper for table engine instances that allows storing different engine
/// types in the same collection.
pub enum TableEngineInstance<FS: FileSystem> {
    PagedBTree(Arc<PagedBTree<FS>>),
    LsmTree(Arc<LsmTree<FS>>),
    PagedBloomFilter(Arc<PagedBloomFilter<FS>>),
    PagedHnswVector(Arc<PagedHnswVector<FS>>),
    PagedRTree(Arc<PagedRTree<FS>>),
    MemoryBTree(Arc<MemoryBTree>),
    MemoryHashTable(Arc<MemoryHashTable>),
    MemoryBlob(Arc<MemoryBlob>),
    // Note: PagedBlob is not included as it doesn't take FS generic parameter
    // TODO: Refactor PagedBlob to take Pager parameter
}

impl<FS: FileSystem> TableEngineInstance<FS> {
    /// Get the table ID.
    pub fn table_id(&self) -> TableId {
        match self {
            Self::PagedBTree(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::LsmTree(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::PagedBloomFilter(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::PagedHnswVector(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::PagedRTree(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::MemoryBTree(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::MemoryHashTable(engine) => crate::table::Table::table_id(engine.as_ref()),
            Self::MemoryBlob(engine) => crate::table::Table::table_id(engine.as_ref()),
        }
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        match self {
            Self::PagedBTree(engine) => crate::table::Table::name(engine.as_ref()),
            Self::LsmTree(engine) => crate::table::Table::name(engine.as_ref()),
            Self::PagedBloomFilter(engine) => crate::table::Table::name(engine.as_ref()),
            Self::PagedHnswVector(engine) => crate::table::Table::name(engine.as_ref()),
            Self::PagedRTree(engine) => crate::table::Table::name(engine.as_ref()),
            Self::MemoryBTree(engine) => crate::table::Table::name(engine.as_ref()),
            Self::MemoryHashTable(engine) => crate::table::Table::name(engine.as_ref()),
            Self::MemoryBlob(engine) => crate::table::Table::name(engine.as_ref()),
        }
    }

    /// Get the engine kind.
    pub fn kind(&self) -> TableEngineKind {
        match self {
            Self::PagedBTree(engine) => engine.kind(),
            Self::LsmTree(engine) => engine.kind(),
            Self::PagedBloomFilter(engine) => engine.kind(),
            Self::PagedHnswVector(engine) => engine.kind(),
            Self::PagedRTree(engine) => engine.kind(),
            Self::MemoryBTree(engine) => engine.kind(),
            Self::MemoryHashTable(engine) => engine.kind(),
            Self::MemoryBlob(engine) => engine.kind(),
        }
    }

    /// Get the root page ID for persistent engines.
    /// Note: This is a placeholder - proper root page tracking needs to be added
    pub fn root_page_id(&self) -> Option<PageId> {
        match self {
            Self::PagedBTree(_) => None, // TODO: Make get_root_page_id public or add accessor
            Self::LsmTree(_) => None, // LSM has manifest, not single root
            Self::PagedBloomFilter(engine) => Some(engine.root_page_id()),
            Self::PagedHnswVector(_) => None, // TODO: Add root_page_id accessor
            Self::PagedRTree(engine) => Some(engine.root_page_id()),
            Self::MemoryBTree(_) => None,
            Self::MemoryHashTable(_) => None,
            Self::MemoryBlob(_) => None,
        }
    }
}

impl<FS: FileSystem> Clone for TableEngineInstance<FS> {
    fn clone(&self) -> Self {
        match self {
            Self::PagedBTree(engine) => Self::PagedBTree(Arc::clone(engine)),
            Self::LsmTree(engine) => Self::LsmTree(Arc::clone(engine)),
            Self::PagedBloomFilter(engine) => Self::PagedBloomFilter(Arc::clone(engine)),
            Self::PagedHnswVector(engine) => Self::PagedHnswVector(Arc::clone(engine)),
            Self::PagedRTree(engine) => Self::PagedRTree(Arc::clone(engine)),
            Self::MemoryBTree(engine) => Self::MemoryBTree(Arc::clone(engine)),
            Self::MemoryHashTable(engine) => Self::MemoryHashTable(Arc::clone(engine)),
            Self::MemoryBlob(engine) => Self::MemoryBlob(Arc::clone(engine)),
        }
    }
}

// =============================================================================
// Table Engine Registry
// =============================================================================

/// Registry for managing table engine instances.
///
/// The registry maintains a mapping from ObjectId to table engine instances,
/// providing factory methods for creating different engine types and managing
/// their lifecycle.
pub struct TableEngineRegistry<FS: FileSystem> {
    /// Map of table/index ID to engine instance
    engines: RwLock<HashMap<TableId, TableEngineInstance<FS>>>,

    /// Pager for persistent engines
    pager: Arc<Pager<FS>>,
}

impl<FS: FileSystem> TableEngineRegistry<FS> {
    /// Create a new table engine registry.
    pub fn new(pager: Arc<Pager<FS>>) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            pager,
        }
    }

    /// Create a new table engine instance.
    /// Returns the engine instance and its root page ID (if applicable).
    pub fn create_engine(
        &self,
        table_id: TableId,
        name: String,
        options: &TableOptions,
    ) -> Result<(TableEngineInstance<FS>, Option<PageId>), RegistryError> {
        match options.engine {
            TableEngineKind::BTree => {
                // Create new BTree with allocated root page
                let btree = PagedBTree::new(table_id, name, self.pager.clone()).map_err(|e| {
                    RegistryError::EngineCreationFailed {
                        engine: options.engine,
                        details: format!("Failed to create BTree: {}", e),
                    }
                })?;
                let root_page_id = btree.get_root_page_id();
                Ok((TableEngineInstance::PagedBTree(Arc::new(btree)), Some(root_page_id)))
            }
            TableEngineKind::LsmTree => {
                // Allocate root page for LSM manifest
                let root_page_id = self.pager.allocate_page(crate::pager::PageType::LsmMeta).map_err(|e| {
                    RegistryError::EngineCreationFailed {
                        engine: options.engine,
                        details: format!("Failed to allocate manifest page: {}", e),
                    }
                })?;

                // Create LSM config from table options
                let lsm_config = LsmConfig::default(); // TODO: Extract from options

                let lsm = crate::table::lsm::LsmTree::new(
                    table_id,
                    name,
                    self.pager.clone(),
                    root_page_id,
                    lsm_config,
                )
                    .map_err(|e| RegistryError::EngineCreationFailed {
                        engine: options.engine,
                        details: format!("Failed to create LSM tree: {}", e),
                    })?;
                Ok((TableEngineInstance::LsmTree(Arc::new(lsm)), Some(root_page_id)))
            }
            TableEngineKind::Bloom => {
                // Create Bloom filter with default parameters
                // TODO: Extract parameters from options
                let num_items = 10000; // Default expected items
                let bits_per_key = 10; // ~1% false positive rate
                
                let bloom = PagedBloomFilter::new(
                    table_id,
                    name,
                    self.pager.clone(),
                    num_items,
                    bits_per_key,
                    None,
                )
                    .map_err(|e| RegistryError::EngineCreationFailed {
                        engine: options.engine,
                        details: format!("Failed to create Bloom filter: {}", e),
                    })?;
                let root_page_id = bloom.root_page_id();
                Ok((TableEngineInstance::PagedBloomFilter(Arc::new(bloom)), Some(root_page_id)))
            }
            TableEngineKind::Memory => {
                // Create in-memory BTree - no root page
                let memory_btree = MemoryBTree::new(table_id, name);
                Ok((TableEngineInstance::MemoryBTree(Arc::new(memory_btree)), None))
            }
            TableEngineKind::Hash => {
                // Create in-memory hash table - no root page
                let hash_table = MemoryHashTable::new(table_id, name);
                Ok((TableEngineInstance::MemoryHashTable(Arc::new(hash_table)), None))
            }
            TableEngineKind::GeoSpatial => {
                // Create R-Tree with default spatial config
                // TODO: Extract config from options
                let spatial_config = SpatialConfig::default();

                let rtree = PagedRTree::new(
                    table_id,
                    name,
                    self.pager.clone(),
                    spatial_config,
                )
                .map_err(|e| RegistryError::EngineCreationFailed {
                    engine: options.engine,
                    details: format!("Failed to create R-Tree: {}", e),
                })?;
                let root_page_id = rtree.root_page_id();
                Ok((TableEngineInstance::PagedRTree(Arc::new(rtree)), Some(root_page_id)))
            }
            TableEngineKind::Blob => {
                // PagedBlob not yet supported in registry (doesn't take FS generic)
                // TODO: Refactor PagedBlob to work with registry
                Err(RegistryError::UnsupportedEngine(options.engine))
            }
            _ => Err(RegistryError::UnsupportedEngine(options.engine)),
        }
    }

    /// Open an existing table engine instance.
    pub fn open_engine(
        &self,
        table_id: TableId,
        name: String,
        options: &TableOptions,
        root_page_id: PageId,
    ) -> Result<TableEngineInstance<FS>, RegistryError> {
        match options.engine {
            TableEngineKind::BTree => {
                let btree = PagedBTree::open(table_id, name, self.pager.clone(), root_page_id);
                Ok(TableEngineInstance::PagedBTree(Arc::new(btree)))
            }
            TableEngineKind::LsmTree => {
                let lsm_config = LsmConfig::default(); // TODO: Extract from options
                let lsm = crate::table::lsm::LsmTree::open(
                    table_id,
                    name,
                    self.pager.clone(),
                    root_page_id,
                    lsm_config,
                )
                    .map_err(|e| RegistryError::EngineOpenFailed {
                        engine: options.engine,
                        details: format!("Failed to open LSM tree: {}", e),
                    })?;
                Ok(TableEngineInstance::LsmTree(Arc::new(lsm)))
            }
            TableEngineKind::Bloom => {
                let bloom = PagedBloomFilter::open(
                    table_id,
                    name,
                    self.pager.clone(),
                    root_page_id,
                )
                    .map_err(|e| RegistryError::EngineOpenFailed {
                        engine: options.engine,
                        details: format!("Failed to open Bloom filter: {}", e),
                    })?;
                Ok(TableEngineInstance::PagedBloomFilter(Arc::new(bloom)))
            }
            TableEngineKind::GeoSpatial => {
                let spatial_config = SpatialConfig::default(); // TODO: Extract from options
                let rtree = PagedRTree::open(
                    table_id,
                    name,
                    self.pager.clone(),
                    root_page_id,
                    spatial_config,
                )
                .map_err(|e| RegistryError::EngineOpenFailed {
                    engine: options.engine,
                    details: format!("Failed to open R-Tree: {}", e),
                })?;
                Ok(TableEngineInstance::PagedRTree(Arc::new(rtree)))
            }
            TableEngineKind::Blob => {
                // PagedBlob not yet supported in registry
                Err(RegistryError::UnsupportedEngine(options.engine))
            }
            TableEngineKind::Memory => {
                // Memory tables don't persist, create new
                let memory_btree = MemoryBTree::new(table_id, name);
                Ok(TableEngineInstance::MemoryBTree(Arc::new(memory_btree)))
            }
            TableEngineKind::Hash => {
                // Hash tables don't persist, create new
                let hash_table = MemoryHashTable::new(table_id, name);
                Ok(TableEngineInstance::MemoryHashTable(Arc::new(hash_table)))
            }
            _ => Err(RegistryError::UnsupportedEngine(options.engine)),
        }
    }

    /// Register a table engine instance.
    pub fn register(
        &self,
        engine: TableEngineInstance<FS>,
    ) -> Result<(), RegistryError> {
        let mut engines = self.engines.write().unwrap();
        let table_id = engine.table_id();

        if engines.contains_key(&table_id) {
            return Err(RegistryError::EngineAlreadyRegistered(table_id));
        }

        engines.insert(table_id, engine);
        Ok(())
    }

    /// Get a table engine instance.
    pub fn get(&self, table_id: TableId) -> Option<TableEngineInstance<FS>> {
        let engines = self.engines.read().unwrap();
        engines.get(&table_id).cloned()
    }

    /// Remove a table engine instance.
    pub fn remove(&self, table_id: TableId) -> Option<TableEngineInstance<FS>> {
        let mut engines = self.engines.write().unwrap();
        engines.remove(&table_id)
    }

    /// List all registered engines.
    pub fn list(&self) -> Vec<TableEngineInstance<FS>> {
        let engines = self.engines.read().unwrap();
        engines.values().cloned().collect()
    }

    /// Get the number of registered engines.
    pub fn count(&self) -> usize {
        let engines = self.engines.read().unwrap();
        engines.len()
    }

    /// Check if an engine is registered.
    pub fn contains(&self, table_id: TableId) -> bool {
        let engines = self.engines.read().unwrap();
        engines.contains_key(&table_id)
    }

    /// Close all engines and clean up resources.
    pub fn close_all(&self) {
        let mut engines = self.engines.write().unwrap();
        engines.clear();
    }
}

// =============================================================================
// Registry Errors
// =============================================================================

/// Errors that can occur during table engine registry operations.
#[derive(Debug)]
pub enum RegistryError {
    /// Engine creation failed
    EngineCreationFailed {
        engine: TableEngineKind,
        details: String,
    },
    /// Engine open failed
    EngineOpenFailed {
        engine: TableEngineKind,
        details: String,
    },
    /// Engine already registered
    EngineAlreadyRegistered(TableId),
    /// Engine not found
    EngineNotFound(TableId),
    /// Unsupported engine type
    UnsupportedEngine(TableEngineKind),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EngineCreationFailed { engine, details } => {
                write!(f, "Failed to create {:?} engine: {}", engine, details)
            }
            Self::EngineOpenFailed { engine, details } => {
                write!(f, "Failed to open {:?} engine: {}", engine, details)
            }
            Self::EngineAlreadyRegistered(id) => {
                write!(f, "Engine {:?} is already registered", id)
            }
            Self::EngineNotFound(id) => {
                write!(f, "Engine {:?} not found in registry", id)
            }
            Self::UnsupportedEngine(engine) => {
                write!(f, "Unsupported engine type: {:?}", engine)
            }
        }
    }
}

impl std::error::Error for RegistryError {}

// Made with Bob
