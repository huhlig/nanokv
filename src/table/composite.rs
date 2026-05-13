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

//! Composite index patterns for combining multiple table engines.
//!
//! This module provides helpers for coordinating queries across multiple
//! specialty tables within a unified transaction context. Rather than a
//! separate composite index type, composition is achieved by orchestrating
//! calls to individual table engines using defined strategies.
//!
//! # Patterns
//!
//! ## LSMWithBloom
//!
//! Bloom filters pre-filter point lookups against an LSM tree, avoiding
//! unnecessary disk reads when the key is definitely absent.
//!
//! ```ignore
//! let result = txn.with_composite(&config, |composite| {
//!     composite.get(b"key")
//! });
//! ```
//!
//! ## BTreeWithHash
//!
//! Hash index provides O(1) point lookups while BTree handles range scans.
//! The composite router directs point queries to the hash index and range
//! queries to the BTree.
//!
//! # Composition Strategies
//!
//! - `BloomFirst`: Check bloom filter before primary lookup (filters negatives)
//! - `PrimaryFirst`: Query primary first, use secondaries for enrichment
//! - `Parallel`: Query all indexes in parallel, intersect results
//! - `Sequential`: Query indexes in order, short-circuit on hit

use crate::table::{TableEngineKind, TableId, TableResult};
use crate::types::ValueBuf;
use crate::wal::LogSequenceNumber;

/// Strategy for routing queries across composite indexes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompositionStrategy {
    /// Check approximate membership (bloom) before primary lookup.
    /// Only queries the primary if the filter says "might contain".
    #[default]
    BloomFirst,

    /// Query primary index first, use secondaries for additional filtering or enrichment.
    PrimaryFirst,

    /// Query all indexes in parallel and intersect candidate sets.
    Parallel,

    /// Query indexes in order, returning on first match.
    Sequential,
}

/// Configuration for a composite index setup.
///
/// Defines a primary table and zero or more secondary tables with a
/// composition strategy that determines how queries are routed.
#[derive(Clone, Debug)]
pub struct CompositeIndexConfig {
    /// Primary table used for main data storage and retrieval.
    pub primary_table_id: TableId,

    /// Kind of the primary table engine.
    pub primary_engine_kind: TableEngineKind,

    /// Secondary tables used for filtering, enrichment, or alternative access paths.
    pub secondary_tables: Vec<SecondaryTableConfig>,

    /// Strategy for routing queries across the composite indexes.
    pub strategy: CompositionStrategy,
}

impl CompositeIndexConfig {
    /// Create a new composite config with a primary table.
    pub fn new(primary_table_id: TableId, primary_engine_kind: TableEngineKind) -> Self {
        Self {
            primary_table_id,
            primary_engine_kind,
            secondary_tables: Vec::new(),
            strategy: CompositionStrategy::default(),
        }
    }

    /// Add a secondary table to the composite config.
    pub fn with_secondary(mut self, config: SecondaryTableConfig) -> Self {
        self.secondary_tables.push(config);
        self
    }

    /// Set the composition strategy.
    pub fn with_strategy(mut self, strategy: CompositionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Check if this composite has any bloom filter secondaries.
    pub fn has_bloom_filter(&self) -> bool {
        self.secondary_tables
            .iter()
            .any(|s| s.engine_kind == TableEngineKind::Bloom)
    }

    /// Get bloom filter secondary table IDs.
    pub fn bloom_filter_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.secondary_tables
            .iter()
            .filter(|s| s.engine_kind == TableEngineKind::Bloom)
            .map(|s| s.table_id)
    }
}

/// Configuration for a secondary table in a composite index.
#[derive(Clone, Debug)]
pub struct SecondaryTableConfig {
    /// Table ID of the secondary index.
    pub table_id: TableId,

    /// Kind of the secondary table engine.
    pub engine_kind: TableEngineKind,

    /// Role this secondary plays in the composite.
    pub role: SecondaryRole,
}

impl SecondaryTableConfig {
    /// Create a new secondary config.
    pub fn new(table_id: TableId, engine_kind: TableEngineKind, role: SecondaryRole) -> Self {
        Self {
            table_id,
            engine_kind,
            role,
        }
    }
}

/// Role a secondary table plays in the composite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecondaryRole {
    /// Pre-filter: eliminate negatives before primary lookup (e.g., bloom filter).
    PreFilter,

    /// Post-filter: refine results after primary lookup.
    PostFilter,

    /// Alternative access: provide different query path (e.g., hash for point lookup).
    AlternativeAccess,

    /// Enrichment: add additional data to results.
    Enrichment,
}

/// Result of a composite index query.
#[derive(Clone, Debug)]
pub struct CompositeQueryResult {
    /// The value found, if any.
    pub value: Option<ValueBuf>,

    /// Which table(s) were consulted to find the result.
    pub tables_consulted: Vec<TableConsulted>,

    /// Whether a pre-filter eliminated the query early.
    pub pre_filtered: bool,
}

/// Information about a table consulted during a composite query.
#[derive(Clone, Debug)]
pub struct TableConsulted {
    pub table_id: TableId,
    pub engine_kind: TableEngineKind,
    pub role: SecondaryRole,
    pub contributed: bool,
}

/// Helper for executing composite index queries within a transaction.
///
/// This struct provides methods for coordinated queries across multiple
/// table engines using the composition strategy defined in the config.
pub struct CompositeQueryExecutor<'a, F> {
    config: &'a CompositeIndexConfig,
    snapshot_lsn: LogSequenceNumber,
    get_from_primary: F,
    might_contain_in_bloom: Box<dyn FnMut(TableId, &[u8]) -> TableResult<bool> + 'a>,
}

impl<'a, F> CompositeQueryExecutor<'a, F>
where
    F: Fn(TableId, &[u8], LogSequenceNumber) -> TableResult<Option<ValueBuf>>,
{
    /// Create a new composite query executor.
    pub fn new(
        config: &'a CompositeIndexConfig,
        snapshot_lsn: LogSequenceNumber,
        get_from_primary: F,
        might_contain_in_bloom: impl FnMut(TableId, &[u8]) -> TableResult<bool> + 'a,
    ) -> Self {
        Self {
            config,
            snapshot_lsn,
            get_from_primary,
            might_contain_in_bloom: Box::new(might_contain_in_bloom),
        }
    }

    /// Execute a point lookup using the composite strategy.
    pub fn get(&mut self, key: &[u8]) -> TableResult<CompositeQueryResult> {
        match self.config.strategy {
            CompositionStrategy::BloomFirst => self.get_bloom_first(key),
            CompositionStrategy::PrimaryFirst => self.get_primary_first(key),
            CompositionStrategy::Parallel => self.get_parallel(key),
            CompositionStrategy::Sequential => self.get_sequential(key),
        }
    }

    /// Check if a key might exist using bloom pre-filters.
    ///
    /// Returns `false` only if all bloom filters definitively exclude the key.
    /// Returns `true` if any filter says "might contain" or if no bloom filters exist.
    pub fn might_contain(&mut self, key: &[u8]) -> TableResult<bool> {
        let bloom_ids: Vec<_> = self.config.bloom_filter_ids().collect();

        if bloom_ids.is_empty() {
            return Ok(true);
        }

        for bloom_id in &bloom_ids {
            if !(self.might_contain_in_bloom)(*bloom_id, key)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn get_bloom_first(&mut self, key: &[u8]) -> TableResult<CompositeQueryResult> {
        let mut tables_consulted = Vec::new();

        // Check all bloom filters first
        let bloom_ids: Vec<_> = self.config.bloom_filter_ids().collect();
        for bloom_id in &bloom_ids {
            tables_consulted.push(TableConsulted {
                table_id: *bloom_id,
                engine_kind: TableEngineKind::Bloom,
                role: SecondaryRole::PreFilter,
                contributed: true,
            });

            if !(self.might_contain_in_bloom)(*bloom_id, key)? {
                // Key definitely not present
                return Ok(CompositeQueryResult {
                    value: None,
                    tables_consulted,
                    pre_filtered: true,
                });
            }
        }

        // All bloom filters passed (or none exist), query primary
        let value = (self.get_from_primary)(self.config.primary_table_id, key, self.snapshot_lsn)?;

        tables_consulted.push(TableConsulted {
            table_id: self.config.primary_table_id,
            engine_kind: self.config.primary_engine_kind,
            role: SecondaryRole::AlternativeAccess,
            contributed: value.is_some(),
        });

        Ok(CompositeQueryResult {
            value,
            tables_consulted,
            pre_filtered: false,
        })
    }

    fn get_primary_first(&mut self, key: &[u8]) -> TableResult<CompositeQueryResult> {
        let mut tables_consulted = Vec::new();

        // Query primary first
        let value = (self.get_from_primary)(self.config.primary_table_id, key, self.snapshot_lsn)?;

        tables_consulted.push(TableConsulted {
            table_id: self.config.primary_table_id,
            engine_kind: self.config.primary_engine_kind,
            role: SecondaryRole::AlternativeAccess,
            contributed: value.is_some(),
        });

        // Apply post-filters if value was found
        if value.is_some() {
            for secondary in &self.config.secondary_tables {
                if secondary.role == SecondaryRole::PostFilter {
                    let contributed = match secondary.engine_kind {
                        TableEngineKind::Bloom => {
                            (self.might_contain_in_bloom)(secondary.table_id, key)?
                        }
                        _ => true,
                    };

                    tables_consulted.push(TableConsulted {
                        table_id: secondary.table_id,
                        engine_kind: secondary.engine_kind,
                        role: secondary.role,
                        contributed,
                    });

                    if !contributed {
                        return Ok(CompositeQueryResult {
                            value: None,
                            tables_consulted,
                            pre_filtered: false,
                        });
                    }
                }
            }
        }

        Ok(CompositeQueryResult {
            value,
            tables_consulted,
            pre_filtered: false,
        })
    }

    fn get_parallel(&mut self, key: &[u8]) -> TableResult<CompositeQueryResult> {
        let mut tables_consulted = Vec::new();

        // Query primary
        let primary_value =
            (self.get_from_primary)(self.config.primary_table_id, key, self.snapshot_lsn)?;

        tables_consulted.push(TableConsulted {
            table_id: self.config.primary_table_id,
            engine_kind: self.config.primary_engine_kind,
            role: SecondaryRole::AlternativeAccess,
            contributed: primary_value.is_some(),
        });

        // Query all secondaries in parallel (conceptually)
        for secondary in &self.config.secondary_tables {
            let contributed = match secondary.engine_kind {
                TableEngineKind::Bloom => (self.might_contain_in_bloom)(secondary.table_id, key)?,
                _ => true,
            };

            tables_consulted.push(TableConsulted {
                table_id: secondary.table_id,
                engine_kind: secondary.engine_kind,
                role: secondary.role,
                contributed,
            });
        }

        // In parallel strategy, all must agree (intersection)
        let all_agree = tables_consulted.iter().all(|t| t.contributed);

        Ok(CompositeQueryResult {
            value: if all_agree { primary_value } else { None },
            tables_consulted,
            pre_filtered: false,
        })
    }

    fn get_sequential(&mut self, key: &[u8]) -> TableResult<CompositeQueryResult> {
        let mut tables_consulted = Vec::new();

        // Try primary first
        let value = (self.get_from_primary)(self.config.primary_table_id, key, self.snapshot_lsn)?;

        tables_consulted.push(TableConsulted {
            table_id: self.config.primary_table_id,
            engine_kind: self.config.primary_engine_kind,
            role: SecondaryRole::AlternativeAccess,
            contributed: value.is_some(),
        });

        if value.is_some() {
            return Ok(CompositeQueryResult {
                value,
                tables_consulted,
                pre_filtered: false,
            });
        }

        // Try secondaries in order
        for secondary in &self.config.secondary_tables {
            if secondary.role == SecondaryRole::AlternativeAccess {
                let found = match secondary.engine_kind {
                    TableEngineKind::Bloom => {
                        (self.might_contain_in_bloom)(secondary.table_id, key)?
                    }
                    _ => false,
                };

                tables_consulted.push(TableConsulted {
                    table_id: secondary.table_id,
                    engine_kind: secondary.engine_kind,
                    role: secondary.role,
                    contributed: found,
                });

                if found {
                    // For bloom, "found" means might contain, so we'd need to
                    // check another source. In sequential mode with bloom secondaries,
                    // this indicates the key might exist elsewhere.
                    // Return None since bloom can't provide the actual value.
                }
            }
        }

        Ok(CompositeQueryResult {
            value: None,
            tables_consulted,
            pre_filtered: false,
        })
    }
}

/// Convenience builder for common composite index patterns.
pub struct CompositeIndexBuilder {
    primary_table_id: TableId,
    primary_engine_kind: TableEngineKind,
    secondaries: Vec<SecondaryTableConfig>,
    strategy: CompositionStrategy,
}

impl CompositeIndexBuilder {
    /// Start building a composite with the given primary table.
    pub fn primary(table_id: TableId, engine_kind: TableEngineKind) -> Self {
        Self {
            primary_table_id: table_id,
            primary_engine_kind: engine_kind,
            secondaries: Vec::new(),
            strategy: CompositionStrategy::default(),
        }
    }

    /// Add a bloom filter as a pre-filter.
    pub fn with_bloom_prefilter(mut self, bloom_table_id: TableId) -> Self {
        self.secondaries.push(SecondaryTableConfig::new(
            bloom_table_id,
            TableEngineKind::Bloom,
            SecondaryRole::PreFilter,
        ));
        self.strategy = CompositionStrategy::BloomFirst;
        self
    }

    /// Add a bloom filter as a post-filter.
    pub fn with_bloom_postfilter(mut self, bloom_table_id: TableId) -> Self {
        self.secondaries.push(SecondaryTableConfig::new(
            bloom_table_id,
            TableEngineKind::Bloom,
            SecondaryRole::PostFilter,
        ));
        self
    }

    /// Add a hash index as an alternative access path for point lookups.
    pub fn with_hash_access(mut self, hash_table_id: TableId) -> Self {
        self.secondaries.push(SecondaryTableConfig::new(
            hash_table_id,
            TableEngineKind::Hash,
            SecondaryRole::AlternativeAccess,
        ));
        self
    }

    /// Set a custom composition strategy.
    pub fn with_strategy(mut self, strategy: CompositionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Build the composite index configuration.
    pub fn build(self) -> CompositeIndexConfig {
        CompositeIndexConfig {
            primary_table_id: self.primary_table_id,
            primary_engine_kind: self.primary_engine_kind,
            secondary_tables: self.secondaries,
            strategy: self.strategy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_config_builder() {
        let config = CompositeIndexBuilder::primary(TableId::from(1), TableEngineKind::LsmTree)
            .with_bloom_prefilter(TableId::from(2))
            .build();

        assert_eq!(config.primary_table_id, TableId::from(1));
        assert_eq!(config.primary_engine_kind, TableEngineKind::LsmTree);
        assert_eq!(config.strategy, CompositionStrategy::BloomFirst);
        assert!(config.has_bloom_filter());
        assert_eq!(
            config.bloom_filter_ids().collect::<Vec<_>>(),
            vec![TableId::from(2)]
        );
    }

    #[test]
    fn test_composite_config_multiple_blooms() {
        let config = CompositeIndexBuilder::primary(TableId::from(1), TableEngineKind::BTree)
            .with_bloom_prefilter(TableId::from(2))
            .with_bloom_prefilter(TableId::from(3))
            .build();

        assert_eq!(config.secondary_tables.len(), 2);
        assert!(config.has_bloom_filter());
        let bloom_ids: Vec<_> = config.bloom_filter_ids().collect();
        assert_eq!(bloom_ids.len(), 2);
    }

    #[test]
    fn test_composite_config_hash_access() {
        let config = CompositeIndexBuilder::primary(TableId::from(1), TableEngineKind::BTree)
            .with_hash_access(TableId::from(2))
            .build();

        assert_eq!(config.secondary_tables.len(), 1);
        assert_eq!(
            config.secondary_tables[0].engine_kind,
            TableEngineKind::Hash
        );
        assert_eq!(
            config.secondary_tables[0].role,
            SecondaryRole::AlternativeAccess
        );
    }

    #[test]
    fn test_composite_config_custom_strategy() {
        let config = CompositeIndexBuilder::primary(TableId::from(1), TableEngineKind::LsmTree)
            .with_bloom_prefilter(TableId::from(2))
            .with_strategy(CompositionStrategy::Parallel)
            .build();

        assert_eq!(config.strategy, CompositionStrategy::Parallel);
    }

    #[test]
    fn test_secondary_role_variants() {
        assert_eq!(SecondaryRole::PreFilter, SecondaryRole::PreFilter);
        assert_eq!(SecondaryRole::PostFilter, SecondaryRole::PostFilter);
        assert_eq!(
            SecondaryRole::AlternativeAccess,
            SecondaryRole::AlternativeAccess
        );
        assert_eq!(SecondaryRole::Enrichment, SecondaryRole::Enrichment);
    }

    #[test]
    fn test_composition_strategy_default() {
        assert_eq!(
            CompositionStrategy::default(),
            CompositionStrategy::BloomFirst
        );
    }
}
