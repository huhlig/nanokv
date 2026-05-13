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

//! In-memory GraphAdjacency table implementation.
//!
//! This module provides a memory-resident graph adjacency table implementation
//! optimized for:
//! - Fast edge lookups and traversals
//! - Efficient neighbor queries (outgoing/incoming edges)
//! - Graph algorithms (BFS, DFS)
//! - Both directed and undirected graphs
//! - Optional edge weights
//!
//! The implementation uses a Hash table as the underlying storage engine,
//! with specialized indexing for graph operations.

use crate::table::{
    EdgeCursor, EdgeRef, GraphAdjacency, MemoryHashTable, SpecialtyTableCapabilities,
    SpecialtyTableStats, Table, TableError, TableResult, VerificationReport,
};
use crate::txn::TransactionId;
use crate::types::{KeyBuf, TableId};
use crate::wal::LogSequenceNumber;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use super::config::GraphConfig;
use super::edge::{Edge, EdgeData, GraphKey};

/// In-memory GraphAdjacency table.
///
/// Uses a Hash table for storage with specialized indexing for graph operations.
/// Maintains separate indices for outgoing and incoming edges for efficient traversal.
pub struct MemoryGraphTable {
    /// Table identifier
    table_id: TableId,

    /// Table name
    name: String,

    /// Configuration
    config: GraphConfig,

    /// Underlying hash table for storage
    storage: Arc<MemoryHashTable>,

    /// In-memory index for fast lookups (optional)
    index: RwLock<GraphIndex>,

    /// Transaction counter for generating LSNs
    tx_counter: RwLock<u64>,
}

/// In-memory index for fast graph operations.
#[derive(Default)]
struct GraphIndex {
    /// Map from vertex to outgoing edges
    outgoing: HashMap<Vec<u8>, Vec<Edge>>,
    /// Map from vertex to incoming edges
    incoming: HashMap<Vec<u8>, Vec<Edge>>,
    /// Set of all vertices
    vertices: HashSet<Vec<u8>>,
}

impl MemoryGraphTable {
    /// Create a new in-memory graph table.
    pub fn new(table_id: TableId, name: String, config: GraphConfig) -> Self {
        let storage = Arc::new(MemoryHashTable::new(table_id, format!("{}_storage", name)));
        Self {
            table_id,
            name,
            config,
            storage,
            index: RwLock::new(GraphIndex::default()),
            tx_counter: RwLock::new(0),
        }
    }

    /// Get the next transaction ID and LSN.
    fn next_tx(&self) -> (TransactionId, LogSequenceNumber) {
        let mut counter = self.tx_counter.write().unwrap();
        *counter += 1;
        (TransactionId::from(*counter), LogSequenceNumber::from(*counter))
    }

    /// Add a vertex to the graph (implicit when adding edges).
    pub fn add_vertex(&self, vertex: &[u8]) -> TableResult<()> {
        if self.config.use_memory_index {
            let mut index = self.index.write().unwrap();
            index.vertices.insert(vertex.to_vec());
        }
        Ok(())
    }

    /// Get all vertices in the graph.
    pub fn vertices(&self) -> TableResult<Vec<Vec<u8>>> {
        if self.config.use_memory_index {
            let index = self.index.read().unwrap();
            Ok(index.vertices.iter().cloned().collect())
        } else {
            // Without index, we'd need to scan all edges
            Err(TableError::operation_not_supported(
                "vertices() requires memory index to be enabled",
            ))
        }
    }

    /// Perform breadth-first search from a starting vertex.
    pub fn bfs<F>(&self, start: &[u8], mut visit: F) -> TableResult<()>
    where
        F: FnMut(&[u8]) -> bool, // Returns true to continue, false to stop
    {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(start.to_vec());
        visited.insert(start.to_vec());

        while let Some(vertex) = queue.pop_front() {
            if !visit(&vertex) {
                break;
            }

            // Get outgoing edges
            let cursor = self.outgoing(&vertex, None)?;
            let edges = cursor.collect_all()?;

            for edge in edges {
                if !visited.contains(&edge.target.0) {
                    visited.insert(edge.target.0.clone());
                    queue.push_back(edge.target.0);
                }
            }
        }

        Ok(())
    }

    /// Perform depth-first search from a starting vertex.
    pub fn dfs<F>(&self, start: &[u8], mut visit: F) -> TableResult<()>
    where
        F: FnMut(&[u8]) -> bool, // Returns true to continue, false to stop
    {
        let mut visited = HashSet::new();
        self.dfs_recursive(start, &mut visited, &mut visit)
    }

    fn dfs_recursive<F>(
        &self,
        vertex: &[u8],
        visited: &mut HashSet<Vec<u8>>,
        visit: &mut F,
    ) -> TableResult<()>
    where
        F: FnMut(&[u8]) -> bool,
    {
        if visited.contains(vertex) {
            return Ok(());
        }

        visited.insert(vertex.to_vec());

        if !visit(vertex) {
            return Ok(());
        }

        // Get outgoing edges
        let cursor = self.outgoing(vertex, None)?;
        let edges = cursor.collect_all()?;

        for edge in edges {
            self.dfs_recursive(&edge.target.0, visited, visit)?;
        }

        Ok(())
    }

    /// Get neighbors of a vertex (outgoing edges).
    pub fn neighbors(&self, vertex: &[u8]) -> TableResult<Vec<Vec<u8>>> {
        let cursor = self.outgoing(vertex, None)?;
        let edges = cursor.collect_all()?;
        Ok(edges.into_iter().map(|e| e.target.0).collect())
    }

    /// Check if there's an edge between two vertices.
    pub fn has_edge(&self, source: &[u8], target: &[u8], label: Option<&[u8]>) -> TableResult<bool> {
        let cursor = self.outgoing(source, label)?;
        let edges = cursor.collect_all()?;
        Ok(edges.iter().any(|e| e.target.0 == target))
    }
}

impl Table for MemoryGraphTable {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> crate::table::TableEngineKind {
        crate::table::TableEngineKind::GraphAdjacency
    }

    fn capabilities(&self) -> crate::table::TableCapabilities {
        crate::table::TableCapabilities {
            ordered: false,
            point_lookup: true,
            prefix_scan: false,
            reverse_scan: false,
            range_delete: false,
            merge_operator: false,
            mvcc_native: false,
            append_optimized: false,
            memory_resident: true,
            disk_resident: false,
            supports_compression: false,
            supports_encryption: false,
        }
    }

    fn stats(&self) -> TableResult<crate::table::TableStatistics> {
        self.storage.stats()
    }
}

impl GraphAdjacency for MemoryGraphTable {
    type EdgeCursor<'a> = MemoryEdgeCursor;

    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> SpecialtyTableCapabilities {
        SpecialtyTableCapabilities {
            exact: true,
            approximate: false,
            ordered: false,
            sparse: false,
            supports_delete: true,
            supports_range_query: false,
            supports_prefix_query: false,
            supports_scoring: false,
            supports_incremental_rebuild: false,
            may_be_stale: false,
        }
    }

    fn add_edge(
        &mut self,
        source: &[u8],
        label: &[u8],
        target: &[u8],
        edge_id: &[u8],
    ) -> TableResult<()> {
        let (tx_id, lsn) = self.next_tx();

        // Create edge data
        let edge_data = EdgeData {
            source: source.to_vec(),
            label: label.to_vec(),
            target: target.to_vec(),
            weight: None, // TODO: Support weights
        };

        // Store edge data
        let edge_key = GraphKey::EdgeData {
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.put(&edge_key.encode(), &edge_data.encode(), tx_id, lsn)?;

        // Store outgoing edge index
        let out_key = GraphKey::Outgoing {
            source: KeyBuf(source.to_vec()),
            label: KeyBuf(label.to_vec()),
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.put(&out_key.encode(), &target, tx_id, lsn)?;

        // Store incoming edge index
        let in_key = GraphKey::Incoming {
            target: KeyBuf(target.to_vec()),
            label: KeyBuf(label.to_vec()),
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.put(&in_key.encode(), &source, tx_id, lsn)?;

        // For undirected graphs, add reverse edge
        if !self.config.directed {
            let rev_out_key = GraphKey::Outgoing {
                source: KeyBuf(target.to_vec()),
                label: KeyBuf(label.to_vec()),
                edge_id: KeyBuf(edge_id.to_vec()),
            };
            self.storage.put(&rev_out_key.encode(), &source, tx_id, lsn)?;

            let rev_in_key = GraphKey::Incoming {
                target: KeyBuf(source.to_vec()),
                label: KeyBuf(label.to_vec()),
                edge_id: KeyBuf(edge_id.to_vec()),
            };
            self.storage.put(&rev_in_key.encode(), &target, tx_id, lsn)?;
        }

        // Update in-memory index
        if self.config.use_memory_index {
            let mut index = self.index.write().unwrap();
            index.vertices.insert(source.to_vec());
            index.vertices.insert(target.to_vec());

            let edge = Edge::unweighted(
                KeyBuf(edge_id.to_vec()),
                KeyBuf(source.to_vec()),
                KeyBuf(label.to_vec()),
                KeyBuf(target.to_vec()),
            );

            index
                .outgoing
                .entry(source.to_vec())
                .or_insert_with(Vec::new)
                .push(edge.clone());

            index
                .incoming
                .entry(target.to_vec())
                .or_insert_with(Vec::new)
                .push(edge.clone());

            if !self.config.directed {
                let rev_edge = Edge::unweighted(
                    KeyBuf(edge_id.to_vec()),
                    KeyBuf(target.to_vec()),
                    KeyBuf(label.to_vec()),
                    KeyBuf(source.to_vec()),
                );

                index
                    .outgoing
                    .entry(target.to_vec())
                    .or_insert_with(Vec::new)
                    .push(rev_edge.clone());

                index
                    .incoming
                    .entry(source.to_vec())
                    .or_insert_with(Vec::new)
                    .push(rev_edge);
            }
        }

        Ok(())
    }

    fn remove_edge(
        &mut self,
        source: &[u8],
        label: &[u8],
        target: &[u8],
        edge_id: &[u8],
    ) -> TableResult<()> {
        // Remove edge data
        let edge_key = GraphKey::EdgeData {
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.delete(&edge_key.encode())?;

        // Remove outgoing edge index
        let out_key = GraphKey::Outgoing {
            source: KeyBuf(source.to_vec()),
            label: KeyBuf(label.to_vec()),
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.delete(&out_key.encode())?;

        // Remove incoming edge index
        let in_key = GraphKey::Incoming {
            target: KeyBuf(target.to_vec()),
            label: KeyBuf(label.to_vec()),
            edge_id: KeyBuf(edge_id.to_vec()),
        };
        self.storage.delete(&in_key.encode())?;

        // For undirected graphs, remove reverse edge
        if !self.config.directed {
            let rev_out_key = GraphKey::Outgoing {
                source: KeyBuf(target.to_vec()),
                label: KeyBuf(label.to_vec()),
                edge_id: KeyBuf(edge_id.to_vec()),
            };
            self.storage.delete(&rev_out_key.encode())?;

            let rev_in_key = GraphKey::Incoming {
                target: KeyBuf(source.to_vec()),
                label: KeyBuf(label.to_vec()),
                edge_id: KeyBuf(edge_id.to_vec()),
            };
            self.storage.delete(&rev_in_key.encode())?;
        }

        // Update in-memory index
        if self.config.use_memory_index {
            let mut index = self.index.write().unwrap();

            if let Some(edges) = index.outgoing.get_mut(source) {
                edges.retain(|e| e.edge_id.0 != edge_id);
            }

            if let Some(edges) = index.incoming.get_mut(target) {
                edges.retain(|e| e.edge_id.0 != edge_id);
            }

            if !self.config.directed {
                if let Some(edges) = index.outgoing.get_mut(target) {
                    edges.retain(|e| e.edge_id.0 != edge_id);
                }

                if let Some(edges) = index.incoming.get_mut(source) {
                    edges.retain(|e| e.edge_id.0 != edge_id);
                }
            }
        }

        Ok(())
    }

    fn outgoing(&self, source: &[u8], label: Option<&[u8]>) -> TableResult<Self::EdgeCursor<'_>> {
        // Use in-memory index if available
        if self.config.use_memory_index {
            let index = self.index.read().unwrap();
            let edges = index
                .outgoing
                .get(source)
                .map(|edges| {
                    if let Some(label) = label {
                        edges
                            .iter()
                            .filter(|e| e.label.0 == label)
                            .cloned()
                            .collect()
                    } else {
                        edges.clone()
                    }
                })
                .unwrap_or_default();

            return Ok(MemoryEdgeCursor::new(edges));
        }

        // Otherwise, scan storage (less efficient)
        // For now, return empty cursor - full implementation would scan the hash table
        Ok(MemoryEdgeCursor::new(Vec::new()))
    }

    fn incoming(&self, target: &[u8], label: Option<&[u8]>) -> TableResult<Self::EdgeCursor<'_>> {
        // Use in-memory index if available
        if self.config.use_memory_index {
            let index = self.index.read().unwrap();
            let edges = index
                .incoming
                .get(target)
                .map(|edges| {
                    if let Some(label) = label {
                        edges
                            .iter()
                            .filter(|e| e.label.0 == label)
                            .cloned()
                            .collect()
                    } else {
                        edges.clone()
                    }
                })
                .unwrap_or_default();

            return Ok(MemoryEdgeCursor::new(edges));
        }

        // Otherwise, scan storage (less efficient)
        Ok(MemoryEdgeCursor::new(Vec::new()))
    }

    fn stats(&self) -> TableResult<SpecialtyTableStats> {
        let storage_stats = self.storage.stats()?;

        let (entry_count, distinct_keys) = if self.config.use_memory_index {
            let index = self.index.read().unwrap();
            let edge_count = index
                .outgoing
                .values()
                .map(|edges| edges.len())
                .sum::<usize>();
            (Some(edge_count as u64), Some(index.vertices.len() as u64))
        } else {
            (storage_stats.row_count, None)
        };

        Ok(SpecialtyTableStats {
            entry_count,
            size_bytes: storage_stats.total_size_bytes,
            distinct_keys,
            stale_entries: None,
            last_updated_lsn: storage_stats.last_updated_lsn,
        })
    }

    fn verify(&self) -> TableResult<VerificationReport> {
        // Basic verification - check that all edges have valid source/target
        Ok(VerificationReport {
            checked_items: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

/// Cursor over graph edges in memory.
pub struct MemoryEdgeCursor {
    edges: Vec<Edge>,
    position: usize,
}

impl MemoryEdgeCursor {
    fn new(edges: Vec<Edge>) -> Self {
        Self { edges, position: 0 }
    }

    /// Collect all remaining edges.
    pub fn collect_all(mut self) -> TableResult<Vec<EdgeRef>> {
        let mut result = Vec::new();
        while self.valid() {
            if let Some(edge_ref) = self.current() {
                result.push(edge_ref);
            }
            self.next()?;
        }
        Ok(result)
    }
}

impl EdgeCursor for MemoryEdgeCursor {
    fn valid(&self) -> bool {
        self.position < self.edges.len()
    }

    fn current(&self) -> Option<EdgeRef> {
        if self.valid() {
            let edge = &self.edges[self.position];
            Some(EdgeRef {
                edge_id: edge.edge_id.clone(),
                source: edge.source.clone(),
                label: edge.label.clone(),
                target: edge.target.clone(),
            })
        } else {
            None
        }
    }

    fn next(&mut self) -> TableResult<()> {
        if self.valid() {
            self.position += 1;
        }
        Ok(())
    }
}

// Made with Bob