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

//! Configuration for GraphAdjacency table engine.

/// Configuration for GraphAdjacency table engine.
#[derive(Debug, Clone)]
pub struct GraphConfig {
    /// Whether the graph is directed or undirected.
    /// In directed graphs, edges have a source and target.
    /// In undirected graphs, edges are bidirectional.
    /// Default: true (directed)
    pub directed: bool,

    /// Whether edges have weights.
    /// Default: false (unweighted)
    pub weighted: bool,

    /// Whether to maintain an in-memory index for fast lookups.
    /// Default: true
    pub use_memory_index: bool,

    /// Maximum number of edges per vertex before warning.
    /// Used for detecting potential performance issues.
    /// Default: 10000
    pub max_edges_per_vertex: usize,

    /// Storage backend to use.
    /// Default: Hash (faster for random access)
    pub storage_backend: GraphStorageBackend,
}

/// Storage backend for graph data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphStorageBackend {
    /// Use Hash table for O(1) lookups (faster for random access)
    Hash,
    /// Use BTree for ordered iteration (better for range queries)
    BTree,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            directed: true,
            weighted: false,
            use_memory_index: true,
            max_edges_per_vertex: 10000,
            storage_backend: GraphStorageBackend::Hash,
        }
    }
}

impl GraphConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether the graph is directed.
    pub fn with_directed(mut self, directed: bool) -> Self {
        self.directed = directed;
        self
    }

    /// Set whether edges have weights.
    pub fn with_weighted(mut self, weighted: bool) -> Self {
        self.weighted = weighted;
        self
    }

    /// Enable or disable memory index.
    pub fn with_memory_index(mut self, use_index: bool) -> Self {
        self.use_memory_index = use_index;
        self
    }

    /// Set the maximum edges per vertex.
    pub fn with_max_edges_per_vertex(mut self, max: usize) -> Self {
        self.max_edges_per_vertex = max;
        self
    }

    /// Set the storage backend.
    pub fn with_storage_backend(mut self, backend: GraphStorageBackend) -> Self {
        self.storage_backend = backend;
        self
    }

    /// Create a configuration for an undirected graph.
    pub fn undirected() -> Self {
        Self::default().with_directed(false)
    }

    /// Create a configuration for a weighted graph.
    pub fn weighted() -> Self {
        Self::default().with_weighted(true)
    }

    /// Create a configuration for an undirected, weighted graph.
    pub fn undirected_weighted() -> Self {
        Self::default().with_directed(false).with_weighted(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GraphConfig::default();
        assert!(config.directed);
        assert!(!config.weighted);
        assert!(config.use_memory_index);
        assert_eq!(config.max_edges_per_vertex, 10000);
        assert_eq!(config.storage_backend, GraphStorageBackend::Hash);
    }

    #[test]
    fn test_config_builder() {
        let config = GraphConfig::new()
            .with_directed(false)
            .with_weighted(true)
            .with_memory_index(false)
            .with_max_edges_per_vertex(5000)
            .with_storage_backend(GraphStorageBackend::BTree);

        assert!(!config.directed);
        assert!(config.weighted);
        assert!(!config.use_memory_index);
        assert_eq!(config.max_edges_per_vertex, 5000);
        assert_eq!(config.storage_backend, GraphStorageBackend::BTree);
    }

    #[test]
    fn test_convenience_constructors() {
        let undirected = GraphConfig::undirected();
        assert!(!undirected.directed);
        assert!(!undirected.weighted);

        let weighted = GraphConfig::weighted();
        assert!(weighted.directed);
        assert!(weighted.weighted);

        let undirected_weighted = GraphConfig::undirected_weighted();
        assert!(!undirected_weighted.directed);
        assert!(undirected_weighted.weighted);
    }
}

// Made with Bob
