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

//! Index Types to Support
//!
//! # Indexes
//! - B/B+ Tree
//! - Hash Index
//! - Adaptive Radix Tree
//! - LSM Tree Index
//!
//! # Spatial Indexes
//! - R-Tree
//! - Quadtree
//! - Octree
//!
//! # Text Indexes
//! - Full-Text Search
//! - Inverted Index
//!
//! # Graph Indexes
//! - Adjacency List
//! - Adjacency Matrix
//! - Edge List
//!
//! # Time Series Indexes
//! - Time Bucket
//! - Time Window
//! - Time Series Database
//!
//! # Geospatial Indexes
//! - Geohash
//! - Quadtree
//! - R-Tree
//!
//! # Vector Indexes
//! - Vector Database
//! - ANN (Approximate Nearest Neighbor)
//! - IVF (Inverted File Index)
//! - HNSW (Hierarchical Navigable Small World)
//!
//! # Bloom Filters
//! - Bloom Filter
//! - Cuckoo Filter
//! - Count-Min Sketch

mod error;
mod traits;

pub use self::error::{IndexError, IndexResult};
pub use self::traits::{
    ApproximateMembershipIndex, CandidateSet, CostEstimate, DenseOrderedIndex, EdgeRef,
    FullTextIndex, GeoHit, GeoPoint, GeoSpatialIndex, GeometryRef, GraphAdjacencyIndex, HnswIndex,
    Index, IndexCapabilities, IndexConsistency, IndexCursor, IndexField, IndexId, IndexInfo,
    IndexKind, IndexOptions, IndexSourceError, IndexSourceVisitor, IndexStats, IvfIndex,
    PhysicalRange, Predicate, QueryBudget, QueryableIndex, RebuildBudget, RebuildProgress,
    RebuildableIndex, ScoredDocument, SparseIndex, SparseQuery, TextField, TextQuery, TimePointRef,
    TimeSeriesIndex, VectorHit, VectorIndex, VectorMetric, VectorSearchOptions,
};
