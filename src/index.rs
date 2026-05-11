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

pub use self::error::{IndexError, IndexResult, IndexSourceError};

// Re-export unified types from table module
pub use crate::table::{IndexConsistency, IndexField, IndexKind};

// Re-export renamed specialty table traits from table module
// These are the new canonical names
pub use crate::table::{
    ApproximateMembership, CandidateSet, CostEstimate, DenseOrdered, EdgeCursor, EdgeRef,
    FullTextSearch, GeoHit, GeoPoint, GeoSpatial, GeometryRef, GraphAdjacency, HnswVector,
    IvfVector, PhysicalRange, Predicate, QueryBudget, QueryablePredicate, Rebuildable,
    RebuildBudget, RebuildProgress, ScoredDocument, SparseOrdered, SparseQuery,
    SpecialtyTableCapabilities, SpecialtyTableCursor, SpecialtyTableSource,
    SpecialtyTableSourceError, SpecialtyTableStats, TextField, TextQuery, TimePointRef,
    TimeSeries, TimeSeriesCursor, VectorHit, VectorMetric, VectorSearch, VectorSearchOptions,
};

// Backward compatibility: re-export old trait names as deprecated type aliases
#[deprecated(since = "0.1.0", note = "Use DenseOrdered from crate::table instead")]
pub use crate::table::DenseOrdered as DenseOrderedIndex;

#[deprecated(since = "0.1.0", note = "Use SparseOrdered from crate::table instead")]
pub use crate::table::SparseOrdered as SparseIndex;

#[deprecated(since = "0.1.0", note = "Use ApproximateMembership from crate::table instead")]
pub use crate::table::ApproximateMembership as ApproximateMembershipIndex;

#[deprecated(since = "0.1.0", note = "Use FullTextSearch from crate::table instead")]
pub use crate::table::FullTextSearch as FullTextIndex;

#[deprecated(since = "0.1.0", note = "Use VectorSearch from crate::table instead")]
pub use crate::table::VectorSearch as VectorIndex;

#[deprecated(since = "0.1.0", note = "Use HnswVector from crate::table instead")]
pub use crate::table::HnswVector as HnswIndex;

#[deprecated(since = "0.1.0", note = "Use IvfVector from crate::table instead")]
pub use crate::table::IvfVector as IvfIndex;

#[deprecated(since = "0.1.0", note = "Use GraphAdjacency from crate::table instead")]
pub use crate::table::GraphAdjacency as GraphAdjacencyIndex;

#[deprecated(since = "0.1.0", note = "Use TimeSeries from crate::table instead")]
pub use crate::table::TimeSeries as TimeSeriesIndex;

#[deprecated(since = "0.1.0", note = "Use GeoSpatial from crate::table instead")]
pub use crate::table::GeoSpatial as GeoSpatialIndex;

#[deprecated(since = "0.1.0", note = "Use QueryablePredicate from crate::table instead")]
pub use crate::table::QueryablePredicate as QueryableIndex;

#[deprecated(since = "0.1.0", note = "Use Rebuildable from crate::table instead")]
pub use crate::table::Rebuildable as RebuildableIndex;

#[deprecated(since = "0.1.0", note = "Use SpecialtyTableCursor from crate::table instead")]
pub use crate::table::SpecialtyTableCursor as IndexCursor;

#[deprecated(since = "0.1.0", note = "Use SpecialtyTableCapabilities from crate::table instead")]
pub use crate::table::SpecialtyTableCapabilities as IndexCapabilities;

#[deprecated(since = "0.1.0", note = "Use SpecialtyTableStats from crate::table instead")]
pub use crate::table::SpecialtyTableStats as IndexStats;

#[deprecated(since = "0.1.0", note = "Use SpecialtyTableSource from crate::table instead")]
pub use crate::table::SpecialtyTableSource as IndexSource;

// Keep the old traits module for backward compatibility with deprecated items
pub use self::traits::{IndexId, IndexInfo, IndexOptions};
