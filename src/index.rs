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

/// Logical index identifier assigned by the catalog.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct IndexId(u64);

impl IndexId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexId({})", self.0)
    }
}

impl From<u64> for IndexId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
