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

//! R-Tree implementation for geospatial indexing.
//!
//! This module provides an R-Tree data structure for efficient spatial queries.
//! The R-Tree is a balanced tree structure that groups nearby objects using their
//! minimum bounding rectangles (MBRs).
//!
//! # Features
//!
//! - Multiple splitting strategies (linear, quadratic, R*-tree)
//! - Support for 2D and 3D spatial data
//! - Efficient intersection and nearest neighbor queries
//! - Persistent storage via pager integration
//!
//! # Architecture
//!
//! The R-Tree uses two types of nodes:
//! - Internal nodes: Store MBRs and child page pointers
//! - Leaf nodes: Store MBRs and object IDs
//!
//! Each node maintains a minimum bounding rectangle that encompasses all
//! its children, enabling efficient spatial pruning during queries.

mod config;
mod mbr;
mod node;
mod paged;
mod split;

pub use self::config::{SpatialConfig, SplitStrategy};
pub use self::mbr::Mbr;
pub use self::paged::PagedRTree;

// Made with Bob
