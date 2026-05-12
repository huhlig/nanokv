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

//! HNSW (Hierarchical Navigable Small World) vector search implementation.
//!
//! This module provides a paged HNSW implementation for approximate nearest
//! neighbor search. HNSW uses a hierarchical graph structure where:
//!
//! - Each vector is a node in multiple layers of a graph
//! - Higher layers are sparser (fewer nodes) for coarse navigation
//! - Lower layers are denser for fine-grained search
//! - Each node maintains bidirectional connections to M neighbors per layer
//!
//! The algorithm provides excellent recall with logarithmic search complexity.

mod paged;

pub use self::paged::{HnswConfig, PagedHnswVector};

// Made with Bob
