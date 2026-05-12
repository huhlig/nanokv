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

//! Tests for HNSW vector search implementation.
//!
//! Note: These tests verify the basic structure and configuration of the HNSW
//! implementation. Full functionality tests (insert, search, delete) require
//! completing the node storage implementation (load_node/store_node methods).

use nanokv::pager::{Pager, PagerConfig};
use nanokv::table::{HnswConfig, PagedHnswVector, VectorMetric, VectorSearch};
use nanokv::vfs::MemoryFileSystem;
use std::sync::Arc;

#[test]
fn test_hnsw_creation() {
    let fs = MemoryFileSystem::new();
    let config_pager = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config_pager).unwrap());

    let config = HnswConfig {
        dimensions: 128,
        metric: VectorMetric::Cosine,
        max_connections: 16,
        max_connections_layer0: 32,
        ef_construction: 200,
        ml: 1.0 / (16.0_f64).ln(),
    };

    let hnsw = PagedHnswVector::new(
        1.into(),
        "test_hnsw".to_string(),
        pager,
        config,
    );

    assert!(hnsw.is_ok(), "HNSW creation should succeed");
    let hnsw = hnsw.unwrap();
    
    // Verify basic properties through VectorSearch trait
    assert_eq!(hnsw.dimensions(), 128);
    assert_eq!(hnsw.metric(), VectorMetric::Cosine);
}

#[test]
fn test_hnsw_euclidean_metric() {
    let fs = MemoryFileSystem::new();
    let config_pager = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config_pager).unwrap());

    let config = HnswConfig {
        dimensions: 3,
        metric: VectorMetric::Euclidean,
        max_connections: 16,
        max_connections_layer0: 32,
        ef_construction: 200,
        ml: 1.0 / (16.0_f64).ln(),
    };

    let hnsw = PagedHnswVector::new(
        1.into(),
        "test_euclidean".to_string(),
        pager,
        config,
    )
    .unwrap();

    assert_eq!(hnsw.metric(), VectorMetric::Euclidean);
    assert_eq!(hnsw.dimensions(), 3);
}

#[test]
fn test_hnsw_cosine_metric() {
    let fs = MemoryFileSystem::new();
    let config_pager = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config_pager).unwrap());

    let config = HnswConfig {
        dimensions: 64,
        metric: VectorMetric::Cosine,
        max_connections: 32,
        max_connections_layer0: 64,
        ef_construction: 400,
        ml: 1.0 / (32.0_f64).ln(),
    };

    let hnsw = PagedHnswVector::new(
        2.into(),
        "test_cosine".to_string(),
        pager,
        config,
    )
    .unwrap();

    assert_eq!(hnsw.metric(), VectorMetric::Cosine);
    assert_eq!(hnsw.dimensions(), 64);
}

#[test]
fn test_hnsw_manhattan_metric() {
    let fs = MemoryFileSystem::new();
    let config_pager = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config_pager).unwrap());

    let config = HnswConfig {
        dimensions: 32,
        metric: VectorMetric::Manhattan,
        max_connections: 16,
        max_connections_layer0: 32,
        ef_construction: 200,
        ml: 1.0 / (16.0_f64).ln(),
    };

    let hnsw = PagedHnswVector::new(
        3.into(),
        "test_manhattan".to_string(),
        pager,
        config,
    )
    .unwrap();

    assert_eq!(hnsw.metric(), VectorMetric::Manhattan);
    assert_eq!(hnsw.dimensions(), 32);
}

#[test]
fn test_hnsw_configuration_parameters() {
    let fs = MemoryFileSystem::new();
    let config_pager = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config_pager).unwrap());

    // Test with custom configuration parameters
    let config = HnswConfig {
        dimensions: 256,
        metric: VectorMetric::Euclidean,
        max_connections: 48,
        max_connections_layer0: 96,
        ef_construction: 500,
        ml: 1.0 / (48.0_f64).ln(),
    };

    let hnsw = PagedHnswVector::new(
        4.into(),
        "test_config".to_string(),
        pager,
        config,
    );

    assert!(hnsw.is_ok(), "HNSW with custom config should succeed");
    let hnsw = hnsw.unwrap();
    
    assert_eq!(hnsw.dimensions(), 256);
    assert_eq!(hnsw.metric(), VectorMetric::Euclidean);
}

// Made with Bob
