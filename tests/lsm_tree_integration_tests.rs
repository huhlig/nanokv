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

//! Integration tests for the LSM Tree storage engine.
//!
//! These tests verify the LSM tree's configuration types and basic structures.
//! Full integration tests require a pager and VFS setup which is complex.
//!
//! Note: Many tests are marked as #[ignore] because the LSM implementation
//! is not yet complete or the API differs from expectations.

use nanokv::table::lsm::{BloomFilterBuilder, CompactionConfig, CompactionStrategy, LsmConfig};

/// Test bloom filter creation
#[test]
fn test_bloom_filter_creation() {
    let builder = BloomFilterBuilder::new(1000);
    let _filter = builder.build();

    // Filter should be created successfully (no panic)
}

/// Test LSM config creation
#[test]
fn test_lsm_config_creation() {
    let config = LsmConfig::default();

    assert!(config.memtable.max_size > 0);
    assert!(!config.compaction.levels.is_empty());
}

/// Test compaction strategies
#[test]
fn test_compaction_strategies() {
    let strategies = vec![CompactionStrategy::Leveled, CompactionStrategy::Universal];

    for strategy in strategies {
        let mut config = CompactionConfig::default();
        config.strategy = strategy;
        assert_eq!(config.strategy, strategy);
    }
}

/// Test default LSM config values
#[test]
fn test_default_lsm_config() {
    let config = LsmConfig::default();

    // Verify reasonable defaults
    assert!(config.memtable.max_size >= 1024 * 1024); // At least 1MB
    assert!(config.sstable.target_size >= 1024); // At least 1KB
    assert!(!config.compaction.levels.is_empty());
    assert!(config.compaction.max_threads > 0);
}

// The following tests are disabled because they require API changes
// or full LSM tree implementation with pager/VFS

/// Test memtable operations (requires full implementation)
#[test]
#[ignore = "Memtable API not fully exposed for testing"]
fn test_memtable_operations() {
    // This would test insert, get, delete operations
    // Requires Memtable to be fully testable without pager
}

/// Test bloom filter operations (requires API changes)
#[test]
#[ignore = "BloomFilter API needs add/query methods"]
fn test_bloom_filter_operations() {
    // This would test add_key and may_contain operations
    // Requires BloomFilterBuilder to expose add method
}

/// Test SSTable operations (requires full implementation)
#[test]
#[ignore = "SSTable requires pager and VFS setup"]
fn test_sstable_operations() {
    // This would test SSTable read/write operations
    // Requires complex pager and VFS initialization
}

/// Test compaction (requires full implementation)
#[test]
fn test_compaction() {
    use nanokv::table::lsm::{CompactionPicker, FileMetadata, Version, VersionEdit};
    use nanokv::wal::LogSequenceNumber;

    // Create a version with L0 files that exceed the size limit
    let mut version = Version::new(7);

    // Add L0 files (each 2MB, total exceeds 10MB limit)
    for i in 0..6u64 {
        let mut metadata = FileMetadata {
            id: nanokv::table::lsm::SStableId::new(i),
            level: 0,
            min_key: format!("key{:04}", i).into_bytes(),
            max_key: format!("key{:04}", i + 1).into_bytes(),
            num_entries: 100,
            total_size: 2 * 1024 * 1024, // 2MB each
            created_lsn: LogSequenceNumber::from(0),
            first_page_id: nanokv::pager::PageId::from(i),
            num_pages: 1,
        };
        // Make key ranges non-overlapping for cleaner test
        metadata.min_key = format!("a{:04}", i).into_bytes();
        metadata.max_key = format!("b{:04}", i).into_bytes();
        let edit = VersionEdit::add_sstable(metadata);
        version = version.apply(&edit).unwrap();
    }

    // Create compaction picker with default config
    let config = nanokv::table::lsm::CompactionConfig::default();
    let picker = CompactionPicker::new(config);

    // Should pick L0 compaction since we exceed the size limit
    let job = picker.pick_compaction(&version);
    assert!(job.is_some(), "Should pick compaction when L0 exceeds size limit");

    let job = job.unwrap();
    assert_eq!(job.source_level, 0);
    assert_eq!(job.target_level, 1);
    assert!(!job.source_files.is_empty(), "Should have source files to compact");
    assert!(job.priority > 1.0, "Priority should be > 1.0 when over limit");

    // Verify input file IDs are unique
    let input_ids = job.input_file_ids();
    assert_eq!(input_ids.len(), job.input_file_count());

    // Verify estimated output size calculation
    let expected_size: u64 = job
        .source_files
        .iter()
        .map(|f| f.total_size)
        .sum::<u64>()
        + job.target_files.iter().map(|f| f.total_size).sum::<u64>();
    assert_eq!(job.estimated_output_size, expected_size);
}

/// Test LSM tree MVCC (requires full implementation)
#[test]
#[ignore = "MVCC testing requires full LSM tree setup"]
fn test_lsm_mvcc() {
    // This would test MVCC snapshot isolation
    // Requires full LSM tree with version chains
}

// Made with Bob
