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

/// Test bloom filter operations
#[test]
fn test_bloom_filter_operations() {
    use nanokv::table::lsm::BloomFilterBuilder;

    // Create a bloom filter using the builder
    let mut filter = BloomFilterBuilder::new(1000)
        .bits_per_key(10)
        .build();

    // Add keys to the bloom filter
    filter.insert(b"key1");
    filter.insert(b"key2");
    filter.insert(b"key3");

    // Test that inserted keys are found
    assert!(filter.contains(b"key1"));
    assert!(filter.contains(b"key2"));
    assert!(filter.contains(b"key3"));

    // Test that non-inserted keys are likely not present
    // (Bloom filters can have false positives but never false negatives)
    assert!(!filter.contains(b"nonexistent_key"));

    // Test filter statistics
    assert_eq!(filter.num_items(), 3);
    assert_eq!(filter.num_bits(), 1000 * 10);

    // Test false positive rate is reasonable
    let fpr = filter.false_positive_rate();
    assert!(fpr > 0.0 && fpr < 1.0);
}

/// Test SSTable operations (requires full implementation)
#[test]
fn test_sstable_operations() {
    use nanokv::pager::{Pager, PagerConfig};
    use nanokv::table::lsm::{SStableConfig, SStableReader, SStableWriter, SStableId};
    use nanokv::txn::VersionChain;
    use nanokv::vfs::MemoryFileSystem;
    use nanokv::wal::LogSequenceNumber;
    use std::sync::Arc;

    // Create pager with MemoryFileSystem
    let fs = MemoryFileSystem::new();
    let config = PagerConfig::default();
    let pager = Arc::new(Pager::create(&fs, "test.db", config).expect("Failed to create pager"));

    // Create SSTable writer
    let sstable_id = SStableId::new(1);
    let sstable_config = SStableConfig::default();
    let mut writer = SStableWriter::new(
        pager.clone(),
        sstable_id,
        0, // Level 0
        sstable_config.clone(),
        3, // estimated entries
    );

    // Add sorted key-value pairs
    let keys = vec!["alpha", "beta", "gamma"];
    for key in &keys {
        let mut chain = VersionChain::new(
            key.as_bytes().to_vec(),
            1.into(),
        );
        chain.commit(LogSequenceNumber::from(1));
        writer.add(key.as_bytes().to_vec(), chain).expect("Failed to add entry");
    }

    // Finish writing SSTable
    let created_lsn = LogSequenceNumber::from(1);
    let metadata = writer.finish(created_lsn).expect("Failed to finish SSTable");

    // Verify metadata
    assert_eq!(metadata.id.as_u64(), 1);
    assert_eq!(metadata.level, 0);
    assert_eq!(metadata.num_entries, 3);
    assert_eq!(metadata.min_key, b"alpha");
    assert_eq!(metadata.max_key, b"gamma");

    // Open SSTable for reading
    let reader = SStableReader::open(
        pager.clone(),
        metadata.first_page_id,
        sstable_config,
    ).expect("Failed to open SSTable");

    // Verify bloom filter (note: bloom filters can have false positives)
    assert!(reader.may_contain(b"alpha"));
    assert!(reader.may_contain(b"beta"));
    assert!(reader.may_contain(b"gamma"));
    // "nonexistent" might be a false positive, so we don't assert it's definitely not present

    // Read back the keys
    let snapshot_lsn = LogSequenceNumber::from(1);
    for key in &keys {
        let value = reader.get(key.as_bytes(), snapshot_lsn)
            .expect("Failed to get key");
        assert!(value.is_some(), "Key {} should exist", key);
        assert_eq!(value.unwrap(), key.as_bytes(), "Value should match key for {}", key);
    }

    // Verify non-existent key
    let result = reader.get(b"nonexistent", snapshot_lsn).expect("Failed to get key");
    assert!(result.is_none(), "Non-existent key should not be found");
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
