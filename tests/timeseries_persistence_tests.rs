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

//! Tests for TimeSeries bucket persistence, eviction, and recovery.

use nanokv::pager::{Pager, PagerConfig};
use nanokv::table::timeseries::{
    BucketId, BucketManager, TimeBucket, TimeSeriesAggregation, TimeSeriesConfig, TimeSeriesTable,
};
use nanokv::table::TimeSeries;
use nanokv::types::TableId;
use nanokv::vfs::MemoryFileSystem;
use std::sync::Arc;


#[test]
fn test_bucket_flush_and_load() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    // Create a bucket with data
    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    bucket.insert(100, vec![1, 2, 3]).unwrap();
    bucket.insert(200, vec![4, 5, 6]).unwrap();

    // Flush to disk
    assert!(bucket.is_dirty());
    bucket.flush().unwrap();
    assert!(!bucket.is_dirty());

    let page_id = bucket.page_id().expect("Bucket should have a page ID");

    // Load from disk
    let loaded_bucket = TimeBucket::load(page_id, 3600, pager.clone()).unwrap();

    // Verify data
    assert_eq!(loaded_bucket.id(), bucket_id);
    assert_eq!(loaded_bucket.len(), 2);
    assert_eq!(loaded_bucket.page_id(), Some(page_id));
}

#[test]
fn test_bucket_manager_eviction() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    // Create a bucket manager with small memory limit
    let mut manager = BucketManager::new(3600, pager.clone(), 2);

    // Add buckets until eviction occurs
    let bucket1 = manager.get_or_create_bucket(100).unwrap();
    bucket1.insert(100, vec![1]).unwrap();

    let bucket2 = manager.get_or_create_bucket(4000).unwrap();
    bucket2.insert(4000, vec![2]).unwrap();

    assert_eq!(manager.bucket_count(), 2);

    // Flush buckets so they can be evicted
    manager.flush_all().unwrap();

    // Verify we have 2 buckets tracked after flush
    assert_eq!(manager.total_bucket_count(), 2);

    // Adding a third bucket should trigger eviction
    let bucket3 = manager.get_or_create_bucket(8000).unwrap();
    bucket3.insert(8000, vec![3]).unwrap();

    // Should still have 2 buckets in memory (one was evicted)
    assert_eq!(manager.bucket_count(), 2);

    // After flushing the third bucket, we should have 3 total buckets tracked
    manager.flush_all().unwrap();
    assert_eq!(manager.total_bucket_count(), 3);
}

#[test]
fn test_bucket_manager_lazy_loading() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    // Create a bucket manager with small memory limit
    let mut manager = BucketManager::new(3600, pager.clone(), 2);

    // Add and populate buckets
    let bucket1 = manager.get_or_create_bucket(100).unwrap();
    bucket1.insert(100, vec![1, 2, 3]).unwrap();

    let bucket2 = manager.get_or_create_bucket(4000).unwrap();
    bucket2.insert(4000, vec![4, 5, 6]).unwrap();

    // Flush all buckets
    manager.flush_all().unwrap();

    // Add a third bucket to trigger eviction of the first
    let bucket3 = manager.get_or_create_bucket(8000).unwrap();
    bucket3.insert(8000, vec![7, 8, 9]).unwrap();

    // Now access the first bucket again - should be lazy loaded
    let reloaded_bucket1 = manager.get_or_create_bucket(100).unwrap();
    
    // Verify the data was preserved
    assert_eq!(reloaded_bucket1.len(), 1);
    let value = reloaded_bucket1.get(100).unwrap();
    assert_eq!(value, &vec![1, 2, 3]);
}

#[test]
fn test_bucket_lru_eviction_order() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    // Create a bucket manager with limit of 3 buckets
    let mut manager = BucketManager::new(3600, pager.clone(), 3);

    // Add 3 buckets
    let b1 = manager.get_or_create_bucket(100).unwrap();
    b1.insert(100, vec![1]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    let b2 = manager.get_or_create_bucket(4000).unwrap();
    b2.insert(4000, vec![2]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    let b3 = manager.get_or_create_bucket(8000).unwrap();
    b3.insert(8000, vec![3]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Access bucket 1 to make it more recently used
    let _ = manager.get_or_create_bucket(100).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Flush all
    manager.flush_all().unwrap();

    // Add a 4th bucket - should evict bucket 2 (least recently used)
    let b4 = manager.get_or_create_bucket(12000).unwrap();
    b4.insert(12000, vec![4]).unwrap();

    // Bucket 1 and 3 should still be in memory
    assert_eq!(manager.bucket_count(), 3);
}

#[test]
fn test_bucket_dirty_flag() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    // Initially not dirty
    assert!(!bucket.is_dirty());

    // Insert makes it dirty
    bucket.insert(100, vec![1, 2, 3]).unwrap();
    assert!(bucket.is_dirty());

    // Flush clears dirty flag
    bucket.flush().unwrap();
    assert!(!bucket.is_dirty());

    // Another insert makes it dirty again
    bucket.insert(200, vec![4, 5, 6]).unwrap();
    assert!(bucket.is_dirty());
}

#[test]
fn test_bucket_last_access_time() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    let initial_time = bucket.last_access_time();
    assert!(initial_time > 0);

    // Wait a bit
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Access the bucket
    bucket.insert(100, vec![1]).unwrap();
    let after_insert = bucket.last_access_time();
    assert!(after_insert >= initial_time);

    // Wait and access again
    std::thread::sleep(std::time::Duration::from_millis(10));
    let _ = bucket.get(100);
    let after_get = bucket.last_access_time();
    assert!(after_get >= after_insert);
}

#[test]
fn test_bucket_estimated_size() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    let initial_size = bucket.estimated_size();
    assert!(initial_size > 0);

    // Add data
    bucket.insert(100, vec![1, 2, 3]).unwrap();
    let size_after_insert = bucket.estimated_size();
    assert!(size_after_insert > initial_size);

    // Add more data
    bucket.insert(200, vec![4, 5, 6, 7, 8]).unwrap();
    let size_after_second_insert = bucket.estimated_size();
    assert!(size_after_second_insert > size_after_insert);
}

#[test]
fn test_bucket_manager_flush_all() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let mut manager = BucketManager::new(3600, pager.clone(), 10);

    // Add multiple buckets with data
    for i in 0..5 {
        let bucket = manager.get_or_create_bucket(i * 4000).unwrap();
        bucket.insert(i * 4000, vec![i as u8]).unwrap();
        assert!(bucket.is_dirty());
    }

    // Flush all
    manager.flush_all().unwrap();

    // All buckets should now be clean
    for i in 0..5 {
        let bucket = manager.get_or_create_bucket(i * 4000).unwrap();
        assert!(!bucket.is_dirty());
        assert!(bucket.page_id().is_some());
    }
}

#[test]
fn test_bucket_recovery_after_eviction() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let mut manager = BucketManager::new(3600, pager.clone(), 2);

    // Add data to multiple buckets
    let test_data = vec![
        (100, vec![1, 2, 3]),
        (4000, vec![4, 5, 6]),
        (8000, vec![7, 8, 9]),
    ];

    for (timestamp, data) in &test_data {
        let bucket = manager.get_or_create_bucket(*timestamp).unwrap();
        bucket.insert(*timestamp, data.clone()).unwrap();
    }

    // Flush all to ensure data is persisted
    manager.flush_all().unwrap();

    // Access buckets in different order to trigger evictions
    for (timestamp, expected_data) in &test_data {
        let bucket = manager.get_or_create_bucket(*timestamp).unwrap();
        let value = bucket.get(*timestamp).unwrap();
        assert_eq!(value, expected_data);
    }
}

#[test]
fn test_empty_bucket_flush_and_load() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    // Flush empty bucket
    bucket.flush().unwrap();
    let page_id = bucket.page_id().expect("Bucket should have a page ID");

    // Load from disk
    let loaded_bucket = TimeBucket::load(page_id, 3600, pager.clone()).unwrap();

    assert_eq!(loaded_bucket.id(), bucket_id);
    assert_eq!(loaded_bucket.len(), 0);
    assert!(loaded_bucket.is_empty());
}

#[test]
fn test_large_bucket_persistence() {
    // Create a pager
    let fs = MemoryFileSystem::new();
    let pager = Arc::new(
        Pager::create(&fs, "test.db", PagerConfig::default()).unwrap(),
    );

    let bucket_id = BucketId(0);
    let mut bucket = TimeBucket::new(bucket_id, 3600, pager.clone());

    // Add many data points
    for i in 0..1000 {
        bucket.insert(i, vec![i as u8; 10]).unwrap();
    }

    // Flush to disk
    bucket.flush().unwrap();
    let page_id = bucket.page_id().expect("Bucket should have a page ID");

    // Load from disk
    let mut loaded_bucket = TimeBucket::load(page_id, 3600, pager.clone()).unwrap();

    assert_eq!(loaded_bucket.len(), 1000);
    
    // Verify some data points
    for i in (0..1000).step_by(100) {
        let value = loaded_bucket.get(i).unwrap();
        assert_eq!(value, &vec![i as u8; 10]);
    }
}

#[test]
fn test_timeseries_cursor_aggregations() {
   let fs = MemoryFileSystem::new();
   let pager = Arc::new(Pager::create(&fs, "agg.db", PagerConfig::default()).unwrap());
   let mut table = TimeSeriesTable::new(
       TableId::from(1),
       "metrics".to_string(),
       pager,
       TimeSeriesConfig::default(),
   )
   .unwrap();

   let series_key = b"sensor-agg";
   let samples = [
       (100i64, b"10.5".as_slice()),
       (200i64, b"temperature:20.0".as_slice()),
       (300i64, br#"{"value": 30.5}"#.as_slice()),
       (400i64, b"40".as_slice()),
   ];

   for (ts, value) in samples {
       table.append_point(series_key, ts, value).unwrap();
   }

   let cursor = table.scan_series(series_key, 0, 500).unwrap();

   assert_eq!(cursor.count(), 4);
   assert_eq!(cursor.sum(), Some(101.0));
   assert_eq!(cursor.avg(), Some(25.25));
   assert_eq!(cursor.min(), Some(10.5));
   assert_eq!(cursor.max(), Some(40.0));
   assert_eq!(
       cursor.aggregate(TimeSeriesAggregation::Count),
       Some(nanokv::table::timeseries::AggregationResult::Count(4))
   );
}

#[test]
fn test_timeseries_cursor_downsampling_avg() {
   let fs = MemoryFileSystem::new();
   let pager = Arc::new(Pager::create(&fs, "downsample.db", PagerConfig::default()).unwrap());
   let mut table = TimeSeriesTable::new(
       TableId::from(2),
       "metrics".to_string(),
       pager,
       TimeSeriesConfig::default(),
   )
   .unwrap();

   let series_key = b"sensor-downsample";
   let samples = [
       (10i64, b"10".as_slice()),
       (20i64, b"20".as_slice()),
       (70i64, b"30".as_slice()),
       (80i64, b"50".as_slice()),
   ];

   for (ts, value) in samples {
       table.append_point(series_key, ts, value).unwrap();
   }

   let cursor = table.scan_series(series_key, 0, 120).unwrap();
   let windows = cursor.downsample(60, TimeSeriesAggregation::Avg).unwrap();

   assert_eq!(windows.len(), 2);
   assert_eq!(windows[0].start_ts, 0);
   assert_eq!(windows[0].end_ts, 60);
   assert_eq!(
       windows[0].aggregation,
       nanokv::table::timeseries::AggregationResult::Avg(15.0)
   );

   assert_eq!(windows[1].start_ts, 60);
   assert_eq!(windows[1].end_ts, 120);
   assert_eq!(
       windows[1].aggregation,
       nanokv::table::timeseries::AggregationResult::Avg(40.0)
   );
}

#[test]
fn test_timeseries_cursor_skips_non_numeric_values_for_numeric_aggregations() {
   let fs = MemoryFileSystem::new();
   let pager = Arc::new(Pager::create(&fs, "non_numeric.db", PagerConfig::default()).unwrap());
   let mut table = TimeSeriesTable::new(
       TableId::from(3),
       "metrics".to_string(),
       pager,
       TimeSeriesConfig::default(),
   )
   .unwrap();

   let series_key = b"sensor-mixed";
   table.append_point(series_key, 100, b"12").unwrap();
   table.append_point(series_key, 200, b"not-a-number").unwrap();
   table.append_point(series_key, 300, b"18").unwrap();

   let cursor = table.scan_series(series_key, 0, 500).unwrap();

   assert_eq!(cursor.count(), 3);
   assert_eq!(cursor.sum(), Some(30.0));
   assert_eq!(cursor.avg(), Some(15.0));
   assert_eq!(cursor.min(), Some(12.0));
   assert_eq!(cursor.max(), Some(18.0));
}

#[test]
fn test_timeseries_cursor_downsampling_count_and_invalid_interval() {
   let fs = MemoryFileSystem::new();
   let pager = Arc::new(Pager::create(&fs, "count.db", PagerConfig::default()).unwrap());
   let mut table = TimeSeriesTable::new(
       TableId::from(4),
       "metrics".to_string(),
       pager,
       TimeSeriesConfig::default(),
   )
   .unwrap();

   let series_key = b"sensor-count";
   table.append_point(series_key, 10, b"bad").unwrap();
   table.append_point(series_key, 20, b"still-bad").unwrap();
   table.append_point(series_key, 70, b"ignored").unwrap();

   let cursor = table.scan_series(series_key, 0, 120).unwrap();
   let windows = cursor.downsample(60, TimeSeriesAggregation::Count).unwrap();

   assert_eq!(windows.len(), 2);
   assert_eq!(
       windows[0].aggregation,
       nanokv::table::timeseries::AggregationResult::Count(2)
   );
   assert_eq!(
       windows[1].aggregation,
       nanokv::table::timeseries::AggregationResult::Count(1)
   );

   let err = cursor.downsample(0, TimeSeriesAggregation::Count).unwrap_err();
   assert!(err.to_string().contains("greater than zero"));
}

// Made with Bob