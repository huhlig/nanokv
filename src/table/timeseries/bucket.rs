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

//! Time-based bucketing for time series data.
//!
//! This module implements time-based bucketing to organize time series data
//! into manageable chunks. Each bucket contains data points within a specific
//! time range, enabling efficient range queries and data management.

use crate::pager::{Page, PageId, PageType, Pager};
use crate::table::TableResult;
use crate::vfs::FileSystem;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a time bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketId(pub u64);

impl BucketId {
    /// Create a new bucket ID from a timestamp and bucket size.
    pub fn from_timestamp(timestamp: i64, bucket_size: u64) -> Self {
        // Ensure we handle negative timestamps correctly
        let bucket_num = if timestamp >= 0 {
            (timestamp as u64) / bucket_size
        } else {
            // For negative timestamps, round down to the bucket
            let abs_ts = (-timestamp) as u64;
            let bucket_offset = abs_ts.div_ceil(bucket_size);
            u64::MAX - bucket_offset + 1
        };
        BucketId(bucket_num)
    }

    /// Get the start timestamp for this bucket.
    pub fn start_timestamp(&self, bucket_size: u64) -> i64 {
        if self.0 > (i64::MAX as u64) / bucket_size {
            // Handle overflow for very large bucket IDs
            i64::MAX
        } else {
            (self.0 * bucket_size) as i64
        }
    }

    /// Get the end timestamp for this bucket (exclusive).
    pub fn end_timestamp(&self, bucket_size: u64) -> i64 {
        self.start_timestamp(bucket_size)
            .saturating_add(bucket_size as i64)
    }
}

/// A time bucket containing data points for a specific time range.
pub struct TimeBucket<FS: FileSystem> {
    /// Bucket identifier
    id: BucketId,

    /// Start timestamp (inclusive)
    start_ts: i64,

    /// End timestamp (exclusive)
    end_ts: i64,

    /// Data points in this bucket: (timestamp, value_key)
    points: BTreeMap<i64, Vec<u8>>,

    /// Page ID where this bucket is stored (if persisted)
    page_id: Option<PageId>,

    /// Pager for persistent storage
    pager: std::sync::Arc<Pager<FS>>,

    /// Whether the bucket has been modified since last flush
    dirty: bool,

    /// Last access time (for LRU eviction)
    last_access_time: u64,
}

impl<FS: FileSystem> TimeBucket<FS> {
    /// Create a new time bucket.
    pub fn new(
        id: BucketId,
        bucket_size: u64,
        pager: std::sync::Arc<Pager<FS>>,
    ) -> Self {
        let start_ts = id.start_timestamp(bucket_size);
        let end_ts = id.end_timestamp(bucket_size);

        Self {
            id,
            start_ts,
            end_ts,
            points: BTreeMap::new(),
            page_id: None,
            pager,
            dirty: false,
            last_access_time: Self::current_timestamp(),
        }
    }

    /// Get current timestamp in seconds since UNIX epoch.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Update the last access time.
    fn touch(&mut self) {
        self.last_access_time = Self::current_timestamp();
    }

    /// Get the bucket ID.
    pub fn id(&self) -> BucketId {
        self.id
    }

    /// Get the start timestamp.
    pub fn start_timestamp(&self) -> i64 {
        self.start_ts
    }

    /// Get the end timestamp.
    pub fn end_timestamp(&self) -> i64 {
        self.end_ts
    }

    /// Check if a timestamp falls within this bucket.
    pub fn contains_timestamp(&self, timestamp: i64) -> bool {
        timestamp >= self.start_ts && timestamp < self.end_ts
    }

    /// Insert a data point into the bucket.
    pub fn insert(&mut self, timestamp: i64, value_key: Vec<u8>) -> TableResult<()> {
        if !self.contains_timestamp(timestamp) {
            return Err(crate::table::TableError::Other(format!(
                "Timestamp {} is outside bucket range [{}, {})",
                timestamp, self.start_ts, self.end_ts
            )));
        }

        self.points.insert(timestamp, value_key);
        self.dirty = true;
        Ok(())
    }

    /// Get a data point by timestamp.
    pub fn get(&mut self, timestamp: i64) -> Option<&Vec<u8>> {
        self.touch();
        self.points.get(&timestamp)
    }

    /// Get the number of points in this bucket.
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if the bucket is empty.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Check if the bucket is dirty (modified since last flush).
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Get an iterator over points in the specified time range.
    pub fn range(&self, start: i64, end: i64) -> impl Iterator<Item = (&i64, &Vec<u8>)> {
        self.points.range(start..end)
    }

    /// Get all points in the bucket.
    pub fn iter(&self) -> impl Iterator<Item = (&i64, &Vec<u8>)> {
        self.points.iter()
    }

    /// Find the latest point before or at the given timestamp.
    pub fn latest_before(&self, timestamp: i64) -> Option<(&i64, &Vec<u8>)> {
        self.points.range(..=timestamp).next_back()
    }

    /// Serialize the bucket to bytes.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write bucket ID (8 bytes)
        bytes.extend_from_slice(&self.id.0.to_le_bytes());

        // Write start timestamp (8 bytes)
        bytes.extend_from_slice(&self.start_ts.to_le_bytes());

        // Write end timestamp (8 bytes)
        bytes.extend_from_slice(&self.end_ts.to_le_bytes());

        // Write last access time (8 bytes)
        bytes.extend_from_slice(&self.last_access_time.to_le_bytes());

        // Write number of points (4 bytes)
        bytes.extend_from_slice(&(self.points.len() as u32).to_le_bytes());

        // Write each point: timestamp (8 bytes) + value_key_len (4 bytes) + value_key
        for (timestamp, value_key) in &self.points {
            bytes.extend_from_slice(&timestamp.to_le_bytes());
            bytes.extend_from_slice(&(value_key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value_key);
        }

        bytes
    }

    /// Deserialize a bucket from bytes.
    pub(crate) fn from_bytes(
        data: &[u8],
        _bucket_size: u64,
        pager: std::sync::Arc<Pager<FS>>,
    ) -> TableResult<Self> {
        let mut pos = 0;

        // Read bucket ID
        if data.len() < pos + 8 {
            return Err(crate::table::TableError::Other(
                "Insufficient data for bucket ID".to_string(),
            ));
        }
        let bucket_id = BucketId(u64::from_le_bytes(
            data[pos..pos + 8].try_into().unwrap(),
        ));
        pos += 8;

        // Read start timestamp
        if data.len() < pos + 8 {
            return Err(crate::table::TableError::Other(
                "Insufficient data for start timestamp".to_string(),
            ));
        }
        let start_ts = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Read end timestamp
        if data.len() < pos + 8 {
            return Err(crate::table::TableError::Other(
                "Insufficient data for end timestamp".to_string(),
            ));
        }
        let end_ts = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Read last access time
        if data.len() < pos + 8 {
            return Err(crate::table::TableError::Other(
                "Insufficient data for last access time".to_string(),
            ));
        }
        let last_access_time = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Read number of points
        if data.len() < pos + 4 {
            return Err(crate::table::TableError::Other(
                "Insufficient data for point count".to_string(),
            ));
        }
        let point_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // Read points
        let mut points = BTreeMap::new();
        for _ in 0..point_count {
            // Read timestamp
            if data.len() < pos + 8 {
                return Err(crate::table::TableError::Other(
                    "Insufficient data for point timestamp".to_string(),
                ));
            }
            let timestamp = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            // Read value_key length
            if data.len() < pos + 4 {
                return Err(crate::table::TableError::Other(
                    "Insufficient data for value_key length".to_string(),
                ));
            }
            let value_key_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            // Read value_key
            if data.len() < pos + value_key_len {
                return Err(crate::table::TableError::Other(
                    "Insufficient data for value_key".to_string(),
                ));
            }
            let value_key = data[pos..pos + value_key_len].to_vec();
            pos += value_key_len;

            points.insert(timestamp, value_key);
        }

        Ok(Self {
            id: bucket_id,
            start_ts,
            end_ts,
            points,
            page_id: None,
            pager,
            dirty: false,
            last_access_time,
        })
    }

    /// Flush the bucket to disk.
    pub fn flush(&mut self) -> TableResult<()> {
        // Always flush to ensure we have a page ID, even for empty buckets
        // Serialize the bucket
        let bucket_data = self.to_bytes();

        // Allocate or reuse a page
        let page_id = if let Some(existing_page_id) = self.page_id {
            existing_page_id
        } else {
            // Allocate a new page
            let new_page_id = self
                .pager
                .allocate_page(PageType::BTreeLeaf)
                .map_err(|e| crate::table::TableError::Other(format!("Failed to allocate page: {}", e)))?;
            self.page_id = Some(new_page_id);
            new_page_id
        };

        // Create a page with the bucket data
        let page_size = self.pager.page_size();
        let mut page = Page::new(page_id, PageType::BTreeLeaf, page_size.data_size());
        page.data_mut().extend_from_slice(&bucket_data);

        // Write the page to disk
        self.pager
            .write_page(&page)
            .map_err(|e| crate::table::TableError::Other(format!("Failed to write page: {}", e)))?;

        self.dirty = false;
        Ok(())
    }

    /// Load a bucket from disk.
    pub fn load(
        page_id: PageId,
        bucket_size: u64,
        pager: std::sync::Arc<Pager<FS>>,
    ) -> TableResult<Self> {
        // Read the page from disk
        let page = pager
            .read_page(page_id)
            .map_err(|e| crate::table::TableError::Other(format!("Failed to read page: {}", e)))?;

        // Deserialize the bucket
        let mut bucket = Self::from_bytes(page.data(), bucket_size, pager)?;
        bucket.page_id = Some(page_id);
        bucket.touch();

        Ok(bucket)
    }

    /// Get the page ID where this bucket is stored.
    pub fn page_id(&self) -> Option<PageId> {
        self.page_id
    }

    /// Set the page ID where this bucket is stored.
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = Some(page_id);
    }

    /// Get the last access time.
    pub fn last_access_time(&self) -> u64 {
        self.last_access_time
    }

    /// Get the estimated size in bytes.
    pub fn estimated_size(&self) -> usize {
        let mut size = 32; // Fixed overhead (id, timestamps, metadata)
        for value_key in self.points.values() {
            size += 8 + 4 + value_key.len(); // timestamp + length + value_key
        }
        size
    }
}

/// Manager for time buckets.
pub struct BucketManager<FS: FileSystem> {
    /// Bucket size in seconds
    bucket_size: u64,

    /// Active buckets in memory
    pub(crate) buckets: BTreeMap<BucketId, TimeBucket<FS>>,

    /// Pager for persistent storage
    pager: std::sync::Arc<Pager<FS>>,

    /// Maximum number of buckets to keep in memory
    max_buckets_in_memory: usize,

    /// Mapping of bucket IDs to their page IDs (for lazy loading)
    bucket_page_map: BTreeMap<BucketId, PageId>,
}

impl<FS: FileSystem> BucketManager<FS> {
    /// Create a new bucket manager.
    pub fn new(
        bucket_size: u64,
        pager: std::sync::Arc<Pager<FS>>,
        max_buckets_in_memory: usize,
    ) -> Self {
        Self {
            bucket_size,
            buckets: BTreeMap::new(),
            pager,
            max_buckets_in_memory,
            bucket_page_map: BTreeMap::new(),
        }
    }

    /// Get or create a bucket for the given timestamp.
    pub fn get_or_create_bucket(&mut self, timestamp: i64) -> TableResult<&mut TimeBucket<FS>> {
        let bucket_id = BucketId::from_timestamp(timestamp, self.bucket_size);

        // Check if bucket is already in memory
        if self.buckets.contains_key(&bucket_id) {
            let bucket = self.buckets.get_mut(&bucket_id).unwrap();
            bucket.touch();
            return Ok(bucket);
        }

        // Check if we need to evict old buckets before loading/creating
        if self.buckets.len() >= self.max_buckets_in_memory {
            self.evict_lru_bucket()?;
        }

        // Try to load from disk if we have a page ID for this bucket
        if let Some(&page_id) = self.bucket_page_map.get(&bucket_id) {
            let bucket = TimeBucket::load(page_id, self.bucket_size, self.pager.clone())?;
            self.buckets.insert(bucket_id, bucket);
        } else {
            // Create a new bucket
            let bucket = TimeBucket::new(bucket_id, self.bucket_size, self.pager.clone());
            self.buckets.insert(bucket_id, bucket);
        }

        Ok(self.buckets.get_mut(&bucket_id).unwrap())
    }

    /// Get a bucket by ID.
    pub fn get_bucket(&self, bucket_id: BucketId) -> Option<&TimeBucket<FS>> {
        self.buckets.get(&bucket_id)
    }

    /// Get a mutable bucket by ID.
    pub fn get_bucket_mut(&mut self, bucket_id: BucketId) -> Option<&mut TimeBucket<FS>> {
        self.buckets.get_mut(&bucket_id)
    }

    /// Get all bucket IDs that overlap with the given time range.
    pub fn get_bucket_ids_in_range(&self, start_ts: i64, end_ts: i64) -> Vec<BucketId> {
        let start_bucket = BucketId::from_timestamp(start_ts, self.bucket_size);
        let end_bucket = BucketId::from_timestamp(end_ts, self.bucket_size);

        self.buckets
            .range(start_bucket..=end_bucket)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Evict the least recently used bucket from memory.
    fn evict_lru_bucket(&mut self) -> TableResult<()> {
        // Find the bucket with the oldest access time
        let lru_bucket_id = self
            .buckets
            .iter()
            .min_by_key(|(_, bucket)| bucket.last_access_time())
            .map(|(id, _)| *id);

        if let Some(bucket_id) = lru_bucket_id
            && let Some(mut bucket) = self.buckets.remove(&bucket_id) {
                // Flush if dirty
                if bucket.is_dirty() {
                    bucket.flush()?;
                }
                
                // Store the page ID mapping for lazy loading
                if let Some(page_id) = bucket.page_id() {
                    self.bucket_page_map.insert(bucket_id, page_id);
                }
            }
        Ok(())
    }

    /// Flush all dirty buckets to disk.
    pub fn flush_all(&mut self) -> TableResult<()> {
        for (bucket_id, bucket) in self.buckets.iter_mut() {
            if bucket.is_dirty() {
                bucket.flush()?;
                // Update page mapping
                if let Some(page_id) = bucket.page_id() {
                    self.bucket_page_map.insert(*bucket_id, page_id);
                }
            }
        }
        Ok(())
    }

    /// Get the number of buckets in memory.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Get the total number of tracked buckets (in memory + on disk).
    pub fn total_bucket_count(&self) -> usize {
        self.bucket_page_map.len()
    }

    /// Register a bucket's page ID for lazy loading.
    pub fn register_bucket_page(&mut self, bucket_id: BucketId, page_id: PageId) {
        self.bucket_page_map.insert(bucket_id, page_id);
    }

    /// Get the page ID for a bucket if it exists.
    pub fn get_bucket_page_id(&self, bucket_id: BucketId) -> Option<PageId> {
        self.bucket_page_map.get(&bucket_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_id_from_timestamp() {
        let bucket_size = 3600; // 1 hour

        // Test positive timestamps
        let id1 = BucketId::from_timestamp(0, bucket_size);
        assert_eq!(id1.0, 0);

        let id2 = BucketId::from_timestamp(3600, bucket_size);
        assert_eq!(id2.0, 1);

        let id3 = BucketId::from_timestamp(7200, bucket_size);
        assert_eq!(id3.0, 2);

        // Test timestamp within bucket
        let id4 = BucketId::from_timestamp(1800, bucket_size);
        assert_eq!(id4.0, 0);
    }

    #[test]
    fn test_bucket_timestamps() {
        let bucket_size = 3600;
        let id = BucketId(5);

        let start = id.start_timestamp(bucket_size);
        let end = id.end_timestamp(bucket_size);

        assert_eq!(start, 18000);
        assert_eq!(end, 21600);
    }

    #[test]
    fn test_bucket_contains_timestamp() {
        let bucket_size = 3600;
        let fs = crate::vfs::MemoryFileSystem::new();
        let pager = std::sync::Arc::new(
            Pager::create(
                &fs,
                "test.db",
                crate::pager::PagerConfig::default(),
            )
            .unwrap(),
        );

        let bucket = TimeBucket::new(BucketId(0), bucket_size, pager);

        assert!(bucket.contains_timestamp(0));
        assert!(bucket.contains_timestamp(1800));
        assert!(bucket.contains_timestamp(3599));
        assert!(!bucket.contains_timestamp(3600));
        assert!(!bucket.contains_timestamp(-1));
    }
}

// Made with Bob