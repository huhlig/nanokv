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

//! In-memory blob storage implementation.
//!
//! This module provides a memory-resident blob storage implementation optimized for:
//! - Temporary blob storage and intermediate results
//! - Fast in-memory operations without disk I/O
//! - Testing and development
//! - Small to medium-sized blobs that fit in memory
//!
//! The implementation uses a HashMap to store blobs by key, with memory tracking
//! and optional size limits.

use crate::table::{
    Table, TableCapabilities, TableEngineKind, TableResult, TableStatistics,
};
use crate::types::{ObjectId, ValueBuf};
use crate::wal::LogSequenceNumber;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Default memory budget for in-memory blob storage (64MB).
const DEFAULT_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// Default maximum blob size (16MB).
const DEFAULT_MAX_BLOB_SIZE: u64 = 16 * 1024 * 1024;

/// Default inline threshold (4KB - blobs smaller than this should be stored inline).
const DEFAULT_INLINE_THRESHOLD: usize = 4 * 1024;

/// In-memory blob storage table.
///
/// Stores blobs in a HashMap with memory tracking and size limits.
/// Suitable for ephemeral blob storage that doesn't need persistence.
pub struct MemoryBlob {
    id: ObjectId,
    name: String,
    /// Shared blob data protected by RwLock for concurrent reads
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Memory usage tracking
    memory_usage: Arc<RwLock<usize>>,
    /// Memory budget in bytes
    memory_budget: usize,
    /// Maximum blob size
    max_blob_size: u64,
    /// Inline threshold
    inline_threshold: usize,
}

impl MemoryBlob {
    /// Create a new in-memory blob storage table.
    pub fn new(id: ObjectId, name: String) -> Self {
        Self::with_config(
            id,
            name,
            DEFAULT_MEMORY_BUDGET,
            DEFAULT_MAX_BLOB_SIZE,
            DEFAULT_INLINE_THRESHOLD,
        )
    }

    /// Create a new in-memory blob storage table with custom configuration.
    pub fn with_config(
        id: ObjectId,
        name: String,
        memory_budget: usize,
        max_blob_size: u64,
        inline_threshold: usize,
    ) -> Self {
        Self {
            id,
            name,
            data: Arc::new(RwLock::new(HashMap::new())),
            memory_usage: Arc::new(RwLock::new(0)),
            memory_budget,
            max_blob_size,
            inline_threshold,
        }
    }

    /// Get current memory usage.
    fn get_memory_usage(&self) -> usize {
        *self.memory_usage.read().unwrap()
    }

    /// Update memory usage by delta (can be negative).
    fn update_memory_usage(&self, delta: isize) {
        let mut usage = self.memory_usage.write().unwrap();
        if delta < 0 {
            *usage = usage.saturating_sub(delta.unsigned_abs());
        } else {
            *usage = usage.saturating_add(delta as usize);
        }
    }

    /// Estimate memory usage of a key-value pair.
    fn estimate_entry_size(key: &[u8], value: &[u8]) -> usize {
        key.len() + value.len() + std::mem::size_of::<Vec<u8>>() * 2
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> TableResult<Option<ValueBuf>> {
        let store = self.data.read().unwrap();
        Ok(store.get(key).map(|v| ValueBuf(v.clone())))
    }

    /// Put a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> TableResult<u64> {
        // Check value size limit
        if value.len() as u64 > self.max_blob_size {
            return Err(crate::table::TableError::Other(format!(
                "Value size {} exceeds maximum {}",
                value.len(),
                self.max_blob_size
            )));
        }

        let mut store = self.data.write().unwrap();
        
        // Calculate memory delta
        let new_size = Self::estimate_entry_size(key, value);
        let old_size = store
            .get(key)
            .map(|v| Self::estimate_entry_size(key, v))
            .unwrap_or(0);
        let delta = new_size as isize - old_size as isize;

        // Check memory budget
        let new_usage = (self.get_memory_usage() as isize + delta) as usize;
        if new_usage > self.memory_budget {
            return Err(crate::table::TableError::Other(format!(
                "Memory budget exceeded: {} > {}",
                new_usage, self.memory_budget
            )));
        }

        // Store the value
        store.insert(key.to_vec(), value.to_vec());
        self.update_memory_usage(delta);

        Ok(value.len() as u64 + key.len() as u64 + 16) // +16 for overhead
    }

    /// Delete a key.
    pub fn delete(&self, key: &[u8]) -> TableResult<bool> {
        let mut store = self.data.write().unwrap();
        if let Some(value) = store.remove(key) {
            let size = Self::estimate_entry_size(key, &value);
            self.update_memory_usage(-(size as isize));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the maximum inline size.
    pub fn max_inline_size(&self) -> usize {
        self.inline_threshold
    }

    /// Get the maximum value size.
    pub fn max_value_size(&self) -> u64 {
        self.max_blob_size
    }
}

impl Table for MemoryBlob {
    fn table_id(&self) -> ObjectId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> TableEngineKind {
        TableEngineKind::Blob
    }

    fn capabilities(&self) -> TableCapabilities {
        TableCapabilities {
            ordered: false,
            point_lookup: true,
            prefix_scan: false,
            reverse_scan: false,
            range_delete: false,
            merge_operator: false,
            mvcc_native: false,
            append_optimized: false,
            memory_resident: true,
            disk_resident: false,
            supports_compression: false,
            supports_encryption: false,
        }
    }

    fn stats(&self) -> TableResult<TableStatistics> {
        let data = self.data.read().unwrap();
        let memory_usage = self.get_memory_usage();

        Ok(TableStatistics {
            row_count: Some(data.len() as u64),
            total_size_bytes: Some(memory_usage as u64),
            key_stats: None,
            value_stats: None,
            histogram: None,
            last_updated_lsn: Some(LogSequenceNumber::default()),
        })
    }
}

// Made with Bob
