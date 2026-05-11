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
    BatchOps, BatchReport, BlobTable, Flushable, MutableTable, OrderedScan, PointLookup,
    SpecialtyTableCapabilities, SpecialtyTableStats, Table, TableCapabilities, TableCursor,
    TableEngineKind, TableReader, TableResult, TableStatistics, TableWriter,
    VerificationReport, WriteBatch,
};
use crate::txn::TransactionId;
use crate::types::{KeyBuf, ObjectId, ScanBounds, ValueBuf};
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
}

impl Table for MemoryBlob {
    type Reader<'a> = MemoryBlobReader<'a>;
    type Writer<'a> = MemoryBlobWriter<'a>;

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

    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>> {
        Ok(MemoryBlobReader {
            table: self,
            snapshot_lsn,
        })
    }

    fn writer(
        &self,
        tx_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Writer<'_>> {
        Ok(MemoryBlobWriter {
            table: self,
            tx_id,
            snapshot_lsn,
        })
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

impl BlobTable for MemoryBlob {
    fn table_id(&self) -> ObjectId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> SpecialtyTableCapabilities {
        SpecialtyTableCapabilities {
            exact: true,
            approximate: false,
            ordered: false,
            sparse: false,
            supports_delete: true,
            supports_range_query: false,
            supports_prefix_query: false,
            supports_scoring: false,
            supports_incremental_rebuild: false,
            may_be_stale: false,
        }
    }

    fn put_blob(&mut self, key: &[u8], data: &[u8]) -> TableResult<u64> {
        // Check blob size limit
        if data.len() as u64 > self.max_blob_size {
            return Err(crate::table::TableError::Other(format!(
                "Blob size {} exceeds maximum {}",
                data.len(),
                self.max_blob_size
            )));
        }

        let mut store = self.data.write().unwrap();
        
        // Calculate memory delta
        let new_size = Self::estimate_entry_size(key, data);
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

        // Store the blob
        store.insert(key.to_vec(), data.to_vec());
        self.update_memory_usage(delta);

        Ok(data.len() as u64)
    }

    fn get_blob(&self, key: &[u8]) -> TableResult<Option<ValueBuf>> {
        let store = self.data.read().unwrap();
        Ok(store.get(key).map(|v| ValueBuf(v.clone())))
    }

    fn delete_blob(&mut self, key: &[u8]) -> TableResult<bool> {
        let mut store = self.data.write().unwrap();
        if let Some(value) = store.remove(key) {
            let size = Self::estimate_entry_size(key, &value);
            self.update_memory_usage(-(size as isize));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn blob_size(&self, key: &[u8]) -> TableResult<Option<u64>> {
        let store = self.data.read().unwrap();
        Ok(store.get(key).map(|v| v.len() as u64))
    }

    fn max_inline_size(&self) -> usize {
        self.inline_threshold
    }

    fn max_blob_size(&self) -> u64 {
        self.max_blob_size
    }

    fn list_keys(&self) -> TableResult<Vec<KeyBuf>> {
        let store = self.data.read().unwrap();
        Ok(store.keys().map(|k| KeyBuf(k.clone())).collect())
    }

    fn stats(&self) -> TableResult<SpecialtyTableStats> {
        let data = self.data.read().unwrap();
        Ok(SpecialtyTableStats {
            entry_count: Some(data.len() as u64),
            size_bytes: Some(self.get_memory_usage() as u64),
            distinct_keys: Some(data.len() as u64),
            stale_entries: Some(0),
            last_updated_lsn: Some(LogSequenceNumber::default()),
        })
    }

    fn verify(&self) -> TableResult<VerificationReport> {
        // Memory blob storage is always consistent
        Ok(VerificationReport {
            checked_items: self.data.read().unwrap().len() as u64,
            errors: vec![],
            warnings: vec![],
        })
    }
}

/// Reader for in-memory blob storage.
pub struct MemoryBlobReader<'a> {
    table: &'a MemoryBlob,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a> TableReader for MemoryBlobReader<'a> {
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    fn approximate_len(&self) -> TableResult<Option<u64>> {
        Ok(Some(self.table.data.read().unwrap().len() as u64))
    }
}

// Stub implementations to satisfy Table trait requirements
// TODO: Remove these when Table trait is refactored (see nanokv-vrq)

impl<'a> PointLookup for MemoryBlobReader<'a> {
    fn get(&self, _key: &[u8], _snapshot_lsn: crate::wal::LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support point lookup - use BlobTable::get_blob instead".to_string(),
        ))
    }
}

impl<'a> crate::table::OrderedScan for MemoryBlobReader<'a> {
    type Cursor<'b> = MemoryBlobCursor where Self: 'b;

    fn scan(&self, _bounds: crate::types::ScanBounds, _snapshot_lsn: crate::wal::LogSequenceNumber) -> TableResult<Self::Cursor<'_>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support ordered scans".to_string(),
        ))
    }
}

/// Stub cursor for blob tables (not actually used).
pub struct MemoryBlobCursor;

impl crate::table::TableCursor for MemoryBlobCursor {
    fn valid(&self) -> bool {
        false
    }

    fn key(&self) -> Option<&[u8]> {
        None
    }

    fn value(&self) -> Option<&[u8]> {
        None
    }

    fn next(&mut self) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn prev(&mut self) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn seek(&mut self, _key: &[u8]) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn first(&mut self) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn last(&mut self) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support cursors".to_string(),
        ))
    }

    fn snapshot_lsn(&self) -> crate::wal::LogSequenceNumber {
        crate::wal::LogSequenceNumber::default()
    }
}

/// Writer for in-memory blob storage.
pub struct MemoryBlobWriter<'a> {
    table: &'a MemoryBlob,
    tx_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a> TableWriter for MemoryBlobWriter<'a> {
    fn tx_id(&self) -> TransactionId {
        self.tx_id
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}

impl<'a> crate::table::MutableTable for MemoryBlobWriter<'a> {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support put - use BlobTable::put_blob instead".to_string(),
        ))
    }

    fn delete(&mut self, _key: &[u8]) -> TableResult<bool> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support delete - use BlobTable::delete_blob instead".to_string(),
        ))
    }

    fn range_delete(&mut self, _bounds: crate::types::ScanBounds) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support range delete".to_string(),
        ))
    }
}

impl<'a> crate::table::BatchOps for MemoryBlobWriter<'a> {
    fn batch_get(&self, _keys: &[&[u8]]) -> TableResult<Vec<Option<ValueBuf>>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support batch operations".to_string(),
        ))
    }

    fn apply_batch(&mut self, _batch: crate::table::WriteBatch) -> TableResult<crate::table::BatchReport> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support batch operations".to_string(),
        ))
    }
}

impl<'a> crate::table::Flushable for MemoryBlobWriter<'a> {
    fn flush(&mut self) -> TableResult<()> {
        // Blob tables are in-memory, no flush needed
        Ok(())
    }
}

// Made with Bob
