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

//! File-based blob storage implementation.
//!
//! This module provides a direct file-based blob storage implementation:
//! - Each blob is stored as a separate file on disk
//! - Suitable for very large blobs (GB+ sizes)
//! - Bypasses the page cache for direct I/O
//! - Can leverage filesystem features (compression, deduplication, etc.)
//!
//! The implementation uses a directory structure to organize blob files,
//! with a B-Tree index mapping keys to file paths.

use crate::table::{
    BatchOps, BatchReport, BlobTable, Flushable, MutableTable, OrderedScan, PointLookup,
    SpecialtyTableCapabilities, SpecialtyTableStats, Table, TableCapabilities, TableCursor,
    TableEngineKind, TableReader, TableResult, TableStatistics, TableWriter,
    VerificationReport, WriteBatch,
};
use crate::txn::TransactionId;
use crate::types::{ObjectId, ScanBounds, ValueBuf};
use crate::wal::LogSequenceNumber;
use std::path::PathBuf;

/// File-based blob storage table.
///
/// Stores each blob as a separate file on disk. This is suitable for very large
/// blobs that would be inefficient to store in pages.
pub struct FileBlob {
    id: ObjectId,
    name: String,
    base_path: PathBuf,
}

impl FileBlob {
    /// Create a new file-based blob storage table.
    pub fn new(id: ObjectId, name: String, base_path: PathBuf) -> Self {
        Self {
            id,
            name,
            base_path,
        }
    }
}

impl Table for FileBlob {
    type Reader<'a> = FileBlobReader<'a>;
    type Writer<'a> = FileBlobWriter<'a>;

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
            memory_resident: false,
            disk_resident: true,
            supports_compression: false, // Relies on filesystem compression
            supports_encryption: false,  // Relies on filesystem encryption
        }
    }

    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>> {
        Ok(FileBlobReader {
            table: self,
            snapshot_lsn,
        })
    }

    fn writer(
        &self,
        tx_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Writer<'_>> {
        Ok(FileBlobWriter {
            table: self,
            tx_id,
            snapshot_lsn,
        })
    }

    fn stats(&self) -> TableResult<TableStatistics> {
        // TODO: Implement actual statistics gathering
        Ok(TableStatistics::default())
    }
}

impl BlobTable for FileBlob {
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

    fn put_blob(&mut self, _key: &[u8], _data: &[u8]) -> TableResult<u64> {
        todo!("Write blob to file")
    }

    fn get_blob(&self, _key: &[u8]) -> TableResult<Option<ValueBuf>> {
        todo!("Read blob from file")
    }

    fn delete_blob(&mut self, _key: &[u8]) -> TableResult<bool> {
        todo!("Delete blob file")
    }

    fn blob_size(&self, _key: &[u8]) -> TableResult<Option<u64>> {
        todo!("Get file size")
    }

    fn max_inline_size(&self) -> usize {
        // For file-based storage, use a larger threshold since we're optimized for large blobs
        64 * 1024 // 64KB
    }

    fn max_blob_size(&self) -> u64 {
        // File-based storage can handle very large blobs
        // Limited primarily by filesystem constraints
        u64::MAX
    }

    fn stats(&self) -> TableResult<SpecialtyTableStats> {
        // TODO: Implement actual statistics gathering
        Ok(SpecialtyTableStats::default())
    }

    fn verify(&self) -> TableResult<VerificationReport> {
        // TODO: Implement verification logic
        Ok(VerificationReport {
            checked_items: 0,
            errors: vec![],
            warnings: vec![],
        })
    }
}

/// Reader for file-based blob storage.
pub struct FileBlobReader<'a> {
    table: &'a FileBlob,
    snapshot_lsn: LogSequenceNumber,
}

// Stub implementations to satisfy Table trait requirements
// TODO: Remove these when Table trait is refactored (see nanokv-vrq)

impl<'a> PointLookup for FileBlobReader<'a> {
    fn get(&self, _key: &[u8], _snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support point lookup - use BlobTable::get_blob instead".to_string(),
        ))
    }
}

impl<'a> OrderedScan for FileBlobReader<'a> {
    type Cursor<'b> = FileBlobCursor where Self: 'b;

    fn scan(&self, _bounds: ScanBounds, _snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Cursor<'_>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support ordered scans".to_string(),
        ))
    }
}

impl<'a> TableReader for FileBlobReader<'a> {
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    fn approximate_len(&self) -> TableResult<Option<u64>> {
        // TODO: Implement actual length calculation
        Ok(Some(0))
    }
}

/// Stub cursor for blob tables (not actually used).
pub struct FileBlobCursor;

impl TableCursor for FileBlobCursor {
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

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        LogSequenceNumber::default()
    }
}

/// Writer for file-based blob storage.
pub struct FileBlobWriter<'a> {
    table: &'a FileBlob,
    tx_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a> TableWriter for FileBlobWriter<'a> {
    fn tx_id(&self) -> TransactionId {
        self.tx_id
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}

impl<'a> MutableTable for FileBlobWriter<'a> {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support put - use BlobTable::put_blob instead".to_string(),
        ))
    }

    fn delete(&mut self, _key: &[u8]) -> TableResult<bool> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support delete - use BlobTable::delete_blob instead".to_string(),
        ))
    }

    fn range_delete(&mut self, _bounds: ScanBounds) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support range delete".to_string(),
        ))
    }
}

impl<'a> BatchOps for FileBlobWriter<'a> {
    fn batch_get(&self, _keys: &[&[u8]]) -> TableResult<Vec<Option<ValueBuf>>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support batch operations".to_string(),
        ))
    }

    fn apply_batch(&mut self, _batch: WriteBatch) -> TableResult<BatchReport> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support batch operations".to_string(),
        ))
    }
}

impl<'a> Flushable for FileBlobWriter<'a> {
    fn flush(&mut self) -> TableResult<()> {
        // File-based blobs flush through filesystem
        Ok(())
    }
}

// Made with Bob
