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

//! Paged blob storage implementation.
//!
//! This module provides a disk-backed blob storage implementation using the pager:
//! - Blobs are stored as linked pages in the page file
//! - Supports large blobs that span multiple pages
//! - Integrates with the page cache for performance
//! - Provides durability through the WAL
//!
//! The implementation uses a B-Tree index to map blob keys to their first page,
//! with pages linked together to form complete blobs.

use crate::table::{
    BatchOps, BatchReport, Flushable, MutableTable, OrderedScan, PointLookup,
    Table, TableCapabilities, TableCursor,
    TableEngineKind, TableReader, TableResult, TableStatistics, TableWriter,
    WriteBatch,
};
use crate::txn::TransactionId;
use crate::types::{KeyBuf, ObjectId, ScanBounds, ValueBuf};
use crate::wal::LogSequenceNumber;

/// Paged blob storage table.
///
/// Stores blobs as linked pages in the pager. This is the primary implementation
/// for disk-resident blob storage that uses the page cache and WAL.
pub struct PagedBlob {
    id: ObjectId,
    name: String,
    page_size: usize,
}

impl PagedBlob {
    /// Create a new paged blob storage table.
    pub fn new(id: ObjectId, name: String, page_size: usize) -> Self {
        Self {
            id,
            name,
            page_size,
        }
    }
}

impl Table for PagedBlob {
    type Reader<'a> = PagedBlobReader<'a>;
    type Writer<'a> = PagedBlobWriter<'a>;

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
            supports_compression: true,
            supports_encryption: true,
        }
    }

    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>> {
        Ok(PagedBlobReader {
            table: self,
            snapshot_lsn,
        })
    }

    fn writer(
        &self,
        tx_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Writer<'_>> {
        Ok(PagedBlobWriter {
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


/// Reader for paged blob storage.
pub struct PagedBlobReader<'a> {
    table: &'a PagedBlob,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a> PointLookup for PagedBlobReader<'a> {
    fn get(&self, _key: &[u8], _snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        // TODO: Implement actual paged blob reading
        todo!("Read linked pages and reconstruct blob")
    }
}

impl<'a> OrderedScan for PagedBlobReader<'a> {
    type Cursor<'b> = PagedBlobCursor where Self: 'b;

    fn scan(&self, _bounds: ScanBounds, _snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Cursor<'_>> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support ordered scans".to_string(),
        ))
    }
}

impl<'a> TableReader for PagedBlobReader<'a> {
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    fn approximate_len(&self) -> TableResult<Option<u64>> {
        // TODO: Implement actual length calculation
        Ok(Some(0))
    }
}

/// Stub cursor for blob tables (not actually used).
pub struct PagedBlobCursor;

impl TableCursor for PagedBlobCursor {
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

/// Writer for paged blob storage.
pub struct PagedBlobWriter<'a> {
    table: &'a PagedBlob,
    tx_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a> MutableTable for PagedBlobWriter<'a> {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> TableResult<u64> {
        // TODO: Implement actual paged blob writing
        todo!("Allocate pages and write blob data")
    }

    fn delete(&mut self, _key: &[u8]) -> TableResult<bool> {
        // TODO: Implement actual paged blob deletion
        todo!("Free all pages in blob chain")
    }

    fn range_delete(&mut self, _bounds: ScanBounds) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "Blob tables do not support range delete".to_string(),
        ))
    }

    fn max_inline_size(&self) -> Option<usize> {
        // Use 1/4 of page size as inline threshold
        Some(self.table.page_size / 4)
    }

    fn max_value_size(&self) -> Option<u64> {
        // With 32-bit page IDs and typical page sizes, we can support very large blobs
        Some(1024 * 1024 * 1024) // 1GB
    }
}

impl<'a> BatchOps for PagedBlobWriter<'a> {
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

impl<'a> Flushable for PagedBlobWriter<'a> {
    fn flush(&mut self) -> TableResult<()> {
        // Blob tables flush through the pager
        Ok(())
    }
}

impl<'a> TableWriter for PagedBlobWriter<'a> {
    fn tx_id(&self) -> TransactionId {
        self.tx_id
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}

// Made with Bob
