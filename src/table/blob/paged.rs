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
    Table, TableCapabilities, TableEngineKind, TableResult, TableStatistics,
};
use crate::types::{TableId, ValueBuf};

/// Paged blob storage table.
///
/// Stores blobs as linked pages in the pager. This is the primary implementation
/// for disk-resident blob storage that uses the page cache and WAL.
pub struct PagedBlob {
    id: TableId,
    name: String,
    page_size: usize,
}

impl PagedBlob {
    /// Create a new paged blob storage table.
    pub fn new(id: TableId, name: String, page_size: usize) -> Self {
        Self {
            id,
            name,
            page_size,
        }
    }

    /// Get a value by key (stub implementation).
    pub fn get(&self, _key: &[u8]) -> TableResult<Option<ValueBuf>> {
        Err(crate::table::TableError::Other(
            "PagedBlob get not yet implemented".to_string(),
        ))
    }

    /// Put a key-value pair (stub implementation).
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "PagedBlob put not yet implemented".to_string(),
        ))
    }

    /// Delete a key (stub implementation).
    pub fn delete(&self, _key: &[u8]) -> TableResult<bool> {
        Err(crate::table::TableError::Other(
            "PagedBlob delete not yet implemented".to_string(),
        ))
    }
}

impl Table for PagedBlob {
    fn table_id(&self) -> TableId {
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

    fn stats(&self) -> TableResult<TableStatistics> {
        // TODO: Implement actual statistics gathering
        Ok(TableStatistics::default())
    }
}

// Made with Bob
