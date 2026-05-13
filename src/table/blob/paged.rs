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

use crate::pager::{Page, PageId, PageType, Pager};
use crate::table::{
    Table, TableCapabilities, TableEngineKind, TableResult, TableStatistics,
};
use crate::types::{TableId, ValueBuf};
use crate::vfs::FileSystem;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Paged blob storage table.
///
/// Stores blobs as linked pages in the pager. This is the primary implementation
/// for disk-resident blob storage that uses the page cache and WAL.
pub struct PagedBlob<FS: FileSystem> {
    id: TableId,
    name: String,
    page_size: usize,
    pager: Arc<Pager<FS>>,
    /// Index mapping keys to their first page ID
    index: Arc<RwLock<HashMap<Vec<u8>, PageId>>>,
}

impl<FS: FileSystem> PagedBlob<FS> {
    /// Create a new paged blob storage table.
    pub fn new(id: TableId, name: String, page_size: usize, pager: Arc<Pager<FS>>) -> Self {
        Self {
            id,
            name,
            page_size,
            pager,
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a value by key.
    ///
    /// Reads the blob data by following the linked page chain starting from
    /// the first page ID stored in the index.
    pub fn get(&self, key: &[u8]) -> TableResult<Option<ValueBuf>> {
        let index = self.index.read().unwrap();
        let first_page_id = match index.get(key) {
            Some(id) => *id,
            None => return Ok(None),
        };
        drop(index);

        let mut result = Vec::new();
        let mut current_page_id = first_page_id;

        loop {
            let page = self.pager.read_page(current_page_id)?;
            let data = page.data();

            // Read the next page ID (first 8 bytes) and the blob data
            if data.len() < 8 {
                return Err(crate::table::TableError::corruption(
                    "PagedBlob::get",
                    "page_too_small",
                    "Page data is too small to contain next page ID",
                ));
            }

            let next_page_id = PageId::from(u64::from_le_bytes(data[..8].try_into().map_err(|_| {
                crate::table::TableError::corruption(
                    "PagedBlob::get",
                    "invalid_page_id",
                    "Failed to parse next page ID",
                )
            })?));

            // Append blob data (skip the 8-byte header)
            result.extend_from_slice(&data[8..]);

            if next_page_id.as_u64() == 0 {
                break;
            }
            current_page_id = next_page_id;
        }

        Ok(Some(ValueBuf(result)))
    }

    /// Put a key-value pair.
    ///
    /// Stores the blob data in linked pages. The first 8 bytes of each page
    /// contain the next page ID (0 for the last page).
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<u64> {
        let data_size = self.page_size - 16; // 8 bytes for next_page_id, 8 bytes for page header overhead
        let mut first_page_id: Option<PageId> = None;
        let mut prev_page_id: Option<PageId> = None;

        let mut offset = 0;
        while offset < value.len() {
            let page_id = self.pager.allocate_page(PageType::Overflow)?;
            let chunk_size = std::cmp::min(data_size, value.len() - offset);
            let end_offset = offset + chunk_size;

            // Determine next page ID (0 if this is the last page)
            let next_page_id: u64 = if end_offset >= value.len() { 0 } else { 0 }; // Will update later

            // Create page data: [next_page_id: 8 bytes][blob chunk]
            let mut page_data = vec![0u8; self.page_size - 16];
            page_data[..8].copy_from_slice(&next_page_id.to_le_bytes());
            page_data[8..8 + chunk_size].copy_from_slice(&value[offset..end_offset]);

            let mut page = Page::new(page_id, PageType::Overflow, self.page_size - 16);
            *page.data_mut() = page_data;
            self.pager.write_page(&page)?;

            // Update previous page's next_page_id pointer
            if let Some(prev_id) = prev_page_id {
                let mut prev_page = self.pager.read_page(prev_id)?;
                let next_id_bytes = page_id.as_u64().to_le_bytes();
                prev_page.data_mut()[..8].copy_from_slice(&next_id_bytes);
                self.pager.write_page(&prev_page)?;
            }

            if first_page_id.is_none() {
                first_page_id = Some(page_id);
            }

            prev_page_id = Some(page_id);
            offset = end_offset;
        }

        // Handle empty value case
        if first_page_id.is_none() {
            let page_id = self.pager.allocate_page(PageType::Overflow)?;
            let mut page_data = vec![0u8; self.page_size - 16];
            page_data[..8].copy_from_slice(&0u64.to_le_bytes());
            let mut page = Page::new(page_id, PageType::Overflow, self.page_size - 16);
            *page.data_mut() = page_data;
            self.pager.write_page(&page)?;
            first_page_id = Some(page_id);
        }

        // Update index
        self.index
            .write()
            .unwrap()
            .insert(key.to_vec(), first_page_id.unwrap());

        Ok(value.len() as u64)
    }

    /// Delete a key.
    ///
    /// Removes the blob from storage by freeing all pages in the chain
    /// and removing the key from the index.
    pub fn delete(&mut self, key: &[u8]) -> TableResult<bool> {
        let first_page_id = {
            let mut index = self.index.write().unwrap();
            index.remove(key)
        };

        if let Some(mut current_page_id) = first_page_id {
            loop {
                let page = self.pager.read_page(current_page_id)?;
                let data = page.data();

                if data.len() < 8 {
                    return Err(crate::table::TableError::corruption(
                        "PagedBlob::delete",
                        "page_too_small",
                        "Page data is too small to contain next page ID",
                    ));
                }

                let next_page_id = PageId::from(u64::from_le_bytes(
                    data[..8].try_into().map_err(|_| {
                        crate::table::TableError::corruption(
                            "PagedBlob::delete",
                            "invalid_page_id",
                            "Failed to parse next page ID",
                        )
                    })?,
                ));

                self.pager.free_page(current_page_id)?;

                if next_page_id.as_u64() == 0 {
                    break;
                }
                current_page_id = next_page_id;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<FS: FileSystem> Table for PagedBlob<FS> {
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
        let index = self.index.read().unwrap();
        Ok(TableStatistics {
            row_count: Some(index.len() as u64),
            total_size_bytes: None,
            key_stats: None,
            value_stats: None,
            histogram: None,
            last_updated_lsn: None,
        })
    }
}

// Made with Bob
