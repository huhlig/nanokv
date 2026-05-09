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

use crate::blob::error::BlobResult;
use crate::pager::PageId;
use crate::types::ValueBuf;

// =============================================================================
// BlobStore Trait
// =============================================================================

/// Trait for blob storage backends.
///
/// Provides an abstraction for storing large binary objects that are too large
/// to fit efficiently in table pages. Different implementations can provide:
/// - Paged storage (linked pages in the pager)
/// - Memory-resident storage (for in-memory tables)
/// - Compressed storage (transparent compression)
/// - Encrypted storage (transparent encryption)
/// - Tiered storage (hot/cold data separation)
///
/// This trait follows the architectural pattern used throughout the codebase
/// where capabilities are defined through traits with concrete implementations.
pub trait BlobStore {
    /// Store a blob and return a reference to it.
    ///
    /// The blob is stored according to the implementation's strategy.
    /// Returns a `BlobRef` that can be used to retrieve the blob later.
    fn put_blob(&mut self, bytes: &[u8]) -> BlobResult<BlobRef>;

    /// Retrieve a blob by reference.
    ///
    /// Returns the complete blob data or an error if the blob is not found
    /// or the reference is invalid/stale.
    fn get_blob(&self, blob: BlobRef) -> BlobResult<ValueBuf>;

    /// Delete a blob and free its storage.
    ///
    /// Returns the list of freed pages for the caller to return to the freelist.
    /// This allows the transaction layer to coordinate page deallocation with
    /// commit/abort logic.
    fn delete_blob(&mut self, blob: BlobRef) -> BlobResult<FreedPages>;

    /// Get the maximum inline size for this blob store.
    ///
    /// Values smaller than this threshold should be stored inline in table pages
    /// rather than as separate blobs. The threshold is typically a function of
    /// the page size (e.g., 1/4 of page size) rather than a hardcoded constant.
    fn max_inline_size(&self) -> usize;

    /// Get the maximum blob size supported by this implementation.
    fn max_blob_size(&self) -> u64 {
        // Default to 1GB max blob size
        1024 * 1024 * 1024
    }
}

// =============================================================================
// PagedBlobStore Implementation
// =============================================================================

/// Pager-backed blob storage implementation.
///
/// Stores blobs as linked pages in the pager. This is the primary implementation
/// for disk-resident tables that use the page cache and WAL.
pub struct PagedBlobStore {
    // TODO: Add pager reference and internal state
    page_size: usize,
}

impl PagedBlobStore {
    /// Create a new paged blob store.
    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }
}

impl BlobStore for PagedBlobStore {
    fn put_blob(&mut self, _bytes: &[u8]) -> BlobResult<BlobRef> {
        todo!("Allocate pages and write blob data")
    }

    fn get_blob(&self, _blob: BlobRef) -> BlobResult<ValueBuf> {
        todo!("Read linked pages and reconstruct blob")
    }

    fn delete_blob(&mut self, _blob: BlobRef) -> BlobResult<FreedPages> {
        todo!("Free all pages in blob chain")
    }

    fn max_inline_size(&self) -> usize {
        // Use 1/4 of page size as inline threshold
        // This balances space efficiency with avoiding excessive blob overhead
        self.page_size / 4
    }

    fn max_blob_size(&self) -> u64 {
        // With 32-bit page IDs and typical page sizes, we can support very large blobs
        // Default to 1GB to be conservative
        1024 * 1024 * 1024
    }
}

// =============================================================================
// Supporting Types
// =============================================================================

/// Reference to a blob stored in the page file.
///
/// The blob reference points to the first page of a potentially multi-page blob.
/// Pages are linked together to form the complete blob.
///
/// The checksum field enables detection of stale references (e.g., after a blob
/// is deleted and its pages are reused for other data).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlobRef {
    /// First page ID of the blob
    first_page: PageId,
    /// Total size of the blob in bytes
    size: u64,
    /// Checksum for stale reference detection
    ///
    /// When a blob is deleted and its pages are reused, the checksum will
    /// no longer match, allowing detection of use-after-free bugs.
    checksum: u32,
}

impl BlobRef {
    /// Create a new blob reference.
    pub fn new(first_page: PageId, size: u64, checksum: u32) -> Self {
        Self {
            first_page,
            size,
            checksum,
        }
    }

    /// Get the first page ID.
    pub fn first_page(&self) -> PageId {
        self.first_page
    }

    /// Get the total size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the checksum.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }
}

/// List of pages freed by deleting a blob.
///
/// The transaction layer uses this to coordinate page deallocation with
/// commit/abort logic. Pages are only returned to the freelist after the
/// transaction commits successfully.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FreedPages {
    pub pages: Vec<PageId>,
}

impl FreedPages {
    /// Create an empty freed pages list.
    pub fn new() -> Self {
        Self { pages: Vec::new() }
    }

    /// Create a freed pages list with a single page.
    pub fn single(page_id: PageId) -> Self {
        Self {
            pages: vec![page_id],
        }
    }

    /// Create a freed pages list from a vector of page IDs.
    pub fn from_pages(pages: Vec<PageId>) -> Self {
        Self { pages }
    }

    /// Get the number of freed pages.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Add a page to the freed list.
    pub fn push(&mut self, page_id: PageId) {
        self.pages.push(page_id);
    }

    /// Extend with pages from another freed list.
    pub fn extend(&mut self, other: FreedPages) {
        self.pages.extend(other.pages);
    }
}
