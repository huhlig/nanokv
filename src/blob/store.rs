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

/// Large binary object store.
///
/// Stores values too large for table pages as linked pages in the pager.
/// Provides efficient storage and retrieval of arbitrarily large values.
pub struct BlobStore {
    // TODO: Add pager reference and internal state
}

impl BlobStore {
    /// Create a new blob store.
    pub fn new() -> Self {
        Self {}
    }

    /// Store a blob and return a reference to it.
    ///
    /// The blob is stored as one or more linked pages in the pager.
    /// Returns a `BlobRef` that can be used to retrieve the blob later.
    pub fn put_blob(&mut self, _bytes: &[u8]) -> BlobResult<BlobRef> {
        todo!("Allocate pages and write blob data")
    }

    /// Retrieve a blob by reference.
    ///
    /// Reads all pages in the blob chain and returns the complete value.
    pub fn get_blob(&self, _blob: BlobRef) -> BlobResult<ValueBuf> {
        todo!("Read linked pages and reconstruct blob")
    }

    /// Delete a blob and free its pages.
    ///
    /// Returns `true` if the blob was found and deleted, `false` if not found.
    pub fn delete_blob(&mut self, _blob: BlobRef) -> BlobResult<bool> {
        todo!("Free all pages in blob chain")
    }

    /// Get the maximum blob size supported.
    pub fn max_blob_size(&self) -> u64 {
        // Default to 1GB max blob size
        1024 * 1024 * 1024
    }

    /// Check if a value should be stored as a blob.
    ///
    /// Values larger than this threshold should be stored as blobs
    /// rather than inline in table pages.
    pub fn should_use_blob(value_size: usize) -> bool {
        // Use blob storage for values larger than 4KB
        value_size > 4096
    }
}

/// Reference to a blob stored in the page file.
///
/// The blob reference points to the first page of a potentially multi-page blob.
/// Pages are linked together to form the complete blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlobRef {
    /// First page ID of the blob
    first_page: PageId,
    /// Total size of the blob in bytes
    size: u64,
}

impl BlobRef {
    /// Create a new blob reference.
    pub fn new(first_page: PageId, size: u64) -> Self {
        Self { first_page, size }
    }

    /// Get the first page ID.
    pub fn first_page(&self) -> PageId {
        self.first_page
    }

    /// Get the total size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
}
