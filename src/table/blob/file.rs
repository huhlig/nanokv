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
//! This module provides a file-system-based blob storage implementation:
//! - Each blob is stored as a separate file
//! - Suitable for very large blobs (GB+)
//! - Leverages OS file system caching
//! - Can use filesystem compression/encryption
//!
//! The implementation uses a directory structure to organize blob files,
//! with metadata stored separately for fast lookups.

use crate::table::{
    Table, TableCapabilities, TableEngineKind, TableResult, TableStatistics,
};
use crate::types::{ObjectId, ValueBuf};
use std::path::PathBuf;

/// File-based blob storage table.
///
/// Stores each blob as a separate file in a directory. This is suitable for
/// very large blobs that benefit from direct file system access.
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

    /// Get a value by key (stub implementation).
    pub fn get(&self, _key: &[u8]) -> TableResult<Option<ValueBuf>> {
        Err(crate::table::TableError::Other(
            "FileBlob get not yet implemented".to_string(),
        ))
    }

    /// Put a key-value pair (stub implementation).
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> TableResult<u64> {
        Err(crate::table::TableError::Other(
            "FileBlob put not yet implemented".to_string(),
        ))
    }

    /// Delete a key (stub implementation).
    pub fn delete(&self, _key: &[u8]) -> TableResult<bool> {
        Err(crate::table::TableError::Other(
            "FileBlob delete not yet implemented".to_string(),
        ))
    }
}

impl Table for FileBlob {
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

    fn stats(&self) -> TableResult<TableStatistics> {
        // TODO: Implement actual statistics gathering
        Ok(TableStatistics::default())
    }
}

// Made with Bob
