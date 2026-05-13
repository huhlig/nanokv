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

use crate::table::{Table, TableCapabilities, TableEngineKind, TableResult, TableStatistics};
use crate::types::{TableId, ValueBuf};
use crate::vfs::{File, FileSystem};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// File-based blob storage table.
///
/// Stores each blob as a separate file in a directory. This is suitable for
/// very large blobs that benefit from direct file system access.
pub struct FileBlob<FS: FileSystem> {
    id: TableId,
    name: String,
    base_path: PathBuf,
    fs: Arc<FS>,
    /// Index mapping keys to their file paths
    index: Arc<RwLock<HashMap<Vec<u8>, PathBuf>>>,
}

impl<FS: FileSystem> FileBlob<FS> {
    /// Create a new file-based blob storage table.
    pub fn new(id: TableId, name: String, base_path: PathBuf, fs: Arc<FS>) -> Self {
        Self {
            id,
            name,
            base_path,
            fs,
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate a file path for a given key.
    fn key_to_path(&self, key: &[u8]) -> PathBuf {
        // Use hex encoding of the key as the filename
        let hex_key: String = key.iter().map(|b| format!("{:02x}", b)).collect();
        self.base_path.join(hex_key)
    }

    /// Get a value by key.
    ///
    /// Reads the blob data from the file system.
    pub fn get(&self, key: &[u8]) -> TableResult<Option<ValueBuf>> {
        let path = self.key_to_path(key);

        // Check if file exists by trying to open it
        match self.fs.open_file(path.to_str().unwrap_or("")) {
            Ok(mut file) => {
                // Get file size
                let size = file.get_size().map_err(|e| {
                    crate::table::TableError::Other(format!("Failed to get file size: {}", e))
                })?;

                // Read file contents
                let mut buffer = vec![0u8; size as usize];
                file.read_at_offset(0, &mut buffer).map_err(|e| {
                    crate::table::TableError::Other(format!("Failed to read file: {}", e))
                })?;

                Ok(Some(ValueBuf(buffer)))
            }
            Err(_) => Ok(None),
        }
    }

    /// Put a key-value pair.
    ///
    /// Stores the blob data as a file on the file system.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<u64> {
        let path = self.key_to_path(key);
        let path_str = path.to_str().unwrap_or("");

        // Create or overwrite the file
        let mut file = self.fs.create_file(path_str).map_err(|e| {
            crate::table::TableError::Other(format!("Failed to create file: {}", e))
        })?;

        file.write_all(value)
            .map_err(|e| crate::table::TableError::Other(format!("Failed to write file: {}", e)))?;

        // Update index
        self.index.write().unwrap().insert(key.to_vec(), path);

        Ok(value.len() as u64)
    }

    /// Delete a key.
    ///
    /// Removes the blob file from the file system and the key from the index.
    pub fn delete(&mut self, key: &[u8]) -> TableResult<bool> {
        let path = self.key_to_path(key);
        let path_str = path.to_str().unwrap_or("");

        // Try to delete the file
        let deleted = match self.fs.remove_file(path_str) {
            Ok(_) => true,
            Err(_) => false,
        };

        // Remove from index
        if deleted {
            self.index.write().unwrap().remove(key);
        }

        Ok(deleted)
    }
}

impl<FS: FileSystem> Table for FileBlob<FS> {
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
            supports_compression: false,
            supports_encryption: false,
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
