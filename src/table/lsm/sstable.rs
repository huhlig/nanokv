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

//! SSTable (Sorted String Table) implementation for LSM tree.
//!
//! An SSTable is an immutable, sorted file format that stores key-value pairs.
//! It consists of:
//! - Data blocks: Contain sorted key-value pairs
//! - Index block: Maps keys to data block offsets
//! - Bloom filter: Probabilistic filter for membership testing
//! - Footer: Metadata about the SSTable
//!
//! # Format
//!
//! ```text
//! ┌─────────────────┐
//! │  Data Block 0   │  ← Sorted key-value pairs
//! ├─────────────────┤
//! │  Data Block 1   │
//! ├─────────────────┤
//! │      ...        │
//! ├─────────────────┤
//! │  Data Block N   │
//! ├─────────────────┤
//! │  Index Block    │  ← Maps keys to data blocks
//! ├─────────────────┤
//! │  Bloom Filter   │  ← Probabilistic membership test
//! ├─────────────────┤
//! │     Footer      │  ← Metadata (offsets, counts, etc.)
//! └─────────────────┘
//! ```

use crate::pager::{Page, PageId, PageType, Pager, PagerResult};
use crate::table::lsm::{BloomFilter, BloomFilterBuilder, SStableConfig};
use crate::table::TableResult;
use crate::txn::VersionChain;
use crate::vfs::FileSystem;
use crate::wal::LogSequenceNumber;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// SSTable identifier (unique within a table).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SStableId(u64);

impl SStableId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SStableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSTable({})", self.0)
    }
}

/// SSTable metadata stored in the footer.
#[derive(Clone, Debug)]
pub struct SStableMetadata {
    /// SSTable ID
    pub id: SStableId,
    
    /// Level in the LSM tree (0 = L0, 1 = L1, etc.)
    pub level: u32,
    
    /// Smallest key in the SSTable
    pub min_key: Vec<u8>,
    
    /// Largest key in the SSTable
    pub max_key: Vec<u8>,
    
    /// Number of key-value pairs
    pub num_entries: u64,
    
    /// Total size in bytes
    pub total_size: u64,
    
    /// LSN when this SSTable was created
    pub created_lsn: LogSequenceNumber,
    
    /// First page ID of this SSTable
    pub first_page_id: PageId,
    
    /// Number of pages used by this SSTable
    pub num_pages: u32,
    
    /// Offset to index block (from start of SSTable)
    pub index_offset: u64,
    
    /// Offset to bloom filter (from start of SSTable)
    pub bloom_filter_offset: u64,
    
    /// Offset to footer (from start of SSTable)
    pub footer_offset: u64,
}

/// SSTable footer (stored at the end of the SSTable).
///
/// The footer contains metadata needed to read the SSTable.
#[derive(Clone, Debug)]
pub struct SStableFooter {
    /// Magic number for validation (0x5353544142 = "SSTAB")
    pub magic: u64,
    
    /// Format version
    pub version: u32,
    
    /// Metadata
    pub metadata: SStableMetadata,
    
    /// Checksum of the entire SSTable (excluding footer)
    pub checksum: [u8; 32],
}

impl SStableFooter {
    const MAGIC: u64 = 0x5353544142; // "SSTAB"
    const VERSION: u32 = 1;
    const SIZE: usize = 256; // Fixed size for footer

    /// Create a new footer.
    pub fn new(metadata: SStableMetadata, checksum: [u8; 32]) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            metadata,
            checksum,
        }
    }

    /// Serialize footer to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        
        // Magic (8 bytes)
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        
        // Version (4 bytes)
        bytes.extend_from_slice(&self.version.to_le_bytes());
        
        // Metadata
        bytes.extend_from_slice(&self.metadata.id.as_u64().to_le_bytes());
        bytes.extend_from_slice(&self.metadata.level.to_le_bytes());
        
        // Min key (length + data, max 64 bytes)
        let min_key_len = self.metadata.min_key.len().min(64);
        bytes.extend_from_slice(&(min_key_len as u32).to_le_bytes());
        bytes.extend_from_slice(&self.metadata.min_key[..min_key_len]);
        bytes.resize(bytes.len() + (64 - min_key_len), 0);
        
        // Max key (length + data, max 64 bytes)
        let max_key_len = self.metadata.max_key.len().min(64);
        bytes.extend_from_slice(&(max_key_len as u32).to_le_bytes());
        bytes.extend_from_slice(&self.metadata.max_key[..max_key_len]);
        bytes.resize(bytes.len() + (64 - max_key_len), 0);
        
        // Counts and offsets
        bytes.extend_from_slice(&self.metadata.num_entries.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.total_size.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.created_lsn.as_u64().to_le_bytes());
        bytes.extend_from_slice(&self.metadata.first_page_id.to_bytes());
        bytes.extend_from_slice(&self.metadata.num_pages.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.index_offset.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.bloom_filter_offset.to_le_bytes());
        bytes.extend_from_slice(&self.metadata.footer_offset.to_le_bytes());
        
        // Checksum (32 bytes)
        bytes.extend_from_slice(&self.checksum);
        
        // Pad to fixed size
        bytes.resize(Self::SIZE, 0);
        
        bytes
    }

    /// Deserialize footer from bytes.
    pub fn from_bytes(bytes: &[u8]) -> TableResult<Self> {
        if bytes.len() < Self::SIZE {
            return Err(crate::table::TableError::Corruption(
                "Footer too small".to_string(),
            ));
        }

        let mut offset = 0;

        // Magic
        let magic = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        if magic != Self::MAGIC {
            return Err(crate::table::TableError::Corruption(
                "Invalid footer magic".to_string(),
            ));
        }

        // Version
        let version = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Metadata
        let id = SStableId::new(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ));
        offset += 8;

        let level = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Min key
        let min_key_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let min_key = bytes[offset..offset + min_key_len].to_vec();
        offset += 64;

        // Max key
        let max_key_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let max_key = bytes[offset..offset + max_key_len].to_vec();
        offset += 64;

        // Counts and offsets
        let num_entries = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let total_size = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let created_lsn = LogSequenceNumber::from(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ));
        offset += 8;
        let first_page_id = PageId::from(u64::from_le_bytes(
            bytes[offset..offset + 8].try_into().unwrap(),
        ));
        offset += 8;
        let num_pages = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let index_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let bloom_filter_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let footer_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // Checksum
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&bytes[offset..offset + 32]);

        let metadata = SStableMetadata {
            id,
            level,
            min_key,
            max_key,
            num_entries,
            total_size,
            created_lsn,
            first_page_id,
            num_pages,
            index_offset,
            bloom_filter_offset,
            footer_offset,
        };

        Ok(Self {
            magic,
            version,
            metadata,
            checksum,
        })
    }
}

/// Data block containing sorted key-value pairs with version chains.
///
/// Format:
/// - Header: num_entries (4 bytes) + compressed_flag (1 byte) + checksum (32 bytes)
/// - Entries: For each entry:
///   - key_len (4 bytes)
///   - key (variable)
///   - version_chain_len (4 bytes)
///   - serialized_version_chain (variable, bincode format)
///
/// Entries are stored in sorted order by key for efficient binary search.
#[derive(Clone, Debug)]
pub struct DataBlock {
    /// Sorted entries (key, version chain)
    entries: Vec<(Vec<u8>, VersionChain)>,
}

impl DataBlock {
    /// Header size: num_entries (4) + compressed_flag (1) + checksum (32)
    const HEADER_SIZE: usize = 37;

    /// Create a new empty data block.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create a data block from sorted entries.
    ///
    /// # Panics
    /// Panics if entries are not sorted by key.
    pub fn from_entries(entries: Vec<(Vec<u8>, VersionChain)>) -> Self {
        // Verify entries are sorted
        for i in 1..entries.len() {
            assert!(
                entries[i - 1].0 < entries[i].0,
                "Entries must be sorted by key"
            );
        }
        Self { entries }
    }

    /// Add an entry to the block.
    ///
    /// # Errors
    /// Returns error if key is not greater than the last key (entries must be sorted).
    pub fn add(&mut self, key: Vec<u8>, chain: VersionChain) -> TableResult<()> {
        if let Some((last_key, _)) = self.entries.last() {
            if &key <= last_key {
                return Err(crate::table::TableError::Other(
                    "Keys must be added in sorted order".to_string(),
                ));
            }
        }
        self.entries.push((key, chain));
        Ok(())
    }

    /// Get the number of entries in this block.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the block is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries in the block.
    pub fn entries(&self) -> &[(Vec<u8>, VersionChain)] {
        &self.entries
    }

    /// Get the first key in the block (smallest).
    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|(k, _)| k.as_slice())
    }

    /// Get the last key in the block (largest).
    pub fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|(k, _)| k.as_slice())
    }

    /// Binary search for a key in the block.
    ///
    /// Returns the index of the entry if found, or None if not found.
    pub fn search(&self, key: &[u8]) -> Option<usize> {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
    }

    /// Get the version chain for a key.
    pub fn get(&self, key: &[u8]) -> Option<&VersionChain> {
        self.search(key).map(|idx| &self.entries[idx].1)
    }

    /// Serialize the data block to bytes.
    ///
    /// Format:
    /// - num_entries (4 bytes)
    /// - compressed_flag (1 byte) - 0 for uncompressed, 1 for compressed
    /// - checksum (32 bytes) - SHA256 of the data portion
    /// - For each entry:
    ///   - key_len (4 bytes)
    ///   - key (variable)
    ///   - version_chain_len (4 bytes)
    ///   - serialized_version_chain (variable, bincode)
    pub fn to_bytes(&self, compress: bool) -> TableResult<Vec<u8>> {
        // Serialize entries
        let mut data = Vec::new();
        for (key, chain) in &self.entries {
            // Key length and data
            data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            data.extend_from_slice(key);

            // Serialize version chain using bincode
            let chain_bytes = bincode::serialize(chain).map_err(|e| {
                crate::table::TableError::Other(format!("Failed to serialize version chain: {}", e))
            })?;

            // Version chain length and data
            data.extend_from_slice(&(chain_bytes.len() as u32).to_le_bytes());
            data.extend_from_slice(&chain_bytes);
        }

        // Optionally compress data
        let (final_data, compressed_flag) = if compress && data.len() > 128 {
            // Only compress if data is larger than 128 bytes
            match lz4_flex::compress_prepend_size(&data) {
                compressed if compressed.len() < data.len() => (compressed, 1u8),
                _ => (data, 0u8), // Compression didn't help, use uncompressed
            }
        } else {
            (data, 0u8)
        };

        // Calculate checksum of the data
        let mut hasher = Sha256::new();
        hasher.update(&final_data);
        let checksum: [u8; 32] = hasher.finalize().into();

        // Build final block with header
        let mut block = Vec::with_capacity(Self::HEADER_SIZE + final_data.len());
        
        // Header
        block.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        block.push(compressed_flag);
        block.extend_from_slice(&checksum);
        
        // Data
        block.extend_from_slice(&final_data);

        Ok(block)
    }

    /// Deserialize a data block from bytes.
    ///
    /// Validates the checksum and decompresses if necessary.
    pub fn from_bytes(bytes: &[u8]) -> TableResult<Self> {
        if bytes.len() < Self::HEADER_SIZE {
            return Err(crate::table::TableError::Corruption(
                "Data block too small".to_string(),
            ));
        }

        let mut offset = 0;

        // Parse header
        let num_entries = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let compressed_flag = bytes[offset];
        offset += 1;

        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&bytes[offset..offset + 32]);
        offset += 32;

        // Extract data portion
        let data_bytes = &bytes[offset..];

        // Verify checksum
        let mut hasher = Sha256::new();
        hasher.update(data_bytes);
        let computed_checksum: [u8; 32] = hasher.finalize().into();
        
        if checksum != computed_checksum {
            return Err(crate::table::TableError::Corruption(
                "Data block checksum mismatch".to_string(),
            ));
        }

        // Decompress if necessary
        let data = if compressed_flag == 1 {
            lz4_flex::decompress_size_prepended(data_bytes).map_err(|e| {
                crate::table::TableError::Corruption(format!("Failed to decompress data block: {}", e))
            })?
        } else {
            data_bytes.to_vec()
        };

        // Deserialize entries
        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 0;

        for _ in 0..num_entries {
            if offset + 4 > data.len() {
                return Err(crate::table::TableError::Corruption(
                    "Truncated data block entry".to_string(),
                ));
            }

            // Read key
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if offset + key_len > data.len() {
                return Err(crate::table::TableError::Corruption(
                    "Truncated key in data block".to_string(),
                ));
            }

            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            // Read version chain
            if offset + 4 > data.len() {
                return Err(crate::table::TableError::Corruption(
                    "Truncated version chain length".to_string(),
                ));
            }

            let chain_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            if offset + chain_len > data.len() {
                return Err(crate::table::TableError::Corruption(
                    "Truncated version chain data".to_string(),
                ));
            }

            let chain_bytes = &data[offset..offset + chain_len];
            offset += chain_len;

            let chain: VersionChain = bincode::deserialize(chain_bytes).map_err(|e| {
                crate::table::TableError::Corruption(format!(
                    "Failed to deserialize version chain: {}",
                    e
                ))
            })?;

            entries.push((key, chain));
        }

        // Verify entries are sorted
        for i in 1..entries.len() {
            if entries[i - 1].0 >= entries[i].0 {
                return Err(crate::table::TableError::Corruption(
                    "Data block entries not sorted".to_string(),
                ));
            }
        }

        Ok(Self { entries })
    }

    /// Estimate the serialized size of this block.
    pub fn estimate_size(&self) -> usize {
        let mut size = Self::HEADER_SIZE;
        for (key, chain) in &self.entries {
            size += 4; // key_len
            size += key.len();
            size += 4; // chain_len
            // Rough estimate for version chain size
            size += bincode::serialized_size(chain).unwrap_or(0) as usize;
        }
        size
    }
}

impl Default for DataBlock {
    fn default() -> Self {
        Self::new()
    }
}

/// Index entry mapping a key to a data block location.
///
/// Each entry stores:
/// - The first key of a data block
/// - The page ID where the data block starts
/// - The offset within that page where the data block starts
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// First key in the data block
    pub first_key: Vec<u8>,
    /// Page ID where the data block starts
    pub page_id: PageId,
    /// Offset within the page (in bytes)
    pub offset: u64,
}

/// Index block for efficient key lookups in an SSTable.
///
/// The index is a sparse index - it contains one entry per data block,
/// not one entry per key. This keeps the index small while still enabling
/// efficient binary search to find the correct data block.
///
/// Format:
/// - Header: num_entries (4 bytes) + checksum (32 bytes)
/// - Entries: For each entry:
///   - key_len (4 bytes)
///   - key (variable)
///   - page_id (8 bytes)
///   - offset (8 bytes)
///
/// Entries are stored in sorted order by key for efficient binary search.
#[derive(Clone, Debug)]
pub struct IndexBlock {
    /// Sorted index entries (one per data block)
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    /// Header size: num_entries (4) + checksum (32)
    const HEADER_SIZE: usize = 36;

    /// Create a new empty index block.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create an index block from sorted entries.
    ///
    /// # Panics
    /// Panics if entries are not sorted by key.
    pub fn from_entries(entries: Vec<IndexEntry>) -> Self {
        // Verify entries are sorted
        for i in 1..entries.len() {
            assert!(
                entries[i - 1].first_key < entries[i].first_key,
                "Index entries must be sorted by key"
            );
        }
        Self { entries }
    }

    /// Add an index entry.
    ///
    /// # Errors
    /// Returns error if key is not greater than the last key (entries must be sorted).
    pub fn add(&mut self, first_key: Vec<u8>, page_id: PageId, offset: u64) -> TableResult<()> {
        if let Some(last_entry) = self.entries.last() {
            if first_key <= last_entry.first_key {
                return Err(crate::table::TableError::Other(
                    "Index entries must be added in sorted order".to_string(),
                ));
            }
        }
        self.entries.push(IndexEntry {
            first_key,
            page_id,
            offset,
        });
        Ok(())
    }

    /// Get the number of entries in this index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get all entries in the index.
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Binary search to find the data block that may contain the given key.
    ///
    /// Returns the index of the entry whose data block should be searched.
    /// Returns None if the key is definitely not in any data block.
    ///
    /// The algorithm finds the rightmost entry whose first_key <= search_key.
    pub fn search(&self, key: &[u8]) -> Option<usize> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search for the rightmost entry with first_key <= key
        let mut left = 0;
        let mut right = self.entries.len();
        let mut result = None;

        while left < right {
            let mid = left + (right - left) / 2;
            if self.entries[mid].first_key.as_slice() <= key {
                result = Some(mid);
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        result
    }

    /// Get the index entry at the given position.
    pub fn get(&self, index: usize) -> Option<&IndexEntry> {
        self.entries.get(index)
    }

    /// Serialize the index block to bytes.
    ///
    /// Format:
    /// - num_entries (4 bytes)
    /// - checksum (32 bytes) - SHA256 of the entries data
    /// - For each entry:
    ///   - key_len (4 bytes)
    ///   - key (variable)
    ///   - page_id (8 bytes)
    ///   - offset (8 bytes)
    pub fn to_bytes(&self) -> TableResult<Vec<u8>> {
        let mut buffer = Vec::new();

        // Reserve space for header
        buffer.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&[0u8; 32]); // Placeholder for checksum

        // Serialize entries
        let entries_start = buffer.len();
        for entry in &self.entries {
            // key_len
            buffer.extend_from_slice(&(entry.first_key.len() as u32).to_le_bytes());
            // key
            buffer.extend_from_slice(&entry.first_key);
            // page_id
            buffer.extend_from_slice(&entry.page_id.as_u64().to_le_bytes());
            // offset
            buffer.extend_from_slice(&entry.offset.to_le_bytes());
        }

        // Calculate checksum of entries data
        let entries_data = &buffer[entries_start..];
        let mut hasher = Sha256::new();
        hasher.update(entries_data);
        let checksum = hasher.finalize();

        // Write checksum to header
        buffer[4..36].copy_from_slice(&checksum);

        Ok(buffer)
    }

    /// Deserialize an index block from bytes.
    ///
    /// # Errors
    /// Returns error if:
    /// - Data is truncated or malformed
    /// - Checksum validation fails
    pub fn from_bytes(bytes: &[u8]) -> TableResult<Self> {
        if bytes.len() < Self::HEADER_SIZE {
            return Err(crate::table::TableError::Other(
                "Index block data too short".to_string(),
            ));
        }

        // Read header
        let num_entries = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let stored_checksum = &bytes[4..36];

        // Verify checksum of entries data
        let entries_data = &bytes[Self::HEADER_SIZE..];
        let mut hasher = Sha256::new();
        hasher.update(entries_data);
        let computed_checksum = hasher.finalize();

        if stored_checksum != computed_checksum.as_slice() {
            return Err(crate::table::TableError::Other(
                "Index block checksum mismatch".to_string(),
            ));
        }

        // Deserialize entries
        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = Self::HEADER_SIZE;

        for _ in 0..num_entries {
            if offset + 4 > bytes.len() {
                return Err(crate::table::TableError::Other(
                    "Truncated index entry (key_len)".to_string(),
                ));
            }

            // Read key_len
            let key_len = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > bytes.len() {
                return Err(crate::table::TableError::Other(
                    "Truncated index entry (key)".to_string(),
                ));
            }

            // Read key
            let first_key = bytes[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset + 16 > bytes.len() {
                return Err(crate::table::TableError::Other(
                    "Truncated index entry (page_id/offset)".to_string(),
                ));
            }

            // Read page_id
            let page_id = PageId::from(u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]));
            offset += 8;

            // Read offset
            let block_offset = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            offset += 8;

            entries.push(IndexEntry {
                first_key,
                page_id,
                offset: block_offset,
            });
        }

        // Verify entries are sorted
        for i in 1..entries.len() {
            if entries[i - 1].first_key >= entries[i].first_key {
                return Err(crate::table::TableError::Other(
                    "Index entries not sorted".to_string(),
                ));
            }
        }

        Ok(Self { entries })
    }

    /// Estimate the serialized size of this index block.
    pub fn estimate_size(&self) -> usize {
        let mut size = Self::HEADER_SIZE;
        for entry in &self.entries {
            size += 4; // key_len
            size += entry.first_key.len(); // key
            size += 8; // page_id
            size += 8; // offset
        }
        size
    }
}

impl Default for IndexBlock {
    fn default() -> Self {
        Self::new()
    }
}

/// SSTable reader for reading data from an immutable SSTable.
pub struct SStableReader<FS: FileSystem> {
    /// Pager for reading pages
    pager: Arc<Pager<FS>>,
    
    /// SSTable metadata
    metadata: SStableMetadata,
    
    /// Bloom filter for membership testing
    bloom_filter: Option<BloomFilter>,
    
    /// Configuration
    config: SStableConfig,
}

impl<FS: FileSystem> SStableReader<FS> {
    /// Open an existing SSTable for reading.
    pub fn open(
        pager: Arc<Pager<FS>>,
        first_page_id: PageId,
        config: SStableConfig,
    ) -> TableResult<Self> {
        // Read the footer from the last page
        // For now, we'll implement a simplified version
        // In a full implementation, we'd read the footer, then the bloom filter and index
        
        todo!("Implement SSTable opening - read footer, bloom filter, and index")
    }

    /// Check if a key might exist in this SSTable using the bloom filter.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if let Some(ref bloom) = self.bloom_filter {
            bloom.contains(key)
        } else {
            true // No bloom filter, assume it might contain the key
        }
    }

    /// Get the value for a key.
    pub fn get(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<Vec<u8>>> {
        // Check bloom filter first
        if !self.may_contain(key) {
            return Ok(None);
        }

        // Binary search in index to find the data block
        // Read the data block
        // Binary search within the data block
        // Check MVCC visibility
        
        todo!("Implement SSTable get operation")
    }

    /// Get metadata about this SSTable.
    pub fn metadata(&self) -> &SStableMetadata {
        &self.metadata
    }
}

/// SSTable writer for creating new SSTables.
pub struct SStableWriter<FS: FileSystem> {
    /// Pager for writing pages
    pager: Arc<Pager<FS>>,
    
    /// SSTable ID
    id: SStableId,
    
    /// Level in the LSM tree
    level: u32,
    
    /// Configuration
    config: SStableConfig,
    
    /// Bloom filter builder
    bloom_builder: Option<BloomFilterBuilder>,
    
    /// Current data being written
    entries: Vec<(Vec<u8>, VersionChain)>,
    
    /// First page ID allocated for this SSTable
    first_page_id: Option<PageId>,
    
    /// Pages allocated for this SSTable
    pages: Vec<PageId>,
}

impl<FS: FileSystem> SStableWriter<FS> {
    /// Create a new SSTable writer.
    pub fn new(
        pager: Arc<Pager<FS>>,
        id: SStableId,
        level: u32,
        config: SStableConfig,
        estimated_entries: usize,
    ) -> Self {
        let bloom_builder = if estimated_entries > 0 {
            Some(
                BloomFilterBuilder::new(estimated_entries)
                    .bits_per_key(10) // ~1% false positive rate
            )
        } else {
            None
        };

        Self {
            pager,
            id,
            level,
            config,
            bloom_builder,
            entries: Vec::new(),
            first_page_id: None,
            pages: Vec::new(),
        }
    }

    /// Add a key-value pair to the SSTable.
    ///
    /// Keys must be added in sorted order.
    pub fn add(&mut self, key: Vec<u8>, chain: VersionChain) -> TableResult<()> {
        // Verify keys are in sorted order
        if let Some((last_key, _)) = self.entries.last() {
            if &key <= last_key {
                return Err(crate::table::TableError::Other(
                    "Keys must be added in sorted order".to_string(),
                ));
            }
        }

        // Add to bloom filter - will be built when finish() is called
        // The bloom filter is built from all keys at once for efficiency

        self.entries.push((key, chain));
        Ok(())
    }

    /// Finish writing the SSTable and return its metadata.
    pub fn finish(mut self, created_lsn: LogSequenceNumber) -> TableResult<SStableMetadata> {
        if self.entries.is_empty() {
            return Err(crate::table::TableError::Other(
                "Cannot create empty SSTable".to_string(),
            ));
        }

        // Build bloom filter
        let _bloom_filter = self.bloom_builder.map(|b| b.build());

        // Get min/max keys
        let min_key = self.entries.first().unwrap().0.clone();
        let max_key = self.entries.last().unwrap().0.clone();

        // Allocate pages and write data
        // This is a simplified placeholder - full implementation would:
        // 1. Write data blocks
        // 2. Build and write index block
        // 3. Write bloom filter
        // 4. Write footer
        
        let first_page_id = self.pager.allocate_page(PageType::LsmData)?;
        self.first_page_id = Some(first_page_id);
        self.pages.push(first_page_id);

        // Create metadata
        let metadata = SStableMetadata {
            id: self.id,
            level: self.level,
            min_key,
            max_key,
            num_entries: self.entries.len() as u64,
            total_size: 0, // Would calculate actual size
            created_lsn,
            first_page_id,
            num_pages: self.pages.len() as u32,
            index_offset: 0,
            bloom_filter_offset: 0,
            footer_offset: 0,
        };

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sstable_id() {
        let id = SStableId::new(42);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(format!("{}", id), "SSTable(42)");
    }

    #[test]
    fn test_footer_serialization() {
        let metadata = SStableMetadata {
            id: SStableId::new(1),
            level: 0,
            min_key: b"aaa".to_vec(),
            max_key: b"zzz".to_vec(),
            num_entries: 100,
            total_size: 4096,
            created_lsn: LogSequenceNumber::from(42),
            first_page_id: PageId::from(10),
            num_pages: 5,
            index_offset: 3000,
            bloom_filter_offset: 3500,
            footer_offset: 3800,
        };

        let footer = SStableFooter::new(metadata, [0u8; 32]);
        let bytes = footer.to_bytes();
        
        assert_eq!(bytes.len(), SStableFooter::SIZE);
        
        let restored = SStableFooter::from_bytes(&bytes).unwrap();
        assert_eq!(restored.magic, SStableFooter::MAGIC);
        assert_eq!(restored.version, SStableFooter::VERSION);
        assert_eq!(restored.metadata.id, footer.metadata.id);
        assert_eq!(restored.metadata.level, footer.metadata.level);
        assert_eq!(restored.metadata.min_key, footer.metadata.min_key);
        assert_eq!(restored.metadata.max_key, footer.metadata.max_key);
    }

    #[test]
    fn test_data_block_empty() {
        let block = DataBlock::new();
        assert!(block.is_empty());
        assert_eq!(block.len(), 0);
        assert_eq!(block.first_key(), None);
        assert_eq!(block.last_key(), None);
    }

    #[test]
    fn test_data_block_add_entries() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        // Add entries in sorted order
        let chain1 = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        block.add(b"key1".to_vec(), chain1).unwrap();
        
        let chain2 = VersionChain::new(b"value2".to_vec(), TransactionId::from(2));
        block.add(b"key2".to_vec(), chain2).unwrap();
        
        let chain3 = VersionChain::new(b"value3".to_vec(), TransactionId::from(3));
        block.add(b"key3".to_vec(), chain3).unwrap();

        assert_eq!(block.len(), 3);
        assert_eq!(block.first_key(), Some(b"key1".as_slice()));
        assert_eq!(block.last_key(), Some(b"key3".as_slice()));
    }

    #[test]
    fn test_data_block_add_unsorted_fails() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        let chain1 = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        block.add(b"key2".to_vec(), chain1).unwrap();
        
        // Try to add a key that's not greater than the last key
        let chain2 = VersionChain::new(b"value2".to_vec(), TransactionId::from(2));
        let result = block.add(b"key1".to_vec(), chain2);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_data_block_binary_search() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            let chain = VersionChain::new(value.into_bytes(), TransactionId::from(i as u64));
            block.add(key.into_bytes(), chain).unwrap();
        }

        // Search for existing keys
        assert_eq!(block.search(b"key00"), Some(0));
        assert_eq!(block.search(b"key05"), Some(5));
        assert_eq!(block.search(b"key09"), Some(9));

        // Search for non-existing keys
        assert_eq!(block.search(b"key10"), None);
        assert_eq!(block.search(b"key"), None);
        assert_eq!(block.search(b"zzz"), None);
    }

    #[test]
    fn test_data_block_get() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        let chain1 = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        block.add(b"key1".to_vec(), chain1).unwrap();
        
        let chain2 = VersionChain::new(b"value2".to_vec(), TransactionId::from(2));
        block.add(b"key2".to_vec(), chain2).unwrap();

        // Get existing key
        let chain = block.get(b"key1").unwrap();
        assert_eq!(chain.value, b"value1");

        // Get non-existing key
        assert!(block.get(b"key3").is_none());
    }

    #[test]
    fn test_data_block_serialization_uncompressed() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let chain = VersionChain::new(value.into_bytes(), TransactionId::from(i as u64));
            block.add(key.into_bytes(), chain).unwrap();
        }

        // Serialize without compression
        let bytes = block.to_bytes(false).unwrap();
        
        // Deserialize
        let restored = DataBlock::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.len(), block.len());
        assert_eq!(restored.first_key(), block.first_key());
        assert_eq!(restored.last_key(), block.last_key());
        
        // Verify all entries
        for i in 0..5 {
            let key = format!("key{}", i);
            let chain = restored.get(key.as_bytes()).unwrap();
            assert_eq!(chain.value, format!("value{}", i).into_bytes());
        }
    }

    #[test]
    fn test_data_block_serialization_compressed() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        // Add enough data to make compression worthwhile
        for i in 0..50 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i).repeat(10); // Repeat to make it compressible
            let chain = VersionChain::new(value.into_bytes(), TransactionId::from(i as u64));
            block.add(key.into_bytes(), chain).unwrap();
        }

        // Serialize with compression
        let bytes = block.to_bytes(true).unwrap();
        
        // Deserialize
        let restored = DataBlock::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.len(), block.len());
        assert_eq!(restored.first_key(), block.first_key());
        assert_eq!(restored.last_key(), block.last_key());
        
        // Verify all entries
        for i in 0..50 {
            let key = format!("key{:03}", i);
            let chain = restored.get(key.as_bytes()).unwrap();
            let expected_value = format!("value{}", i).repeat(10);
            assert_eq!(chain.value, expected_value.into_bytes());
        }
    }

    #[test]
    fn test_data_block_checksum_validation() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        let chain = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        block.add(b"key1".to_vec(), chain).unwrap();

        let mut bytes = block.to_bytes(false).unwrap();
        
        // Corrupt the data (after the header)
        if bytes.len() > DataBlock::HEADER_SIZE + 10 {
            bytes[DataBlock::HEADER_SIZE + 10] ^= 0xFF;
        }
        
        // Deserialization should fail due to checksum mismatch
        let result = DataBlock::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::table::TableError::Corruption(_)
        ));
    }

    #[test]
    fn test_data_block_version_chain() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        // Create a version chain with multiple versions
        let mut chain = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        chain.commit(LogSequenceNumber::from(10));
        
        let chain = chain.prepend(b"value2".to_vec(), TransactionId::from(2));
        
        block.add(b"key1".to_vec(), chain).unwrap();

        // Serialize and deserialize
        let bytes = block.to_bytes(false).unwrap();
        let restored = DataBlock::from_bytes(&bytes).unwrap();
        
        // Verify version chain is preserved
        let restored_chain = restored.get(b"key1").unwrap();
        assert_eq!(restored_chain.value, b"value2");
        assert!(restored_chain.prev_version.is_some());
        
        let prev = restored_chain.prev_version.as_ref().unwrap();
        assert_eq!(prev.value, b"value1");
        assert_eq!(prev.commit_lsn, Some(LogSequenceNumber::from(10)));
    }

    #[test]
    fn test_data_block_from_entries() {
        use crate::txn::TransactionId;

        let entries = vec![
            (b"key1".to_vec(), VersionChain::new(b"value1".to_vec(), TransactionId::from(1))),
            (b"key2".to_vec(), VersionChain::new(b"value2".to_vec(), TransactionId::from(2))),
            (b"key3".to_vec(), VersionChain::new(b"value3".to_vec(), TransactionId::from(3))),
        ];

        let block = DataBlock::from_entries(entries);
        
        assert_eq!(block.len(), 3);
        assert_eq!(block.first_key(), Some(b"key1".as_slice()));
        assert_eq!(block.last_key(), Some(b"key3".as_slice()));
    }

    #[test]
    #[should_panic(expected = "Entries must be sorted by key")]
    fn test_data_block_from_entries_unsorted_panics() {
        use crate::txn::TransactionId;

        let entries = vec![
            (b"key2".to_vec(), VersionChain::new(b"value2".to_vec(), TransactionId::from(2))),
            (b"key1".to_vec(), VersionChain::new(b"value1".to_vec(), TransactionId::from(1))),
        ];

        DataBlock::from_entries(entries);
    }

    #[test]
    fn test_data_block_estimate_size() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let chain = VersionChain::new(value.into_bytes(), TransactionId::from(i as u64));
            block.add(key.into_bytes(), chain).unwrap();
        }

        let estimated = block.estimate_size();
        let actual = block.to_bytes(false).unwrap().len();
        
        // Estimate should be reasonably close to actual size
        // Allow 20% margin for estimation error
        let diff = if estimated > actual {
            estimated - actual
        } else {
            actual - estimated
        };
        
        assert!(diff < actual / 5, "Estimate {} too far from actual {}", estimated, actual);
    }

    #[test]
    fn test_data_block_truncated_data() {
        use crate::txn::TransactionId;

        let mut block = DataBlock::new();
        let chain = VersionChain::new(b"value1".to_vec(), TransactionId::from(1));
        block.add(b"key1".to_vec(), chain).unwrap();

        let bytes = block.to_bytes(false).unwrap();
        
        // Try to deserialize truncated data
        let truncated = &bytes[..bytes.len() - 10];
        let result = DataBlock::from_bytes(truncated);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_data_block_empty_serialization() {
        // Empty blocks should not be serializable in practice,
        // but let's test the edge case
        let block = DataBlock::new();
        let bytes = block.to_bytes(false).unwrap();
        
        let restored = DataBlock::from_bytes(&bytes).unwrap();
        assert!(restored.is_empty());
    }

    // ===== IndexBlock Tests =====

    #[test]
    fn test_index_block_empty() {
        let block = IndexBlock::new();
        assert!(block.is_empty());
        assert_eq!(block.len(), 0);
        assert!(block.entries().is_empty());
    }

    #[test]
    fn test_index_block_add_entries() {
        let mut block = IndexBlock::new();
        
        block.add(b"apple".to_vec(), PageId::from(1), 0).unwrap();
        block.add(b"banana".to_vec(), PageId::from(2), 100).unwrap();
        block.add(b"cherry".to_vec(), PageId::from(3), 200).unwrap();
        
        assert_eq!(block.len(), 3);
        assert!(!block.is_empty());
        
        let entries = block.entries();
        assert_eq!(entries[0].first_key, b"apple");
        assert_eq!(entries[0].page_id, PageId::from(1));
        assert_eq!(entries[0].offset, 0);
        
        assert_eq!(entries[1].first_key, b"banana");
        assert_eq!(entries[1].page_id, PageId::from(2));
        assert_eq!(entries[1].offset, 100);
        
        assert_eq!(entries[2].first_key, b"cherry");
        assert_eq!(entries[2].page_id, PageId::from(3));
        assert_eq!(entries[2].offset, 200);
    }

    #[test]
    fn test_index_block_add_unsorted_fails() {
        let mut block = IndexBlock::new();
        
        block.add(b"banana".to_vec(), PageId::from(1), 0).unwrap();
        
        // Try to add a key that's not greater than the last key
        let result = block.add(b"apple".to_vec(), PageId::from(2), 100);
        assert!(result.is_err());
        
        // Try to add the same key again
        let result = block.add(b"banana".to_vec(), PageId::from(3), 200);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_block_binary_search() {
        let mut block = IndexBlock::new();
        
        // Add entries for data blocks starting with these keys
        block.add(b"apple".to_vec(), PageId::from(1), 0).unwrap();
        block.add(b"dog".to_vec(), PageId::from(2), 100).unwrap();
        block.add(b"monkey".to_vec(), PageId::from(3), 200).unwrap();
        block.add(b"zebra".to_vec(), PageId::from(4), 300).unwrap();
        
        // Search for keys before first block
        assert_eq!(block.search(b"aaa"), None);
        assert_eq!(block.search(b"aardvark"), None);
        
        // Search for keys in first block (>= "apple", < "dog")
        assert_eq!(block.search(b"apple"), Some(0));
        assert_eq!(block.search(b"banana"), Some(0));
        assert_eq!(block.search(b"cat"), Some(0));
        
        // Search for keys in second block (>= "dog", < "monkey")
        assert_eq!(block.search(b"dog"), Some(1));
        assert_eq!(block.search(b"elephant"), Some(1));
        assert_eq!(block.search(b"lion"), Some(1));
        
        // Search for keys in third block (>= "monkey", < "zebra")
        assert_eq!(block.search(b"monkey"), Some(2));
        assert_eq!(block.search(b"panda"), Some(2));
        
        // Search for keys in fourth block (>= "zebra")
        assert_eq!(block.search(b"zebra"), Some(3));
        assert_eq!(block.search(b"zoo"), Some(3));
    }

    #[test]
    fn test_index_block_search_empty() {
        let block = IndexBlock::new();
        assert_eq!(block.search(b"any_key"), None);
    }

    #[test]
    fn test_index_block_search_single_entry() {
        let mut block = IndexBlock::new();
        block.add(b"middle".to_vec(), PageId::from(1), 0).unwrap();
        
        // Key before the entry
        assert_eq!(block.search(b"aaa"), None);
        
        // Key equal to the entry
        assert_eq!(block.search(b"middle"), Some(0));
        
        // Key after the entry
        assert_eq!(block.search(b"zzz"), Some(0));
    }

    #[test]
    fn test_index_block_get() {
        let mut block = IndexBlock::new();
        
        block.add(b"apple".to_vec(), PageId::from(1), 0).unwrap();
        block.add(b"banana".to_vec(), PageId::from(2), 100).unwrap();
        
        let entry = block.get(0).unwrap();
        assert_eq!(entry.first_key, b"apple");
        assert_eq!(entry.page_id, PageId::from(1));
        assert_eq!(entry.offset, 0);
        
        let entry = block.get(1).unwrap();
        assert_eq!(entry.first_key, b"banana");
        
        assert!(block.get(2).is_none());
    }

    #[test]
    fn test_index_block_serialization() {
        let mut block = IndexBlock::new();
        
        block.add(b"apple".to_vec(), PageId::from(1), 0).unwrap();
        block.add(b"banana".to_vec(), PageId::from(2), 100).unwrap();
        block.add(b"cherry".to_vec(), PageId::from(3), 200).unwrap();
        
        let bytes = block.to_bytes().unwrap();
        
        // Verify header
        let num_entries = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(num_entries, 3);
        
        // Deserialize and verify
        let restored = IndexBlock::from_bytes(&bytes).unwrap();
        assert_eq!(restored.len(), 3);
        
        let entries = restored.entries();
        assert_eq!(entries[0].first_key, b"apple");
        assert_eq!(entries[0].page_id, PageId::from(1));
        assert_eq!(entries[0].offset, 0);
        
        assert_eq!(entries[1].first_key, b"banana");
        assert_eq!(entries[1].page_id, PageId::from(2));
        assert_eq!(entries[1].offset, 100);
        
        assert_eq!(entries[2].first_key, b"cherry");
        assert_eq!(entries[2].page_id, PageId::from(3));
        assert_eq!(entries[2].offset, 200);
    }

    #[test]
    fn test_index_block_checksum_validation() {
        let mut block = IndexBlock::new();
        block.add(b"test".to_vec(), PageId::from(1), 0).unwrap();
        
        let mut bytes = block.to_bytes().unwrap();
        
        // Corrupt the checksum
        bytes[4] ^= 0xFF;
        
        let result = IndexBlock::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_block_from_entries() {
        let entries = vec![
            IndexEntry {
                first_key: b"apple".to_vec(),
                page_id: PageId::from(1),
                offset: 0,
            },
            IndexEntry {
                first_key: b"banana".to_vec(),
                page_id: PageId::from(2),
                offset: 100,
            },
        ];
        
        let block = IndexBlock::from_entries(entries);
        assert_eq!(block.len(), 2);
        assert_eq!(block.entries()[0].first_key, b"apple");
        assert_eq!(block.entries()[1].first_key, b"banana");
    }

    #[test]
    #[should_panic(expected = "Index entries must be sorted by key")]
    fn test_index_block_from_entries_unsorted_panics() {
        let entries = vec![
            IndexEntry {
                first_key: b"banana".to_vec(),
                page_id: PageId::from(1),
                offset: 0,
            },
            IndexEntry {
                first_key: b"apple".to_vec(),
                page_id: PageId::from(2),
                offset: 100,
            },
        ];
        
        IndexBlock::from_entries(entries);
    }

    #[test]
    fn test_index_block_estimate_size() {
        let mut block = IndexBlock::new();
        
        // Empty block
        let size = block.estimate_size();
        assert_eq!(size, IndexBlock::HEADER_SIZE);
        
        // Add entries
        block.add(b"apple".to_vec(), PageId::from(1), 0).unwrap();
        block.add(b"banana".to_vec(), PageId::from(2), 100).unwrap();
        
        let size = block.estimate_size();
        // Header + 2 entries * (4 + key_len + 8 + 8)
        let expected = IndexBlock::HEADER_SIZE 
            + (4 + 5 + 8 + 8)  // "apple"
            + (4 + 6 + 8 + 8); // "banana"
        assert_eq!(size, expected);
    }

    #[test]
    fn test_index_block_truncated_data() {
        let mut block = IndexBlock::new();
        block.add(b"test".to_vec(), PageId::from(1), 0).unwrap();
        
        let bytes = block.to_bytes().unwrap();
        
        // Truncate at various points
        let result = IndexBlock::from_bytes(&bytes[..10]);
        assert!(result.is_err());
        
        let result = IndexBlock::from_bytes(&bytes[..IndexBlock::HEADER_SIZE]);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_block_empty_serialization() {
        let block = IndexBlock::new();
        let bytes = block.to_bytes().unwrap();
        
        let restored = IndexBlock::from_bytes(&bytes).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_index_block_large_keys() {
        let mut block = IndexBlock::new();
        
        // Add entries with large keys
        let large_key1 = vec![b'a'; 1000];
        let large_key2 = vec![b'b'; 1000];
        
        block.add(large_key1.clone(), PageId::from(1), 0).unwrap();
        block.add(large_key2.clone(), PageId::from(2), 100).unwrap();
        
        let bytes = block.to_bytes().unwrap();
        let restored = IndexBlock::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.len(), 2);
        assert_eq!(restored.entries()[0].first_key, large_key1);
        assert_eq!(restored.entries()[1].first_key, large_key2);
    }

    #[test]
    fn test_index_block_many_entries() {
        let mut block = IndexBlock::new();
        
        // Add many entries
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            block.add(key, PageId::from(i as u64), i as u64 * 100).unwrap();
        }
        
        assert_eq!(block.len(), 100);
        
        // Test serialization
        let bytes = block.to_bytes().unwrap();
        let restored = IndexBlock::from_bytes(&bytes).unwrap();
        
        assert_eq!(restored.len(), 100);
        
        // Verify a few entries
        assert_eq!(restored.entries()[0].first_key, b"key_0000");
        assert_eq!(restored.entries()[50].first_key, b"key_0050");
        assert_eq!(restored.entries()[99].first_key, b"key_0099");
        
        // Test binary search
        assert_eq!(restored.search(b"key_0025"), Some(25));
        assert_eq!(restored.search(b"key_0075"), Some(75));
    }

    #[test]
    fn test_index_block_unsorted_deserialization_fails() {
        // Manually create bytes with unsorted entries
        let mut bytes = Vec::new();
        
        // Header: num_entries = 2
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 32]); // Placeholder checksum
        
        let entries_start = bytes.len();
        
        // Entry 1: "banana"
        bytes.extend_from_slice(&6u32.to_le_bytes());
        bytes.extend_from_slice(b"banana");
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        
        // Entry 2: "apple" (unsorted!)
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(b"apple");
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&100u64.to_le_bytes());
        
        // Calculate and write checksum
        let entries_data = &bytes[entries_start..];
        let mut hasher = Sha256::new();
        hasher.update(entries_data);
        let checksum = hasher.finalize();
        bytes[4..36].copy_from_slice(&checksum);
        
        // Should fail because entries are not sorted
        let result = IndexBlock::from_bytes(&bytes);
        assert!(result.is_err());
    }
}

// Made with Bob
