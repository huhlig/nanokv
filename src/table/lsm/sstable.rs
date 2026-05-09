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
}

// Made with Bob
