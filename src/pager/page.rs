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

//! Page structures and types

use crate::pager::{CompressionType, EncryptionType, PagerError, PagerResult};
use sha2::{Digest, Sha256};

/// Page identifier (0-based)
pub type PageId = u64;

/// Page type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Free page (available for allocation)
    Free = 0,
    /// Superblock page (database metadata)
    Superblock = 1,
    /// Free list page (tracks free pages)
    FreeList = 2,
    /// B-Tree internal node
    BTreeInternal = 3,
    /// B-Tree leaf node
    BTreeLeaf = 4,
    /// Overflow page (for large values)
    Overflow = 5,
    /// LSM level metadata
    LsmMeta = 6,
    /// LSM data page
    LsmData = 7,
}

impl PageType {
    /// Convert from u8 representation
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(PageType::Free),
            1 => Some(PageType::Superblock),
            2 => Some(PageType::FreeList),
            3 => Some(PageType::BTreeInternal),
            4 => Some(PageType::BTreeLeaf),
            5 => Some(PageType::Overflow),
            6 => Some(PageType::LsmMeta),
            7 => Some(PageType::LsmData),
            _ => None,
        }
    }

    /// Convert to u8 representation
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Page header (32 bytes)
///
/// Layout:
/// - Bytes 0-7: Page ID (u64)
/// - Byte 8: Page type (u8)
/// - Byte 9: Compression type (u8)
/// - Byte 10: Encryption type (u8)
/// - Byte 11: Flags (u8)
/// - Bytes 12-15: Uncompressed size (u32)
/// - Bytes 16-19: Compressed size (u32)
/// - Bytes 20-23: Reserved (u32)
/// - Bytes 24-31: Reserved (u64)
#[derive(Debug, Clone)]
pub struct PageHeader {
    /// Page identifier
    pub page_id: PageId,
    /// Page type
    pub page_type: PageType,
    /// Compression type used
    pub compression: CompressionType,
    /// Encryption type used
    pub encryption: EncryptionType,
    /// Flags (reserved for future use)
    pub flags: u8,
    /// Uncompressed data size
    pub uncompressed_size: u32,
    /// Compressed data size (0 if not compressed)
    pub compressed_size: u32,
}

impl PageHeader {
    /// Size of the page header in bytes
    pub const SIZE: usize = 32;

    /// Create a new page header
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        Self {
            page_id,
            page_type,
            compression: CompressionType::None,
            encryption: EncryptionType::None,
            flags: 0,
            uncompressed_size: 0,
            compressed_size: 0,
        }
    }

    /// Serialize the header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        
        // Page ID (8 bytes)
        bytes[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        
        // Page type (1 byte)
        bytes[8] = self.page_type.to_u8();
        
        // Compression type (1 byte)
        bytes[9] = self.compression.to_u8();
        
        // Encryption type (1 byte)
        bytes[10] = self.encryption.to_u8();
        
        // Flags (1 byte)
        bytes[11] = self.flags;
        
        // Uncompressed size (4 bytes)
        bytes[12..16].copy_from_slice(&self.uncompressed_size.to_le_bytes());
        
        // Compressed size (4 bytes)
        bytes[16..20].copy_from_slice(&self.compressed_size.to_le_bytes());
        
        // Reserved bytes remain 0
        
        bytes
    }

    /// Deserialize the header from bytes
    pub fn from_bytes(bytes: &[u8]) -> PagerResult<Self> {
        if bytes.len() < Self::SIZE {
            return Err(PagerError::InternalError(
                "Insufficient bytes for page header".to_string(),
            ));
        }

        let page_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        
        let page_type = PageType::from_u8(bytes[8])
            .ok_or_else(|| PagerError::InvalidPageType(bytes[8]))?;
        
        let compression = CompressionType::from_u8(bytes[9])
            .ok_or_else(|| PagerError::InternalError(format!("Invalid compression type: {}", bytes[9])))?;
        
        let encryption = EncryptionType::from_u8(bytes[10])
            .ok_or_else(|| PagerError::InternalError(format!("Invalid encryption type: {}", bytes[10])))?;
        
        let flags = bytes[11];
        
        let uncompressed_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let compressed_size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());

        Ok(Self {
            page_id,
            page_type,
            compression,
            encryption,
            flags,
            uncompressed_size,
            compressed_size,
        })
    }
}

/// Page structure
///
/// Layout: [Header: 32B][Data: variable][Checksum: 32B]
pub struct Page {
    /// Page header
    pub header: PageHeader,
    /// Page data (uncompressed, unencrypted)
    pub data: Vec<u8>,
}

impl Page {
    /// Checksum size (SHA-256)
    pub const CHECKSUM_SIZE: usize = 32;

    /// Create a new page
    pub fn new(page_id: PageId, page_type: PageType, data_capacity: usize) -> Self {
        Self {
            header: PageHeader::new(page_id, page_type),
            data: Vec::with_capacity(data_capacity),
        }
    }

    /// Calculate SHA-256 checksum of the page
    pub fn calculate_checksum(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(&self.header.to_bytes());
        // Only hash the actual data length, not padding
        hasher.update(&(self.data.len() as u32).to_le_bytes());
        hasher.update(&self.data);
        hasher.finalize().into()
    }

    /// Verify the checksum of the page
    pub fn verify_checksum(&self, expected: &[u8; 32]) -> bool {
        let actual = self.calculate_checksum();
        actual == *expected
    }

    /// Serialize the page to bytes (with checksum)
    pub fn to_bytes(&self, page_size: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(page_size);
        
        // Update header with actual data size
        let mut header = self.header.clone();
        header.uncompressed_size = self.data.len() as u32;
        
        // Write header
        bytes.extend_from_slice(&header.to_bytes());
        
        // Write data
        bytes.extend_from_slice(&self.data);
        
        // Pad to page_size - CHECKSUM_SIZE
        let data_end = page_size - Self::CHECKSUM_SIZE;
        if bytes.len() < data_end {
            bytes.resize(data_end, 0);
        }
        
        // Calculate and append checksum (using updated header)
        let page_with_header = Self {
            header,
            data: self.data.clone(),
        };
        let checksum = page_with_header.calculate_checksum();
        bytes.extend_from_slice(&checksum);
        
        bytes
    }

    /// Deserialize the page from bytes (with checksum verification)
    pub fn from_bytes(bytes: &[u8], verify_checksum: bool) -> PagerResult<Self> {
        if bytes.len() < PageHeader::SIZE + Self::CHECKSUM_SIZE {
            return Err(PagerError::InternalError(
                "Insufficient bytes for page".to_string(),
            ));
        }

        // Parse header
        let header = PageHeader::from_bytes(&bytes[0..PageHeader::SIZE])?;
        let page_id = header.page_id;
        let data_len = header.uncompressed_size as usize;

        // Extract data (only the actual data, not padding)
        let data_start = PageHeader::SIZE;
        let data_end = data_start + data_len;
        
        if data_end > bytes.len() - Self::CHECKSUM_SIZE {
            return Err(PagerError::InternalError(
                "Invalid data length in header".to_string(),
            ));
        }
        
        let data = bytes[data_start..data_end].to_vec();

        // Extract checksum
        let checksum_start = bytes.len() - Self::CHECKSUM_SIZE;
        let checksum: [u8; 32] = bytes[checksum_start..].try_into().map_err(|_| {
            PagerError::InternalError("Invalid checksum size".to_string())
        })?;

        let page = Self { header, data };

        // Verify checksum if requested
        if verify_checksum && !page.verify_checksum(&checksum) {
            return Err(PagerError::ChecksumMismatch(page_id));
        }

        Ok(page)
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.header.page_id
    }

    /// Get the page type
    pub fn page_type(&self) -> PageType {
        self.header.page_type
    }

    /// Get the data slice
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable data slice
    pub fn data_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_type_conversion() {
        assert_eq!(PageType::from_u8(0), Some(PageType::Free));
        assert_eq!(PageType::from_u8(1), Some(PageType::Superblock));
        assert_eq!(PageType::from_u8(255), None);
        
        assert_eq!(PageType::Free.to_u8(), 0);
        assert_eq!(PageType::Superblock.to_u8(), 1);
    }

    #[test]
    fn test_page_header_serialization() {
        let header = PageHeader::new(42, PageType::BTreeLeaf);
        let bytes = header.to_bytes();
        let deserialized = PageHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(deserialized.page_id, 42);
        assert_eq!(deserialized.page_type, PageType::BTreeLeaf);
    }

    #[test]
    fn test_page_checksum() {
        let mut page = Page::new(1, PageType::BTreeLeaf, 100);
        page.data.extend_from_slice(b"test data");
        
        let checksum = page.calculate_checksum();
        assert!(page.verify_checksum(&checksum));
        
        // Modify data and verify checksum fails
        page.data[0] = b'X';
        assert!(!page.verify_checksum(&checksum));
    }

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new(5, PageType::BTreeInternal, 100);
        page.data.extend_from_slice(b"test page data");
        
        let bytes = page.to_bytes(4096);
        assert_eq!(bytes.len(), 4096);
        
        let deserialized = Page::from_bytes(&bytes, true).unwrap();
        assert_eq!(deserialized.page_id(), 5);
        assert_eq!(deserialized.page_type(), PageType::BTreeInternal);
        assert_eq!(deserialized.data(), b"test page data");
    }
}

// Made with Bob
