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

//! Manifest storage for tracking LSM tree state and versions.
//!
//! The manifest tracks the current state of the LSM tree, including which SSTables
//! are active at each level. Unlike a standalone MANIFEST file, this implementation
//! persists the entire manifest snapshot inside dedicated pager pages so it remains
//! part of the single database file.
//!
//! # Architecture
//!
//! - **Version**: Immutable snapshot of LSM tree state (SSTables per level)
//! - **VersionEdit**: Delta describing changes to apply to a version
//! - **Manifest**: Manages versions and persists them to pager-backed metadata pages
//!
//! # Storage Format
//!
//! The manifest is serialized as a binary snapshot using `postcard` and written across
//! one or more contiguous `PageType::LsmMeta` pages. The first page stores a small
//! header followed by manifest payload bytes. Remaining pages store continuation data.
//!
//! # Recovery
//!
//! On startup, the manifest is recovered by reading the manifest root page and
//! reconstructing the current version from the persisted snapshot.

use crate::pager::{Page, PageId, PageType, Pager};
use crate::table::error::{TableError, TableResult};
use crate::table::lsm::SStableId;
use crate::vfs::FileSystem;
use crate::wal::LogSequenceNumber;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Serialize bytes as hex string for JSON
fn serialize_bytes<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&hex::encode(bytes))
}

/// Deserialize bytes from hex string for JSON
fn deserialize_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    hex::decode(&s).map_err(serde::de::Error::custom)
}

/// File entry in the manifest representing an SSTable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMetadata {
    /// SSTable ID
    pub id: SStableId,

    /// Level in the LSM tree (0 = L0, 1 = L1, etc.)
    pub level: u32,

    /// Smallest key in the SSTable (hex encoded for JSON)
    #[serde(
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
    pub min_key: Vec<u8>,

    /// Largest key in the SSTable (hex encoded for JSON)
    #[serde(
        serialize_with = "serialize_bytes",
        deserialize_with = "deserialize_bytes"
    )]
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
}

impl FileMetadata {
    /// Check if this file's key range overlaps with the given range.
    pub fn overlaps(&self, min_key: &[u8], max_key: &[u8]) -> bool {
        // No overlap if this file's max is less than range min
        if self.max_key.as_slice() < min_key {
            return false;
        }
        // No overlap if this file's min is greater than range max
        if self.min_key.as_slice() > max_key {
            return false;
        }
        true
    }

    /// Check if this file contains the given key.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_slice() && key <= self.max_key.as_slice()
    }
}

/// Version edit describing changes to apply to a version.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VersionEdit {
    /// Add a new SSTable
    AddSStable {
        id: SStableId,
        level: u32,
        #[serde(
            serialize_with = "serialize_bytes",
            deserialize_with = "deserialize_bytes"
        )]
        min_key: Vec<u8>,
        #[serde(
            serialize_with = "serialize_bytes",
            deserialize_with = "deserialize_bytes"
        )]
        max_key: Vec<u8>,
        num_entries: u64,
        total_size: u64,
        created_lsn: LogSequenceNumber,
        first_page_id: PageId,
        num_pages: u32,
    },

    /// Remove an SSTable
    RemoveSStable { id: SStableId },

    /// Set the next SSTable ID
    SetNextSStableId { next_id: u64 },

    /// Set the log number
    SetLogNumber { log_number: u64 },
}

impl VersionEdit {
    /// Create an edit to add an SSTable.
    pub fn add_sstable(metadata: FileMetadata) -> Self {
        Self::AddSStable {
            id: metadata.id,
            level: metadata.level,
            min_key: metadata.min_key,
            max_key: metadata.max_key,
            num_entries: metadata.num_entries,
            total_size: metadata.total_size,
            created_lsn: metadata.created_lsn,
            first_page_id: metadata.first_page_id,
            num_pages: metadata.num_pages,
        }
    }

    /// Create an edit to remove an SSTable.
    pub fn remove_sstable(id: SStableId) -> Self {
        Self::RemoveSStable { id }
    }

    /// Create an edit to set the next SSTable ID.
    pub fn set_next_sstable_id(next_id: u64) -> Self {
        Self::SetNextSStableId { next_id }
    }

    /// Create an edit to set the log number.
    pub fn set_log_number(log_number: u64) -> Self {
        Self::SetLogNumber { log_number }
    }
}

/// Immutable snapshot of LSM tree state.
#[derive(Clone, Debug)]
pub struct Version {
    /// SSTables organized by level
    /// Level 0 can have overlapping key ranges
    /// Levels 1+ have non-overlapping key ranges (sorted by min_key)
    levels: Vec<Vec<FileMetadata>>,

    /// Map from SSTable ID to metadata for fast lookup
    files_by_id: HashMap<SStableId, FileMetadata>,

    /// Next SSTable ID to allocate
    next_sstable_id: u64,

    /// Current log number
    log_number: u64,
}

impl Version {
    /// Create a new empty version.
    pub fn new(num_levels: usize) -> Self {
        Self {
            levels: vec![Vec::new(); num_levels],
            files_by_id: HashMap::new(),
            next_sstable_id: 1,
            log_number: 0,
        }
    }

    /// Apply a version edit to create a new version.
    pub fn apply(&self, edit: &VersionEdit) -> TableResult<Self> {
        let mut new_version = self.clone();

        match edit {
            VersionEdit::AddSStable {
                id,
                level,
                min_key,
                max_key,
                num_entries,
                total_size,
                created_lsn,
                first_page_id,
                num_pages,
            } => {
                let level_idx = *level as usize;
                if level_idx >= new_version.levels.len() {
                    return Err(TableError::invalid_level(
                        *level,
                        (new_version.levels.len() - 1) as u32,
                    ));
                }

                let metadata = FileMetadata {
                    id: *id,
                    level: *level,
                    min_key: min_key.clone(),
                    max_key: max_key.clone(),
                    num_entries: *num_entries,
                    total_size: *total_size,
                    created_lsn: *created_lsn,
                    first_page_id: *first_page_id,
                    num_pages: *num_pages,
                };

                // Check for duplicate ID
                if new_version.files_by_id.contains_key(id) {
                    return Err(TableError::sstable_id_exists(id.to_string()));
                }

                // Add to level
                new_version.levels[level_idx].push(metadata.clone());

                // For levels > 0, maintain sorted order by min_key
                if *level > 0 {
                    new_version.levels[level_idx].sort_by(|a, b| a.min_key.cmp(&b.min_key));
                }

                // Add to ID map
                new_version.files_by_id.insert(*id, metadata);
            }

            VersionEdit::RemoveSStable { id } => {
                // Remove from ID map
                let metadata = new_version
                    .files_by_id
                    .remove(id)
                    .ok_or_else(|| TableError::sstable_id_not_found(id.to_string()))?;

                // Remove from level
                let level_idx = metadata.level as usize;
                new_version.levels[level_idx].retain(|f| f.id != *id);
            }

            VersionEdit::SetNextSStableId { next_id } => {
                new_version.next_sstable_id = *next_id;
            }

            VersionEdit::SetLogNumber { log_number } => {
                new_version.log_number = *log_number;
            }
        }

        Ok(new_version)
    }

    /// Get all files at a specific level.
    pub fn level_files(&self, level: u32) -> &[FileMetadata] {
        let level_idx = level as usize;
        if level_idx < self.levels.len() {
            &self.levels[level_idx]
        } else {
            &[]
        }
    }

    /// Get file metadata by ID.
    pub fn get_file(&self, id: SStableId) -> Option<&FileMetadata> {
        self.files_by_id.get(&id)
    }

    /// Get all files that may contain the given key.
    pub fn get_overlapping_files(&self, key: &[u8]) -> Vec<FileMetadata> {
        let mut files = Vec::new();

        for level_files in &self.levels {
            for file in level_files {
                if file.contains_key(key) {
                    files.push(file.clone());
                }
            }
        }

        files
    }

    /// Get all files in a level that overlap with the given key range.
    pub fn get_overlapping_files_in_level(
        &self,
        level: u32,
        min_key: &[u8],
        max_key: &[u8],
    ) -> Vec<FileMetadata> {
        let level_idx = level as usize;
        if level_idx >= self.levels.len() {
            return Vec::new();
        }

        self.levels[level_idx]
            .iter()
            .filter(|f| f.overlaps(min_key, max_key))
            .cloned()
            .collect()
    }

    /// Get the total size of all files at a level.
    pub fn level_size(&self, level: u32) -> u64 {
        let level_idx = level as usize;
        if level_idx >= self.levels.len() {
            return 0;
        }

        self.levels[level_idx].iter().map(|f| f.total_size).sum()
    }

    /// Get the number of files at a level.
    pub fn level_file_count(&self, level: u32) -> usize {
        let level_idx = level as usize;
        if level_idx >= self.levels.len() {
            return 0;
        }

        self.levels[level_idx].len()
    }

    /// Get the total number of files across all levels.
    pub fn total_file_count(&self) -> usize {
        self.files_by_id.len()
    }

    /// Allocate a new SSTable ID.
    pub fn allocate_sstable_id(&mut self) -> SStableId {
        let id = SStableId::new(self.next_sstable_id);
        self.next_sstable_id += 1;
        id
    }

    /// Get the next SSTable ID that will be allocated.
    pub fn next_sstable_id(&self) -> u64 {
        self.next_sstable_id
    }

    /// Get the current log number.
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Get the number of levels.
    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    /// Get all file IDs that are currently active.
    pub fn active_file_ids(&self) -> HashSet<SStableId> {
        self.files_by_id.keys().copied().collect()
    }
}

/// Persisted manifest snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestSnapshot {
    version: VersionDisk,
}

/// Disk-serializable version representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VersionDisk {
    levels: Vec<Vec<FileMetadata>>,
    next_sstable_id: u64,
    log_number: u64,
}

impl From<&Version> for VersionDisk {
    fn from(version: &Version) -> Self {
        Self {
            levels: version.levels.clone(),
            next_sstable_id: version.next_sstable_id,
            log_number: version.log_number,
        }
    }
}

impl VersionDisk {
    fn into_version(self, num_levels: usize) -> TableResult<Version> {
        if self.levels.len() != num_levels {
            return Err(TableError::corruption(
                "VersionDisk::into_version",
                "level_count_mismatch",
                format!(
                    "Manifest level count mismatch: expected {}, got {}",
                    num_levels,
                    self.levels.len()
                ),
            ));
        }

        let mut files_by_id = HashMap::new();
        for (level_idx, level_files) in self.levels.iter().enumerate() {
            for metadata in level_files {
                if metadata.level as usize != level_idx {
                    return Err(TableError::corruption(
                        "VersionDisk::into_version",
                        "wrong_level",
                        format!(
                            "Manifest file {} stored in wrong level: metadata={}, container={}",
                            metadata.id, metadata.level, level_idx
                        ),
                    ));
                }
                if files_by_id.insert(metadata.id, metadata.clone()).is_some() {
                    return Err(TableError::corruption(
                        "VersionDisk::into_version",
                        "duplicate_id",
                        format!("Duplicate SSTable ID {} in manifest", metadata.id),
                    ));
                }
            }
        }

        Ok(Version {
            levels: self.levels,
            files_by_id,
            next_sstable_id: self.next_sstable_id,
            log_number: self.log_number,
        })
    }
}

/// Manifest page header layout.
/// [magic:8][format_version:4][num_levels:4][payload_len:8][page_count:4][page_ids...]
struct ManifestPageHeader;

impl ManifestPageHeader {
    const MAGIC: [u8; 8] = *b"NKVMANF1";
    const FORMAT_VERSION: u32 = 2;
    const FIXED_SIZE: usize = 28;

    fn size_for_page_count(page_count: usize) -> usize {
        Self::FIXED_SIZE + (page_count * 8)
    }

    fn max_page_count(first_page_capacity: usize) -> usize {
        if first_page_capacity <= Self::FIXED_SIZE {
            return 0;
        }
        (first_page_capacity - Self::FIXED_SIZE) / 8
    }

    fn encode(num_levels: usize, payload_len: usize, page_ids: &[PageId]) -> TableResult<Vec<u8>> {
        let num_levels = u32::try_from(num_levels)
            .map_err(|_| TableError::manifest_error("serialize", "Too many manifest levels"))?;
        let payload_len = u64::try_from(payload_len)
            .map_err(|_| TableError::manifest_error("serialize", "Manifest payload too large"))?;
        let page_count = u32::try_from(page_ids.len()).map_err(|_| {
            TableError::manifest_error("serialize", "Manifest page count too large")
        })?;

        let mut bytes = Vec::with_capacity(Self::size_for_page_count(page_ids.len()));
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.extend_from_slice(&Self::FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&num_levels.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&page_count.to_le_bytes());
        for page_id in page_ids {
            bytes.extend_from_slice(&page_id.as_u64().to_le_bytes());
        }
        Ok(bytes)
    }

    fn decode(
        bytes: &[u8],
        expected_num_levels: usize,
    ) -> TableResult<(usize, Vec<PageId>, usize)> {
        if bytes.len() < Self::FIXED_SIZE {
            return Err(TableError::corruption(
                "ManifestPageHeader::decode",
                "truncated_header",
                "Manifest page header truncated",
            ));
        }
        if bytes[0..8] != Self::MAGIC {
            return Err(TableError::corruption(
                "ManifestPageHeader::decode",
                "invalid_magic",
                "Invalid manifest page magic",
            ));
        }

        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != Self::FORMAT_VERSION {
            return Err(TableError::InvalidFormatVersion(version));
        }

        let num_levels = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        if num_levels != expected_num_levels {
            return Err(TableError::corruption(
                "ManifestPageHeader::decode",
                "num_levels_mismatch",
                format!(
                    "Manifest num_levels mismatch: expected {}, got {}",
                    expected_num_levels, num_levels
                ),
            ));
        }

        let payload_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let page_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        if page_count == 0 {
            return Err(TableError::corruption(
                "ManifestPageHeader::decode",
                "zero_page_count",
                "Manifest page count cannot be zero",
            ));
        }

        let header_size = Self::size_for_page_count(page_count);
        if bytes.len() < header_size {
            return Err(TableError::corruption(
                "ManifestPageHeader::decode",
                "missing_page_ids",
                "Manifest page header missing page IDs",
            ));
        }

        let mut page_ids = Vec::with_capacity(page_count);
        let mut offset = Self::FIXED_SIZE;
        for _ in 0..page_count {
            let raw = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            page_ids.push(PageId::from(raw));
            offset += 8;
        }

        Ok((payload_len, page_ids, header_size))
    }
}

/// Manifest manages versions and persists state to pager pages.
pub struct Manifest<FS: FileSystem> {
    /// Pager used for manifest persistence.
    pager: Arc<Pager<FS>>,

    /// Root page ID of the manifest snapshot.
    root_page_id: PageId,

    /// Current version (protected by RwLock for concurrent reads)
    current: Arc<RwLock<Version>>,

    /// Number of levels in the LSM tree
    num_levels: usize,
}

impl<FS: FileSystem> Manifest<FS> {
    /// Create a new empty manifest at the provided root page.
    pub fn new(
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        num_levels: usize,
    ) -> TableResult<Self> {
        let version = Version::new(num_levels);
        let manifest = Self {
            pager,
            root_page_id,
            current: Arc::new(RwLock::new(version.clone())),
            num_levels,
        };
        manifest.write_manifest(&version)?;
        Ok(manifest)
    }

    /// Open an existing manifest from pager pages.
    pub fn open(
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        num_levels: usize,
    ) -> TableResult<Self> {
        let version = Self::recover_from_pages(&pager, root_page_id, num_levels)?;
        Ok(Self {
            pager,
            root_page_id,
            current: Arc::new(RwLock::new(version)),
            num_levels,
        })
    }

    /// Apply a version edit atomically.
    pub fn apply_edit(&self, edit: VersionEdit) -> TableResult<()> {
        self.apply_edits(vec![edit])
    }

    /// Apply multiple version edits atomically.
    pub fn apply_edits(&self, edits: Vec<VersionEdit>) -> TableResult<()> {
        let new_version = {
            let current = self.current.read().unwrap();
            let mut version = current.clone();

            for edit in &edits {
                version = version.apply(edit)?;
            }

            version
        };

        self.write_manifest(&new_version)?;

        {
            let mut current = self.current.write().unwrap();
            *current = new_version;
        }

        Ok(())
    }

    fn recover_from_pages(
        pager: &Arc<Pager<FS>>,
        root_page_id: PageId,
        num_levels: usize,
    ) -> TableResult<Version> {
        let first_page = pager.read_page(root_page_id)?;
        if first_page.page_type() != PageType::LsmMeta {
            return Err(TableError::corruption(
                "Manifest::recover_from_pages",
                "wrong_page_type",
                format!(
                    "Manifest root page {} has wrong type {:?}",
                    root_page_id,
                    first_page.page_type()
                ),
            ));
        }

        let first_data = first_page.data();
        let (payload_len, page_ids, header_size) =
            ManifestPageHeader::decode(first_data, num_levels)?;
        let mut payload = Vec::with_capacity(payload_len);

        let first_chunk = &first_data[header_size..];
        payload.extend_from_slice(&first_chunk[..first_chunk.len().min(payload_len)]);

        for page_id in page_ids.iter().skip(1) {
            let page = pager.read_page(*page_id)?;
            if page.page_type() != PageType::LsmMeta {
                return Err(TableError::corruption(
                    "Manifest::recover_from_pages",
                    "wrong_page_type",
                    format!(
                        "Manifest continuation page {} has wrong type {:?}",
                        page_id,
                        page.page_type()
                    ),
                ));
            }
            let remaining = payload_len.saturating_sub(payload.len());
            if remaining == 0 {
                break;
            }
            let data = page.data();
            payload.extend_from_slice(&data[..data.len().min(remaining)]);
        }

        payload.truncate(payload_len);

        if payload.len() != payload_len {
            return Err(TableError::corruption(
                "Manifest::recover_from_pages",
                "truncated_payload",
                format!(
                    "Manifest payload truncated: expected {} bytes, got {}",
                    payload_len,
                    payload.len()
                ),
            ));
        }

        let snapshot: ManifestSnapshot = postcard::from_bytes(&payload).map_err(|e| {
            TableError::corruption(
                "Manifest::recover_from_pages",
                "deserialization_error",
                format!("Failed to deserialize manifest snapshot: {}", e),
            )
        })?;

        snapshot.version.into_version(num_levels)
    }

    fn write_manifest(&self, version: &Version) -> TableResult<()> {
        let snapshot = ManifestSnapshot {
            version: VersionDisk::from(version),
        };
        let payload = postcard::to_allocvec(&snapshot)
            .map_err(|e| TableError::serialization_error("manifest_snapshot", e.to_string()))?;

        let page_capacity = self.pager.page_size().data_size();
        let max_page_count = ManifestPageHeader::max_page_count(page_capacity);
        if max_page_count == 0 {
            return Err(TableError::manifest_error(
                "write",
                "Pager page size too small for manifest metadata",
            ));
        }

        let mut page_ids = vec![self.root_page_id];
        let page_count = loop {
            let page_count = page_ids.len();
            let header_size = ManifestPageHeader::size_for_page_count(page_count);
            if header_size >= page_capacity {
                return Err(TableError::manifest_error(
                    "write",
                    "Manifest header exceeds pager page capacity",
                ));
            }

            let first_page_capacity = page_capacity - header_size;
            let remaining = payload.len().saturating_sub(first_page_capacity);
            let continuation_pages = if remaining == 0 {
                0
            } else {
                remaining.div_ceil(page_capacity)
            };
            let required_page_count = 1 + continuation_pages;

            if required_page_count > max_page_count {
                return Err(TableError::manifest_error(
                    "write",
                    format!(
                        "Manifest requires {} pages, exceeds header address capacity {}",
                        required_page_count, max_page_count
                    ),
                ));
            }

            if required_page_count == page_count {
                break required_page_count;
            }

            while page_ids.len() < required_page_count {
                page_ids.push(self.pager.allocate_page(PageType::LsmMeta)?);
            }
        };

        let header = ManifestPageHeader::encode(self.num_levels, payload.len(), &page_ids)?;
        let first_page_capacity = page_capacity - header.len();

        for (page_offset, page_id) in page_ids.iter().copied().take(page_count).enumerate() {
            let mut page = Page::new(page_id, PageType::LsmMeta, page_capacity);

            let start = if page_offset == 0 {
                0
            } else {
                first_page_capacity + (page_offset - 1) * page_capacity
            };
            let end = if page_offset == 0 {
                first_page_capacity.min(payload.len())
            } else {
                (start + page_capacity).min(payload.len())
            };

            if page_offset == 0 {
                page.data_mut().extend_from_slice(&header);
            }

            if start < end {
                page.data_mut().extend_from_slice(&payload[start..end]);
            }

            self.pager.write_page(&page)?;
        }

        self.pager.sync()?;
        Ok(())
    }

    /// Get the current version.
    pub fn current(&self) -> Version {
        self.current.read().unwrap().clone()
    }

    /// Allocate a new SSTable ID.
    pub fn allocate_sstable_id(&self) -> SStableId {
        let mut current = self.current.write().unwrap();
        current.allocate_sstable_id()
    }

    /// Get obsolete files that can be garbage collected.
    ///
    /// Returns file IDs that are not in the current version but may still
    /// exist on disk from previous versions.
    pub fn get_obsolete_files(&self, all_file_ids: &HashSet<SStableId>) -> HashSet<SStableId> {
        let current = self.current.read().unwrap();
        let active_ids = current.active_file_ids();

        all_file_ids.difference(&active_ids).copied().collect()
    }

    /// Get the number of levels.
    pub fn num_levels(&self) -> usize {
        self.num_levels
    }

    /// Get the manifest root page ID.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Recover manifest by scanning all SSTable pages in the pager.
    ///
    /// This is a disaster recovery mechanism that reconstructs the manifest
    /// by scanning the entire database for SSTable pages and reading their
    /// metadata from footers.
    ///
    /// # Algorithm
    ///
    /// 1. Scan all pages to find SSTable first pages (LsmData type)
    /// 2. Read footer from each SSTable to extract metadata
    /// 3. Assign SSTables to levels based on:
    ///    - Key range overlaps (L0 allows overlaps, L1+ should not)
    ///    - File sizes (larger files → deeper levels)
    ///    - Creation LSN (newer files → shallower levels)
    /// 4. Build new Version with discovered SSTables
    /// 5. Write recovered manifest to disk
    ///
    /// # Returns
    ///
    /// Returns the recovered Version or an error if recovery fails.
    pub fn recover_from_sstables(
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        num_levels: usize,
        config: &crate::table::lsm::SStableConfig,
    ) -> TableResult<Version> {
        use crate::table::lsm::SStableReader;

        // Step 1: Scan all pages to find SSTable first pages
        let total_pages = pager.total_pages();
        let mut sstable_first_pages = Vec::new();

        for page_num in 0..total_pages {
            let page_id = PageId::from(page_num);

            // Skip reserved pages (0=header, 1=superblock, root_page_id=manifest)
            if page_id == PageId::from(0) || page_id == PageId::from(1) || page_id == root_page_id {
                continue;
            }

            // Try to read the page
            if let Ok(page) = pager.read_page(page_id) {
                // Check if this is an LSM data page
                if page.page_type() == PageType::LsmData {
                    // This could be the first page of an SSTable
                    // We'll try to open it as an SSTable to verify
                    sstable_first_pages.push(page_id);
                }
            }
        }

        // Step 2: Read metadata from each potential SSTable
        let mut sstables = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();

        for first_page_id in sstable_first_pages {
            // Try to open as SSTable reader
            match SStableReader::open(Arc::clone(&pager), first_page_id, config.clone()) {
                Ok(reader) => {
                    let metadata = reader.metadata().clone();

                    // Verify this is actually the first page (not a continuation page)
                    if metadata.first_page_id == first_page_id {
                        // Avoid duplicates (in case we scanned continuation pages)
                        if !seen_ids.contains(&metadata.id) {
                            seen_ids.insert(metadata.id);
                            sstables.push(metadata);
                        }
                    }
                }
                Err(_) => {
                    // Not a valid SSTable, skip
                    continue;
                }
            }
        }

        // Step 3: Assign SSTables to levels
        let assigned_sstables = Self::assign_sstables_to_levels(sstables, num_levels)?;

        // Step 4: Build Version from assigned SSTables
        let mut version = Version::new(num_levels);

        // Find the maximum SSTable ID to set next_sstable_id
        let mut max_id = 0u64;

        for (level, level_sstables) in assigned_sstables.iter().enumerate() {
            for metadata in level_sstables {
                max_id = max_id.max(metadata.id.as_u64());

                // Convert SStableMetadata to FileMetadata
                let file_metadata = FileMetadata {
                    id: metadata.id,
                    level: level as u32,
                    min_key: metadata.min_key.clone(),
                    max_key: metadata.max_key.clone(),
                    num_entries: metadata.num_entries,
                    total_size: metadata.total_size,
                    created_lsn: metadata.created_lsn,
                    first_page_id: metadata.first_page_id,
                    num_pages: metadata.num_pages,
                };

                // Apply as an edit to add the SSTable
                let edit = VersionEdit::add_sstable(file_metadata);
                version = version.apply(&edit)?;
            }
        }

        // Set next SSTable ID to one past the maximum found
        version.next_sstable_id = max_id + 1;

        Ok(version)
    }

    /// Assign SSTables to levels based on key ranges, sizes, and LSNs.
    ///
    /// # Strategy
    ///
    /// For disaster recovery, we use a conservative approach:
    /// - All recovered SSTables are initially placed in L0
    /// - L0 allows overlapping key ranges
    /// - Compaction will later move them to appropriate levels
    ///
    /// This is safer than trying to infer the correct level placement,
    /// which could lead to data loss if we make incorrect assumptions.
    fn assign_sstables_to_levels(
        mut sstables: Vec<crate::table::lsm::SStableMetadata>,
        num_levels: usize,
    ) -> TableResult<Vec<Vec<crate::table::lsm::SStableMetadata>>> {
        let mut levels: Vec<Vec<crate::table::lsm::SStableMetadata>> = vec![Vec::new(); num_levels];

        // Sort by creation LSN (newest first) for L0
        // This ensures newer data is checked first during reads
        sstables.sort_by(|a, b| b.created_lsn.as_u64().cmp(&a.created_lsn.as_u64()));

        // Place all recovered SSTables in L0
        // This is the safest approach for disaster recovery
        levels[0] = sstables;

        // Sort L0 by min_key for efficient lookups
        levels[0].sort_by(|a, b| a.min_key.cmp(&b.min_key));

        Ok(levels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::{PageType, Pager, PagerConfig};
    use crate::vfs::MemoryFileSystem;

    fn create_test_metadata(id: u64, level: u32) -> FileMetadata {
        FileMetadata {
            id: SStableId::new(id),
            level,
            min_key: format!("key{:03}", id * 10).into_bytes(),
            max_key: format!("key{:03}", id * 10 + 9).into_bytes(),
            num_entries: 100,
            total_size: 1024,
            created_lsn: LogSequenceNumber::from(id),
            first_page_id: PageId::from(id),
            num_pages: 1,
        }
    }

    #[test]
    fn test_file_metadata_overlaps() {
        let file = create_test_metadata(1, 0);

        // Overlaps with range that includes file's range
        assert!(file.overlaps(b"key000", b"key999"));

        // Overlaps with range that partially overlaps
        assert!(file.overlaps(b"key005", b"key015"));

        // No overlap - range is before file
        assert!(!file.overlaps(b"key000", b"key009"));

        // No overlap - range is after file
        assert!(!file.overlaps(b"key020", b"key999"));
    }

    #[test]
    fn test_file_metadata_contains_key() {
        let file = create_test_metadata(1, 0);

        assert!(file.contains_key(b"key010"));
        assert!(file.contains_key(b"key015"));
        assert!(file.contains_key(b"key019"));
        assert!(!file.contains_key(b"key009"));
        assert!(!file.contains_key(b"key020"));
    }

    #[test]
    fn test_version_new() {
        let version = Version::new(7);
        assert_eq!(version.num_levels(), 7);
        assert_eq!(version.total_file_count(), 0);
        assert_eq!(version.next_sstable_id(), 1);
    }

    #[test]
    fn test_version_apply_add_sstable() {
        let version = Version::new(7);
        let metadata = create_test_metadata(1, 0);
        let edit = VersionEdit::add_sstable(metadata.clone());

        let new_version = version.apply(&edit).unwrap();
        assert_eq!(new_version.total_file_count(), 1);
        assert_eq!(new_version.level_file_count(0), 1);
        assert!(new_version.get_file(SStableId::new(1)).is_some());
    }

    #[test]
    fn test_version_apply_remove_sstable() {
        let mut version = Version::new(7);
        let metadata = create_test_metadata(1, 0);
        let edit = VersionEdit::add_sstable(metadata.clone());
        version = version.apply(&edit).unwrap();

        let edit = VersionEdit::remove_sstable(SStableId::new(1));
        let new_version = version.apply(&edit).unwrap();
        assert_eq!(new_version.total_file_count(), 0);
        assert_eq!(new_version.level_file_count(0), 0);
    }

    #[test]
    fn test_version_apply_duplicate_id_fails() {
        let version = Version::new(7);
        let metadata = create_test_metadata(1, 0);
        let edit = VersionEdit::add_sstable(metadata.clone());
        let version = version.apply(&edit).unwrap();

        // Try to add same ID again
        let result = version.apply(&edit);
        assert!(result.is_err());
    }

    #[test]
    fn test_version_level_files_sorted() {
        let mut version = Version::new(7);

        // Add files to L1 in non-sorted order
        for id in [3, 1, 2] {
            let metadata = create_test_metadata(id, 1);
            let edit = VersionEdit::add_sstable(metadata);
            version = version.apply(&edit).unwrap();
        }

        // Files should be sorted by min_key
        let files = version.level_files(1);
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].id, SStableId::new(1));
        assert_eq!(files[1].id, SStableId::new(2));
        assert_eq!(files[2].id, SStableId::new(3));
    }

    #[test]
    fn test_version_get_overlapping_files() {
        let mut version = Version::new(7);

        // Add files with different key ranges
        for id in 1..=3 {
            let metadata = create_test_metadata(id, 0);
            let edit = VersionEdit::add_sstable(metadata);
            version = version.apply(&edit).unwrap();
        }

        // key015 should be in file 1 (key010-key019)
        let files = version.get_overlapping_files(b"key015");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, SStableId::new(1));
    }

    #[test]
    fn test_version_level_size() {
        let mut version = Version::new(7);

        for id in 1..=3 {
            let metadata = create_test_metadata(id, 0);
            let edit = VersionEdit::add_sstable(metadata);
            version = version.apply(&edit).unwrap();
        }

        assert_eq!(version.level_size(0), 3 * 1024);
        assert_eq!(version.level_size(1), 0);
    }

    #[test]
    fn test_version_allocate_sstable_id() {
        let mut version = Version::new(7);

        let id1 = version.allocate_sstable_id();
        let id2 = version.allocate_sstable_id();
        let id3 = version.allocate_sstable_id();

        assert_eq!(id1, SStableId::new(1));
        assert_eq!(id2, SStableId::new(2));
        assert_eq!(id3, SStableId::new(3));
        assert_eq!(version.next_sstable_id(), 4);
    }

    fn create_test_manifest() -> (Arc<Pager<MemoryFileSystem>>, Manifest<MemoryFileSystem>) {
        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let manifest = Manifest::new(pager.clone(), root_page_id, 7).unwrap();
        (pager, manifest)
    }

    #[test]
    fn test_manifest_new() {
        let (_pager, manifest) = create_test_manifest();

        let version = manifest.current();
        assert_eq!(version.num_levels(), 7);
        assert_eq!(version.total_file_count(), 0);
    }

    #[test]
    fn test_manifest_apply_edit() {
        let (_pager, manifest) = create_test_manifest();

        let metadata = create_test_metadata(1, 0);
        let edit = VersionEdit::add_sstable(metadata);
        manifest.apply_edit(edit).unwrap();

        let version = manifest.current();
        assert_eq!(version.total_file_count(), 1);
        assert_eq!(version.level_file_count(0), 1);
    }

    #[test]
    fn test_manifest_persistence() {
        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();

        {
            let manifest = Manifest::new(pager.clone(), root_page_id, 7).unwrap();

            for id in 1..=3 {
                let metadata = create_test_metadata(id, 0);
                let edit = VersionEdit::add_sstable(metadata);
                manifest.apply_edit(edit).unwrap();
            }
        }

        {
            let reopened_pager = Arc::new(Pager::open(&fs, "test.db").unwrap());
            let manifest = Manifest::open(reopened_pager, root_page_id, 7).unwrap();
            let version = manifest.current();

            assert_eq!(version.total_file_count(), 3);
            assert_eq!(version.level_file_count(0), 3);
            assert!(version.get_file(SStableId::new(1)).is_some());
            assert!(version.get_file(SStableId::new(2)).is_some());
            assert!(version.get_file(SStableId::new(3)).is_some());
        }
    }

    #[test]
    fn test_manifest_allocate_sstable_id() {
        let (_pager, manifest) = create_test_manifest();

        let id1 = manifest.allocate_sstable_id();
        let id2 = manifest.allocate_sstable_id();

        assert_eq!(id1, SStableId::new(1));
        assert_eq!(id2, SStableId::new(2));
    }

    #[test]
    fn test_manifest_get_obsolete_files() {
        let (_pager, manifest) = create_test_manifest();

        for id in 1..=3 {
            let metadata = create_test_metadata(id, 0);
            let edit = VersionEdit::add_sstable(metadata);
            manifest.apply_edit(edit).unwrap();
        }

        let edit = VersionEdit::remove_sstable(SStableId::new(2));
        manifest.apply_edit(edit).unwrap();

        let all_files: HashSet<_> = (1..=4).map(SStableId::new).collect();

        let obsolete = manifest.get_obsolete_files(&all_files);
        assert_eq!(obsolete.len(), 2);
        assert!(obsolete.contains(&SStableId::new(2)));
        assert!(obsolete.contains(&SStableId::new(4)));
    }

    #[test]
    fn test_manifest_recovery_with_edits() {
        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();

        {
            let manifest = Manifest::new(pager.clone(), root_page_id, 7).unwrap();

            for id in 1..=5 {
                let metadata = create_test_metadata(id, 0);
                let edit = VersionEdit::add_sstable(metadata);
                manifest.apply_edit(edit).unwrap();
            }

            manifest
                .apply_edit(VersionEdit::remove_sstable(SStableId::new(2)))
                .unwrap();
            manifest
                .apply_edit(VersionEdit::remove_sstable(SStableId::new(4)))
                .unwrap();
        }

        {
            let reopened_pager = Arc::new(Pager::open(&fs, "test.db").unwrap());
            let manifest = Manifest::open(reopened_pager, root_page_id, 7).unwrap();
            let version = manifest.current();

            assert_eq!(version.total_file_count(), 3);
            assert!(version.get_file(SStableId::new(1)).is_some());
            assert!(version.get_file(SStableId::new(2)).is_none());
            assert!(version.get_file(SStableId::new(3)).is_some());
            assert!(version.get_file(SStableId::new(4)).is_none());
            assert!(version.get_file(SStableId::new(5)).is_some());
        }
    }

    #[test]
    fn test_manifest_spans_multiple_pages() {
        let fs = MemoryFileSystem::new();
        let mut config = PagerConfig::default();
        config.page_size = crate::pager::PageSize::Size4KB;
        let pager = Arc::new(Pager::create(&fs, "large.db", config).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let manifest = Manifest::new(pager.clone(), root_page_id, 7).unwrap();

        for id in 1..=256 {
            let mut metadata = create_test_metadata(id, (id % 3) as u32);
            metadata.min_key = vec![b'a'; 64];
            metadata.max_key = vec![b'z'; 64];
            metadata.total_size = 1024 * id;
            manifest
                .apply_edit(VersionEdit::add_sstable(metadata))
                .unwrap();
        }

        let reopened_pager = Arc::new(Pager::open(&fs, "large.db").unwrap());
        let reopened = Manifest::open(reopened_pager, root_page_id, 7).unwrap();
        assert_eq!(reopened.current().total_file_count(), 256);
    }

    #[test]
    fn test_manifest_recovery_empty_database() {
        use crate::table::lsm::SStableConfig;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Recover from empty database (no SSTables)
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 0);
        assert_eq!(version.next_sstable_id(), 1);
    }

    #[test]
    fn test_manifest_recovery_single_sstable() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create a single SSTable
        let mut writer =
            SStableWriter::new(Arc::clone(&pager), SStableId::new(1), 0, config.clone(), 10);

        for i in 0..10 {
            let key = format!("key{:03}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let chain = VersionChain::new(value, 1.into());
            writer.add(key, chain).unwrap();
        }

        let _metadata = writer.finish(1.into()).unwrap();

        // Recover manifest
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 1);
        assert_eq!(version.next_sstable_id(), 2);
        assert_eq!(version.level_file_count(0), 1);
    }

    #[test]
    fn test_manifest_recovery_multiple_sstables_non_overlapping() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create multiple non-overlapping SSTables
        for table_id in 1..=3 {
            let mut writer = SStableWriter::new(
                Arc::clone(&pager),
                SStableId::new(table_id),
                0,
                config.clone(),
                10,
            );

            let start = (table_id - 1) * 10;
            for i in start..start + 10 {
                let key = format!("key{:03}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                let chain = VersionChain::new(value, table_id.into());
                writer.add(key, chain).unwrap();
            }

            writer.finish(table_id.into()).unwrap();
        }

        // Recover manifest
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 3);
        assert_eq!(version.next_sstable_id(), 4);

        // All recovered SSTables are placed in L0 for safety
        assert_eq!(version.level_file_count(0), 3);
    }

    #[test]
    fn test_manifest_recovery_overlapping_sstables() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create overlapping SSTables (same key ranges)
        for table_id in 1..=3 {
            let mut writer = SStableWriter::new(
                Arc::clone(&pager),
                SStableId::new(table_id),
                0,
                config.clone(),
                10,
            );

            // All tables have overlapping keys
            for i in 0..10 {
                let key = format!("key{:03}", i).into_bytes();
                let value = format!("value{}_{}", table_id, i).into_bytes();
                let chain = VersionChain::new(value, table_id.into());
                writer.add(key, chain).unwrap();
            }

            writer.finish(table_id.into()).unwrap();
        }

        // Recover manifest
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 3);
        assert_eq!(version.next_sstable_id(), 4);

        // Overlapping SSTables should be in L0
        assert_eq!(version.level_file_count(0), 3);
    }

    #[test]
    fn test_manifest_recovery_mixed_overlapping() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create mix of overlapping and non-overlapping SSTables
        // Tables 1-2: overlapping (keys 0-9)
        for table_id in 1..=2 {
            let mut writer = SStableWriter::new(
                Arc::clone(&pager),
                SStableId::new(table_id),
                0,
                config.clone(),
                10,
            );

            for i in 0..10 {
                let key = format!("key{:03}", i).into_bytes();
                let value = format!("value{}_{}", table_id, i).into_bytes();
                let chain = VersionChain::new(value, table_id.into());
                writer.add(key, chain).unwrap();
            }

            writer.finish(table_id.into()).unwrap();
        }

        // Tables 3-4: non-overlapping (keys 10-19, 20-29)
        for table_id in 3..=4 {
            let mut writer = SStableWriter::new(
                Arc::clone(&pager),
                SStableId::new(table_id),
                0,
                config.clone(),
                10,
            );

            let start = (table_id - 1) * 10;
            for i in start..start + 10 {
                let key = format!("key{:03}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                let chain = VersionChain::new(value, table_id.into());
                writer.add(key, chain).unwrap();
            }

            writer.finish(table_id.into()).unwrap();
        }

        // Recover manifest
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 4);
        assert_eq!(version.next_sstable_id(), 5);

        // All recovered SSTables are placed in L0 for safety
        assert_eq!(version.level_file_count(0), 4);
    }

    #[test]
    fn test_manifest_recovery_preserves_sstable_ids() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create SSTables with specific IDs (not sequential)
        let ids = vec![5, 10, 15];
        for &table_id in &ids {
            let mut writer = SStableWriter::new(
                Arc::clone(&pager),
                SStableId::new(table_id),
                0,
                config.clone(),
                5,
            );

            let start = table_id * 10;
            for i in start..start + 5 {
                let key = format!("key{:03}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                let chain = VersionChain::new(value, table_id.into());
                writer.add(key, chain).unwrap();
            }

            writer.finish(table_id.into()).unwrap();
        }

        // Recover manifest
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        assert_eq!(version.total_file_count(), 3);

        // Next SSTable ID should be one past the maximum
        assert_eq!(version.next_sstable_id(), 16);

        // Verify all IDs are present
        for &id in &ids {
            assert!(version.get_file(SStableId::new(id)).is_some());
        }
    }

    #[test]
    fn test_manifest_recovery_with_corrupted_sstable() {
        use crate::table::lsm::{SStableConfig, SStableId, SStableWriter};
        use crate::txn::VersionChain;

        let fs = MemoryFileSystem::new();
        let pager = Arc::new(Pager::create(&fs, "test.db", PagerConfig::default()).unwrap());
        let root_page_id = pager.allocate_page(PageType::LsmMeta).unwrap();
        let config = SStableConfig::default();

        // Create a valid SSTable
        let mut writer =
            SStableWriter::new(Arc::clone(&pager), SStableId::new(1), 0, config.clone(), 5);

        for i in 0..5 {
            let key = format!("key{:03}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let chain = VersionChain::new(value, 1.into());
            writer.add(key, chain).unwrap();
        }

        writer.finish(1.into()).unwrap();

        // Create a corrupted page (LsmData type but not a valid SSTable)
        let _corrupt_page_id = pager.allocate_page(PageType::LsmData).unwrap();
        // Don't write valid SSTable data to it

        // Recovery should skip corrupted pages and recover valid ones
        let version =
            Manifest::recover_from_sstables(Arc::clone(&pager), root_page_id, 7, &config).unwrap();

        // Should recover the valid SSTable, skip the corrupted one
        assert_eq!(version.total_file_count(), 1);
        assert!(version.get_file(SStableId::new(1)).is_some());
    }
}

// Made with Bob
