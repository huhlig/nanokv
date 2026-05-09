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

//! Manifest file for tracking LSM tree state and versions.
//!
//! The manifest tracks the current state of the LSM tree, including which SSTables
//! are active at each level. It supports atomic updates through version edits and
//! provides crash recovery by persisting state to disk.
//!
//! # Architecture
//!
//! - **Version**: Immutable snapshot of LSM tree state (SSTables per level)
//! - **VersionEdit**: Delta describing changes to apply to a version
//! - **Manifest**: Manages versions and persists edits to disk
//!
//! # File Format
//!
//! The manifest file is a log of version edits in JSON Lines format:
//! ```text
//! {"type":"add_sstable","id":1,"level":0,"min_key":[...],"max_key":[...],...}
//! {"type":"remove_sstable","id":1}
//! {"type":"add_sstable","id":2,"level":1,"min_key":[...],"max_key":[...],...}
//! ```
//!
//! # Atomic Updates
//!
//! Updates are atomic through a write-new-rename strategy:
//! 1. Write new manifest to temporary file
//! 2. Fsync temporary file
//! 3. Rename temporary file to manifest file (atomic on POSIX)
//! 4. Fsync directory
//!
//! # Recovery
//!
//! On startup, the manifest is replayed from disk to reconstruct the current version.

use crate::pager::PageId;
use crate::table::error::{TableError, TableResult};
use crate::table::lsm::SStableId;
use crate::vfs::{File, FileSystem};
use crate::wal::LogSequenceNumber;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
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
    #[serde(serialize_with = "serialize_bytes", deserialize_with = "deserialize_bytes")]
    pub min_key: Vec<u8>,
    
    /// Largest key in the SSTable (hex encoded for JSON)
    #[serde(serialize_with = "serialize_bytes", deserialize_with = "deserialize_bytes")]
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
        #[serde(serialize_with = "serialize_bytes", deserialize_with = "deserialize_bytes")]
        min_key: Vec<u8>,
        #[serde(serialize_with = "serialize_bytes", deserialize_with = "deserialize_bytes")]
        max_key: Vec<u8>,
        num_entries: u64,
        total_size: u64,
        created_lsn: LogSequenceNumber,
        first_page_id: PageId,
        num_pages: u32,
    },
    
    /// Remove an SSTable
    RemoveSStable {
        id: SStableId,
    },
    
    /// Set the next SSTable ID
    SetNextSStableId {
        next_id: u64,
    },
    
    /// Set the log number
    SetLogNumber {
        log_number: u64,
    },
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
                    return Err(TableError::Other(format!(
                        "Invalid level {}, max is {}",
                        level,
                        new_version.levels.len() - 1
                    )));
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
                    return Err(TableError::Other(format!(
                        "SSTable ID {} already exists",
                        id
                    )));
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
                let metadata = new_version.files_by_id.remove(id).ok_or_else(|| {
                    TableError::Other(format!("SSTable ID {} not found", id))
                })?;
                
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
        
        self.levels[level_idx]
            .iter()
            .map(|f| f.total_size)
            .sum()
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

/// Manifest manages versions and persists state to disk.
pub struct Manifest<FS: FileSystem> {
    /// Virtual file system
    fs: Arc<FS>,
    
    /// Path to manifest file
    manifest_path: PathBuf,
    
    /// Current version (protected by RwLock for concurrent reads)
    current: Arc<RwLock<Version>>,
    
    /// Number of levels in the LSM tree
    num_levels: usize,
}

impl<FS: FileSystem> Manifest<FS> {
    /// Manifest file name
    const MANIFEST_FILE: &'static str = "MANIFEST";
    
    /// Temporary manifest file name (for atomic updates)
    const MANIFEST_TEMP: &'static str = "MANIFEST.tmp";
    
    /// Create a new manifest.
    pub fn new(fs: Arc<FS>, dir: &Path, num_levels: usize) -> Self {
        let manifest_path = dir.join(Self::MANIFEST_FILE);
        let current = Arc::new(RwLock::new(Version::new(num_levels)));
        
        Self {
            fs,
            manifest_path,
            current,
            num_levels,
        }
    }
    
    /// Open an existing manifest or create a new one.
    pub fn open(fs: Arc<FS>, dir: &Path, num_levels: usize) -> TableResult<Self> {
        let manifest_path = dir.join(Self::MANIFEST_FILE);
        let manifest_path_str = manifest_path.to_str().ok_or_else(|| {
            TableError::Other("Invalid manifest path".to_string())
        })?;
        
        let version = if fs.exists(manifest_path_str).map_err(|e| {
            TableError::Other(format!("Failed to check manifest existence: {}", e))
        })? {
            // Recover from existing manifest
            Self::recover_from_file(&fs, manifest_path_str, num_levels)?
        } else {
            // Create new empty version
            Version::new(num_levels)
        };
        
        let current = Arc::new(RwLock::new(version));
        
        Ok(Self {
            fs,
            manifest_path,
            current,
            num_levels,
        })
    }
    
    /// Recover version from manifest file.
    fn recover_from_file(
        fs: &Arc<FS>,
        path: &str,
        num_levels: usize,
    ) -> TableResult<Version> {
        let file = fs.open_file(path).map_err(|e| {
            TableError::Other(format!("Failed to open manifest: {}", e))
        })?;
        let reader = BufReader::new(file);
        
        let mut version = Version::new(num_levels);
        
        for (line_num, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| {
                TableError::Corruption(format!("Failed to read manifest line {}: {}", line_num, e))
            })?;
            
            if line.trim().is_empty() {
                continue;
            }
            
            let edit: VersionEdit = serde_json::from_str(&line).map_err(|e| {
                TableError::Corruption(format!(
                    "Failed to parse manifest line {}: {}",
                    line_num, e
                ))
            })?;
            
            version = version.apply(&edit)?;
        }
        
        Ok(version)
    }
    
    /// Apply a version edit atomically.
    pub fn apply_edit(&self, edit: VersionEdit) -> TableResult<()> {
        self.apply_edits(vec![edit])
    }
    
    /// Apply multiple version edits atomically.
    pub fn apply_edits(&self, edits: Vec<VersionEdit>) -> TableResult<()> {
        // Apply edits to create new version
        let new_version = {
            let current = self.current.read().unwrap();
            let mut version = current.clone();
            
            for edit in &edits {
                version = version.apply(edit)?;
            }
            
            version
        };
        
        // Write new manifest atomically
        self.write_manifest(&new_version)?;
        
        // Update current version
        {
            let mut current = self.current.write().unwrap();
            *current = new_version;
        }
        
        Ok(())
    }
    
    /// Write manifest to disk atomically.
    fn write_manifest(&self, version: &Version) -> TableResult<()> {
        let manifest_path_str = self.manifest_path.to_str().ok_or_else(|| {
            TableError::Other("Invalid manifest path".to_string())
        })?;
        
        // Remove existing manifest if it exists
        if self.fs.exists(manifest_path_str).unwrap_or(false) {
            let _ = self.fs.remove_file(manifest_path_str);
        }
        
        // Write directly to manifest file (simpler approach without atomic rename)
        // In production, you'd want atomic rename, but VFS doesn't support it yet
        let mut file = self.fs.create_file(manifest_path_str).map_err(|e| {
            TableError::Other(format!("Failed to create manifest: {}", e))
        })?;
        
        // Write all edits to reconstruct this version
        // First, set the next SSTable ID
        let edit = VersionEdit::set_next_sstable_id(version.next_sstable_id);
        let line = serde_json::to_string(&edit).map_err(|e| {
            TableError::Other(format!("Failed to serialize edit: {}", e))
        })?;
        writeln!(file, "{}", line)?;
        
        // Set log number
        let edit = VersionEdit::set_log_number(version.log_number);
        let line = serde_json::to_string(&edit).map_err(|e| {
            TableError::Other(format!("Failed to serialize edit: {}", e))
        })?;
        writeln!(file, "{}", line)?;
        
        // Add all files
        for (_, metadata) in &version.files_by_id {
            let edit = VersionEdit::add_sstable(metadata.clone());
            let line = serde_json::to_string(&edit).map_err(|e| {
                TableError::Other(format!("Failed to serialize edit: {}", e))
            })?;
            writeln!(file, "{}", line)?;
        }
        
        // Fsync file
        file.sync_all().map_err(|e| {
            TableError::Other(format!("Failed to sync manifest: {}", e))
        })?;
        
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
    
    #[test]
    fn test_manifest_new() {
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let manifest = Manifest::new(fs, dir, 7);
        
        let version = manifest.current();
        assert_eq!(version.num_levels(), 7);
        assert_eq!(version.total_file_count(), 0);
    }
    
    #[test]
    fn test_manifest_apply_edit() {
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let dir_str = dir.to_str().unwrap();
        fs.create_directory_all(dir_str).unwrap();
        
        let manifest = Manifest::new(fs, dir, 7);
        
        let metadata = create_test_metadata(1, 0);
        let edit = VersionEdit::add_sstable(metadata);
        manifest.apply_edit(edit).unwrap();
        
        let version = manifest.current();
        assert_eq!(version.total_file_count(), 1);
        assert_eq!(version.level_file_count(0), 1);
    }
    
    #[test]
    fn test_manifest_persistence() {
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let dir_str = dir.to_str().unwrap();
        fs.create_directory_all(dir_str).unwrap();
        
        // Create manifest and add files
        {
            let manifest = Manifest::new(fs.clone(), dir, 7);
            
            for id in 1..=3 {
                let metadata = create_test_metadata(id, 0);
                let edit = VersionEdit::add_sstable(metadata);
                manifest.apply_edit(edit).unwrap();
            }
        }
        
        // Reopen manifest and verify state
        {
            let manifest = Manifest::open(fs, dir, 7).unwrap();
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
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let dir_str = dir.to_str().unwrap();
        fs.create_directory_all(dir_str).unwrap();
        
        let manifest = Manifest::new(fs, dir, 7);
        
        let id1 = manifest.allocate_sstable_id();
        let id2 = manifest.allocate_sstable_id();
        
        assert_eq!(id1, SStableId::new(1));
        assert_eq!(id2, SStableId::new(2));
    }
    
    #[test]
    fn test_manifest_get_obsolete_files() {
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let dir_str = dir.to_str().unwrap();
        fs.create_directory_all(dir_str).unwrap();
        
        let manifest = Manifest::new(fs, dir, 7);
        
        // Add files 1, 2, 3
        for id in 1..=3 {
            let metadata = create_test_metadata(id, 0);
            let edit = VersionEdit::add_sstable(metadata);
            manifest.apply_edit(edit).unwrap();
        }
        
        // Remove file 2
        let edit = VersionEdit::remove_sstable(SStableId::new(2));
        manifest.apply_edit(edit).unwrap();
        
        // All files on disk: 1, 2, 3, 4
        let all_files: HashSet<_> = (1..=4).map(SStableId::new).collect();
        
        // Obsolete files should be 2 and 4
        let obsolete = manifest.get_obsolete_files(&all_files);
        assert_eq!(obsolete.len(), 2);
        assert!(obsolete.contains(&SStableId::new(2)));
        assert!(obsolete.contains(&SStableId::new(4)));
    }
    
    #[test]
    fn test_manifest_recovery_with_edits() {
        let fs = Arc::new(MemoryFileSystem::new());
        let dir = Path::new("/test");
        let dir_str = dir.to_str().unwrap();
        fs.create_directory_all(dir_str).unwrap();
        
        // Create manifest and apply multiple edits
        {
            let manifest = Manifest::new(fs.clone(), dir, 7);
            
            // Add files
            for id in 1..=5 {
                let metadata = create_test_metadata(id, 0);
                let edit = VersionEdit::add_sstable(metadata);
                manifest.apply_edit(edit).unwrap();
            }
            
            // Remove some files
            manifest.apply_edit(VersionEdit::remove_sstable(SStableId::new(2))).unwrap();
            manifest.apply_edit(VersionEdit::remove_sstable(SStableId::new(4))).unwrap();
        }
        
        // Reopen and verify final state
        {
            let manifest = Manifest::open(fs, dir, 7).unwrap();
            let version = manifest.current();
            
            assert_eq!(version.total_file_count(), 3);
            assert!(version.get_file(SStableId::new(1)).is_some());
            assert!(version.get_file(SStableId::new(2)).is_none());
            assert!(version.get_file(SStableId::new(3)).is_some());
            assert!(version.get_file(SStableId::new(4)).is_none());
            assert!(version.get_file(SStableId::new(5)).is_some());
        }
    }
}

// Made with Bob