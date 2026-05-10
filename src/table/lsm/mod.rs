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

//! LSM tree storage engine implementation.
//!
//! This module provides a Log-Structured Merge (LSM) tree storage engine
//! optimized for write-heavy workloads. The LSM tree consists of:
//!
//! - **Memtable**: In-memory write buffer for recent writes
//! - **SSTables**: Immutable sorted string tables on disk
//! - **Bloom filters**: Probabilistic filters to reduce disk I/O
//! - **Compaction**: Background process to merge and optimize SSTables
//!
//! # Architecture
//!
//! ```text
//! Writes → Memtable → Immutable Memtable → L0 SSTable
//!                                              ↓
//!                                         Compaction
//!                                              ↓
//!                                    L1, L2, ..., Ln SSTables
//! ```
//!
//! # Features
//!
//! - Write-optimized: Sequential writes to memtable, batch flushes to disk
//! - MVCC support: Version chains for snapshot isolation
//! - Bloom filters: Reduce unnecessary disk reads
//! - Leveled compaction: Exponential level sizes, non-overlapping SSTables
//! - Compression: Optional per-block compression
//! - Encryption: Optional per-block encryption

mod bloom;
mod compaction;
mod config;
mod iterator;
mod manifest;
mod memtable;
mod sstable;

pub use self::bloom::{BloomFilter, BloomFilterBuilder};
pub use self::compaction::{
    CompactionExecutor, CompactionJob, CompactionManager, CompactionPicker, CompactionStats,
};
pub use self::config::{
    BloomFilterConfig, BlockCacheConfig, CacheEvictionPolicy, CompactionConfig,
    CompactionStrategy, LevelConfig, LsmConfig, MemtableConfig, MemtableType, SStableConfig,
};
pub use self::iterator::{
    Direction, LsmEntry, LsmIterator, MemtableIterator, MergeIterator, SStableIterator,
};
pub use self::manifest::{FileMetadata, Manifest, Version, VersionEdit};
pub use self::memtable::Memtable;
pub use self::sstable::{
    DataBlock, SStableFooter, SStableId, SStableMetadata, SStableReader, SStableWriter,
};

// Made with Bob


// =============================================================================
// LSM Tree TableEngine Implementation
// =============================================================================

use crate::pager::{PageId, Pager};
use crate::table::error::{TableError, TableResult};
use crate::table::{
    BatchOps, BatchReport, Flushable, MutableTable, OrderedScan, PointLookup, TableCapabilities,
    TableCursor, TableEngine, TableEngineKind, TableId, TableReader, TableStatistics,
    TableWriter, WriteBatch,
};
use crate::txn::TransactionId;
use crate::types::{Bound, ScanBounds, ValueBuf};
use crate::vfs::FileSystem;
use crate::wal::LogSequenceNumber;
use std::sync::{Arc, RwLock};

/// LSM Tree storage engine.
///
/// Provides a write-optimized storage engine using log-structured merge trees.
/// Writes go to an in-memory memtable, which is periodically flushed to disk
/// as immutable SSTables. Background compaction merges SSTables to maintain
/// read performance.
pub struct LsmTree<FS: FileSystem> {
    /// Table identifier
    table_id: TableId,
    
    /// Table name
    name: String,
    
    /// Pager for disk I/O
    pager: Arc<Pager<FS>>,
    
    /// Configuration
    config: LsmConfig,
    
    /// Active memtable (accepts writes)
    active_memtable: Arc<RwLock<Memtable>>,
    
    /// Immutable memtables (being flushed)
    immutable_memtables: Arc<RwLock<Vec<Memtable>>>,
    
    /// Manifest for version management
    manifest: Arc<Manifest<FS>>,
    
    /// Compaction manager
    compaction_manager: Arc<CompactionManager<FS>>,
}

impl<FS: FileSystem> LsmTree<FS> {
    /// Create a new LSM tree.
    pub fn new(
        table_id: TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        config: LsmConfig,
    ) -> TableResult<Self> {
        let num_levels = config.compaction.levels.len();
        let manifest = Arc::new(Manifest::new(
            pager.clone(),
            root_page_id,
            num_levels,
        )?);
        
        let active_memtable = Arc::new(RwLock::new(Memtable::new(
            config.memtable.max_size,
        )));
        
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        
        let compaction_manager = Arc::new(CompactionManager::new(
            pager.clone(),
            manifest.clone(),
            config.compaction.clone(),
        ));
        
        Ok(Self {
            table_id,
            name,
            pager,
            config,
            active_memtable,
            immutable_memtables,
            manifest,
            compaction_manager,
        })
    }
    
    /// Open an existing LSM tree.
    pub fn open(
        table_id: TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        config: LsmConfig,
    ) -> TableResult<Self> {
        let num_levels = config.compaction.levels.len();
        let manifest = Arc::new(Manifest::open(
            pager.clone(),
            root_page_id,
            num_levels,
        )?);
        
        let active_memtable = Arc::new(RwLock::new(Memtable::new(
            config.memtable.max_size,
        )));
        
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));
        
        let compaction_manager = Arc::new(CompactionManager::new(
            pager.clone(),
            manifest.clone(),
            config.compaction.clone(),
        ));
        
        Ok(Self {
            table_id,
            name,
            pager,
            config,
            active_memtable,
            immutable_memtables,
            manifest,
            compaction_manager,
        })
    }
    
    /// Get a value from the LSM tree at a specific snapshot.
    fn get_internal(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<Vec<u8>>> {
        // 1. Check active memtable
        {
            let memtable = self.active_memtable.read().unwrap();
            if let Some(value) = memtable.get(key, snapshot_lsn)? {
                return Ok(Some(value));
            }
        }
        
        // 2. Check immutable memtables (newest to oldest)
        {
            let immutable = self.immutable_memtables.read().unwrap();
            for memtable in immutable.iter().rev() {
                if let Some(value) = memtable.get(key, snapshot_lsn)? {
                    return Ok(Some(value));
                }
            }
        }
        
        // 3. Check SSTables (L0 to Ln)
        let version = self.manifest.current();
        
        // Check each level
        for level in 0..version.num_levels() {
            let files = version.level_files(level as u32);
            
            if level == 0 {
                // L0 files may overlap, check all in reverse order (newest first)
                for file in files.iter().rev() {
                    if !file.contains_key(key) {
                        continue;
                    }
                    
                    let reader = SStableReader::open(
                        self.pager.clone(),
                        file.first_page_id,
                        self.config.sstable.clone(),
                    )?;
                    
                    // Check bloom filter first
                    if !reader.may_contain(key) {
                        continue;
                    }
                    
                    if let Some(value) = reader.get(key, snapshot_lsn)? {
                        return Ok(Some(value));
                    }
                }
            } else {
                // L1+ files don't overlap, binary search
                let file_idx = files.binary_search_by(|f| {
                    if key < f.min_key.as_slice() {
                        std::cmp::Ordering::Greater
                    } else if key > f.max_key.as_slice() {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                
                if let Ok(idx) = file_idx {
                    let file = &files[idx];
                    let reader = SStableReader::open(
                        self.pager.clone(),
                        file.first_page_id,
                        self.config.sstable.clone(),
                    )?;
                    
                    // Check bloom filter first
                    if !reader.may_contain(key) {
                        continue;
                    }
                    
                    if let Some(value) = reader.get(key, snapshot_lsn)? {
                        return Ok(Some(value));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    /// Insert a key-value pair into the active memtable.
    fn insert_internal(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        tx_id: TransactionId,
        commit_lsn: Option<LogSequenceNumber>,
    ) -> TableResult<()> {
        let memtable = self.active_memtable.write().unwrap();
        
        // Try to insert
        match memtable.insert(key.clone(), value.clone(), tx_id, commit_lsn) {
            Ok(()) => Ok(()),
            Err(TableError::MemtableFull) => {
                // Memtable is full, need to rotate
                drop(memtable);
                self.rotate_memtable()?;
                
                // Retry insert on new memtable
                let new_memtable = self.active_memtable.write().unwrap();
                new_memtable.insert(key, value, tx_id, commit_lsn)
            }
            Err(e) => Err(e),
        }
    }
    
    /// Delete a key from the active memtable (inserts tombstone).
    fn delete_internal(
        &self,
        key: Vec<u8>,
        tx_id: TransactionId,
        commit_lsn: Option<LogSequenceNumber>,
    ) -> TableResult<()> {
        let memtable = self.active_memtable.write().unwrap();
        
        // Try to delete (insert tombstone)
        match memtable.delete(key.clone(), tx_id, commit_lsn) {
            Ok(()) => Ok(()),
            Err(TableError::MemtableFull) => {
                // Memtable is full, need to rotate
                drop(memtable);
                self.rotate_memtable()?;
                
                // Retry delete on new memtable
                let new_memtable = self.active_memtable.write().unwrap();
                new_memtable.delete(key, tx_id, commit_lsn)
            }
            Err(e) => Err(e),
        }
    }
    
    /// Rotate the active memtable to immutable state.
    fn rotate_memtable(&self) -> TableResult<()> {
        let mut active = self.active_memtable.write().unwrap();
        let mut immutable = self.immutable_memtables.write().unwrap();
        
        // Make current memtable immutable
        active.make_immutable();
        
        // Move to immutable list
        let old_memtable = std::mem::replace(
            &mut *active,
            Memtable::new(self.config.memtable.max_size),
        );
        immutable.push(old_memtable);
        
        // TODO: Trigger background flush
        
        Ok(())
    }
    
    /// Create iterators for all data sources.
    fn create_iterators(
        &self,
        direction: Direction,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Vec<Box<dyn LsmIterator>>> {
        let mut iterators: Vec<Box<dyn LsmIterator>> = Vec::new();
        let mut priority = 0;
        
        // Active memtable (highest priority)
        {
            let memtable = self.active_memtable.read().unwrap();
            let iter = MemtableIterator::new(&*memtable, direction, priority)?;
            iterators.push(Box::new(iter));
            priority += 1;
        }
        
        // Immutable memtables (reverse order = newest first)
        {
            let immutable = self.immutable_memtables.read().unwrap();
            for memtable in immutable.iter().rev() {
                let iter = MemtableIterator::new(memtable, direction, priority)?;
                iterators.push(Box::new(iter));
                priority += 1;
            }
        }
        
        // SSTables (L0 to Ln)
        let version = self.manifest.current();
        for level in 0..version.num_levels() {
            let files = version.level_files(level as u32);
            
            for file in files.iter().rev() {
                let reader = Arc::new(SStableReader::open(
                    self.pager.clone(),
                    file.first_page_id,
                    self.config.sstable.clone(),
                )?);
                
                let iter = SStableIterator::new(
                    reader,
                    direction,
                    priority,
                )?;
                iterators.push(Box::new(iter));
                priority += 1;
            }
        }
        
        Ok(iterators)
    }
}

impl<FS: FileSystem> TableEngine for LsmTree<FS> {
    type Reader<'a> = LsmReader<'a, FS> where Self: 'a;
    type Writer<'a> = LsmWriter<'a, FS> where Self: 'a;
    
    fn table_id(&self) -> TableId {
        self.table_id
    }
    
    fn name(&self) -> &str {
        &self.name
    }
    
    fn kind(&self) -> TableEngineKind {
        TableEngineKind::LsmTree
    }
    
    fn capabilities(&self) -> TableCapabilities {
        TableCapabilities {
            ordered: true,
            point_lookup: true,
            prefix_scan: false, // Could be enabled with proper index
            reverse_scan: true,
            range_delete: true,
            merge_operator: false,
            mvcc_native: true,
            append_optimized: true,
            memory_resident: false,
            disk_resident: true,
            supports_compression: self.config.sstable.compression.is_some(),
            supports_encryption: self.config.sstable.encryption.is_some(),
        }
    }
    
    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>> {
        Ok(LsmReader {
            tree: self,
            snapshot_lsn,
        })
    }
    
    fn writer(
        &self,
        tx_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Writer<'_>> {
        Ok(LsmWriter {
            tree: self,
            tx_id,
            snapshot_lsn,
            pending_changes: Vec::new(),
        })
    }
    
    fn stats(&self) -> TableResult<TableStatistics> {
        let version = self.manifest.current();
        let mut total_entries = 0u64;
        let mut total_size = 0u64;
        
        for level in 0..version.num_levels() {
            let files = version.level_files(level as u32);
            for file in files {
                total_entries += file.num_entries;
                total_size += file.total_size;
            }
        }
        
        Ok(TableStatistics {
            row_count: Some(total_entries),
            total_size_bytes: Some(total_size),
            key_stats: None,
            value_stats: None,
            histogram: None,
            last_updated_lsn: None,
        })
    }
}

// =============================================================================
// Reader and Writer
// =============================================================================

/// Read-only view of the LSM tree at a specific snapshot.
pub struct LsmReader<'a, FS: FileSystem> {
    tree: &'a LsmTree<FS>,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a, FS: FileSystem> PointLookup for LsmReader<'a, FS> {
    fn get(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        self.tree
            .get_internal(key, snapshot_lsn)
            .map(|opt| opt.map(ValueBuf))
    }
}

impl<'a, FS: FileSystem> OrderedScan for LsmReader<'a, FS> {
    type Cursor<'b> = LsmCursor<'b, FS> where Self: 'b;
    
    fn scan(
        &self,
        bounds: ScanBounds,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Cursor<'_>> {
        LsmCursor::new(self.tree, bounds, snapshot_lsn)
    }
}

impl<'a, FS: FileSystem> TableReader for LsmReader<'a, FS> {
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
    
    fn approximate_len(&self) -> TableResult<Option<u64>> {
        let stats = self.tree.stats()?;
        Ok(stats.row_count)
    }
}

/// Write view of the LSM tree for a specific transaction.
pub struct LsmWriter<'a, FS: FileSystem> {
    tree: &'a LsmTree<FS>,
    tx_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
    pending_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<'a, FS: FileSystem> MutableTable for LsmWriter<'a, FS> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<()> {
        self.pending_changes
            .push((key.to_vec(), Some(value.to_vec())));
        Ok(())
    }
    
    fn delete(&mut self, key: &[u8]) -> TableResult<bool> {
        // Check if key exists
        let exists = self.tree.get_internal(key, self.snapshot_lsn)?.is_some();
        if exists {
            self.pending_changes.push((key.to_vec(), None));
        }
        Ok(exists)
    }
    
    fn range_delete(&mut self, _bounds: ScanBounds) -> TableResult<u64> {
        // TODO: Implement range delete
        todo!("Range delete not yet implemented for LSM tree")
    }
}

impl<'a, FS: FileSystem> BatchOps for LsmWriter<'a, FS> {
    fn batch_get(&self, keys: &[&[u8]]) -> TableResult<Vec<Option<ValueBuf>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(
                self.tree
                    .get_internal(key, self.snapshot_lsn)?
                    .map(ValueBuf),
            );
        }
        Ok(results)
    }
    
    fn apply_batch<'b>(&mut self, batch: WriteBatch<'b>) -> TableResult<BatchReport> {
        let mut report = BatchReport::default();
        report.attempted = batch.mutations.len() as u64;
        
        for mutation in batch.mutations {
            match mutation {
                crate::table::Mutation::Put { key, value } => {
                    self.put(&key, &value)?;
                    report.applied += 1;
                    report.bytes_written += key.len() as u64 + value.len() as u64;
                }
                crate::table::Mutation::Delete { key } => {
                    if self.delete(&key)? {
                        report.deleted += 1;
                    }
                    report.applied += 1;
                }
                crate::table::Mutation::RangeDelete { bounds } => {
                    let deleted = self.range_delete(bounds)?;
                    report.deleted += deleted;
                    report.applied += 1;
                }
                crate::table::Mutation::Merge { .. } => {
                    // Merge not supported
                    continue;
                }
            }
        }
        
        Ok(report)
    }
}

impl<'a, FS: FileSystem> Flushable for LsmWriter<'a, FS> {
    fn flush(&mut self) -> TableResult<()> {
        if self.pending_changes.is_empty() {
            return Ok(());
        }
        
        // Apply all pending changes
        for (key, value_opt) in self.pending_changes.drain(..) {
            match value_opt {
                Some(value) => {
                    // Insert or update
                    self.tree
                        .insert_internal(key, value, self.tx_id, Some(self.snapshot_lsn))?;
                }
                None => {
                    // Delete (insert tombstone)
                    self.tree
                        .delete_internal(key, self.tx_id, Some(self.snapshot_lsn))?;
                }
            }
        }
        
        Ok(())
    }
}

impl<'a, FS: FileSystem> TableWriter for LsmWriter<'a, FS> {
    fn tx_id(&self) -> TransactionId {
        self.tx_id
    }
    
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}

// =============================================================================
// Cursor
// =============================================================================

/// Cursor for iterating over the LSM tree.
pub struct LsmCursor<'a, FS: FileSystem> {
    tree: &'a LsmTree<FS>,
    snapshot_lsn: LogSequenceNumber,
    bounds: ScanBounds,
    merge_iterator: Option<MergeIterator>,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    exhausted: bool,
    initialized: bool,
}

impl<'a, FS: FileSystem> LsmCursor<'a, FS> {
    fn new(
        tree: &'a LsmTree<FS>,
        bounds: ScanBounds,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self> {
        Ok(Self {
            tree,
            snapshot_lsn,
            bounds,
            merge_iterator: None,
            current_key: None,
            current_value: None,
            exhausted: false,
            initialized: false,
        })
    }
    
    fn is_in_bounds(&self, key: &[u8]) -> bool {
        match &self.bounds {
            ScanBounds::All => true,
            ScanBounds::Prefix(prefix) => key.starts_with(&prefix.0),
            ScanBounds::Range { start, end } => {
                let after_start = match start {
                    Bound::Included(k) => key >= k.0.as_slice(),
                    Bound::Excluded(k) => key > k.0.as_slice(),
                    Bound::Unbounded => true,
                };
                let before_end = match end {
                    Bound::Included(k) => key <= k.0.as_slice(),
                    Bound::Excluded(k) => key < k.0.as_slice(),
                    Bound::Unbounded => true,
                };
                after_start && before_end
            }
        }
    }
    
    fn ensure_initialized(&mut self) -> TableResult<()> {
        if self.initialized {
            return Ok(());
        }
        
        // Create merge iterator
        let iterators = self.tree.create_iterators(Direction::Forward, self.snapshot_lsn)?;
        let mut merge_iter = MergeIterator::new(iterators, Direction::Forward, self.snapshot_lsn)?;
        
        // Position at start of bounds
        match &self.bounds {
            ScanBounds::All => {
                merge_iter.seek_to_first()?;
            }
            ScanBounds::Prefix(prefix) => {
                merge_iter.seek(&prefix.0)?;
            }
            ScanBounds::Range { start, .. } => {
                match start {
                    Bound::Included(k) | Bound::Excluded(k) => {
                        merge_iter.seek(&k.0)?;
                    }
                    Bound::Unbounded => {
                        merge_iter.seek_to_first()?;
                    }
                }
            }
        }
        
        self.merge_iterator = Some(merge_iter);
        self.initialized = true;
        self.load_current()
    }
    
    fn load_current(&mut self) -> TableResult<()> {
        let merge_iter = self.merge_iterator.as_mut().unwrap();
        
        if let Some((key, value)) = merge_iter.current() {
            let key_vec = key.to_vec();
            let value_vec = value.to_vec();
            
            if self.is_in_bounds(&key_vec) {
                self.current_key = Some(key_vec);
                self.current_value = Some(value_vec);
                self.exhausted = false;
            } else {
                self.current_key = None;
                self.current_value = None;
                self.exhausted = true;
            }
        } else {
            self.current_key = None;
            self.current_value = None;
            self.exhausted = true;
        }
        
        Ok(())
    }
}

impl<'a, FS: FileSystem> TableCursor for LsmCursor<'a, FS> {
    fn valid(&self) -> bool {
        !self.exhausted && self.current_key.is_some()
    }
    
    fn key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }
    
    fn value(&self) -> Option<&[u8]> {
        self.current_value.as_deref()
    }
    
    fn next(&mut self) -> TableResult<()> {
        self.ensure_initialized()?;
        
        if self.exhausted {
            return Ok(());
        }
        
        let merge_iter = self.merge_iterator.as_mut().unwrap();
        merge_iter.next()?;
        self.load_current()
    }
    
    fn prev(&mut self) -> TableResult<()> {
        // TODO: Implement reverse iteration
        Err(TableError::CursorError(
            "Reverse iteration not yet implemented for LSM cursor".to_string(),
        ))
    }
    
    fn seek(&mut self, key: &[u8]) -> TableResult<()> {
        self.ensure_initialized()?;
        
        let merge_iter = self.merge_iterator.as_mut().unwrap();
        merge_iter.seek(key)?;
        self.load_current()
    }
    
    fn seek_for_prev(&mut self, _key: &[u8]) -> TableResult<()> {
        // TODO: Implement reverse seek
        Err(TableError::CursorError(
            "Reverse seek not yet implemented for LSM cursor".to_string(),
        ))
    }
    
    fn first(&mut self) -> TableResult<()> {
        self.ensure_initialized()?;
        
        let merge_iter = self.merge_iterator.as_mut().unwrap();
        merge_iter.seek_to_first()?;
        self.load_current()
    }
    
    fn last(&mut self) -> TableResult<()> {
        // TODO: Implement last
        Err(TableError::CursorError(
            "Last not yet implemented for LSM cursor".to_string(),
        ))
    }
    
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}
