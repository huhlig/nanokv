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

//! AppendLog table engine implementation.
//!
//! This module provides an append-only log storage engine optimized for
//! write-heavy workloads where data is only appended, never updated in place.
//!
//! # Architecture
//!
//! ```text
//! Writes → Active Segment → Roll to Immutable Segment
//!                              ↓
//!                         Compaction (optional)
//!                              ↓
//!                         Archived Segments
//! ```
//!
//! # Features
//!
//! - **Append-only**: Sequential writes for maximum throughput
//! - **Segment rolling**: Automatic segment creation when size threshold reached
//! - **In-memory index**: Fast point lookups via offset index
//! - **Sequential scans**: Efficient range queries over time-ordered data
//! - **Optional compaction**: Merge old segments to reclaim space
//! - **Retention policies**: Automatic cleanup of old data
//! - **Compression**: Optional per-segment compression
//!
//! # Use Cases
//!
//! - Event logs and audit trails
//! - Time-series data (simpler alternative to TimeSeries engine)
//! - Write-ahead logs
//! - Message queues
//! - Append-only databases

mod config;
mod segment;

pub use self::config::{AppendLogConfig, CompressionType, RetentionPolicy};
pub use self::segment::{Segment, SegmentId, SegmentMetadata};

use crate::pager::{PageId, Pager};
use crate::table::{
    Flushable, MutableTable, OrderedScan, PointLookup, Table, TableCapabilities,
    TableCursor, TableEngineKind, TableResult, TableStatistics,
};
use crate::types::{Bound, ScanBounds, ValueBuf};
use crate::vfs::FileSystem;
use crate::wal::LogSequenceNumber;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

// =============================================================================
// AppendLog Table Implementation
// =============================================================================

/// AppendLog storage engine.
///
/// Provides an append-only log storage engine optimized for write-heavy
/// workloads. Data is written sequentially to segments, with an in-memory
/// index for fast lookups.
pub struct AppendLog<FS: FileSystem> {
    /// Table identifier
    table_id: crate::types::TableId,

    /// Table name
    name: String,

    /// Configuration
    config: AppendLogConfig,

    /// Pager for persistent storage
    pager: Arc<Pager<FS>>,

    /// Root page ID (stores metadata)
    root_page_id: PageId,

    /// Internal state
    state: RwLock<AppendLogState>,
}

/// Internal mutable state of the AppendLog.
struct AppendLogState {
    /// Active segment being written to
    active_segment: Segment,

    /// Immutable segments (segment_id -> segment)
    immutable_segments: BTreeMap<SegmentId, Segment>,

    /// In-memory index: key -> (segment_id, offset)
    index: BTreeMap<Vec<u8>, (SegmentId, u64)>,

    /// Next segment ID to allocate
    next_segment_id: SegmentId,

    /// Total number of entries
    entry_count: u64,

    /// Total size in bytes
    total_size: u64,
}

impl<FS: FileSystem> AppendLog<FS> {
    /// Create a new AppendLog table.
    pub fn new(
        table_id: crate::types::TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        config: AppendLogConfig,
    ) -> TableResult<Self> {
        // Allocate root page for metadata - use LsmMeta as placeholder
        let root_page_id = pager
            .allocate_page(crate::pager::PageType::LsmMeta)
            .map_err(|e| crate::table::TableError::Other(format!("Failed to allocate root page: {}", e)))?;

        // Create initial active segment
        let active_segment = Segment::new(SegmentId(0), pager.clone())?;

        let state = AppendLogState {
            active_segment,
            immutable_segments: BTreeMap::new(),
            index: BTreeMap::new(),
            next_segment_id: SegmentId(1),
            entry_count: 0,
            total_size: 0,
        };

        Ok(Self {
            table_id,
            name,
            config,
            pager,
            root_page_id,
            state: RwLock::new(state),
        })
    }

    /// Open an existing AppendLog table.
    pub fn open(
        table_id: crate::types::TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        config: AppendLogConfig,
    ) -> TableResult<Self> {
        // TODO: Load metadata from root page
        // For now, create a new active segment
        let active_segment = Segment::new(SegmentId(0), pager.clone())?;

        let state = AppendLogState {
            active_segment,
            immutable_segments: BTreeMap::new(),
            index: BTreeMap::new(),
            next_segment_id: SegmentId(1),
            entry_count: 0,
            total_size: 0,
        };

        Ok(Self {
            table_id,
            name,
            config,
            pager,
            root_page_id,
            state: RwLock::new(state),
        })
    }

    /// Get the root page ID.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Check if active segment should be rolled.
    fn should_roll_segment(state: &AppendLogState, config: &AppendLogConfig) -> bool {
        state.active_segment.size() >= config.segment_size
    }

    /// Roll the active segment to immutable.
    fn roll_segment(state: &mut AppendLogState, pager: Arc<Pager<FS>>) -> TableResult<()> {
        // Move active segment to immutable
        let old_segment_id = state.active_segment.id();
        let old_segment = std::mem::replace(
            &mut state.active_segment,
            Segment::new(state.next_segment_id, pager)?,
        );

        state.immutable_segments.insert(old_segment_id, old_segment);
        state.next_segment_id = SegmentId(state.next_segment_id.0 + 1);

        debug!("Rolled segment {} to immutable", old_segment_id.0);

        Ok(())
    }

    /// Apply retention policy to remove old segments.
    fn apply_retention(state: &mut AppendLogState, policy: &RetentionPolicy) -> TableResult<()> {
        match policy {
            RetentionPolicy::None => Ok(()),
            RetentionPolicy::MaxSegments(max) => {
                // Remove oldest segments if we exceed the limit
                while state.immutable_segments.len() > *max {
                    if let Some((segment_id, _)) = state.immutable_segments.iter().next() {
                        let segment_id = *segment_id;
                        state.immutable_segments.remove(&segment_id);
                        // Remove index entries for this segment
                        state.index.retain(|_, (seg_id, _)| *seg_id != segment_id);
                        debug!("Removed segment {} due to retention policy", segment_id.0);
                    } else {
                        break;
                    }
                }
                Ok(())
            }
            RetentionPolicy::MaxAge(duration) => {
                // Remove segments older than the specified duration
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let cutoff = now.saturating_sub(duration.as_secs());

                let to_remove: Vec<SegmentId> = state
                    .immutable_segments
                    .iter()
                    .filter(|(_, seg)| seg.created_at() < cutoff)
                    .map(|(id, _)| *id)
                    .collect();

                for segment_id in to_remove {
                    state.immutable_segments.remove(&segment_id);
                    state.index.retain(|_, (seg_id, _)| *seg_id != segment_id);
                    debug!("Removed segment {} due to age retention", segment_id.0);
                }
                Ok(())
            }
        }
    }
}

// =============================================================================
// Table Trait Implementation
// =============================================================================

impl<FS: FileSystem> Table for AppendLog<FS> {
    fn table_id(&self) -> crate::types::TableId {
        self.table_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> TableEngineKind {
        TableEngineKind::AppendLog
    }

    fn capabilities(&self) -> TableCapabilities {
        TableCapabilities {
            ordered: true,
            point_lookup: true,
            prefix_scan: false,
            reverse_scan: false,
            range_delete: false,
            merge_operator: false,
            mvcc_native: false,
            append_optimized: true,
            memory_resident: false,
            disk_resident: true,
            supports_compression: true,
            supports_encryption: false,
        }
    }

    fn stats(&self) -> TableResult<TableStatistics> {
        let state = self.state.read().unwrap();
        Ok(TableStatistics {
            row_count: Some(state.entry_count),
            total_size_bytes: Some(state.total_size),
            key_stats: None,
            value_stats: None,
            histogram: None,
            last_updated_lsn: Some(LogSequenceNumber::from(0)), // TODO: Track LSN
        })
    }
}

// =============================================================================
// PointLookup Trait Implementation
// =============================================================================

impl<FS: FileSystem> PointLookup for AppendLog<FS> {
    fn get(&self, key: &[u8], _snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        let state = self.state.read().unwrap();

        // Look up key in index
        if let Some((segment_id, offset)) = state.index.get(key) {
            // Find the segment
            if *segment_id == state.active_segment.id() {
                state.active_segment.read_at(*offset)
            } else if let Some(segment) = state.immutable_segments.get(segment_id) {
                segment.read_at(*offset)
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

// =============================================================================
// MutableTable Trait Implementation
// =============================================================================

impl<FS: FileSystem> MutableTable for AppendLog<FS> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<u64> {
        let mut state = self.state.write().unwrap();

        // Check if we need to roll the segment
        if Self::should_roll_segment(&state, &self.config) {
            Self::roll_segment(&mut state, self.pager.clone())?;
            Self::apply_retention(&mut state, &self.config.retention_policy)?;
        }

        // Append to active segment
        let offset = state.active_segment.append(key, value)?;
        let segment_id = state.active_segment.id();

        // Update index
        state.index.insert(key.to_vec(), (segment_id, offset));

        // Update statistics
        state.entry_count += 1;
        let bytes_written = (key.len() + value.len()) as u64;
        state.total_size += bytes_written;

        Ok(bytes_written)
    }

    fn delete(&mut self, key: &[u8]) -> TableResult<bool> {
        let mut state = self.state.write().unwrap();
        
        // Remove from index
        let existed = state.index.remove(key).is_some();
        
        if existed {
            state.entry_count = state.entry_count.saturating_sub(1);
        }
        
        Ok(existed)
    }

    fn range_delete(&mut self, _bounds: ScanBounds) -> TableResult<u64> {
        // Range delete not supported for append-only logs
        Err(crate::table::TableError::Other(
            "Range delete not supported for AppendLog".to_string(),
        ))
    }
}

// =============================================================================
// OrderedScan Trait Implementation
// =============================================================================

impl<FS: FileSystem> OrderedScan for AppendLog<FS> {
    type Cursor<'a> = AppendLogCursor<'a, FS> where FS: 'a;

    fn scan(&self, bounds: ScanBounds, _snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Cursor<'_>> {
        AppendLogCursor::new(self, bounds)
    }
}

// =============================================================================
// Flushable Trait Implementation
// =============================================================================

impl<FS: FileSystem> Flushable for AppendLog<FS> {
    fn flush(&mut self) -> TableResult<()> {
        let state = self.state.read().unwrap();
        state.active_segment.flush()?;
        Ok(())
    }
}

// =============================================================================
// Cursor Implementation
// =============================================================================

/// Cursor for scanning AppendLog entries.
pub struct AppendLogCursor<'a, FS: FileSystem> {
    /// Reference to the AppendLog
    log: &'a AppendLog<FS>,

    /// Current position in the index
    current: Option<(Vec<u8>, (SegmentId, u64))>,

    /// Iterator over index entries
    iter: std::vec::IntoIter<(Vec<u8>, (SegmentId, u64))>,
}

impl<'a, FS: FileSystem> AppendLogCursor<'a, FS> {
    fn new(log: &'a AppendLog<FS>, bounds: ScanBounds) -> TableResult<Self> {
        let state = log.state.read().unwrap();

        // Collect entries within bounds
        let entries: Vec<_> = match bounds {
            ScanBounds::All => {
                state.index.iter().map(|(k, v)| (k.clone(), *v)).collect()
            }
            ScanBounds::Prefix(prefix) => {
                state.index
                    .range(prefix.0.clone()..)
                    .take_while(|(k, _)| k.starts_with(&prefix.0))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect()
            }
            ScanBounds::Range { start, end } => {
                let start_bound = match start {
                    Bound::Unbounded => std::ops::Bound::Unbounded,
                    Bound::Included(k) => std::ops::Bound::Included(k.0),
                    Bound::Excluded(k) => std::ops::Bound::Excluded(k.0),
                };
                let end_bound = match end {
                    Bound::Unbounded => std::ops::Bound::Unbounded,
                    Bound::Included(k) => std::ops::Bound::Included(k.0),
                    Bound::Excluded(k) => std::ops::Bound::Excluded(k.0),
                };
                state.index
                    .range((start_bound, end_bound))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect()
            }
        };

        let mut iter = entries.into_iter();
        let current = iter.next();

        Ok(Self {
            log,
            current,
            iter,
        })
    }
}

impl<'a, FS: FileSystem> TableCursor for AppendLogCursor<'a, FS> {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> Option<&[u8]> {
        self.current.as_ref().map(|(k, _)| k.as_slice())
    }

    fn value(&self) -> Option<&[u8]> {
        // AppendLog cursor doesn't support direct value access
        // Users should use get() with the key instead
        None
    }

    fn next(&mut self) -> TableResult<()> {
        self.current = self.iter.next();
        Ok(())
    }

    fn prev(&mut self) -> TableResult<()> {
        // Reverse scan not supported
        Err(crate::table::TableError::Other(
            "Reverse scan not supported for AppendLog".to_string(),
        ))
    }

    fn seek(&mut self, _key: &[u8]) -> TableResult<()> {
        // Seek not efficiently supported without rebuilding iterator
        Err(crate::table::TableError::Other(
            "Seek not supported for AppendLog cursor".to_string(),
        ))
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Seek for prev not supported for AppendLog".to_string(),
        ))
    }

    fn first(&mut self) -> TableResult<()> {
        // Reset to beginning - would need to rebuild iterator
        Err(crate::table::TableError::Other(
            "First not supported for AppendLog cursor".to_string(),
        ))
    }

    fn last(&mut self) -> TableResult<()> {
        Err(crate::table::TableError::Other(
            "Last not supported for AppendLog cursor".to_string(),
        ))
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        LogSequenceNumber::from(0) // TODO: Track snapshot LSN
    }
}

// Made with Bob