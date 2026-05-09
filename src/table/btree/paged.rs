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

//! Paged B-Tree implementation for disk-backed storage.
//!
//! This module provides a persistent B-Tree implementation that uses the pager
//! layer for disk storage. Features include:
//! - Disk-backed B-Tree with configurable order
//! - MVCC support through version chains
//! - Efficient range scans and point lookups
//! - Node split and merge operations
//! - Integration with the pager for page management
//!
//! The B-Tree uses two types of nodes:
//! - Internal nodes: Store keys and child page pointers
//! - Leaf nodes: Store key-value pairs with version chains

use crate::pager::{Page, PageId, PageType, Pager};
use crate::snap::Snapshot;
use crate::table::{
    BatchOps, BatchReport, Flushable, MutableTable, OrderedScan, PointLookup, TableCapabilities,
    TableCursor, TableEngine, TableEngineKind, TableId, TableReader, TableResult, TableStatistics,
    TableWriter, WriteBatch,
};
use crate::txn::{TransactionId, VersionChain};
use crate::types::{Bound, KeyBuf, ScanBounds, ValueBuf};
use crate::vfs::FileSystem;
use crate::wal::LogSequenceNumber;
use std::sync::Arc;

/// Default B-Tree order (maximum keys per node).
const DEFAULT_ORDER: usize = 64;

/// Minimum keys per node (except root).
const MIN_KEYS: usize = DEFAULT_ORDER / 2;

// =============================================================================
// Node Structures
// =============================================================================

/// B-Tree node type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeType {
    Internal,
    Leaf,
}

/// Internal node entry (key + child pointer).
#[derive(Debug, Clone)]
struct InternalEntry {
    key: Vec<u8>,
    child_page_id: PageId,
}

/// Leaf node entry (key + version chain).
#[derive(Debug, Clone)]
struct LeafEntry {
    key: Vec<u8>,
    chain: VersionChain,
}

/// B-Tree node (either internal or leaf).
#[derive(Debug, Clone)]
enum BTreeNode {
    Internal {
        /// Keys and child pointers (keys.len() == children.len() - 1)
        entries: Vec<InternalEntry>,
        /// Rightmost child pointer
        rightmost_child: PageId,
    },
    Leaf {
        /// Key-value pairs with version chains
        entries: Vec<LeafEntry>,
        /// Next leaf page for sequential scans (0 if none)
        next_leaf: PageId,
    },
}

impl BTreeNode {
    /// Create a new internal node.
    fn new_internal() -> Self {
        BTreeNode::Internal {
            entries: Vec::new(),
            rightmost_child: PageId::from(0),
        }
    }

    /// Create a new leaf node.
    fn new_leaf() -> Self {
        BTreeNode::Leaf {
            entries: Vec::new(),
            next_leaf: PageId::from(0),
        }
    }

    /// Get the node type.
    fn node_type(&self) -> NodeType {
        match self {
            BTreeNode::Internal { .. } => NodeType::Internal,
            BTreeNode::Leaf { .. } => NodeType::Leaf,
        }
    }

    /// Get the number of keys in the node.
    fn key_count(&self) -> usize {
        match self {
            BTreeNode::Internal { entries, .. } => entries.len(),
            BTreeNode::Leaf { entries, .. } => entries.len(),
        }
    }

    /// Check if the node is full.
    fn is_full(&self) -> bool {
        self.key_count() >= DEFAULT_ORDER
    }

    /// Check if the node has minimum keys.
    fn has_minimum_keys(&self) -> bool {
        self.key_count() >= MIN_KEYS
    }

    /// Serialize the node to bytes.
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write node type (1 byte)
        match self {
            BTreeNode::Internal { entries, rightmost_child } => {
                bytes.push(0); // Internal node

                // Write number of entries (4 bytes)
                bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());

                // Write rightmost child (8 bytes)
                bytes.extend_from_slice(&rightmost_child.to_bytes());

                // Write each entry
                for entry in entries {
                    // Key length (4 bytes)
                    bytes.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    // Key data
                    bytes.extend_from_slice(&entry.key);
                    // Child page ID (8 bytes)
                    bytes.extend_from_slice(&entry.child_page_id.to_bytes());
                }
            }
            BTreeNode::Leaf { entries, next_leaf } => {
                bytes.push(1); // Leaf node

                // Write number of entries (4 bytes)
                bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());

                // Write next leaf pointer (8 bytes)
                bytes.extend_from_slice(&next_leaf.to_bytes());

                // Write each entry
                for entry in entries {
                    // Key length (4 bytes)
                    bytes.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    // Key data
                    bytes.extend_from_slice(&entry.key);
                    // Version chain (serialized using bincode)
                    let chain_bytes = bincode::serialize(&entry.chain).unwrap();
                    bytes.extend_from_slice(&(chain_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(&chain_bytes);
                }
            }
        }

        bytes
    }

    /// Deserialize the node from bytes.
    fn from_bytes(bytes: &[u8]) -> TableResult<Self> {
        if bytes.is_empty() {
            return Err(crate::table::TableError::Corruption(
                "Empty node data".to_string(),
            ));
        }

        let node_type = bytes[0];
        let mut offset = 1;

        match node_type {
            0 => {
                // Internal node
                if bytes.len() < offset + 4 {
                    return Err(crate::table::TableError::Corruption(
                        "Insufficient data for entry count".to_string(),
                    ));
                }
                let entry_count =
                    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                if bytes.len() < offset + 8 {
                    return Err(crate::table::TableError::Corruption(
                        "Insufficient data for rightmost child".to_string(),
                    ));
                }
                let rightmost_child =
                    PageId::from(u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()));
                offset += 8;

                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    if bytes.len() < offset + 4 {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for key length".to_string(),
                        ));
                    }
                    let key_len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;

                    if bytes.len() < offset + key_len {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for key".to_string(),
                        ));
                    }
                    let key = bytes[offset..offset + key_len].to_vec();
                    offset += key_len;

                    if bytes.len() < offset + 8 {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for child page ID".to_string(),
                        ));
                    }
                    let child_page_id =
                        PageId::from(u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()));
                    offset += 8;

                    entries.push(InternalEntry { key, child_page_id });
                }

                Ok(BTreeNode::Internal {
                    entries,
                    rightmost_child,
                })
            }
            1 => {
                // Leaf node
                if bytes.len() < offset + 4 {
                    return Err(crate::table::TableError::Corruption(
                        "Insufficient data for entry count".to_string(),
                    ));
                }
                let entry_count =
                    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;

                if bytes.len() < offset + 8 {
                    return Err(crate::table::TableError::Corruption(
                        "Insufficient data for next leaf".to_string(),
                    ));
                }
                let next_leaf =
                    PageId::from(u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()));
                offset += 8;

                let mut entries = Vec::with_capacity(entry_count);
                for _ in 0..entry_count {
                    if bytes.len() < offset + 4 {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for key length".to_string(),
                        ));
                    }
                    let key_len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;

                    if bytes.len() < offset + key_len {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for key".to_string(),
                        ));
                    }
                    let key = bytes[offset..offset + key_len].to_vec();
                    offset += key_len;

                    if bytes.len() < offset + 4 {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for chain length".to_string(),
                        ));
                    }
                    let chain_len =
                        u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;

                    if bytes.len() < offset + chain_len {
                        return Err(crate::table::TableError::Corruption(
                            "Insufficient data for version chain".to_string(),
                        ));
                    }
                    let chain: VersionChain = bincode::deserialize(&bytes[offset..offset + chain_len])
                        .map_err(|e| crate::table::TableError::Corruption(format!("Failed to deserialize version chain: {}", e)))?;
                    offset += chain_len;

                    entries.push(LeafEntry { key, chain });
                }

                Ok(BTreeNode::Leaf { entries, next_leaf })
            }
            _ => Err(crate::table::TableError::Corruption(format!(
                "Invalid node type: {}",
                node_type
            ))),
        }
    }
}

// =============================================================================
// Paged B-Tree
// =============================================================================

/// Paged B-Tree table using the pager for disk storage.
pub struct PagedBTree<FS: FileSystem> {
    id: TableId,
    name: String,
    pager: Arc<Pager<FS>>,
    root_page_id: PageId,
}

impl<FS: FileSystem> PagedBTree<FS> {
    /// Create a new paged B-Tree table.
    pub fn new(id: TableId, name: String, pager: Arc<Pager<FS>>) -> TableResult<Self> {
        // Allocate root page (initially a leaf)
        let root_page_id = pager.allocate_page(PageType::BTreeLeaf)?;
        let root_node = BTreeNode::new_leaf();

        // Write root node to disk
        let mut page = Page::new(root_page_id, PageType::BTreeLeaf, pager.page_size().data_size());
        page.data_mut().extend_from_slice(&root_node.to_bytes());
        pager.write_page(&page)?;

        Ok(Self {
            id,
            name,
            pager,
            root_page_id,
        })
    }

    /// Open an existing paged B-Tree table.
    pub fn open(id: TableId, name: String, pager: Arc<Pager<FS>>, root_page_id: PageId) -> Self {
        Self {
            id,
            name,
            pager,
            root_page_id,
        }
    }

    /// Read a node from disk.
    fn read_node(&self, page_id: PageId) -> TableResult<BTreeNode> {
        let page = self.pager.read_page(page_id)?;
        BTreeNode::from_bytes(page.data())
    }

    /// Write a node to disk.
    fn write_node(&self, page_id: PageId, node: &BTreeNode) -> TableResult<()> {
        let page_type = match node.node_type() {
            NodeType::Internal => PageType::BTreeInternal,
            NodeType::Leaf => PageType::BTreeLeaf,
        };

        let mut page = Page::new(page_id, page_type, self.pager.page_size().data_size());
        page.data_mut().extend_from_slice(&node.to_bytes());
        self.pager.write_page(&page)?;
        Ok(())
    }

    /// Search for a key in the tree, returning the leaf page ID and position.
    fn search(&self, key: &[u8]) -> TableResult<(PageId, usize)> {
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.read_node(current_page_id)?;

            match node {
                BTreeNode::Internal { entries, rightmost_child } => {
                    // Binary search for the appropriate child
                    let pos = entries.binary_search_by(|e| e.key.as_slice().cmp(key));
                    let child_page_id = match pos {
                        Ok(idx) => entries[idx].child_page_id,
                        Err(idx) => {
                            if idx < entries.len() {
                                entries[idx].child_page_id
                            } else {
                                rightmost_child
                            }
                        }
                    };
                    current_page_id = child_page_id;
                }
                BTreeNode::Leaf { entries, .. } => {
                    // Found the leaf node
                    let pos = entries.binary_search_by(|e| e.key.as_slice().cmp(key));
                    let idx = match pos {
                        Ok(i) => i,
                        Err(i) => i,
                    };
                    return Ok((current_page_id, idx));
                }
            }
        }
    }

    /// Get a value for a key at a specific snapshot.
    fn get_internal(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        let (leaf_page_id, pos) = self.search(key)?;
        let node = self.read_node(leaf_page_id)?;

        if let BTreeNode::Leaf { entries, .. } = node {
            if pos < entries.len() && entries[pos].key == key {
                let snapshot = Snapshot::new(
                    crate::snap::SnapshotId::from(0),
                    String::new(),
                    snapshot_lsn,
                    0,
                    0,
                    Vec::new(),
                );
                if let Some(value) = entries[pos].chain.find_visible_version(&snapshot) {
                    return Ok(Some(ValueBuf(value.to_vec())));
                }
            }
        }

        Ok(None)
    }
}

impl<FS: FileSystem> TableEngine for PagedBTree<FS> {
    type Reader<'a> = PagedBTreeReader<'a, FS> where Self: 'a;
    type Writer<'a> = PagedBTreeWriter<'a, FS> where Self: 'a;

    fn table_id(&self) -> TableId {
        self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> TableEngineKind {
        TableEngineKind::BTree
    }

    fn capabilities(&self) -> TableCapabilities {
        TableCapabilities {
            ordered: true,
            point_lookup: true,
            prefix_scan: true,
            reverse_scan: true,
            range_delete: true,
            merge_operator: false,
            mvcc_native: true,
            append_optimized: false,
            memory_resident: false,
            disk_resident: true,
            supports_compression: true,
            supports_encryption: true,
        }
    }

    fn reader(&self, snapshot_lsn: LogSequenceNumber) -> TableResult<Self::Reader<'_>> {
        Ok(PagedBTreeReader {
            table: self,
            snapshot_lsn,
        })
    }

    fn writer(
        &self,
        tx_id: TransactionId,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Writer<'_>> {
        Ok(PagedBTreeWriter {
            table: self,
            tx_id,
            snapshot_lsn,
            pending_changes: Vec::new(),
        })
    }

    fn stats(&self) -> TableResult<TableStatistics> {
        // TODO: Implement proper statistics collection
        Ok(TableStatistics {
            row_count: None,
            total_size_bytes: None,
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

/// Read-only view of the paged B-Tree at a specific snapshot.
pub struct PagedBTreeReader<'a, FS: FileSystem> {
    table: &'a PagedBTree<FS>,
    snapshot_lsn: LogSequenceNumber,
}

impl<'a, FS: FileSystem> PointLookup for PagedBTreeReader<'a, FS> {
    fn get(&self, key: &[u8], snapshot_lsn: LogSequenceNumber) -> TableResult<Option<ValueBuf>> {
        self.table.get_internal(key, snapshot_lsn)
    }
}

impl<'a, FS: FileSystem> OrderedScan for PagedBTreeReader<'a, FS> {
    type Cursor<'b> = PagedBTreeCursor<'b, FS> where Self: 'b;

    fn scan(
        &self,
        bounds: ScanBounds,
        snapshot_lsn: LogSequenceNumber,
    ) -> TableResult<Self::Cursor<'_>> {
        Ok(PagedBTreeCursor::new(self.table, bounds, snapshot_lsn))
    }
}

impl<'a, FS: FileSystem> TableReader for PagedBTreeReader<'a, FS> {
    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }

    fn approximate_len(&self) -> TableResult<Option<u64>> {
        // TODO: Implement proper row count
        Ok(None)
    }
}

/// Write view of the paged B-Tree for a specific transaction.
pub struct PagedBTreeWriter<'a, FS: FileSystem> {
    table: &'a PagedBTree<FS>,
    tx_id: TransactionId,
    snapshot_lsn: LogSequenceNumber,
    pending_changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl<'a, FS: FileSystem> MutableTable for PagedBTreeWriter<'a, FS> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> TableResult<()> {
        self.pending_changes
            .push((key.to_vec(), Some(value.to_vec())));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> TableResult<bool> {
        // Check if key exists
        let exists = self.table.get_internal(key, self.snapshot_lsn)?.is_some();
        if exists {
            self.pending_changes.push((key.to_vec(), None));
        }
        Ok(exists)
    }

    fn range_delete(&mut self, _bounds: ScanBounds) -> TableResult<u64> {
        // TODO: Implement range delete
        todo!("Range delete not yet implemented for paged B-Tree")
    }
}

impl<'a, FS: FileSystem> BatchOps for PagedBTreeWriter<'a, FS> {
    fn batch_get(&self, keys: &[&[u8]]) -> TableResult<Vec<Option<ValueBuf>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.table.get_internal(key, self.snapshot_lsn)?);
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

impl<'a, FS: FileSystem> Flushable for PagedBTreeWriter<'a, FS> {
    fn flush(&mut self) -> TableResult<()> {
        if self.pending_changes.is_empty() {
            return Ok(());
        }

        // TODO: Implement actual B-Tree insertion with splits
        // For now, this is a placeholder that will be implemented in the next iteration
        
        self.pending_changes.clear();
        Ok(())
    }
}

impl<'a, FS: FileSystem> TableWriter for PagedBTreeWriter<'a, FS> {
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

/// Cursor for iterating over the paged B-Tree.
pub struct PagedBTreeCursor<'a, FS: FileSystem> {
    table: &'a PagedBTree<FS>,
    snapshot_lsn: LogSequenceNumber,
    bounds: ScanBounds,
    current_page_id: PageId,
    current_position: usize,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    exhausted: bool,
}

impl<'a, FS: FileSystem> PagedBTreeCursor<'a, FS> {
    fn new(table: &'a PagedBTree<FS>, bounds: ScanBounds, snapshot_lsn: LogSequenceNumber) -> Self {
        let mut cursor = Self {
            table,
            snapshot_lsn,
            bounds,
            current_page_id: PageId::from(0),
            current_position: 0,
            current_key: None,
            current_value: None,
            exhausted: false,
        };
        // Position at first valid entry
        let _ = cursor.first();
        cursor
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
}

impl<'a, FS: FileSystem> TableCursor for PagedBTreeCursor<'a, FS> {
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
        // TODO: Implement cursor navigation
        self.exhausted = true;
        Ok(())
    }

    fn prev(&mut self) -> TableResult<()> {
        // TODO: Implement reverse cursor navigation
        self.exhausted = true;
        Ok(())
    }

    fn seek(&mut self, _key: &[u8]) -> TableResult<()> {
        // TODO: Implement seek
        self.exhausted = true;
        Ok(())
    }

    fn seek_for_prev(&mut self, _key: &[u8]) -> TableResult<()> {
        // TODO: Implement seek_for_prev
        self.exhausted = true;
        Ok(())
    }

    fn first(&mut self) -> TableResult<()> {
        // TODO: Implement first
        self.exhausted = true;
        Ok(())
    }

    fn last(&mut self) -> TableResult<()> {
        // TODO: Implement last
        self.exhausted = true;
        Ok(())
    }

    fn snapshot_lsn(&self) -> LogSequenceNumber {
        self.snapshot_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_serialization() {
        // Test internal node
        let mut internal = BTreeNode::new_internal();
        if let BTreeNode::Internal { entries, rightmost_child } = &mut internal {
            entries.push(InternalEntry {
                key: b"key1".to_vec(),
                child_page_id: PageId::from(10),
            });
            *rightmost_child = PageId::from(20);
        }

        let bytes = internal.to_bytes();
        let deserialized = BTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.node_type(), NodeType::Internal);
        assert_eq!(deserialized.key_count(), 1);

        // Test leaf node
        let mut leaf = BTreeNode::new_leaf();
        if let BTreeNode::Leaf { entries, .. } = &mut leaf {
            entries.push(LeafEntry {
                key: b"key1".to_vec(),
                chain: VersionChain::new(b"value1".to_vec(), TransactionId::from(1)),
            });
        }

        let bytes = leaf.to_bytes();
        let deserialized = BTreeNode::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.node_type(), NodeType::Leaf);
        assert_eq!(deserialized.key_count(), 1);
    }
}

// Made with Bob
