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

//! Paged R-Tree implementation for geospatial indexing.

use crate::pager::{PageId, PageType, Pager};
use crate::table::{
    GeoHit, GeoPoint, GeoSpatial, GeometryRef, SpecialtyTableCapabilities, SpecialtyTableStats,
    Table, TableCapabilities, TableEngineKind, TableError, TableResult, VerificationReport,
};
use crate::types::{KeyBuf, TableId};
use crate::vfs::FileSystem;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, RwLock};
use tracing::{debug, instrument};

use super::config::SpatialConfig;
use super::mbr::Mbr;
use super::node::{InternalEntry, LeafEntry, RTreeNode};
use super::split::{split_internal_entries, split_leaf_entries};

/// Paged R-Tree for geospatial indexing.
///
/// This implementation stores the R-Tree structure across multiple pages,
/// allowing it to scale beyond available memory.
pub struct PagedRTree<FS: FileSystem> {
    /// Table identifier
    table_id: TableId,

    /// Table name
    name: String,

    /// Pager for page management
    pager: Arc<Pager<FS>>,

    /// Root page ID
    root_page_id: RwLock<PageId>,

    /// Configuration
    config: SpatialConfig,

    /// Tree height (number of levels)
    height: RwLock<u32>,

    /// Number of objects indexed
    object_count: RwLock<usize>,

    /// Cache of recently accessed nodes
    node_cache: RwLock<HashMap<PageId, RTreeNode>>,
}

impl<FS: FileSystem> PagedRTree<FS> {
    /// Create a new paged R-Tree.
    pub fn new(
        table_id: TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        config: SpatialConfig,
    ) -> TableResult<Self> {
        config
            .validate()
            .map_err(|e| TableError::Other(format!("Invalid spatial config: {}", e)))?;

        // Allocate root page
        let root_page_id = pager.allocate_page(PageType::RTreeNode)?;

        // Create empty root leaf node
        let root_node = RTreeNode::new_leaf();
        Self::write_node(&pager, root_page_id, &root_node)?;

        Ok(Self {
            table_id,
            name,
            pager,
            root_page_id: RwLock::new(root_page_id),
            config,
            height: RwLock::new(1),
            object_count: RwLock::new(0),
            node_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Open an existing paged R-Tree.
    pub fn open(
        table_id: TableId,
        name: String,
        pager: Arc<Pager<FS>>,
        root_page_id: PageId,
        config: SpatialConfig,
    ) -> TableResult<Self> {
        config
            .validate()
            .map_err(|e| TableError::Other(format!("Invalid spatial config: {}", e)))?;

        // Read root node to determine tree height
        let root_node = Self::read_node(&pager, root_page_id)?;
        let height = Self::calculate_height(&pager, root_page_id, &root_node)?;
        let object_count = Self::count_objects(&pager, root_page_id, &root_node)?;

        Ok(Self {
            table_id,
            name,
            pager,
            root_page_id: RwLock::new(root_page_id),
            config,
            height: RwLock::new(height),
            object_count: RwLock::new(object_count),
            node_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Get the root page ID.
    pub fn root_page_id(&self) -> PageId {
        *self.root_page_id.read().unwrap()
    }

    /// Read a node from a page.
    fn read_node(pager: &Pager<FS>, page_id: PageId) -> TableResult<RTreeNode> {
        let page = pager.read_page(page_id)?;
        RTreeNode::from_bytes(page.data()).map_err(|e| {
            TableError::corruption(
                format!("page {}", page_id),
                "rtree_node_decode",
                format!("Failed to parse node: {}", e),
            )
        })
    }

    /// Write a node to a page.
    fn write_node(pager: &Pager<FS>, page_id: PageId, node: &RTreeNode) -> TableResult<()> {
        let mut page = crate::pager::Page::new(page_id, PageType::RTreeNode, pager.page_size().data_size());
        page.data_mut().extend_from_slice(&node.to_bytes());
        let page_len = page.data().len();
        let header_len = crate::pager::PageHeader::SIZE;
        let checksum_len = crate::pager::Page::CHECKSUM_SIZE;
        let total_page_size = pager.page_size().to_u32() as usize;
        if header_len + page_len + checksum_len > total_page_size {
            return Err(TableError::Other(format!(
                "R-Tree node too large for page {page_id}: payload={} total_limit={}",
                page_len,
                total_page_size - header_len - checksum_len
            )));
        }
        pager.write_page(&page)?;
        Ok(())
    }

    /// Calculate the height of the tree.
    fn calculate_height(
        pager: &Pager<FS>,
        _page_id: PageId,
        node: &RTreeNode,
    ) -> TableResult<u32> {
        match node {
            RTreeNode::Leaf { .. } => Ok(1),
            RTreeNode::Internal { entries, level, .. } => {
                if entries.is_empty() {
                    Ok(*level + 1)
                } else {
                    // Recursively check first child
                    let child_node = Self::read_node(pager, entries[0].child_page_id)?;
                    let child_height = Self::calculate_height(pager, entries[0].child_page_id, &child_node)?;
                    Ok(child_height + 1)
                }
            }
        }
    }

    /// Count the total number of objects in the tree.
    fn count_objects(
        pager: &Pager<FS>,
        _page_id: PageId,
        node: &RTreeNode,
    ) -> TableResult<usize> {
        match node {
            RTreeNode::Leaf { entries, .. } => Ok(entries.len()),
            RTreeNode::Internal { entries, .. } => {
                let mut count = 0;
                for entry in entries {
                    let child_node = Self::read_node(pager, entry.child_page_id)?;
                    count += Self::count_objects(pager, entry.child_page_id, &child_node)?;
                }
                Ok(count)
            }
        }
    }

    /// Insert a geometry into the tree.
    #[instrument(skip(self, geometry))]
    fn insert_internal(&self, id: &[u8], geometry: GeometryRef<'_>) -> TableResult<()> {
        let mbr = self.geometry_to_mbr(geometry)?;
        let object_id = KeyBuf(id.to_vec());
        let entry = LeafEntry::new(mbr, object_id);

        let root_page_id = self.root_page_id();
        let root_node = Self::read_node(&self.pager, root_page_id)
            .map_err(|e| TableError::Other(format!("Failed to read root node {root_page_id} before insert of {:?}: {e}", id)))?;

        // Find the appropriate leaf node
        let leaf_page_id = self.choose_leaf(root_page_id, &root_node, &entry.mbr)?;
        let mut leaf_node = Self::read_node(&self.pager, leaf_page_id)
            .map_err(|e| TableError::Other(format!("Failed to read leaf node {leaf_page_id} before insert of {:?}: {e}", id)))?;

        // Add entry to leaf
        leaf_node
            .add_leaf_entry(entry)
            .map_err(TableError::Other)?;

        // Check if split is needed
        if leaf_node.entry_count() > self.config.max_entries_per_node {
            self.split_node(leaf_page_id, leaf_node)?;
        } else {
            Self::write_node(&self.pager, leaf_page_id, &leaf_node)?;
        }

        // Increment object count
        *self.object_count.write().unwrap() += 1;

        Ok(())
    }

    /// Choose the best leaf node for inserting an entry.
    fn choose_leaf(
        &self,
        page_id: PageId,
        node: &RTreeNode,
        mbr: &Mbr,
    ) -> TableResult<PageId> {
        match node {
            RTreeNode::Leaf { .. } => Ok(page_id),
            RTreeNode::Internal { entries, .. } => {
                // Find entry with minimum area increase
                let mut best_idx = 0;
                let mut min_increase = f64::INFINITY;
                let mut min_area = f64::INFINITY;

                for (i, entry) in entries.iter().enumerate() {
                    let increase = entry.mbr.area_increase(mbr);
                    let area = entry.mbr.area();

                    if increase < min_increase || (increase == min_increase && area < min_area) {
                        min_increase = increase;
                        min_area = area;
                        best_idx = i;
                    }
                }

                let child_page_id = entries[best_idx].child_page_id;
                let child_node = Self::read_node(&self.pager, child_page_id)
                    .map_err(|e| TableError::Other(format!("Failed to read child node {child_page_id} during choose_leaf: {e}")))?;
                self.choose_leaf(child_page_id, &child_node, mbr)
            }
        }
    }

    /// Split a node that has overflowed.
    fn split_node(&self, page_id: PageId, node: RTreeNode) -> TableResult<()> {
        match node {
            RTreeNode::Leaf { entries, parent_page_id, next_leaf } => {
                let split_result = split_leaf_entries(entries, self.config.split_strategy, self.config.dimensions);

                // Create new leaf node for right split
                let new_page_id = self.pager.allocate_page(PageType::RTreeNode)?;

                let mut left_node = RTreeNode::new_leaf();
                left_node.set_parent_page_id(parent_page_id);
                let mut right_node = RTreeNode::new_leaf();
                right_node.set_parent_page_id(parent_page_id);

                for entry in split_result.left {
                    left_node.add_leaf_entry(entry).map_err(TableError::Other)?;
                }
                for entry in split_result.right {
                    right_node.add_leaf_entry(entry).map_err(TableError::Other)?;
                }

                // Update leaf chain
                if let RTreeNode::Leaf { next_leaf: ref mut left_next, .. } = left_node {
                    *left_next = new_page_id;
                }
                if let RTreeNode::Leaf { next_leaf: ref mut right_next, .. } = right_node {
                    *right_next = next_leaf;
                }

                // Write nodes
                Self::write_node(&self.pager, page_id, &left_node)?;
                Self::write_node(&self.pager, new_page_id, &right_node)?;

                // Update parent
                let left_mbr = left_node.calculate_mbr(self.config.dimensions);
                let right_mbr = right_node.calculate_mbr(self.config.dimensions);

                if parent_page_id == PageId::from(0) {
                    // Root split: move the original root contents to a new left child page,
                    // reuse the current page as the internal root, and write the right child separately.
                    let left_page_id = self.pager.allocate_page(PageType::RTreeNode)?;

                    if let RTreeNode::Leaf { parent_page_id, .. } = &mut left_node {
                        *parent_page_id = page_id;
                    }
                    if let RTreeNode::Leaf { parent_page_id, .. } = &mut right_node {
                        *parent_page_id = page_id;
                    }

                    let root_level = *self.height.read().unwrap();
                    let mut root_node = RTreeNode::new_internal(root_level);
                    root_node
                        .add_internal_entry(InternalEntry::new(left_mbr, left_page_id))
                        .map_err(TableError::Other)?;
                    root_node
                        .add_internal_entry(InternalEntry::new(right_mbr, new_page_id))
                        .map_err(TableError::Other)?;

                    Self::write_node(&self.pager, left_page_id, &left_node)?;
                    Self::write_node(&self.pager, new_page_id, &right_node)?;
                    Self::write_node(&self.pager, page_id, &root_node)?;
                    *self.height.write().unwrap() += 1;
                } else {
                    // Update existing parent
                    self.update_parent_after_split(parent_page_id, page_id, left_mbr, new_page_id, right_mbr)?;
                }
            }
            RTreeNode::Internal { entries, parent_page_id, level } => {
                let split_result = split_internal_entries(entries, self.config.split_strategy, self.config.dimensions);

                let new_page_id = self.pager.allocate_page(PageType::RTreeNode)?;

                let mut left_node = RTreeNode::new_internal(level);
                left_node.set_parent_page_id(parent_page_id);
                let mut right_node = RTreeNode::new_internal(level);
                right_node.set_parent_page_id(parent_page_id);

                for entry in split_result.left {
                    left_node.add_internal_entry(entry).map_err(TableError::Other)?;
                }
                for entry in split_result.right {
                    right_node.add_internal_entry(entry).map_err(TableError::Other)?;
                }

                Self::write_node(&self.pager, page_id, &left_node)?;
                Self::write_node(&self.pager, new_page_id, &right_node)?;
                self.update_children_parent_page_ids(page_id, &left_node)?;
                self.update_children_parent_page_ids(new_page_id, &right_node)?;

                let left_mbr = left_node.calculate_mbr(self.config.dimensions);
                let right_mbr = right_node.calculate_mbr(self.config.dimensions);

                if parent_page_id == PageId::from(0) {
                    let left_page_id = self.pager.allocate_page(PageType::RTreeNode)?;

                    if let RTreeNode::Internal { parent_page_id, .. } = &mut left_node {
                        *parent_page_id = page_id;
                    }
                    if let RTreeNode::Internal { parent_page_id, .. } = &mut right_node {
                        *parent_page_id = page_id;
                    }

                    let root_level = *self.height.read().unwrap();
                    let mut root_node = RTreeNode::new_internal(root_level);
                    root_node
                        .add_internal_entry(InternalEntry::new(left_mbr, left_page_id))
                        .map_err(TableError::Other)?;
                    root_node
                        .add_internal_entry(InternalEntry::new(right_mbr, new_page_id))
                        .map_err(TableError::Other)?;

                    Self::write_node(&self.pager, left_page_id, &left_node)?;
                    Self::write_node(&self.pager, new_page_id, &right_node)?;
                    Self::write_node(&self.pager, page_id, &root_node)?;
                    self.update_children_parent_page_ids(left_page_id, &left_node)?;
                    self.update_children_parent_page_ids(new_page_id, &right_node)?;
                    *self.height.write().unwrap() += 1;
                } else {
                    self.update_parent_after_split(parent_page_id, page_id, left_mbr, new_page_id, right_mbr)?;
                }
            }
        }

        Ok(())
    }

    /// Update parent page IDs for all children of an internal node.
    fn update_children_parent_page_ids(&self, parent_page_id: PageId, node: &RTreeNode) -> TableResult<()> {
        if let Some(entries) = node.internal_entries() {
            for entry in entries {
                let mut child_node = Self::read_node(&self.pager, entry.child_page_id)?;
                child_node.set_parent_page_id(parent_page_id);
                Self::write_node(&self.pager, entry.child_page_id, &child_node)?;
            }
        }

        Ok(())
    }

    /// Find the leaf page containing the specified object ID.
    fn find_leaf_containing_id(&self, page_id: PageId, object_id: &[u8]) -> TableResult<Option<PageId>> {
        let node = Self::read_node(&self.pager, page_id)?;
        match node {
            RTreeNode::Leaf { entries, .. } => Ok(entries
                .iter()
                .any(|entry| entry.object_id.as_ref() == object_id)
                .then_some(page_id)),
            RTreeNode::Internal { entries, .. } => {
                for entry in entries {
                    if let Some(found_page_id) = self.find_leaf_containing_id(entry.child_page_id, object_id)? {
                        return Ok(Some(found_page_id));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Delete an object from the tree.
    fn delete_internal(&self, id: &[u8]) -> TableResult<()> {
        let root_page_id = self.root_page_id();
        let Some(leaf_page_id) = self.find_leaf_containing_id(root_page_id, id)? else {
            return Ok(());
        };

        let mut leaf_node = Self::read_node(&self.pager, leaf_page_id)?;
        let parent_page_id = leaf_node.parent_page_id();

        let removed = if let Some(entries) = leaf_node.leaf_entries_mut() {
            if let Some(index) = entries.iter().position(|entry| entry.object_id.as_ref() == id) {
                entries.remove(index);
                true
            } else {
                false
            }
        } else {
            false
        };

        if !removed {
            return Ok(());
        }

        Self::write_node(&self.pager, leaf_page_id, &leaf_node)?;
        self.condense_tree(leaf_page_id, leaf_node, parent_page_id)?;

        let mut object_count = self.object_count.write().unwrap();
        if *object_count > 0 {
            *object_count -= 1;
        }

        Ok(())
    }

    /// Condense tree after deletion, handling underflow and root shrinking.
    fn condense_tree(
        &self,
        mut page_id: PageId,
        mut node: RTreeNode,
        mut parent_page_id: PageId,
    ) -> TableResult<()> {
        loop {
            let is_root = page_id == self.root_page_id();

            if is_root {
                self.adjust_root_after_delete(page_id, node)?;
                return Ok(());
            }

            let needs_underflow_handling = node.entry_count() < self.config.min_entries_per_node;
            if needs_underflow_handling {
                let orphaned_entries = self.collect_entries_for_reinsertion(&node);
                self.remove_child_from_parent(parent_page_id, page_id)?;

                let parent_node = Self::read_node(&self.pager, parent_page_id)?;
                page_id = parent_page_id;
                parent_page_id = parent_node.parent_page_id();
                node = parent_node;

                self.reinsert_entries(orphaned_entries)?;
                continue;
            }

            self.update_node_mbr_in_parent(parent_page_id, page_id, &node)?;
            let parent_node = Self::read_node(&self.pager, parent_page_id)?;
            page_id = parent_page_id;
            parent_page_id = parent_node.parent_page_id();
            node = parent_node;
        }
    }

    /// Remove a child reference from an internal parent node.
    fn remove_child_from_parent(&self, parent_page_id: PageId, child_page_id: PageId) -> TableResult<()> {
        let mut parent_node = Self::read_node(&self.pager, parent_page_id)?;
        if let Some(entries) = parent_node.internal_entries_mut() {
            if let Some(index) = entries.iter().position(|entry| entry.child_page_id == child_page_id) {
                entries.remove(index);
            }
        }
        Self::write_node(&self.pager, parent_page_id, &parent_node)
    }

    /// Update a node's MBR entry inside its parent.
    fn update_node_mbr_in_parent(
        &self,
        parent_page_id: PageId,
        child_page_id: PageId,
        child_node: &RTreeNode,
    ) -> TableResult<()> {
        let mut parent_node = Self::read_node(&self.pager, parent_page_id)?;
        if let Some(entries) = parent_node.internal_entries_mut() {
            if let Some(entry) = entries.iter_mut().find(|entry| entry.child_page_id == child_page_id) {
                entry.mbr = child_node.calculate_mbr(self.config.dimensions);
            }
        }
        Self::write_node(&self.pager, parent_page_id, &parent_node)
    }

    /// Adjust the root node after deletion, shrinking tree height when possible.
    fn adjust_root_after_delete(&self, root_page_id: PageId, root_node: RTreeNode) -> TableResult<()> {
        match root_node {
            RTreeNode::Internal { entries, .. } if entries.len() == 1 => {
                let child_page_id = entries[0].child_page_id;
                let mut child_node = Self::read_node(&self.pager, child_page_id)?;
                child_node.set_parent_page_id(PageId::from(0));
                Self::write_node(&self.pager, child_page_id, &child_node)?;
                *self.root_page_id.write().unwrap() = child_page_id;

                let mut height = self.height.write().unwrap();
                if *height > 1 {
                    *height -= 1;
                }
                Ok(())
            }
            RTreeNode::Internal { entries, level, .. } if entries.is_empty() => {
                let empty_root = RTreeNode::new_internal(level);
                Self::write_node(&self.pager, root_page_id, &empty_root)?;
                Ok(())
            }
            RTreeNode::Leaf { entries, .. } if entries.is_empty() => {
                let empty_root = RTreeNode::new_leaf();
                Self::write_node(&self.pager, root_page_id, &empty_root)?;
                *self.height.write().unwrap() = 1;
                Ok(())
            }
            other => {
                Self::write_node(&self.pager, root_page_id, &other)?;
                Ok(())
            }
        }
    }

    /// Reinsert orphaned entries without triggering recursive condense behavior.
    fn reinsert_entries(&self, entries: Vec<LeafEntry>) -> TableResult<()> {
        for entry in entries {
            let root_page_id = self.root_page_id();
            let root_node = Self::read_node(&self.pager, root_page_id)?;
            let leaf_page_id = self.choose_leaf(root_page_id, &root_node, &entry.mbr)?;
            let mut leaf_node = Self::read_node(&self.pager, leaf_page_id)?;

            leaf_node
                .add_leaf_entry(entry)
                .map_err(TableError::Other)?;

            if leaf_node.entry_count() > self.config.max_entries_per_node {
                self.split_node(leaf_page_id, leaf_node)?;
            } else {
                Self::write_node(&self.pager, leaf_page_id, &leaf_node)?;
            }
        }

        Ok(())
    }

    /// Collect leaf entries to reinsert after an underflowed subtree is removed.
    fn collect_entries_for_reinsertion(&self, node: &RTreeNode) -> Vec<LeafEntry> {
        match node {
            RTreeNode::Leaf { entries, .. } => entries.clone(),
            RTreeNode::Internal { entries, .. } => {
                let mut collected = Vec::new();
                for entry in entries {
                    if let Ok(child_node) = Self::read_node(&self.pager, entry.child_page_id) {
                        collected.extend(self.collect_entries_for_reinsertion(&child_node));
                    }
                }
                collected
            }
        }
    }

    /// Create a new root node after a split.
    fn create_new_root(
        &self,
        left_page_id: PageId,
        left_mbr: Mbr,
        right_page_id: PageId,
        right_mbr: Mbr,
    ) -> TableResult<()> {
        let new_root_page_id = self.pager.allocate_page(PageType::RTreeNode)?;

        let mut new_root = RTreeNode::new_internal(*self.height.read().unwrap());
        new_root
            .add_internal_entry(InternalEntry::new(left_mbr, left_page_id))
            .map_err(TableError::Other)?;
        new_root
            .add_internal_entry(InternalEntry::new(right_mbr, right_page_id))
            .map_err(TableError::Other)?;

        let mut left_node = Self::read_node(&self.pager, left_page_id)?;
        left_node.set_parent_page_id(new_root_page_id);
        Self::write_node(&self.pager, left_page_id, &left_node)?;

        let mut right_node = Self::read_node(&self.pager, right_page_id)?;
        right_node.set_parent_page_id(new_root_page_id);
        Self::write_node(&self.pager, right_page_id, &right_node)?;

        Self::write_node(&self.pager, new_root_page_id, &new_root)
            .map_err(|e| TableError::Other(format!("Failed to write new root page {new_root_page_id}: {e}")))?;

        // Update root page ID and height
        *self.root_page_id.write().unwrap() = new_root_page_id;
        *self.height.write().unwrap() += 1;

        debug!("Created new root at page {}, height now {}", new_root_page_id, *self.height.read().unwrap());

        Ok(())
    }

    /// Update parent node after a child split.
    fn update_parent_after_split(
        &self,
        parent_page_id: PageId,
        old_child_id: PageId,
        old_mbr: Mbr,
        new_child_id: PageId,
        new_mbr: Mbr,
    ) -> TableResult<()> {
        let mut parent_node = Self::read_node(&self.pager, parent_page_id)?;

        // Find and update the old entry
        if let Some(entries) = parent_node.internal_entries_mut() {
            for entry in entries.iter_mut() {
                if entry.child_page_id == old_child_id {
                    entry.mbr = old_mbr;
                    break;
                }
            }
            // Add new entry
            entries.push(InternalEntry::new(new_mbr, new_child_id));
        }

        // Check if parent needs to split
        if parent_node.entry_count() > self.config.max_entries_per_node {
            self.split_node(parent_page_id, parent_node)?;
        } else {
            Self::write_node(&self.pager, parent_page_id, &parent_node)?;
        }

        Ok(())
    }

    /// Convert a geometry reference to an MBR.
    fn geometry_to_mbr(&self, geometry: GeometryRef<'_>) -> TableResult<Mbr> {
        match geometry {
            GeometryRef::Point(point) => Ok(Mbr::from_point_2d(point)),
            GeometryRef::BoundingBox { min, max } => Ok(Mbr::from_points_2d(min, max)),
            GeometryRef::Wkb(_wkb) => {
                // TODO: Parse WKB format
                Err(TableError::operation_not_supported(
                    "WKB geometry parsing not yet implemented",
                ))
            }
        }
    }

    /// Search for geometries that intersect with a query geometry.
    #[instrument(skip(self, query))]
    fn search_intersects(&self, query: GeometryRef<'_>, limit: usize) -> TableResult<Vec<GeoHit>> {
        let query_mbr = self.geometry_to_mbr(query)?;
        let mut results = Vec::new();

        let root_page_id = self.root_page_id();
        let root_node = Self::read_node(&self.pager, root_page_id)?;

        self.search_intersects_recursive(root_page_id, &root_node, &query_mbr, &mut results, limit)?;

        Ok(results)
    }

    /// Recursive helper for intersection search.
    fn search_intersects_recursive(
        &self,
        page_id: PageId,
        node: &RTreeNode,
        query_mbr: &Mbr,
        results: &mut Vec<GeoHit>,
        limit: usize,
    ) -> TableResult<()> {
        if results.len() >= limit {
            return Ok(());
        }

        match node {
            RTreeNode::Leaf { entries, .. } => {
                for entry in entries {
                    if entry.mbr.intersects(query_mbr) {
                        results.push(GeoHit {
                            id: entry.object_id.clone(),
                            distance: None,
                        });
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
            RTreeNode::Internal { entries, .. } => {
                for entry in entries {
                    if entry.mbr.intersects(query_mbr) {
                        let child_node = Self::read_node(&self.pager, entry.child_page_id)?;
                        self.search_intersects_recursive(
                            entry.child_page_id,
                            &child_node,
                            query_mbr,
                            results,
                            limit,
                        )?;
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Search for the nearest geometries to a point.
    #[instrument(skip(self))]
    fn search_nearest(&self, point: GeoPoint, limit: usize) -> TableResult<Vec<GeoHit>> {
        let mut heap = BinaryHeap::new();
        let mut results = Vec::new();

        let root_page_id = self.root_page_id();
        let root_node = Self::read_node(&self.pager, root_page_id)?;

        // Priority queue entry: (negative distance, page_id, is_leaf)
        heap.push(std::cmp::Reverse((
            -root_node.calculate_mbr(self.config.dimensions).min_distance(point) as i64,
            root_page_id,
            matches!(root_node, RTreeNode::Leaf { .. }),
        )));

        while let Some(std::cmp::Reverse((neg_dist, page_id, is_leaf))) = heap.pop() {
            if results.len() >= limit {
                break;
            }

            let node = Self::read_node(&self.pager, page_id)?;

            match node {
                RTreeNode::Leaf { entries, .. } => {
                    for entry in entries {
                        let distance = entry.mbr.min_distance(point);
                        results.push((distance, entry.object_id.clone()));
                    }
                }
                RTreeNode::Internal { entries, .. } => {
                    for entry in entries {
                        let distance = entry.mbr.min_distance(point);
                        heap.push(std::cmp::Reverse((
                            -(distance as i64),
                            entry.child_page_id,
                            false,
                        )));
                    }
                }
            }
        }

        // Sort by distance and take top limit
        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        results.truncate(limit);

        Ok(results
            .into_iter()
            .map(|(distance, id)| GeoHit {
                id,
                distance: Some(distance as f32),
            })
            .collect())
    }
}

// Implement Table trait
impl<FS: FileSystem> Table for PagedRTree<FS> {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> TableEngineKind {
        TableEngineKind::GeoSpatial
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

    fn stats(&self) -> TableResult<crate::table::TableStatistics> {
        let count = *self.object_count.read().unwrap();
        Ok(crate::table::TableStatistics {
            row_count: Some(count as u64),
            total_size_bytes: None, // TODO: Calculate actual size
            key_stats: Some(crate::table::KeyStatistics {
                min_size: 0,
                max_size: 0,
                avg_size: 0.0,
                distinct_count: Some(count as u64),
            }),
            value_stats: Some(crate::table::ValueStatistics {
                min_size: 0,
                max_size: 0,
                avg_size: 0.0,
                null_count: None,
            }),
            histogram: None,
            last_updated_lsn: None,
        })
    }
}

// Implement GeoSpatial trait
impl<FS: FileSystem> GeoSpatial for PagedRTree<FS> {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> SpecialtyTableCapabilities {
        SpecialtyTableCapabilities {
            exact: true,
            approximate: false,
            ordered: false,
            sparse: false,
            supports_delete: true,
            supports_range_query: true,
            supports_prefix_query: false,
            supports_scoring: true,
            supports_incremental_rebuild: false,
            may_be_stale: false,
        }
    }

    fn insert_geometry(&mut self, id: &[u8], geometry: GeometryRef<'_>) -> TableResult<()> {
        self.insert_internal(id, geometry)
    }

    fn delete_geometry(&mut self, id: &[u8]) -> TableResult<()> {
        self.delete_internal(id)
    }

    fn intersects(&self, query: GeometryRef<'_>, limit: usize) -> TableResult<Vec<GeoHit>> {
        self.search_intersects(query, limit)
    }

    fn nearest(&self, point: GeoPoint, limit: usize) -> TableResult<Vec<GeoHit>> {
        self.search_nearest(point, limit)
    }

    fn stats(&self) -> TableResult<SpecialtyTableStats> {
        let count = *self.object_count.read().unwrap();
        Ok(SpecialtyTableStats {
            entry_count: Some(count as u64),
            size_bytes: None, // TODO: Calculate actual size
            distinct_keys: Some(count as u64),
            stale_entries: None,
            last_updated_lsn: None,
        })
    }

    fn verify(&self) -> TableResult<VerificationReport> {
        // TODO: Implement verification
        Ok(VerificationReport {
            checked_items: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        })
    }
}

// Made with Bob
