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

//! STR (Sort-Tile-Recursive) bulk loading algorithm for R-Tree.
//!
//! This algorithm creates a well-balanced R-Tree by sorting entries and
//! partitioning them into tiles, resulting in minimal overlap and good
//! query performance.

use super::config::SpatialConfig;
use super::mbr::Mbr;
use super::node::{InternalEntry, LeafEntry, RTreeNode};
use crate::pager::{PageId, PageType, Pager};
use crate::table::TableError;
use crate::vfs::FileSystem;

/// Entry to be bulk loaded, with precomputed center coordinates.
#[derive(Clone)]
struct BulkEntry {
    mbr: Mbr,
    object_id: crate::types::KeyBuf,
    center_x: f64,
    center_y: f64,
}

/// Result of bulk loading a level.
struct BulkLevel {
    /// Page IDs of nodes at this level
    page_ids: Vec<PageId>,
    /// MBRs of those nodes
    mbrs: Vec<Mbr>,
}

/// Perform STR bulk loading of leaf entries.
///
/// Returns the root page ID, tree height, and object count.
pub fn str_bulk_load<FS: FileSystem>(
    pager: &Pager<FS>,
    entries: Vec<LeafEntry>,
    config: &SpatialConfig,
) -> Result<(PageId, u32, usize), TableError> {
    if entries.is_empty() {
        let root_page_id = pager.allocate_page(PageType::RTreeNode)?;
        let root_node = RTreeNode::new_leaf();
        write_node(pager, root_page_id, &root_node)?;
        return Ok((root_page_id, 1, 0));
    }

    let max_entries = config.max_entries_per_node;
    let dimensions = config.dimensions;

    // Convert to bulk entries with precomputed centers
    let mut bulk_entries: Vec<BulkEntry> = entries
        .into_iter()
        .map(|entry| {
            let center = entry.mbr.center();
            BulkEntry {
                mbr: entry.mbr,
                object_id: entry.object_id,
                center_x: center.x,
                center_y: center.y,
            }
        })
        .collect();

    let object_count = bulk_entries.len();

    // Calculate number of slices: S = ceil(sqrt(N / M))
    let n = bulk_entries.len() as f64;
    let m = max_entries as f64;
    let num_slices = (n / m).sqrt().ceil() as usize;
    let num_slices = num_slices.max(1);

    // Step 1: Sort by x-coordinate
    bulk_entries.sort_by(|a, b| a.center_x.partial_cmp(&b.center_x).unwrap());

    // Step 2: Divide into vertical slices and sort each by y-coordinate
    // Then group into leaf nodes
    let mut leaf_groups: Vec<Vec<LeafEntry>> = Vec::new();

    for slice_idx in 0..num_slices {
        // Calculate slice boundaries
        let slice_start = (slice_idx * bulk_entries.len()) / num_slices;
        let slice_end = ((slice_idx + 1) * bulk_entries.len()) / num_slices;
        let slice = &mut bulk_entries[slice_start..slice_end];

        // Sort by y-coordinate within this slice
        slice.sort_by(|a, b| a.center_y.partial_cmp(&b.center_y).unwrap());

        // Group into leaf nodes of max_entries each
        for chunk in slice.chunks(max_entries) {
            let leaf_entries: Vec<LeafEntry> = chunk
                .iter()
                .map(|e| LeafEntry::new(e.mbr, e.object_id.clone()))
                .collect();
            leaf_groups.push(leaf_entries);
        }
    }

    // Step 3: Create leaf nodes and write them to pages
    let mut leaf_page_ids = Vec::new();
    let mut leaf_mbrs = Vec::new();

    for (i, group) in leaf_groups.into_iter().enumerate() {
        let page_id = pager.allocate_page(PageType::RTreeNode)?;
        let mut node = RTreeNode::new_leaf();

        for entry in group {
            node.add_leaf_entry(entry).map_err(TableError::Other)?;
        }

        write_node(pager, page_id, &node)?;

        let mbr = node.calculate_mbr(dimensions);
        leaf_page_ids.push(page_id);
        leaf_mbrs.push(mbr);

        // Link leaf nodes for sequential scans
        if i > 0 {
            let prev_page_id = leaf_page_ids[i - 1];
            let mut prev_node = read_node(pager, prev_page_id)?;
            if let RTreeNode::Leaf {
                next_leaf: ref mut next,
                ..
            } = prev_node
            {
                *next = page_id;
            }
            write_node(pager, prev_page_id, &prev_node)?;
        }
    }

    // Step 4: Build internal levels recursively
    let mut current_level = BulkLevel {
        page_ids: leaf_page_ids,
        mbrs: leaf_mbrs,
    };

    let mut height = 1u32;

    while current_level.page_ids.len() > 1 {
        current_level = build_internal_level(
            pager,
            current_level.page_ids,
            current_level.mbrs,
            max_entries,
            dimensions,
            height,
        )?;
        height += 1;
    }

    let root_page_id = current_level.page_ids[0];

    Ok((root_page_id, height, object_count))
}

/// Build an internal level from the level below.
fn build_internal_level<FS: FileSystem>(
    pager: &Pager<FS>,
    child_page_ids: Vec<PageId>,
    child_mbrs: Vec<Mbr>,
    max_entries: usize,
    dimensions: usize,
    level: u32,
) -> Result<BulkLevel, TableError> {
    // Create internal entries
    let mut entries: Vec<(InternalEntry, f64, f64)> = child_page_ids
        .into_iter()
        .zip(child_mbrs.into_iter())
        .map(|(page_id, mbr)| {
            let center = mbr.center();
            (InternalEntry::new(mbr, page_id), center.x, center.y)
        })
        .collect();

    // Calculate number of slices
    let n = entries.len() as f64;
    let m = max_entries as f64;
    let num_slices = (n / m).sqrt().ceil() as usize;
    let num_slices = num_slices.max(1);

    // Sort by x-coordinate
    entries.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let mut node_groups: Vec<Vec<InternalEntry>> = Vec::new();

    for slice_idx in 0..num_slices {
        let slice_start = (slice_idx * entries.len()) / num_slices;
        let slice_end = ((slice_idx + 1) * entries.len()) / num_slices;
        let slice = &mut entries[slice_start..slice_end];

        // Sort by y-coordinate within this slice
        slice.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());

        // Group into nodes of max_entries each
        for chunk in slice.chunks(max_entries) {
            let node_entries: Vec<InternalEntry> =
                chunk.iter().map(|(entry, _, _)| entry.clone()).collect();
            node_groups.push(node_entries);
        }
    }

    // Create internal nodes and write them
    let mut page_ids = Vec::new();
    let mut mbrs = Vec::new();

    for group in node_groups {
        let page_id = pager.allocate_page(PageType::RTreeNode)?;
        let mut node = RTreeNode::new_internal(level);

        for entry in group {
            node.add_internal_entry(entry).map_err(TableError::Other)?;
        }

        write_node(pager, page_id, &node)?;

        // Update children's parent page IDs
        if let Some(entries) = node.internal_entries() {
            for entry in entries {
                let mut child_node = read_node(pager, entry.child_page_id)?;
                child_node.set_parent_page_id(page_id);
                write_node(pager, entry.child_page_id, &child_node)?;
            }
        }

        let mbr = node.calculate_mbr(dimensions);
        page_ids.push(page_id);
        mbrs.push(mbr);
    }

    Ok(BulkLevel { page_ids, mbrs })
}

/// Read a node from a page.
fn read_node<FS: FileSystem>(pager: &Pager<FS>, page_id: PageId) -> Result<RTreeNode, TableError> {
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
fn write_node<FS: FileSystem>(
    pager: &Pager<FS>,
    page_id: PageId,
    node: &RTreeNode,
) -> Result<(), TableError> {
    let mut page =
        crate::pager::Page::new(page_id, PageType::RTreeNode, pager.page_size().data_size());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::{PageSize, PagerConfig};
    use crate::table::GeoPoint;
    use crate::types::KeyBuf;
    use crate::vfs::MemoryFileSystem;
    use std::sync::Arc;

    fn create_test_pager(fs: &MemoryFileSystem, path: &str) -> Arc<Pager<MemoryFileSystem>> {
        let config = PagerConfig::new()
            .with_page_size(PageSize::Size16KB)
            .with_cache_capacity(0);
        Arc::new(Pager::create(fs, path, config).unwrap())
    }

    #[test]
    fn test_bulk_load_empty() {
        let fs = MemoryFileSystem::new();
        let pager = create_test_pager(&fs, "/test_bulk_load_empty.db");
        let config = SpatialConfig::default();

        let (root_page_id, height, count) = str_bulk_load(&pager, Vec::new(), &config).unwrap();

        assert!(root_page_id.as_u64() > 0);
        assert_eq!(height, 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_bulk_load_small() {
        let fs = MemoryFileSystem::new();
        let pager = create_test_pager(&fs, "/test_bulk_load_small.db");
        let config = SpatialConfig::default().with_max_entries(64);

        let entries: Vec<LeafEntry> = (0..100)
            .map(|i| {
                let x = (i % 10) as f64;
                let y = (i / 10) as f64;
                let mbr = Mbr::from_point_2d(GeoPoint { x, y });
                LeafEntry::new(mbr, KeyBuf(format!("point_{}", i).into_bytes()))
            })
            .collect();

        let (root_page_id, height, count) = str_bulk_load(&pager, entries, &config).unwrap();

        assert!(root_page_id.as_u64() > 0);
        assert_eq!(count, 100);
        assert!(height >= 1);

        // Verify the tree structure by reading the root
        let root_node = read_node(&pager, root_page_id).unwrap();
        if let RTreeNode::Internal { entries, level, .. } = root_node {
            assert!(!entries.is_empty());
            assert_eq!(level, height - 1);
        }
    }

    #[test]
    fn test_bulk_load_large() {
        let fs = MemoryFileSystem::new();
        let pager = create_test_pager(&fs, "/test_bulk_load_large.db");
        let config = SpatialConfig::default().with_max_entries(64);

        let entries: Vec<LeafEntry> = (0..10_000)
            .map(|i| {
                let x = (i % 100) as f64;
                let y = (i / 100) as f64;
                let mbr = Mbr::from_point_2d(GeoPoint { x, y });
                LeafEntry::new(mbr, KeyBuf(format!("point_{}", i).into_bytes()))
            })
            .collect();

        let (root_page_id, height, count) = str_bulk_load(&pager, entries, &config).unwrap();

        assert!(root_page_id.as_u64() > 0);
        assert_eq!(count, 10_000);
        assert!(height >= 2);

        // Verify the tree is balanced by checking all leaf nodes are accessible
        let mut leaf_count = 0;
        let mut object_count = 0;

        fn count_leaves<FS: FileSystem>(
            pager: &Pager<FS>,
            page_id: PageId,
            leaf_count: &mut usize,
            object_count: &mut usize,
        ) -> Result<(), TableError> {
            let node = read_node(pager, page_id)?;
            match node {
                RTreeNode::Leaf { entries, .. } => {
                    *leaf_count += 1;
                    *object_count += entries.len();
                }
                RTreeNode::Internal { entries, .. } => {
                    for entry in entries {
                        count_leaves(pager, entry.child_page_id, leaf_count, object_count)?;
                    }
                }
            }
            Ok(())
        }

        count_leaves(&pager, root_page_id, &mut leaf_count, &mut object_count).unwrap();

        assert_eq!(object_count, 10_000);
        assert!(leaf_count > 1);
    }

    #[test]
    fn test_bulk_load_intersects() {
        let fs = MemoryFileSystem::new();
        let pager = create_test_pager(&fs, "/test_bulk_load_intersects.db");
        let config = SpatialConfig::default().with_max_entries(64);

        let entries: Vec<LeafEntry> = (0..1000)
            .map(|i| {
                let x = (i % 50) as f64;
                let y = (i / 50) as f64;
                let mbr = Mbr::from_point_2d(GeoPoint { x, y });
                LeafEntry::new(mbr, KeyBuf(format!("point_{}", i).into_bytes()))
            })
            .collect();

        let (root_page_id, _, _) = str_bulk_load(&pager, entries, &config).unwrap();

        // Perform intersection query
        let query_mbr =
            Mbr::from_points_2d(GeoPoint { x: 10.0, y: 5.0 }, GeoPoint { x: 20.0, y: 10.0 });

        fn search_intersects<FS: FileSystem>(
            pager: &Pager<FS>,
            page_id: PageId,
            query: &Mbr,
            results: &mut Vec<KeyBuf>,
        ) -> Result<(), TableError> {
            let node = read_node(pager, page_id)?;
            match node {
                RTreeNode::Leaf { entries, .. } => {
                    for entry in entries {
                        if entry.mbr.intersects(query) {
                            results.push(entry.object_id);
                        }
                    }
                }
                RTreeNode::Internal { entries, .. } => {
                    for entry in entries {
                        if entry.mbr.intersects(query) {
                            search_intersects(pager, entry.child_page_id, query, results)?;
                        }
                    }
                }
            }
            Ok(())
        }

        let mut results = Vec::new();
        search_intersects(&pager, root_page_id, &query_mbr, &mut results).unwrap();

        // Should find points in the range [10,20] x [5,10]
        assert!(
            results.len() >= 11 * 5,
            "Expected at least 55 results, got {}",
            results.len()
        );
    }

    #[test]
    fn test_bulk_load_leaf_chain() -> Result<(), TableError> {
        let fs = MemoryFileSystem::new();
        let pager = create_test_pager(&fs, "/test_bulk_load_leaf_chain.db");
        let config = SpatialConfig::default().with_max_entries(64);

        let entries: Vec<LeafEntry> = (0..200)
            .map(|i| {
                let mbr = Mbr::from_point_2d(GeoPoint {
                    x: (i % 20) as f64,
                    y: (i / 20) as f64,
                });
                LeafEntry::new(mbr, KeyBuf(format!("point_{}", i).into_bytes()))
            })
            .collect();

        let (root_page_id, _, _) = str_bulk_load(&pager, entries, &config)?;

        // Find first leaf by traversing leftmost path
        fn find_first_leaf<FS: FileSystem>(
            pager: &Pager<FS>,
            page_id: PageId,
        ) -> Result<PageId, TableError> {
            let node = read_node(pager, page_id)?;
            match node {
                RTreeNode::Leaf { .. } => Ok(page_id),
                RTreeNode::Internal { entries, .. } => {
                    find_first_leaf(pager, entries[0].child_page_id)
                }
            }
        }

        let first_leaf = find_first_leaf(&pager, root_page_id)?;

        // Traverse leaf chain and count leaves
        let mut leaf_count = 0;
        let mut total_objects = 0;
        let mut current_page = first_leaf;

        while current_page.as_u64() != 0 {
            let node = read_node(&pager, current_page)?;
            if let RTreeNode::Leaf {
                entries, next_leaf, ..
            } = node
            {
                leaf_count += 1;
                total_objects += entries.len();
                current_page = next_leaf;
            } else {
                break;
            }
        }

        assert_eq!(total_objects, 200);
        assert!(leaf_count > 1);

        Ok(())
    }
}
