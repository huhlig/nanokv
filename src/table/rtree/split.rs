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

//! R-Tree node splitting strategies.

use super::config::SplitStrategy;
use super::mbr::Mbr;
use super::node::{InternalEntry, LeafEntry};

/// Result of a node split operation.
#[derive(Debug)]
pub struct SplitResult<T> {
    /// Entries for the first (left) node
    pub left: Vec<T>,
    /// Entries for the second (right) node
    pub right: Vec<T>,
}

/// Split internal node entries using the configured strategy.
pub fn split_internal_entries(
    entries: Vec<InternalEntry>,
    strategy: SplitStrategy,
    dimensions: usize,
) -> SplitResult<InternalEntry> {
    match strategy {
        SplitStrategy::Linear => linear_split_internal(entries, dimensions),
        SplitStrategy::Quadratic => quadratic_split_internal(entries, dimensions),
        SplitStrategy::RStar => rstar_split_internal(entries, dimensions),
        SplitStrategy::Hilbert => hilbert_split_internal(entries, dimensions),
    }
}

/// Split leaf node entries using the configured strategy.
pub fn split_leaf_entries(
    entries: Vec<LeafEntry>,
    strategy: SplitStrategy,
    dimensions: usize,
) -> SplitResult<LeafEntry> {
    match strategy {
        SplitStrategy::Linear => linear_split_leaf(entries, dimensions),
        SplitStrategy::Quadratic => quadratic_split_leaf(entries, dimensions),
        SplitStrategy::RStar => rstar_split_leaf(entries, dimensions),
        SplitStrategy::Hilbert => hilbert_split_leaf(entries, dimensions),
    }
}

// =============================================================================
// Linear Split (O(n) complexity)
// =============================================================================

/// Linear split for internal entries.
/// Picks two entries that are farthest apart along one dimension.
fn linear_split_internal(
    mut entries: Vec<InternalEntry>,
    dimensions: usize,
) -> SplitResult<InternalEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    // Find the dimension with the largest spread
    let (seed1, seed2) = pick_linear_seeds_internal(&entries, dimensions);

    let mut left = vec![entries.swap_remove(seed1)];
    let seed2_adjusted = if seed2 > seed1 { seed2 - 1 } else { seed2 };
    let mut right = vec![entries.swap_remove(seed2_adjusted)];

    // Distribute remaining entries
    distribute_entries_internal(entries, &mut left, &mut right);

    SplitResult { left, right }
}

/// Linear split for leaf entries.
fn linear_split_leaf(mut entries: Vec<LeafEntry>, dimensions: usize) -> SplitResult<LeafEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    let (seed1, seed2) = pick_linear_seeds_leaf(&entries, dimensions);

    let mut left = vec![entries.swap_remove(seed1)];
    let seed2_adjusted = if seed2 > seed1 { seed2 - 1 } else { seed2 };
    let mut right = vec![entries.swap_remove(seed2_adjusted)];

    distribute_entries_leaf(entries, &mut left, &mut right);

    SplitResult { left, right }
}

/// Pick two seed entries that are farthest apart (internal).
fn pick_linear_seeds_internal(entries: &[InternalEntry], dimensions: usize) -> (usize, usize) {
    let mut max_separation = f64::NEG_INFINITY;
    let mut seed1 = 0;
    let mut seed2 = 1;

    for dim in 0..dimensions {
        let (min_idx, max_idx) = find_extreme_entries_internal(entries, dim);
        let separation = entries[max_idx].mbr.max[dim] - entries[min_idx].mbr.min[dim];

        if separation > max_separation {
            max_separation = separation;
            seed1 = min_idx;
            seed2 = max_idx;
        }
    }

    (seed1, seed2)
}

/// Pick two seed entries that are farthest apart (leaf).
fn pick_linear_seeds_leaf(entries: &[LeafEntry], dimensions: usize) -> (usize, usize) {
    let mut max_separation = f64::NEG_INFINITY;
    let mut seed1 = 0;
    let mut seed2 = 1;

    for dim in 0..dimensions {
        let (min_idx, max_idx) = find_extreme_entries_leaf(entries, dim);
        let separation = entries[max_idx].mbr.max[dim] - entries[min_idx].mbr.min[dim];

        if separation > max_separation {
            max_separation = separation;
            seed1 = min_idx;
            seed2 = max_idx;
        }
    }

    (seed1, seed2)
}

// =============================================================================
// Quadratic Split (O(n²) complexity)
// =============================================================================

/// Quadratic split for internal entries.
/// Picks two entries that would waste the most area if grouped.
fn quadratic_split_internal(
    mut entries: Vec<InternalEntry>,
    dimensions: usize,
) -> SplitResult<InternalEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    let (seed1, seed2) = pick_quadratic_seeds_internal(&entries);

    let mut left = vec![entries.swap_remove(seed1)];
    let seed2_adjusted = if seed2 > seed1 { seed2 - 1 } else { seed2 };
    let mut right = vec![entries.swap_remove(seed2_adjusted)];

    // Distribute remaining entries, preferring the group that needs less enlargement
    while !entries.is_empty() {
        let idx = pick_next_quadratic_internal(&entries, &left, &right, dimensions);
        let entry = entries.swap_remove(idx);

        let left_mbr = calculate_combined_mbr_internal(&left, dimensions);
        let right_mbr = calculate_combined_mbr_internal(&right, dimensions);

        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);

        if left_increase < right_increase {
            left.push(entry);
        } else {
            right.push(entry);
        }
    }

    SplitResult { left, right }
}

/// Quadratic split for leaf entries.
fn quadratic_split_leaf(mut entries: Vec<LeafEntry>, dimensions: usize) -> SplitResult<LeafEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    let (seed1, seed2) = pick_quadratic_seeds_leaf(&entries);

    let mut left = vec![entries.swap_remove(seed1)];
    let seed2_adjusted = if seed2 > seed1 { seed2 - 1 } else { seed2 };
    let mut right = vec![entries.swap_remove(seed2_adjusted)];

    while !entries.is_empty() {
        let idx = pick_next_quadratic_leaf(&entries, &left, &right, dimensions);
        let entry = entries.swap_remove(idx);

        let left_mbr = calculate_combined_mbr_leaf(&left, dimensions);
        let right_mbr = calculate_combined_mbr_leaf(&right, dimensions);

        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);

        if left_increase < right_increase {
            left.push(entry);
        } else {
            right.push(entry);
        }
    }

    SplitResult { left, right }
}

/// Pick two seed entries with maximum wasted area (internal).
fn pick_quadratic_seeds_internal(entries: &[InternalEntry]) -> (usize, usize) {
    let mut max_waste = f64::NEG_INFINITY;
    let mut seed1 = 0;
    let mut seed2 = 1;

    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let combined = entries[i].mbr.union(&entries[j].mbr);
            let waste = combined.area() - entries[i].mbr.area() - entries[j].mbr.area();

            if waste > max_waste {
                max_waste = waste;
                seed1 = i;
                seed2 = j;
            }
        }
    }

    (seed1, seed2)
}

/// Pick two seed entries with maximum wasted area (leaf).
fn pick_quadratic_seeds_leaf(entries: &[LeafEntry]) -> (usize, usize) {
    let mut max_waste = f64::NEG_INFINITY;
    let mut seed1 = 0;
    let mut seed2 = 1;

    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            let combined = entries[i].mbr.union(&entries[j].mbr);
            let waste = combined.area() - entries[i].mbr.area() - entries[j].mbr.area();

            if waste > max_waste {
                max_waste = waste;
                seed1 = i;
                seed2 = j;
            }
        }
    }

    (seed1, seed2)
}

/// Pick next entry for quadratic split (internal).
fn pick_next_quadratic_internal(
    entries: &[InternalEntry],
    left: &[InternalEntry],
    right: &[InternalEntry],
    dimensions: usize,
) -> usize {
    let left_mbr = calculate_combined_mbr_internal(left, dimensions);
    let right_mbr = calculate_combined_mbr_internal(right, dimensions);

    let mut max_diff = f64::NEG_INFINITY;
    let mut best_idx = 0;

    for (i, entry) in entries.iter().enumerate() {
        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);
        let diff = (left_increase - right_increase).abs();

        if diff > max_diff {
            max_diff = diff;
            best_idx = i;
        }
    }

    best_idx
}

/// Pick next entry for quadratic split (leaf).
fn pick_next_quadratic_leaf(
    entries: &[LeafEntry],
    left: &[LeafEntry],
    right: &[LeafEntry],
    dimensions: usize,
) -> usize {
    let left_mbr = calculate_combined_mbr_leaf(left, dimensions);
    let right_mbr = calculate_combined_mbr_leaf(right, dimensions);

    let mut max_diff = f64::NEG_INFINITY;
    let mut best_idx = 0;

    for (i, entry) in entries.iter().enumerate() {
        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);
        let diff = (left_increase - right_increase).abs();

        if diff > max_diff {
            max_diff = diff;
            best_idx = i;
        }
    }

    best_idx
}

// =============================================================================
// R*-tree Split (best quality)
// =============================================================================

/// R*-tree split for internal entries.
/// Uses sophisticated heuristics to minimize overlap and perimeter.
fn rstar_split_internal(
    entries: Vec<InternalEntry>,
    dimensions: usize,
) -> SplitResult<InternalEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    // Try splits along each axis and choose the best
    let mut best_split = None;
    let mut best_cost = f64::INFINITY;

    for axis in 0..dimensions {
        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| {
            a.mbr.min[axis]
                .partial_cmp(&b.mbr.min[axis])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Try different split points (typically M/2 to M - M/2)
        let min_entries = sorted.len() / 3;
        let max_entries = sorted.len() - min_entries;

        for split_point in min_entries..=max_entries {
            let left: Vec<_> = sorted[..split_point].to_vec();
            let right: Vec<_> = sorted[split_point..].to_vec();

            let left_mbr = calculate_combined_mbr_internal(&left, dimensions);
            let right_mbr = calculate_combined_mbr_internal(&right, dimensions);

            // Cost = overlap + perimeter
            let overlap = left_mbr.overlap_area(&right_mbr);
            let perimeter = left_mbr.perimeter() + right_mbr.perimeter();
            let cost = overlap + perimeter * 0.1; // Weight perimeter less than overlap

            if cost < best_cost {
                best_cost = cost;
                best_split = Some((left, right));
            }
        }
    }

    let (left, right) = best_split.unwrap_or_else(|| {
        // Fallback to simple split
        let mid = entries.len() / 2;
        let mut entries = entries;
        let right = entries.split_off(mid);
        (entries, right)
    });

    SplitResult { left, right }
}

/// R*-tree split for leaf entries.
fn rstar_split_leaf(entries: Vec<LeafEntry>, dimensions: usize) -> SplitResult<LeafEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    let mut best_split = None;
    let mut best_cost = f64::INFINITY;

    for axis in 0..dimensions {
        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| {
            a.mbr.min[axis]
                .partial_cmp(&b.mbr.min[axis])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let min_entries = sorted.len() / 3;
        let max_entries = sorted.len() - min_entries;

        for split_point in min_entries..=max_entries {
            let left: Vec<_> = sorted[..split_point].to_vec();
            let right: Vec<_> = sorted[split_point..].to_vec();

            let left_mbr = calculate_combined_mbr_leaf(&left, dimensions);
            let right_mbr = calculate_combined_mbr_leaf(&right, dimensions);

            let overlap = left_mbr.overlap_area(&right_mbr);
            let perimeter = left_mbr.perimeter() + right_mbr.perimeter();
            let cost = overlap + perimeter * 0.1;

            if cost < best_cost {
                best_cost = cost;
                best_split = Some((left, right));
            }
        }
    }

    let (left, right) = best_split.unwrap_or_else(|| {
        let mid = entries.len() / 2;
        let mut entries = entries;
        let right = entries.split_off(mid);
        (entries, right)
    });

    SplitResult { left, right }
}

// =============================================================================
// Hilbert Split (space-filling curve ordering)
// =============================================================================

/// Hilbert split for internal entries.
/// Sorts entries by their Hilbert curve value and splits at the midpoint.
fn hilbert_split_internal(
    mut entries: Vec<InternalEntry>,
    dimensions: usize,
) -> SplitResult<InternalEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    // Compute Hilbert values for each entry using the MBR center point
    let mut entries_with_hilbert: Vec<(InternalEntry, u64)> = entries
        .drain(..)
        .map(|entry| {
            let center = entry.mbr.center();
            let z = if dimensions >= 3 {
                (entry.mbr.min[2] + entry.mbr.max[2]) / 2.0
            } else {
                0.0
            };
            let hilbert_value = compute_hilbert_value(center.x, center.y, z);
            (entry, hilbert_value)
        })
        .collect();

    // Sort by Hilbert value
    entries_with_hilbert.sort_by_key(|(_, h)| *h);

    // Split at midpoint
    let mid = entries_with_hilbert.len() / 2;
    let right = entries_with_hilbert.split_off(mid);

    let left = entries_with_hilbert
        .into_iter()
        .map(|(entry, _)| entry)
        .collect();
    let right = right.into_iter().map(|(entry, _)| entry).collect();

    SplitResult { left, right }
}

/// Hilbert split for leaf entries.
/// Sorts entries by their Hilbert curve value and splits at the midpoint.
fn hilbert_split_leaf(mut entries: Vec<LeafEntry>, dimensions: usize) -> SplitResult<LeafEntry> {
    if entries.len() < 2 {
        return SplitResult {
            left: entries,
            right: Vec::new(),
        };
    }

    let mut entries_with_hilbert: Vec<(LeafEntry, u64)> = entries
        .drain(..)
        .map(|entry| {
            let center = entry.mbr.center();
            let z = if dimensions >= 3 {
                (entry.mbr.min[2] + entry.mbr.max[2]) / 2.0
            } else {
                0.0
            };
            let hilbert_value = compute_hilbert_value(center.x, center.y, z);
            (entry, hilbert_value)
        })
        .collect();

    entries_with_hilbert.sort_by_key(|(_, h)| *h);

    let mid = entries_with_hilbert.len() / 2;
    let right = entries_with_hilbert.split_off(mid);

    let left = entries_with_hilbert
        .into_iter()
        .map(|(entry, _)| entry)
        .collect();
    let right = right.into_iter().map(|(entry, _)| entry).collect();

    SplitResult { left, right }
}

// =============================================================================
// Hilbert Space-Filling Curve
// =============================================================================

/// Compute the Hilbert curve value for a 2D or 3D point.
///
/// Uses the standard algorithm for mapping multi-dimensional coordinates
/// to a 1D value along the Hilbert space-filling curve. Coordinates are
/// normalized to a 16-bit range for efficient computation.
fn compute_hilbert_value(x: f64, y: f64, z: f64) -> u64 {
    const BITS: u32 = 16;
    const MAX_VAL: f64 = (1 << BITS) as f64 - 1.0;

    // Normalize coordinates to [0, MAX_VAL]
    let nx = ((x / 360.0) + 0.5).clamp(0.0, 1.0) * MAX_VAL;
    let ny = ((y / 180.0) + 0.5).clamp(0.0, 1.0) * MAX_VAL;

    let mut ix = nx as u32;
    let mut iy = ny as u32;

    // 2D Hilbert curve: convert (x, y) to distance d along the curve
    // Using the algorithm from "Programming the Hilbert Curve" by John Skilling
    let mut d = 0u64;
    let mut s = 1u32 << (BITS - 1);

    while s > 0 {
        let rx = if ix & s != 0 { 1 } else { 0 };
        let ry = if iy & s != 0 { 1 } else { 0 };

        d += (s as u64 * s as u64) * ((3 * rx) ^ ry);

        // Rotate/reflect
        if ry == 0 {
            if rx == 1 {
                ix = s - 1 - (ix & (s - 1));
                iy = s - 1 - (iy & (s - 1));
            }
            // Swap x and y
            let temp = ix;
            ix = iy;
            iy = temp;
        }

        s >>= 1;
    }

    // For 3D, interleave z bits into the result
    if z != 0.0 {
        let nz = ((z / 360.0) + 0.5).clamp(0.0, 1.0) * MAX_VAL;
        let iz = nz as u32;

        let mut result = 0u64;
        for i in 0..BITS {
            let x_bit = ((ix >> i) & 1) as u64;
            let y_bit = ((iy >> i) & 1) as u64;
            let z_bit = ((iz >> i) & 1) as u64;
            result |= x_bit << (i * 3);
            result |= y_bit << (i * 3 + 1);
            result |= z_bit << (i * 3 + 2);
        }
        result
    } else {
        d
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Find entries with extreme values in a dimension (internal).
fn find_extreme_entries_internal(entries: &[InternalEntry], dim: usize) -> (usize, usize) {
    let mut min_idx = 0;
    let mut max_idx = 0;
    let mut min_val = entries[0].mbr.min[dim];
    let mut max_val = entries[0].mbr.max[dim];

    for (i, entry) in entries.iter().enumerate().skip(1) {
        if entry.mbr.min[dim] < min_val {
            min_val = entry.mbr.min[dim];
            min_idx = i;
        }
        if entry.mbr.max[dim] > max_val {
            max_val = entry.mbr.max[dim];
            max_idx = i;
        }
    }

    (min_idx, max_idx)
}

/// Find entries with extreme values in a dimension (leaf).
fn find_extreme_entries_leaf(entries: &[LeafEntry], dim: usize) -> (usize, usize) {
    let mut min_idx = 0;
    let mut max_idx = 0;
    let mut min_val = entries[0].mbr.min[dim];
    let mut max_val = entries[0].mbr.max[dim];

    for (i, entry) in entries.iter().enumerate().skip(1) {
        if entry.mbr.min[dim] < min_val {
            min_val = entry.mbr.min[dim];
            min_idx = i;
        }
        if entry.mbr.max[dim] > max_val {
            max_val = entry.mbr.max[dim];
            max_idx = i;
        }
    }

    (min_idx, max_idx)
}

/// Distribute entries between two groups (internal).
fn distribute_entries_internal(
    entries: Vec<InternalEntry>,
    left: &mut Vec<InternalEntry>,
    right: &mut Vec<InternalEntry>,
) {
    for entry in entries {
        let left_mbr = calculate_combined_mbr_internal(left, left[0].mbr.dimensions);
        let right_mbr = calculate_combined_mbr_internal(right, right[0].mbr.dimensions);

        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);

        if left_increase < right_increase {
            left.push(entry);
        } else if right_increase < left_increase {
            right.push(entry);
        } else {
            // Tie: add to smaller group
            if left.len() <= right.len() {
                left.push(entry);
            } else {
                right.push(entry);
            }
        }
    }
}

/// Distribute entries between two groups (leaf).
fn distribute_entries_leaf(
    entries: Vec<LeafEntry>,
    left: &mut Vec<LeafEntry>,
    right: &mut Vec<LeafEntry>,
) {
    for entry in entries {
        let left_mbr = calculate_combined_mbr_leaf(left, left[0].mbr.dimensions);
        let right_mbr = calculate_combined_mbr_leaf(right, right[0].mbr.dimensions);

        let left_increase = left_mbr.area_increase(&entry.mbr);
        let right_increase = right_mbr.area_increase(&entry.mbr);

        if left_increase < right_increase {
            left.push(entry);
        } else if right_increase < left_increase {
            right.push(entry);
        } else {
            if left.len() <= right.len() {
                left.push(entry);
            } else {
                right.push(entry);
            }
        }
    }
}

/// Calculate combined MBR for internal entries.
fn calculate_combined_mbr_internal(entries: &[InternalEntry], dimensions: usize) -> Mbr {
    let mut mbr = Mbr::empty(dimensions);
    for entry in entries {
        mbr.expand(&entry.mbr);
    }
    mbr
}

/// Calculate combined MBR for leaf entries.
fn calculate_combined_mbr_leaf(entries: &[LeafEntry], dimensions: usize) -> Mbr {
    let mut mbr = Mbr::empty(dimensions);
    for entry in entries {
        mbr.expand(&entry.mbr);
    }
    mbr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::GeoPoint;
    use crate::txn::TransactionId;
    use crate::types::KeyBuf;

    fn create_test_leaf_entries() -> Vec<LeafEntry> {
        let tx_id = TransactionId::from(1);
        vec![
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 0.0, y: 0.0 }, GeoPoint { x: 1.0, y: 1.0 }),
                KeyBuf(b"obj1".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 2.0, y: 2.0 }, GeoPoint { x: 3.0, y: 3.0 }),
                KeyBuf(b"obj2".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 4.0, y: 4.0 }, GeoPoint { x: 5.0, y: 5.0 }),
                KeyBuf(b"obj3".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 6.0, y: 6.0 }, GeoPoint { x: 7.0, y: 7.0 }),
                KeyBuf(b"obj4".to_vec()),
                tx_id,
            ),
        ]
    }

    #[test]
    fn test_linear_split() {
        let entries = create_test_leaf_entries();
        let result = split_leaf_entries(entries, SplitStrategy::Linear, 2);

        assert!(!result.left.is_empty());
        assert!(!result.right.is_empty());
        assert_eq!(result.left.len() + result.right.len(), 4);
    }

    #[test]
    fn test_quadratic_split() {
        let entries = create_test_leaf_entries();
        let result = split_leaf_entries(entries, SplitStrategy::Quadratic, 2);

        assert!(!result.left.is_empty());
        assert!(!result.right.is_empty());
        assert_eq!(result.left.len() + result.right.len(), 4);
    }

    #[test]
    fn test_rstar_split() {
        let entries = create_test_leaf_entries();
        let result = split_leaf_entries(entries, SplitStrategy::RStar, 2);

        assert!(!result.left.is_empty());
        assert!(!result.right.is_empty());
        assert_eq!(result.left.len() + result.right.len(), 4);
    }

    #[test]
    fn test_hilbert_split() {
        let entries = create_test_leaf_entries();
        let result = split_leaf_entries(entries, SplitStrategy::Hilbert, 2);

        assert!(!result.left.is_empty());
        assert!(!result.right.is_empty());
        assert_eq!(result.left.len() + result.right.len(), 4);
    }

    #[test]
    fn test_hilbert_split_preserves_spatial_clustering() {
        // Create entries that are clearly clustered in two groups
        let tx_id = TransactionId::from(1);
        let entries = vec![
            // Cluster 1: bottom-left
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 0.0, y: 0.0 }, GeoPoint { x: 0.5, y: 0.5 }),
                KeyBuf(b"a1".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 0.1, y: 0.1 }, GeoPoint { x: 0.6, y: 0.6 }),
                KeyBuf(b"a2".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 0.2, y: 0.2 }, GeoPoint { x: 0.7, y: 0.7 }),
                KeyBuf(b"a3".to_vec()),
                tx_id,
            ),
            // Cluster 2: top-right
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 8.0, y: 8.0 }, GeoPoint { x: 8.5, y: 8.5 }),
                KeyBuf(b"b1".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 8.1, y: 8.1 }, GeoPoint { x: 8.6, y: 8.6 }),
                KeyBuf(b"b2".to_vec()),
                tx_id,
            ),
            LeafEntry::new(
                Mbr::from_points_2d(GeoPoint { x: 8.2, y: 8.2 }, GeoPoint { x: 8.7, y: 8.7 }),
                KeyBuf(b"b3".to_vec()),
                tx_id,
            ),
        ];

        let result = split_leaf_entries(entries, SplitStrategy::Hilbert, 2);

        assert_eq!(result.left.len(), 3);
        assert_eq!(result.right.len(), 3);

        // Verify that entries in each group are spatially close
        let left_mbr = calculate_combined_mbr_leaf(&result.left, 2);
        let right_mbr = calculate_combined_mbr_leaf(&result.right, 2);

        // The groups should not overlap much (Hilbert should cluster well)
        let overlap = left_mbr.overlap_area(&right_mbr);
        let total_area = left_mbr.area() + right_mbr.area();
        // Overlap should be less than 50% of total area for good clustering
        assert!(
            overlap < total_area * 0.5,
            "Hilbert split should produce well-clustered groups"
        );
    }

    #[test]
    fn test_hilbert_value_ordering() {
        // Test that Hilbert values preserve spatial ordering
        let v1 = compute_hilbert_value(0.0, 0.0, 0.0);
        let v2 = compute_hilbert_value(1.0, 1.0, 0.0);
        let v3 = compute_hilbert_value(100.0, 100.0, 0.0);

        // Points further apart should have more different Hilbert values
        let diff_12 = (v1 as i64 - v2 as i64).abs();
        let diff_13 = (v1 as i64 - v3 as i64).abs();

        // v1 and v3 are much further apart than v1 and v2
        assert!(
            diff_13 > diff_12,
            "Hilbert values should reflect spatial distance"
        );
    }
}

// Made with Bob
