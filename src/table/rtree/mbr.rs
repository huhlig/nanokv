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

//! Minimum Bounding Rectangle (MBR) implementation.

use crate::table::GeoPoint;

/// Minimum Bounding Rectangle (MBR) for spatial indexing.
///
/// An MBR is an axis-aligned rectangle that completely contains a spatial object.
/// It's defined by minimum and maximum coordinates in each dimension.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Mbr {
    /// Minimum coordinates (lower-left corner in 2D)
    pub min: [f64; 3],
    /// Maximum coordinates (upper-right corner in 2D)
    pub max: [f64; 3],
    /// Number of dimensions (2 or 3)
    pub dimensions: usize,
}

impl Mbr {
    /// Create a new 2D MBR from a point.
    pub fn from_point_2d(point: GeoPoint) -> Self {
        Self {
            min: [point.x, point.y, 0.0],
            max: [point.x, point.y, 0.0],
            dimensions: 2,
        }
    }

    /// Create a new 2D MBR from two points.
    pub fn from_points_2d(p1: GeoPoint, p2: GeoPoint) -> Self {
        Self {
            min: [p1.x.min(p2.x), p1.y.min(p2.y), 0.0],
            max: [p1.x.max(p2.x), p1.y.max(p2.y), 0.0],
            dimensions: 2,
        }
    }

    /// Create a new 3D MBR from coordinates.
    pub fn from_coords_3d(min_x: f64, min_y: f64, min_z: f64, max_x: f64, max_y: f64, max_z: f64) -> Self {
        Self {
            min: [min_x, min_y, min_z],
            max: [max_x, max_y, max_z],
            dimensions: 3,
        }
    }

    /// Create an empty MBR (invalid bounds).
    pub fn empty(dimensions: usize) -> Self {
        Self {
            min: [f64::INFINITY; 3],
            max: [f64::NEG_INFINITY; 3],
            dimensions,
        }
    }

    /// Check if the MBR is valid (min <= max in all dimensions).
    pub fn is_valid(&self) -> bool {
        for i in 0..self.dimensions {
            if self.min[i] > self.max[i] {
                return false;
            }
        }
        true
    }

    /// Check if the MBR is empty (no area/volume).
    pub fn is_empty(&self) -> bool {
        for i in 0..self.dimensions {
            if self.min[i] > self.max[i] {
                return true;
            }
        }
        false
    }

    /// Calculate the area (2D) or volume (3D) of the MBR.
    pub fn area(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }

        let mut result = 1.0;
        for i in 0..self.dimensions {
            result *= self.max[i] - self.min[i];
        }
        result
    }

    /// Calculate the perimeter (2D) or surface area (3D) of the MBR.
    pub fn perimeter(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }

        if self.dimensions == 2 {
            2.0 * ((self.max[0] - self.min[0]) + (self.max[1] - self.min[1]))
        } else {
            // 3D surface area
            let dx = self.max[0] - self.min[0];
            let dy = self.max[1] - self.min[1];
            let dz = self.max[2] - self.min[2];
            2.0 * (dx * dy + dy * dz + dz * dx)
        }
    }

    /// Calculate the margin (sum of edge lengths).
    /// Used in R*-tree split algorithm.
    pub fn margin(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }

        let mut sum = 0.0;
        for i in 0..self.dimensions {
            sum += self.max[i] - self.min[i];
        }
        sum
    }

    /// Expand this MBR to include another MBR.
    pub fn expand(&mut self, other: &Mbr) {
        for i in 0..self.dimensions.min(other.dimensions) {
            self.min[i] = self.min[i].min(other.min[i]);
            self.max[i] = self.max[i].max(other.max[i]);
        }
    }

    /// Create a new MBR that is the union of this and another MBR.
    pub fn union(&self, other: &Mbr) -> Mbr {
        let mut result = *self;
        result.expand(other);
        result
    }

    /// Calculate the intersection of this MBR with another.
    pub fn intersection(&self, other: &Mbr) -> Mbr {
        let mut result = Mbr::empty(self.dimensions);
        
        for i in 0..self.dimensions.min(other.dimensions) {
            result.min[i] = self.min[i].max(other.min[i]);
            result.max[i] = self.max[i].min(other.max[i]);
        }
        
        result.dimensions = self.dimensions;
        result
    }

    /// Check if this MBR intersects with another.
    pub fn intersects(&self, other: &Mbr) -> bool {
        for i in 0..self.dimensions.min(other.dimensions) {
            if self.max[i] < other.min[i] || self.min[i] > other.max[i] {
                return false;
            }
        }
        true
    }

    /// Check if this MBR contains another MBR.
    pub fn contains(&self, other: &Mbr) -> bool {
        for i in 0..self.dimensions.min(other.dimensions) {
            if other.min[i] < self.min[i] || other.max[i] > self.max[i] {
                return false;
            }
        }
        true
    }

    /// Check if this MBR contains a point.
    pub fn contains_point(&self, point: GeoPoint) -> bool {
        point.x >= self.min[0] && point.x <= self.max[0] &&
        point.y >= self.min[1] && point.y <= self.max[1]
    }

    /// Calculate the area increase needed to include another MBR.
    pub fn area_increase(&self, other: &Mbr) -> f64 {
        let union = self.union(other);
        union.area() - self.area()
    }

    /// Calculate the overlap area with another MBR.
    pub fn overlap_area(&self, other: &Mbr) -> f64 {
        let intersection = self.intersection(other);
        if intersection.is_valid() {
            intersection.area()
        } else {
            0.0
        }
    }

    /// Calculate the center point of the MBR.
    pub fn center(&self) -> GeoPoint {
        GeoPoint {
            x: (self.min[0] + self.max[0]) / 2.0,
            y: (self.min[1] + self.max[1]) / 2.0,
        }
    }

    /// Calculate the distance from the center of this MBR to a point.
    pub fn center_distance(&self, point: GeoPoint) -> f64 {
        let center = self.center();
        let dx = center.x - point.x;
        let dy = center.y - point.y;
        (dx * dx + dy * dy).sqrt()
    }

    /// Calculate the minimum distance from this MBR to a point.
    /// Returns 0 if the point is inside the MBR.
    pub fn min_distance(&self, point: GeoPoint) -> f64 {
        let mut sum = 0.0;

        // X dimension
        if point.x < self.min[0] {
            let d = self.min[0] - point.x;
            sum += d * d;
        } else if point.x > self.max[0] {
            let d = point.x - self.max[0];
            sum += d * d;
        }

        // Y dimension
        if point.y < self.min[1] {
            let d = self.min[1] - point.y;
            sum += d * d;
        } else if point.y > self.max[1] {
            let d = point.y - self.max[1];
            sum += d * d;
        }

        sum.sqrt()
    }

    /// Calculate the maximum distance from this MBR to a point.
    pub fn max_distance(&self, point: GeoPoint) -> f64 {
        // Find the farthest corner
        let corners = [
            (self.min[0], self.min[1]),
            (self.min[0], self.max[1]),
            (self.max[0], self.min[1]),
            (self.max[0], self.max[1]),
        ];

        corners
            .iter()
            .map(|(x, y)| {
                let dx = x - point.x;
                let dy = y - point.y;
                dx * dx + dy * dy
            })
            .fold(0.0, f64::max)
            .sqrt()
    }

    /// Serialize the MBR to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(1 + 6 * 8);
        bytes.push(self.dimensions as u8);
        
        for i in 0..self.dimensions {
            bytes.extend_from_slice(&self.min[i].to_le_bytes());
            bytes.extend_from_slice(&self.max[i].to_le_bytes());
        }
        
        bytes
    }

    /// Deserialize an MBR from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("Empty byte array".to_string());
        }

        let dimensions = bytes[0] as usize;
        if !(2..=3).contains(&dimensions) {
            return Err(format!("Invalid dimensions: {}", dimensions));
        }

        let expected_len = 1 + dimensions * 2 * 8;
        if bytes.len() < expected_len {
            return Err(format!(
                "Insufficient bytes: expected {}, got {}",
                expected_len,
                bytes.len()
            ));
        }

        let mut min = [0.0; 3];
        let mut max = [0.0; 3];
        let mut offset = 1;

        for i in 0..dimensions {
            let min_bytes: [u8; 8] = bytes[offset..offset + 8].try_into().unwrap();
            min[i] = f64::from_le_bytes(min_bytes);
            offset += 8;

            let max_bytes: [u8; 8] = bytes[offset..offset + 8].try_into().unwrap();
            max[i] = f64::from_le_bytes(max_bytes);
            offset += 8;
        }

        Ok(Mbr {
            min,
            max,
            dimensions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_point() {
        let point = GeoPoint { x: 1.0, y: 2.0 };
        let mbr = Mbr::from_point_2d(point);
        
        assert_eq!(mbr.min[0], 1.0);
        assert_eq!(mbr.min[1], 2.0);
        assert_eq!(mbr.max[0], 1.0);
        assert_eq!(mbr.max[1], 2.0);
        assert_eq!(mbr.dimensions, 2);
        assert!(mbr.is_valid());
    }

    #[test]
    fn test_from_points() {
        let p1 = GeoPoint { x: 1.0, y: 2.0 };
        let p2 = GeoPoint { x: 3.0, y: 4.0 };
        let mbr = Mbr::from_points_2d(p1, p2);
        
        assert_eq!(mbr.min[0], 1.0);
        assert_eq!(mbr.min[1], 2.0);
        assert_eq!(mbr.max[0], 3.0);
        assert_eq!(mbr.max[1], 4.0);
    }

    #[test]
    fn test_area() {
        let mbr = Mbr::from_coords_3d(0.0, 0.0, 0.0, 2.0, 3.0, 4.0);
        assert_eq!(mbr.area(), 24.0); // 2 * 3 * 4
    }

    #[test]
    fn test_perimeter_2d() {
        let p1 = GeoPoint { x: 0.0, y: 0.0 };
        let p2 = GeoPoint { x: 2.0, y: 3.0 };
        let mbr = Mbr::from_points_2d(p1, p2);
        assert_eq!(mbr.perimeter(), 10.0); // 2 * (2 + 3)
    }

    #[test]
    fn test_intersects() {
        let mbr1 = Mbr::from_points_2d(
            GeoPoint { x: 0.0, y: 0.0 },
            GeoPoint { x: 2.0, y: 2.0 },
        );
        let mbr2 = Mbr::from_points_2d(
            GeoPoint { x: 1.0, y: 1.0 },
            GeoPoint { x: 3.0, y: 3.0 },
        );
        let mbr3 = Mbr::from_points_2d(
            GeoPoint { x: 3.0, y: 3.0 },
            GeoPoint { x: 4.0, y: 4.0 },
        );

        assert!(mbr1.intersects(&mbr2));
        assert!(!mbr1.intersects(&mbr3));
    }

    #[test]
    fn test_contains() {
        let outer = Mbr::from_points_2d(
            GeoPoint { x: 0.0, y: 0.0 },
            GeoPoint { x: 4.0, y: 4.0 },
        );
        let inner = Mbr::from_points_2d(
            GeoPoint { x: 1.0, y: 1.0 },
            GeoPoint { x: 3.0, y: 3.0 },
        );

        assert!(outer.contains(&inner));
        assert!(!inner.contains(&outer));
    }

    #[test]
    fn test_contains_point() {
        let mbr = Mbr::from_points_2d(
            GeoPoint { x: 0.0, y: 0.0 },
            GeoPoint { x: 2.0, y: 2.0 },
        );

        assert!(mbr.contains_point(GeoPoint { x: 1.0, y: 1.0 }));
        assert!(mbr.contains_point(GeoPoint { x: 0.0, y: 0.0 }));
        assert!(mbr.contains_point(GeoPoint { x: 2.0, y: 2.0 }));
        assert!(!mbr.contains_point(GeoPoint { x: 3.0, y: 3.0 }));
    }

    #[test]
    fn test_min_distance() {
        let mbr = Mbr::from_points_2d(
            GeoPoint { x: 0.0, y: 0.0 },
            GeoPoint { x: 2.0, y: 2.0 },
        );

        // Point inside
        assert_eq!(mbr.min_distance(GeoPoint { x: 1.0, y: 1.0 }), 0.0);

        // Point outside
        let dist = mbr.min_distance(GeoPoint { x: 3.0, y: 0.0 });
        assert!((dist - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_serialization() {
        let mbr = Mbr::from_points_2d(
            GeoPoint { x: 1.0, y: 2.0 },
            GeoPoint { x: 3.0, y: 4.0 },
        );

        let bytes = mbr.to_bytes();
        let deserialized = Mbr::from_bytes(&bytes).unwrap();

        assert_eq!(mbr, deserialized);
    }
}

// Made with Bob
