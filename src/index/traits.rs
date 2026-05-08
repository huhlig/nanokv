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

//! Index traits for various index types.
//!
//! This module defines traits for different index families:
//! - Dense and sparse ordered indexes
//! - Approximate membership (Bloom filters)
//! - Full-text search
//! - Vector similarity (HNSW, IVF)
//! - Graph adjacency
//! - Time-series
//! - Geospatial

use crate::types::{Bound, KeyBuf, KeyEncoding, ScanBounds};
use crate::table::{TableId, VerificationReport};
use crate::wal::LogSequenceNumber;

/// Logical index identifier assigned by the catalog.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct IndexId(u64);

impl IndexId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexId({})", self.0)
    }
}

impl From<u64> for IndexId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Options for creating an index.
#[derive(Clone, Debug)]
pub struct IndexOptions {
    pub kind: IndexKind,
    pub fields: Vec<IndexField>,
    pub unique: bool,
    pub consistency: IndexConsistency,
    pub format_version: u32,
}

/// High-level index family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    DenseOrdered,
    SparseOrdered,
    Hash,
    Bitmap,
    Bloom,
    FullText,
    VectorHnsw,
    VectorIvf,
    GeoSpatial,
    TimeSeries,
    GraphAdjacency,
    Custom(u32),
}

/// Index field specification.
#[derive(Clone, Debug)]
pub struct IndexField {
    pub name: String,
    pub encoding: KeyEncoding,
    pub descending: bool,
}

/// Index consistency model.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexConsistency {
    /// Index updates are part of the same transaction commit.
    Synchronous,
    /// Index updates may lag but are replayable/recoverable.
    Deferred,
    /// Index may be stale and must expose staleness to query planners.
    StaleQueryable,
    /// Index is rebuilt out of band and not used when stale.
    RebuildRequired,
}

/// Index metadata from the catalog.
#[derive(Clone, Debug)]
pub struct IndexInfo {
    pub id: IndexId,
    pub table_id: TableId,
    pub name: String,
    pub options: IndexOptions,
    pub root: Option<crate::pager::PhysicalLocation>,
    pub created_lsn: LogSequenceNumber,
    pub stale: bool,
}

// =============================================================================
// Index base traits and families
// =============================================================================

/// Common control plane for every index family.
pub trait Index {
    type Error;

    fn index_id(&self) -> IndexId;

    fn table_id(&self) -> TableId;

    fn name(&self) -> &str;

    fn kind(&self) -> IndexKind;

    fn capabilities(&self) -> IndexCapabilities;

    fn stats(&self) -> Result<IndexStats, Self::Error>;

    fn verify(&self) -> Result<VerificationReport, Self::Error>;

    fn rebuild(&mut self, source: &dyn IndexSource) -> Result<(), Self::Error>;
}

/// Declared index capabilities.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexCapabilities {
    pub exact: bool,
    pub approximate: bool,
    pub ordered: bool,
    pub sparse: bool,
    pub supports_delete: bool,
    pub supports_range_query: bool,
    pub supports_prefix_query: bool,
    pub supports_scoring: bool,
    pub supports_incremental_rebuild: bool,
    pub may_be_stale: bool,
}

/// Cursor over ordered index entries.
pub trait IndexCursor {
    type Error;

    fn valid(&self) -> bool;

    fn index_key(&self) -> Option<&[u8]>;

    fn primary_key(&self) -> Option<&[u8]>;

    fn next(&mut self) -> Result<(), Self::Error>;

    fn prev(&mut self) -> Result<(), Self::Error>;

    fn seek(&mut self, index_key: &[u8]) -> Result<(), Self::Error>;
}

/// Dense index: one or more index entries per logical record.
pub trait DenseOrderedIndex: Index {
    type Cursor<'a>: IndexCursor<Error = Self::Error>
    where
        Self: 'a;

    fn insert_entry(&mut self, index_key: &[u8], primary_key: &[u8]) -> Result<(), Self::Error>;

    fn delete_entry(&mut self, index_key: &[u8], primary_key: &[u8]) -> Result<(), Self::Error>;

    fn scan(&self, bounds: ScanBounds) -> Result<Self::Cursor<'_>, Self::Error>;
}

/// Sparse index: maps summarized keys/statistics to candidate physical ranges.
pub trait SparseIndex: Index {
    fn add_marker(
        &mut self,
        marker_key: &[u8],
        target: PhysicalLocation,
    ) -> Result<(), Self::Error>;

    fn remove_marker(
        &mut self,
        marker_key: &[u8],
        target: PhysicalLocation,
    ) -> Result<bool, Self::Error>;

    fn find_candidate_ranges(
        &self,
        query: SparseQuery<'_>,
    ) -> Result<Vec<PhysicalRange>, Self::Error>;
}

/// Approximate membership index such as a Bloom filter.
pub trait ApproximateMembershipIndex: Index {
    fn insert_key(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    /// Returns false only when the key is definitely absent.
    fn might_contain(&self, key: &[u8]) -> Result<bool, Self::Error>;

    /// Returns the estimated false-positive rate when known.
    fn false_positive_rate(&self) -> Option<f64>;
}

/// Full-text index with field-aware tokenization, posting lists, and scoring.
pub trait FullTextIndex: Index {
    fn index_document(
        &mut self,
        doc_id: &[u8],
        fields: &[TextField<'_>],
    ) -> Result<(), Self::Error>;

    fn delete_document(&mut self, doc_id: &[u8]) -> Result<(), Self::Error>;

    fn search(
        &self,
        query: TextQuery<'_>,
        limit: usize,
    ) -> Result<Vec<ScoredDocument>, Self::Error>;
}

/// Shared vector-search interface for HNSW, IVF, flat, and hybrid vector indexes.
pub trait VectorIndex: Index {
    fn dimensions(&self) -> usize;

    fn metric(&self) -> VectorMetric;

    fn insert_vector(&mut self, id: &[u8], vector: &[f32]) -> Result<(), Self::Error>;

    fn delete_vector(&mut self, id: &[u8]) -> Result<(), Self::Error>;

    fn search_vector(
        &self,
        query: &[f32],
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorHit>, Self::Error>;
}

/// HNSW-specific controls.
pub trait HnswIndex: VectorIndex {
    fn set_ef_construction(&mut self, ef: usize);

    fn set_max_connections(&mut self, m: usize);
}

/// IVF-specific controls.
pub trait IvfIndex: VectorIndex {
    fn train(&mut self, samples: &[&[f32]]) -> Result<(), Self::Error>;

    fn centroid_count(&self) -> usize;
}

/// Graph adjacency index optimized for incoming/outgoing edge traversal.
pub trait GraphAdjacencyIndex: Index {
    fn add_edge(
        &mut self,
        source: &[u8],
        label: &[u8],
        target: &[u8],
        edge_id: &[u8],
    ) -> Result<(), Self::Error>;

    fn remove_edge(
        &mut self,
        source: &[u8],
        label: &[u8],
        target: &[u8],
        edge_id: &[u8],
    ) -> Result<(), Self::Error>;

    fn outgoing(&self, source: &[u8], label: Option<&[u8]>) -> Result<Vec<EdgeRef>, Self::Error>;

    fn incoming(&self, target: &[u8], label: Option<&[u8]>) -> Result<Vec<EdgeRef>, Self::Error>;
}

/// Time-series index optimized for append, range, retention, and latest-before queries.
pub trait TimeSeriesIndex: Index {
    fn append_point(
        &mut self,
        series_key: &[u8],
        timestamp: i64,
        value_key: &[u8],
    ) -> Result<(), Self::Error>;

    fn scan_series(
        &self,
        series_key: &[u8],
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<TimePointRef>, Self::Error>;

    fn latest_before(
        &self,
        series_key: &[u8],
        timestamp: i64,
    ) -> Result<Option<TimePointRef>, Self::Error>;
}

/// Geospatial index abstraction for point and region queries.
pub trait GeoSpatialIndex: Index {
    fn insert_geometry(&mut self, id: &[u8], geometry: GeometryRef<'_>) -> Result<(), Self::Error>;

    fn delete_geometry(&mut self, id: &[u8]) -> Result<(), Self::Error>;

    fn intersects(&self, query: GeometryRef<'_>, limit: usize) -> Result<Vec<GeoHit>, Self::Error>;

    fn nearest(&self, point: GeoPoint, limit: usize) -> Result<Vec<GeoHit>, Self::Error>;
}

/// Incremental rebuild lifecycle for indexes that may become stale.
pub trait RebuildableIndex: Index {
    fn mark_stale(&mut self) -> Result<(), Self::Error>;

    fn is_stale(&self) -> bool;

    fn rebuild_incremental(
        &mut self,
        source: &dyn IndexSource,
        budget: RebuildBudget,
    ) -> Result<RebuildProgress, Self::Error>;
}

// =============================================================================
// Query planning and cost estimation
// =============================================================================

/// Common interface for indexes that can participate in query planning.
pub trait QueryableIndex: Index {
    fn estimate(&self, predicate: Predicate<'_>) -> Result<CostEstimate, Self::Error>;

    fn query_candidates(
        &self,
        predicate: Predicate<'_>,
        budget: QueryBudget,
    ) -> Result<CandidateSet, Self::Error>;
}

/// Predicate understood by the generic query-planning layer.
#[derive(Clone, Debug)]
pub enum Predicate<'a> {
    Eq {
        field: std::borrow::Cow<'a, str>,
        value: std::borrow::Cow<'a, [u8]>,
    },
    Range {
        field: std::borrow::Cow<'a, str>,
        start: Bound<std::borrow::Cow<'a, [u8]>>,
        end: Bound<std::borrow::Cow<'a, [u8]>>,
    },
    Prefix {
        field: std::borrow::Cow<'a, str>,
        prefix: std::borrow::Cow<'a, [u8]>,
    },
    Text {
        field: Option<std::borrow::Cow<'a, str>>,
        query: std::borrow::Cow<'a, str>,
    },
    VectorKnn {
        field: std::borrow::Cow<'a, str>,
        vector: std::borrow::Cow<'a, [f32]>,
        k: usize,
    },
    GeoIntersects {
        field: std::borrow::Cow<'a, str>,
        geometry: GeometryRef<'a>,
    },
    And(Vec<Predicate<'a>>),
    Or(Vec<Predicate<'a>>),
    Not(Box<Predicate<'a>>),
}

/// Cost/selectivity estimate for query planning.
#[derive(Clone, Debug, Default)]
pub struct CostEstimate {
    pub estimated_rows: Option<u64>,
    pub selectivity: Option<f64>,
    pub io_cost: Option<f64>,
    pub cpu_cost: Option<f64>,
    pub memory_cost_bytes: Option<u64>,
    pub exact: bool,
    pub ordered: bool,
}

/// Candidate primary-key set produced by an index.
#[derive(Clone, Debug)]
pub enum CandidateSet {
    Exact(Vec<KeyBuf>),
    Approximate(Vec<KeyBuf>),
    PhysicalRanges(Vec<PhysicalRange>),
    Empty,
    Unknown,
}

/// Query budget for approximate or incremental index queries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryBudget {
    pub max_results: Option<usize>,
    pub max_pages: Option<u64>,
    pub max_millis: Option<u64>,
}

// =============================================================================
// Supporting types
// =============================================================================

/// Source abstraction used to rebuild indexes.
pub trait IndexSource {
    fn scan_rows(
        &self,
        bounds: ScanBounds,
        visitor: &mut dyn IndexSourceVisitor,
    ) -> Result<(), IndexSourceError>;
}

pub trait IndexSourceVisitor {
    fn visit(&mut self, primary_key: &[u8], value: &[u8]) -> Result<(), IndexSourceError>;
}

#[derive(Debug)]
pub struct IndexSourceError {
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct SparseQuery<'a> {
    pub key_range: Option<(&'a [u8], &'a [u8])>,
    pub min_max_filter: Option<(&'a [u8], &'a [u8])>,
}

#[derive(Clone, Debug)]
pub struct TextField<'a> {
    pub name: &'a str,
    pub text: &'a str,
    pub boost: f32,
}

#[derive(Clone, Debug)]
pub struct TextQuery<'a> {
    pub query: &'a str,
    pub default_field: Option<&'a str>,
    pub require_positions: bool,
}

#[derive(Clone, Debug)]
pub struct ScoredDocument {
    pub doc_id: KeyBuf,
    pub score: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    Dot,
    Euclidean,
    Manhattan,
}

#[derive(Clone, Debug)]
pub struct VectorSearchOptions {
    pub limit: usize,
    pub ef_search: Option<usize>,
    pub probes: Option<usize>,
    pub filter: Option<Predicate<'static>>,
}

#[derive(Clone, Debug)]
pub struct VectorHit {
    pub id: KeyBuf,
    pub distance: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeRef {
    pub edge_id: KeyBuf,
    pub source: KeyBuf,
    pub label: KeyBuf,
    pub target: KeyBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimePointRef {
    pub series_key: KeyBuf,
    pub timestamp: i64,
    pub value_key: KeyBuf,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    pub x: f64,
    pub y: f64,
}

#[derive(Clone, Debug)]
pub enum GeometryRef<'a> {
    Point(GeoPoint),
    BoundingBox { min: GeoPoint, max: GeoPoint },
    Wkb(&'a [u8]),
}

#[derive(Clone, Debug)]
pub struct GeoHit {
    pub id: KeyBuf,
    pub distance: Option<f64>,
}

#[derive(Clone, Debug, Default)]
pub struct IndexStats {
    pub entry_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub distinct_keys: Option<u64>,
    pub stale_entries: Option<u64>,
    pub last_updated_lsn: Option<LogSequenceNumber>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RebuildBudget {
    pub max_rows: Option<u64>,
    pub max_pages: Option<u64>,
    pub max_millis: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct RebuildProgress {
    pub complete: bool,
    pub rows_scanned: u64,
    pub rows_indexed: u64,
    pub resume_key: Option<KeyBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalLocation {
    pub page_id: crate::pager::PageId,
    pub offset: u32,
    pub length: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalRange {
    pub start: PhysicalLocation,
    pub end: PhysicalLocation,
}
