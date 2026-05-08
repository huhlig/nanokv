//! Embedded single-file key-value storage traits.
//!
//! This module defines a storage-kernel interface for an embedded, single-file
//! database backend intended to support higher-level relational, document,
//! graph, and time-series database interfaces.
//!
//! # Design goals
//!
//! The API deliberately avoids treating every structure as a `BTreeMap`.
//! A B+Tree table, LSM table, ART memtable, dense secondary index, sparse
//! zone-map index, Bloom filter, full-text index, HNSW graph, and IVF vector
//! index have different consistency, maintenance, query, and mutation models.
//!
//! Instead, this file uses capability-oriented traits:
//!
//! - [`KvDatabase`] owns the file, catalog, transactions, WAL, and page store.
//! - [`KvTransaction`] provides snapshot-aware reads and atomic writes.
//! - [`TableEngine`] describes physical table implementations.
//! - [`PointLookup`], [`OrderedScan`], [`MutableTable`], [`BatchOps`], and
//!   [`Flushable`] describe table capabilities.
//! - [`Index`] is the common metadata/control plane for indexes.
//! - Specialized traits model ordered, sparse, approximate, full-text, vector,
//!   graph, geospatial, and time-series indexes.
//! - [`Maintainable`], [`StatisticsProvider`], and [`ConsistencyVerifier`]
//!   expose compaction, statistics, verification, and repair.
//! - [`PageStore`], [`ExtentAllocator`], and [`Wal`] define lower-level single-file
//!   storage services.
//!
//! The file is intentionally interface-focused. Most types are lightweight
//! records or placeholders meant to be refined by concrete implementations.

#![allow(unused)]

use core::ops::RangeInclusive;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

// =============================================================================
// Core identifiers and basic data containers
// =============================================================================

/// Logical table identifier assigned by the catalog.
pub type TableId = u64;

/// Logical index identifier assigned by the catalog.
pub type IndexId = u64;

/// Transaction identifier.
pub type TxId = u64;

/// Snapshot identifier.
pub type SnapshotId = u64;

/// Monotonic log sequence number.
///
/// Implementations may encode term, segment, offset, shard, or epoch information
/// in a richer internal representation. The public trait only requires stable
/// ordering.
pub type Lsn = u64;

/// Page identifier inside a single-file database.
pub type PageId = u64;

/// Logical version used by MVCC-capable engines.
pub type Version = u64;

/// Owned key buffer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBuf(pub Vec<u8>);

impl AsRef<[u8]> for KeyBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Owned value buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueBuf(pub Vec<u8>);

impl AsRef<[u8]> for ValueBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A key-value entry returned by owned iterators or batch operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: KeyBuf,
    pub value: ValueBuf,
}

/// Defines whether a bound is inclusive, exclusive, or unbounded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bound<T> {
    Included(T),
    Excluded(T),
    Unbounded,
}

/// Common scan bounds for ordered tables and ordered indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScanBounds {
    /// Scan the full ordered keyspace.
    All,
    /// Scan keys beginning with the supplied prefix.
    Prefix(KeyBuf),
    /// Scan a bounded range.
    Range {
        start: Bound<KeyBuf>,
        end: Bound<KeyBuf>,
    },
}

/// Durability policy for a write transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Useful for ephemeral in-memory or test engines.
    MemoryOnly,
    /// Write to WAL but do not force the file to stable storage immediately.
    WalOnly,
    /// Flush dirty buffers before reporting commit.
    FlushOnCommit,
    /// Force durable sync before reporting commit.
    SyncOnCommit,
}

/// Transaction isolation level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    SnapshotIsolation,
}

/// Table/index mutation type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationKind {
    Insert,
    Update,
    Upsert,
    Delete,
    RangeDelete,
}

/// Result of a successful commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitInfo {
    pub tx_id: TxId,
    pub commit_lsn: Lsn,
    pub durable_lsn: Option<Lsn>,
}

// =============================================================================
// Database and transaction layer
// =============================================================================

/// Top-level embedded database interface.
///
/// This trait owns the catalog, file allocation, transaction manager, WAL,
/// and registered table/index engines. ACID semantics should be coordinated at
/// this layer rather than by independently stacking transactional wrappers around
/// individual tables.
pub trait KvDatabase {
    type Error;

    type Tx<'db>: KvTransaction<Error = Self::Error>
    where
        Self: 'db;

    /// Begin a read-only transaction using the latest stable snapshot.
    fn begin_read(&self) -> Result<Self::Tx<'_>, Self::Error>;

    /// Begin a write transaction with the requested durability policy.
    fn begin_write(&self, durability: Durability) -> Result<Self::Tx<'_>, Self::Error>;

    /// Create a logical table using a chosen physical engine.
    fn create_table(&self, name: &str, options: TableOptions) -> Result<TableId, Self::Error>;

    /// Drop a logical table and its dependent indexes.
    fn drop_table(&self, table: TableId) -> Result<(), Self::Error>;

    /// Open an existing table by name.
    fn open_table(&self, name: &str) -> Result<Option<TableId>, Self::Error>;

    /// Return catalog-visible tables.
    fn list_tables(&self) -> Result<Vec<TableInfo>, Self::Error>;

    /// Create an index over a table.
    fn create_index(&self, table: TableId, name: &str, options: IndexOptions) -> Result<IndexId, Self::Error>;

    /// Drop an index.
    fn drop_index(&self, index: IndexId) -> Result<(), Self::Error>;

    /// Return catalog-visible indexes for a table.
    fn list_indexes(&self, table: TableId) -> Result<Vec<IndexInfo>, Self::Error>;
}

/// Transaction interface.
///
/// A transaction coordinates table writes, index writes, WAL records, page
/// allocation, and visibility rules as one commit unit.
pub trait KvTransaction {
    type Error;

    type Cursor<'tx>: KvCursor<Error = Self::Error>
    where
        Self: 'tx;

    fn id(&self) -> TxId;

    fn isolation_level(&self) -> IsolationLevel;

    fn snapshot_lsn(&self) -> Lsn;

    fn get(&self, table: TableId, key: &[u8]) -> Result<Option<ValueBuf>, Self::Error>;

    fn put(&mut self, table: TableId, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&mut self, table: TableId, key: &[u8]) -> Result<bool, Self::Error>;

    fn range_delete(&mut self, table: TableId, bounds: ScanBounds) -> Result<u64, Self::Error>;

    fn cursor(&self, table: TableId, bounds: ScanBounds) -> Result<Self::Cursor<'_>, Self::Error>;

    fn apply_batch(&mut self, table: TableId, batch: WriteBatch<'_>) -> Result<BatchReport, Self::Error>;

    fn commit(self) -> Result<CommitInfo, Self::Error>;

    fn rollback(self) -> Result<(), Self::Error>;
}

/// Ordered cursor over table or index entries.
///
/// Cursors should document invalidation rules. Snapshot cursors are preferred:
/// once opened, they read from a stable view and are not invalidated by concurrent
/// writers.
pub trait KvCursor {
    type Error;

    fn valid(&self) -> bool;

    fn key(&self) -> Option<&[u8]>;

    fn value(&self) -> Option<&[u8]>;

    fn first(&mut self) -> Result<(), Self::Error>;

    fn last(&mut self) -> Result<(), Self::Error>;

    fn seek(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    /// Seek to the greatest key less than or equal to the supplied key.
    ///
    /// This is essential for time-series `latest_before`, graph adjacency windows,
    /// version-chain lookups, and descending query plans.
    fn seek_prev(&mut self, key: &[u8]) -> Result<(), Self::Error>;

    fn next(&mut self) -> Result<(), Self::Error>;

    fn prev(&mut self) -> Result<(), Self::Error>;
}

// =============================================================================
// Table capability traits
// =============================================================================

/// Point lookup capability.
pub trait PointLookup {
    type Error;

    fn get(&self, key: &[u8]) -> Result<Option<ValueBuf>, Self::Error>;

    fn contains(&self, key: &[u8]) -> Result<bool, Self::Error> {
        Ok(self.get(key)?.is_some())
    }
}

/// Ordered scan capability.
pub trait OrderedScan {
    type Error;

    type Cursor<'a>: KvCursor<Error = Self::Error>
    where
        Self: 'a;

    fn scan(&self, bounds: ScanBounds) -> Result<Self::Cursor<'_>, Self::Error>;
}

/// Prefix scan capability.
pub trait PrefixScan: OrderedScan {
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Self::Cursor<'_>, Self::Error> {
        self.scan(ScanBounds::Prefix(KeyBuf(prefix.to_vec())))
    }
}

/// Mutation capability.
pub trait MutableTable {
    type Error;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    fn delete(&mut self, key: &[u8]) -> Result<bool, Self::Error>;

    fn range_delete(&mut self, bounds: ScanBounds) -> Result<u64, Self::Error>;
}

/// Batch operation capability.
pub trait BatchOps {
    type Error;

    fn batch_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<ValueBuf>>, Self::Error>;

    fn apply_batch(&mut self, batch: WriteBatch<'_>) -> Result<BatchReport, Self::Error>;
}

/// Flush capability.
pub trait Flushable {
    type Error;

    fn flush(&mut self) -> Result<(), Self::Error>;
}

/// Marker trait for a full ordered key-value table.
///
/// This is intentionally composed from smaller capabilities instead of being the
/// only table abstraction. Sparse indexes, Bloom filters, LSM segments, and
/// vector indexes should not be forced to implement this trait.
pub trait OrderedKvTable:
    PointLookup + OrderedScan + MutableTable + BatchOps + Flushable
{
}

impl<T> OrderedKvTable for T where T: PointLookup + OrderedScan + MutableTable + BatchOps + Flushable {}

/// Physical table engine.
///
/// Implemented by B+Tree, LSM, ART, in-memory, append-only, or hybrid engines.
pub trait TableEngine {
    type Error;

    type Reader<'a>: TableReader<Error = Self::Error>
    where
        Self: 'a;

    type Writer<'a>: TableWriter<Error = Self::Error>
    where
        Self: 'a;

    fn table_id(&self) -> TableId;

    fn name(&self) -> &str;

    fn kind(&self) -> TableEngineKind;

    fn capabilities(&self) -> TableCapabilities;

    fn reader(&self, snapshot_lsn: Lsn) -> Result<Self::Reader<'_>, Self::Error>;

    fn writer(&self, tx_id: TxId) -> Result<Self::Writer<'_>, Self::Error>;

    fn stats(&self) -> Result<TableStatistics, Self::Error>;
}

/// Read view over a table engine.
pub trait TableReader: PointLookup + OrderedScan {
    fn snapshot_lsn(&self) -> Lsn;

    fn approximate_len(&self) -> Result<Option<u64>, Self::Error>;
}

/// Write view over a table engine.
pub trait TableWriter: MutableTable + BatchOps + Flushable {
    fn tx_id(&self) -> TxId;
}

/// Physical table implementation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableEngineKind {
    BTree,
    BPlusTree,
    LsmTree,
    Art,
    Hash,
    Memory,
    AppendLog,
    ColumnarSegment,
    Custom(u32),
}

/// Declared table capabilities.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableCapabilities {
    pub ordered: bool,
    pub point_lookup: bool,
    pub prefix_scan: bool,
    pub reverse_scan: bool,
    pub range_delete: bool,
    pub merge_operator: bool,
    pub mvcc_native: bool,
    pub append_optimized: bool,
    pub memory_resident: bool,
    pub disk_resident: bool,
    pub supports_compression: bool,
    pub supports_encryption: bool,
}

// =============================================================================
// Index base traits and families
// =============================================================================

/// Common control plane for every index family.
///
/// This trait is intentionally not the query interface for all indexes. Ordered
/// indexes, sparse indexes, Bloom filters, full-text indexes, and vector indexes
/// expose different query methods through specialized traits.
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
///
/// Sparse indexes should usually return candidate ranges, not final answers.
pub trait SparseIndex: Index {
    fn add_marker(&mut self, marker_key: &[u8], target: PhysicalLocation) -> Result<(), Self::Error>;

    fn remove_marker(&mut self, marker_key: &[u8], target: PhysicalLocation) -> Result<bool, Self::Error>;

    fn find_candidate_ranges(&self, query: SparseQuery<'_>) -> Result<Vec<PhysicalRange>, Self::Error>;
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
    fn index_document(&mut self, doc_id: &[u8], fields: &[TextField<'_>]) -> Result<(), Self::Error>;

    fn delete_document(&mut self, doc_id: &[u8]) -> Result<(), Self::Error>;

    fn search(&self, query: TextQuery<'_>, limit: usize) -> Result<Vec<ScoredDocument>, Self::Error>;
}

/// Shared vector-search interface for HNSW, IVF, flat, and hybrid vector indexes.
pub trait VectorIndex: Index {
    fn dimensions(&self) -> usize;

    fn metric(&self) -> VectorMetric;

    fn insert_vector(&mut self, id: &[u8], vector: &[f32]) -> Result<(), Self::Error>;

    fn delete_vector(&mut self, id: &[u8]) -> Result<(), Self::Error>;

    fn search_vector(&self, query: &[f32], options: VectorSearchOptions) -> Result<Vec<VectorHit>, Self::Error>;
}

/// HNSW-specific controls.
pub trait HnswIndex: VectorIndex {
    fn set_ef_construction(&mut self, ef: usize);

    fn set_max_connections(&mut self, m: usize);
}

/// IVF-specific controls.
///
/// Unlike HNSW, IVF commonly needs a training phase over representative samples.
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
    fn append_point(&mut self, series_key: &[u8], timestamp: i64, value_key: &[u8]) -> Result<(), Self::Error>;

    fn scan_series(
        &self,
        series_key: &[u8],
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<TimePointRef>, Self::Error>;

    fn latest_before(&self, series_key: &[u8], timestamp: i64) -> Result<Option<TimePointRef>, Self::Error>;
}

/// Geospatial index abstraction for point and region queries.
pub trait GeoSpatialIndex: Index {
    fn insert_geometry(&mut self, id: &[u8], geometry: GeometryRef<'_>) -> Result<(), Self::Error>;

    fn delete_geometry(&mut self, id: &[u8]) -> Result<(), Self::Error>;

    fn intersects(&self, query: GeometryRef<'_>, limit: usize) -> Result<Vec<GeoHit>, Self::Error>;

    fn nearest(&self, point: GeoPoint, limit: usize) -> Result<Vec<GeoHit>, Self::Error>;
}

// =============================================================================
// Query planning and cost estimation
// =============================================================================

/// Common interface for indexes that can participate in query planning.
///
/// Specialized indexes may expose richer native query APIs, but this trait lets
/// higher-level systems ask: "Can you help with this predicate, and how costly
/// would it be?"
pub trait QueryableIndex: Index {
    fn estimate(&self, predicate: Predicate<'_>) -> Result<CostEstimate, Self::Error>;

    fn query_candidates(&self, predicate: Predicate<'_>, budget: QueryBudget) -> Result<CandidateSet, Self::Error>;
}

/// Predicate understood by the generic query-planning layer.
#[derive(Clone, Debug)]
pub enum Predicate<'a> {
    Eq { field: Cow<'a, str>, value: Cow<'a, [u8]> },
    Range { field: Cow<'a, str>, start: Bound<Cow<'a, [u8]>>, end: Bound<Cow<'a, [u8]>> },
    Prefix { field: Cow<'a, str>, prefix: Cow<'a, [u8]> },
    Text { field: Option<Cow<'a, str>>, query: Cow<'a, str> },
    VectorKnn { field: Cow<'a, str>, vector: Cow<'a, [f32]>, k: usize },
    GeoIntersects { field: Cow<'a, str>, geometry: GeometryRef<'a> },
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
// Maintenance, statistics, verification, and repair
// =============================================================================

/// Maintenance operations common to tables, indexes, and storage files.
pub trait Maintainable {
    type Error;

    fn compact(&mut self, options: CompactionOptions) -> Result<CompactionReport, Self::Error>;

    fn checkpoint(&mut self) -> Result<CheckpointInfo, Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;

    fn vacuum(&mut self, options: VacuumOptions) -> Result<VacuumReport, Self::Error>;
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

/// Statistics provider for query planning and diagnostics.
pub trait StatisticsProvider {
    type Error;

    fn statistics(&self) -> Result<TableStatistics, Self::Error>;

    fn refresh_statistics(&mut self, budget: WorkBudget) -> Result<(), Self::Error>;
}

/// Consistency verification and repair.
pub trait ConsistencyVerifier {
    type Error;

    fn verify(&self, scope: VerifyScope) -> Result<VerificationReport, Self::Error>;

    fn repair(&mut self, plan: RepairPlan) -> Result<RepairReport, Self::Error>;
}

// =============================================================================
// Single-file page, extent, blob, and WAL layer
// =============================================================================

/// Fixed-size page storage interface.
pub trait PageStore {
    type Error;

    fn page_size(&self) -> usize;

    fn allocate_page(&mut self, page_type: PageType) -> Result<PageId, Self::Error>;

    fn free_page(&mut self, page_id: PageId) -> Result<(), Self::Error>;

    fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), Self::Error>;

    fn write_page(&mut self, page_id: PageId, buf: &[u8]) -> Result<(), Self::Error>;

    fn sync(&mut self) -> Result<(), Self::Error>;
}

/// Extent allocator for variable-sized structures, large values, posting lists,
/// vector graph nodes, and LSM segments.
pub trait ExtentAllocator {
    type Error;

    fn allocate_extent(&mut self, pages: u32, purpose: ExtentPurpose) -> Result<Extent, Self::Error>;

    fn free_extent(&mut self, extent: Extent) -> Result<(), Self::Error>;
}

/// Large binary object store for values too large or fragmented for table pages.
pub trait BlobStore {
    type Error;

    fn put_blob(&mut self, bytes: &[u8]) -> Result<BlobRef, Self::Error>;

    fn get_blob(&self, blob: BlobRef) -> Result<ValueBuf, Self::Error>;

    fn delete_blob(&mut self, blob: BlobRef) -> Result<bool, Self::Error>;
}

/// Write-ahead log interface.
pub trait Wal {
    type Error;

    fn append(&mut self, record: WalRecord<'_>) -> Result<Lsn, Self::Error>;

    fn flush(&mut self, through: Lsn) -> Result<(), Self::Error>;

    fn durable_lsn(&self) -> Lsn;

    fn recover(&mut self, visitor: &mut dyn WalReplayVisitor<Error = Self::Error>) -> Result<(), Self::Error>;
}

/// Visitor used during WAL replay.
pub trait WalReplayVisitor {
    type Error;

    fn visit(&mut self, lsn: Lsn, record: WalRecord<'_>) -> Result<(), Self::Error>;
}

// =============================================================================
// Catalog, options, and metadata records
// =============================================================================

#[derive(Clone, Debug)]
pub struct TableOptions {
    pub engine: TableEngineKind,
    pub key_encoding: KeyEncoding,
    pub compression: Option<CompressionKind>,
    pub encryption: Option<EncryptionKind>,
    pub page_size: Option<usize>,
    pub format_version: u32,
}

#[derive(Clone, Debug)]
pub struct IndexOptions {
    pub kind: IndexKind,
    pub fields: Vec<IndexField>,
    pub unique: bool,
    pub consistency: IndexConsistency,
    pub format_version: u32,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub id: TableId,
    pub name: String,
    pub options: TableOptions,
    pub root: Option<PhysicalLocation>,
    pub created_lsn: Lsn,
}

#[derive(Clone, Debug)]
pub struct IndexInfo {
    pub id: IndexId,
    pub table_id: TableId,
    pub name: String,
    pub options: IndexOptions,
    pub root: Option<PhysicalLocation>,
    pub created_lsn: Lsn,
    pub stale: bool,
}

#[derive(Clone, Debug)]
pub struct IndexField {
    pub name: String,
    pub encoding: KeyEncoding,
    pub descending: bool,
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyEncoding {
    RawBytes,
    LexicographicTuple,
    BigEndianInteger,
    Utf8,
    TimestampMicros,
    Custom(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionKind {
    None,
    Lz4,
    Zstd,
    Snappy,
    Custom(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncryptionKind {
    None,
    AesGcm,
    ChaCha20Poly1305,
    Custom(u32),
}

// =============================================================================
// Batch and mutation records
// =============================================================================

/// A batch of table mutations applied under a transaction.
#[derive(Clone, Debug)]
pub struct WriteBatch<'a> {
    pub mutations: Vec<Mutation<'a>>,
}

#[derive(Clone, Debug)]
pub enum Mutation<'a> {
    Put { key: Cow<'a, [u8]>, value: Cow<'a, [u8]> },
    Delete { key: Cow<'a, [u8]> },
    RangeDelete { bounds: ScanBounds },
    Merge { key: Cow<'a, [u8]>, operand: Cow<'a, [u8]> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchReport {
    pub attempted: u64,
    pub applied: u64,
    pub deleted: u64,
    pub bytes_written: u64,
}

// =============================================================================
// Statistics records
// =============================================================================

#[derive(Clone, Debug, Default)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub total_size_bytes: Option<u64>,
    pub key_stats: Option<KeyStatistics>,
    pub value_stats: Option<ValueStatistics>,
    pub histogram: Option<Histogram>,
    pub last_updated_lsn: Option<Lsn>,
}

#[derive(Clone, Debug, Default)]
pub struct KeyStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub distinct_count: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ValueStatistics {
    pub min_size: usize,
    pub max_size: usize,
    pub avg_size: f64,
    pub null_count: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Debug)]
pub struct HistogramBucket {
    pub lower: KeyBuf,
    pub upper: KeyBuf,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct IndexStats {
    pub entry_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub distinct_keys: Option<u64>,
    pub stale_entries: Option<u64>,
    pub last_updated_lsn: Option<Lsn>,
}

// =============================================================================
// Maintenance records
// =============================================================================

#[derive(Clone, Debug, Default)]
pub struct CompactionOptions {
    pub full: bool,
    pub max_pages: Option<u64>,
    pub target_level: Option<u32>,
}

#[derive(Clone, Debug, Default)]
pub struct CompactionReport {
    pub pages_read: u64,
    pub pages_written: u64,
    pub bytes_reclaimed: u64,
    pub output_lsn: Option<Lsn>,
}

#[derive(Clone, Debug, Default)]
pub struct CheckpointInfo {
    pub checkpoint_lsn: Lsn,
    pub durable_lsn: Lsn,
    pub pages_flushed: u64,
}

#[derive(Clone, Debug, Default)]
pub struct VacuumOptions {
    pub aggressive: bool,
    pub max_pages: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct VacuumReport {
    pub pages_freed: u64,
    pub bytes_reclaimed: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkBudget {
    pub max_pages: Option<u64>,
    pub max_millis: Option<u64>,
    pub max_items: Option<u64>,
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

// =============================================================================
// Verification and repair records
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyScope {
    Catalog,
    Table(TableId),
    Index(IndexId),
    Page(PageId),
    FullDatabase,
}

#[derive(Clone, Debug, Default)]
pub struct VerificationReport {
    pub checked_items: u64,
    pub errors: Vec<ConsistencyError>,
    pub warnings: Vec<ConsistencyWarning>,
}

#[derive(Clone, Debug)]
pub struct ConsistencyError {
    pub error_type: ConsistencyErrorType,
    pub location: String,
    pub description: String,
    pub severity: Severity,
}

#[derive(Clone, Debug)]
pub struct ConsistencyWarning {
    pub location: String,
    pub description: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsistencyErrorType {
    InvalidPointer,
    CorruptedPage,
    CorruptedIndex,
    OrphanedPage,
    MissingPage,
    ChecksumMismatch,
    WalMismatch,
    CatalogMismatch,
    IndexTableMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Clone, Debug)]
pub struct RepairPlan {
    pub actions: Vec<RepairAction>,
}

#[derive(Clone, Debug)]
pub enum RepairAction {
    RebuildIndex(IndexId),
    ReclaimOrphanedPages,
    RestoreFromWal { through: Lsn },
    DropCorruptedObject { table: Option<TableId>, index: Option<IndexId> },
}

#[derive(Clone, Debug, Default)]
pub struct RepairReport {
    pub actions_attempted: u64,
    pub actions_succeeded: u64,
    pub unrepaired_errors: Vec<ConsistencyError>,
}

// =============================================================================
// Physical storage records
// =============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageType {
    Catalog,
    Freelist,
    BTreeInternal,
    BTreeLeaf,
    LsmSegment,
    ArtNode,
    Blob,
    Index,
    Wal,
    Custom(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtentPurpose {
    LargeValue,
    PostingList,
    VectorData,
    HnswGraph,
    LsmRun,
    Blob,
    Custom(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub start_page: PageId,
    pub page_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalLocation {
    pub page_id: PageId,
    pub offset: u32,
    pub length: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalRange {
    pub start: PhysicalLocation,
    pub end: PhysicalLocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobRef {
    pub location: PhysicalLocation,
    pub length: u64,
}

#[derive(Clone, Debug)]
pub enum WalRecord<'a> {
    Begin { tx_id: TxId },
    Commit { tx_id: TxId },
    Rollback { tx_id: TxId },
    TableMutation { tx_id: TxId, table: TableId, mutation: Mutation<'a> },
    IndexMutation { tx_id: TxId, index: IndexId, key: Cow<'a, [u8]>, value: Cow<'a, [u8]> },
    PageWrite { page_id: PageId, before_crc: u32, after: Cow<'a, [u8]> },
    Checkpoint { durable_lsn: Lsn },
}

// =============================================================================
// Index-specific records
// =============================================================================

/// Source abstraction used to rebuild indexes without tying them to one table implementation.
pub trait IndexSource {
    fn scan_rows(&self, bounds: ScanBounds, visitor: &mut dyn IndexSourceVisitor) -> Result<(), IndexSourceError>;
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

// =============================================================================
// Optional implementation helper traits
// =============================================================================

/// Merge operator for LSM-style engines or document patch semantics.
pub trait MergeOperator {
    type Error;

    fn name(&self) -> &str;

    fn merge(&self, key: &[u8], existing: Option<&[u8]>, operand: &[u8]) -> Result<ValueBuf, Self::Error>;
}

/// Comparator for custom key orderings.
///
/// Prefer bytewise lexicographic order where possible. Higher layers can encode
/// typed compound keys into sortable byte strings.
pub trait KeyComparator {
    fn compare(&self, left: &[u8], right: &[u8]) -> core::cmp::Ordering;
}

/// Tokenizer used by full-text indexes.
pub trait Tokenizer {
    type Error;

    fn tokenize(&self, field: &str, text: &str, visitor: &mut dyn TokenVisitor) -> Result<(), Self::Error>;
}

pub trait TokenVisitor {
    fn token(&mut self, term: &str, position: u32, start_offset: u32, end_offset: u32);
}

/// Codec for serializing records into pages, WAL records, or blobs.
pub trait Codec<T> {
    type Error;

    fn encode(&self, value: &T, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    fn decode(&self, input: &[u8]) -> Result<T, Self::Error>;
}

// =============================================================================
// Layering guidance
// =============================================================================

/// This zero-sized type exists only as documentation for the recommended layering.
///
/// Recommended stack:
///
/// ```text
/// KvDatabase
///   ├── Catalog
///   ├── TransactionManager
///   ├── Wal
///   ├── PageStore / ExtentAllocator / BlobStore
///   ├── TableEngine registry
///   │     ├── B+Tree table
///   │     ├── LSM table
///   │     ├── ART table
///   │     └── Memory table
///   └── Index registry
///         ├── DenseOrderedIndex
///         ├── SparseIndex
///         ├── ApproximateMembershipIndex
///         ├── FullTextIndex
///         ├── VectorIndex / HnswIndex / IvfIndex
///         ├── GraphAdjacencyIndex
///         ├── TimeSeriesIndex
///         └── GeoSpatialIndex
/// ```
///
/// Important rule: WAL, transaction commit, page allocation, table mutation, and
/// index mutation should be coordinated by [`KvDatabase`] / [`KvTransaction`].
/// Wrappers such as `WalTable<T>` or `TransactionalTable<T>` are useful for tests
/// or prototypes, but are risky as the primary ACID model because wrapper order
/// changes semantics.
pub struct LayeringGuidance;
