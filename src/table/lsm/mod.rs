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
mod config;

pub use self::bloom::{BloomFilter, BloomFilterBuilder};
pub use self::config::{
    BloomFilterConfig, BlockCacheConfig, CacheEvictionPolicy, CompactionConfig,
    CompactionStrategy, LevelConfig, LsmConfig, MemtableConfig, MemtableType, SStableConfig,
};

// Made with Bob
