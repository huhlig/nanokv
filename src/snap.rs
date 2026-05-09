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

use crate::txn::TransactionId;
use crate::wal::LogSequenceNumber;
use std::fmt::Formatter;

/// Snapshot identifier.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct SnapshotId(u64);

impl From<u64> for SnapshotId {
    fn from(value: u64) -> Self {
        SnapshotId(value)
    }
}

impl std::fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SnapshotId({})", self.0)
    }
}

/// A named, persistent snapshot of the database at a specific LSN.
///
/// Snapshots enable point-in-time queries, backups, and long-running analytics
/// without blocking writers. They pin the necessary pages/segments in memory
/// or on disk until explicitly released.
///
/// # Lifecycle
///
/// 1. Create snapshot with [`KvDatabase::create_snapshot`]
/// 2. Use snapshot LSN to open read transactions
/// 3. Release snapshot with [`KvDatabase::release_snapshot`] when done
///
/// # Examples
///
/// ```ignore
/// // Create a snapshot for backup
/// let snapshot = db.create_snapshot("backup-2024")?;
///
/// // Use the snapshot LSN for consistent reads
/// let tx = db.begin_read_at(snapshot.lsn)?;
/// // ... perform backup operations ...
///
/// // Release when done
/// db.release_snapshot(snapshot.id)?;
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Unique snapshot identifier.
    pub id: SnapshotId,
    /// User-provided name for the snapshot.
    pub name: String,
    /// LSN at which the snapshot was taken.
    pub lsn: LogSequenceNumber,
    /// Timestamp when the snapshot was created.
    pub created_at: i64,
    /// Estimated size in bytes (pages/segments pinned).
    pub size_bytes: u64,
    // TODO(MVCC): Add visibility information for MVCC
    // Transactions that were active when this snapshot was created.
    // Used to determine version visibility: a version is visible if it was
    // committed before this snapshot's LSN AND was not created by a transaction
    // in the active_txns list.
    /// Transactions active at snapshot creation time
    pub active_txns: Vec<TransactionId>,
}

impl Snapshot {
    /// Determines if a version is visible to this snapshot.
    ///
    /// A version is visible if:
    /// 1. It was committed before this snapshot's LSN
    /// 2. It was not created by a transaction that was active at snapshot time
    ///
    /// # Arguments
    ///
    /// * `version_lsn` - The LSN at which the version was committed
    /// * `created_by` - The transaction ID that created the version
    ///
    /// # Returns
    ///
    /// `true` if the version is visible to this snapshot, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```
    /// # use nanokv::snap::Snapshot;
    /// # use nanokv::wal::LogSequenceNumber;
    /// # use nanokv::txn::TransactionId;
    /// # use nanokv::snap::SnapshotId;
    /// let snapshot = Snapshot {
    ///     id: SnapshotId::from(1),
    ///     name: "test".to_string(),
    ///     lsn: LogSequenceNumber::from(100),
    ///     created_at: 0,
    ///     size_bytes: 0,
    ///     active_txns: vec![TransactionId::from(5)],
    /// };
    ///
    /// // Version committed before snapshot and not by active transaction
    /// assert!(snapshot.is_visible(LogSequenceNumber::from(50), TransactionId::from(1)));
    ///
    /// // Version committed after snapshot
    /// assert!(!snapshot.is_visible(LogSequenceNumber::from(150), TransactionId::from(1)));
    ///
    /// // Version created by active transaction
    /// assert!(!snapshot.is_visible(LogSequenceNumber::from(50), TransactionId::from(5)));
    /// ```
    pub fn is_visible(&self, version_lsn: LogSequenceNumber, created_by: TransactionId) -> bool {
        // Version must be committed before this snapshot
        if version_lsn > self.lsn {
            return false;
        }
        // Version must not be from a transaction that was active at snapshot time
        !self.active_txns.contains(&created_by)
    }
}
