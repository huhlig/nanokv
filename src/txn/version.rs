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

use crate::snap::Snapshot;
use crate::txn::TransactionId;
use crate::wal::LogSequenceNumber;

// TODO(MVCC): Version chain structure for storing multiple versions of a value
// This enables MVCC by maintaining a linked list of value versions.
// Each version tracks:
// - The value data
// - Which transaction created it
// - When it was committed (None if uncommitted)
// - Link to previous (older) version
//
// Usage: Tables store the head of the version chain for each key.
// When reading, traverse the chain to find the first visible version
// based on the transaction's snapshot LSN and active transaction list.
//
// Garbage collection: Periodically remove versions older than the
// minimum visible LSN across all active snapshots.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VersionChain {
    /// Current value data
    pub value: Vec<u8>,
    /// Transaction that created this version
    pub created_by: TransactionId,
    /// LSN when this version was committed (None if uncommitted)
    pub commit_lsn: Option<LogSequenceNumber>,
    /// Previous version (older), if any
    pub prev_version: Option<Box<VersionChain>>,
}

impl VersionChain {
    /// Create a new version chain entry
    pub fn new(value: Vec<u8>, created_by: TransactionId) -> Self {
        Self {
            value,
            created_by,
            commit_lsn: None,
            prev_version: None,
        }
    }

    /// Mark this version as committed at the given LSN
    pub fn commit(&mut self, lsn: LogSequenceNumber) {
        self.commit_lsn = Some(lsn);
    }

    /// Add a new version to the front of the chain
    pub fn prepend(self, value: Vec<u8>, created_by: TransactionId) -> Self {
        Self {
            value,
            created_by,
            commit_lsn: None,
            prev_version: Some(Box::new(self)),
        }
    }

    /// Find the newest version visible to the provided snapshot.
    ///
    /// Traverses the chain from newest to oldest and returns the first version
    /// that is committed and visible at the snapshot's LSN and transaction set.
    /// Uncommitted versions are never visible.
    pub fn find_visible_version(&self, snapshot: &Snapshot) -> Option<&[u8]> {
        let mut current = Some(self);

        while let Some(version) = current {
            if let Some(commit_lsn) = version.commit_lsn
                && snapshot.is_visible(commit_lsn, version.created_by) {
                    return Some(version.value.as_slice());
                }

            current = version.prev_version.as_deref();
        }

        None
    }

    /// Remove obsolete committed versions older than the visibility watermark.
    ///
    /// Retains:
    /// - all uncommitted versions
    /// - committed versions with `commit_lsn >= min_visible_lsn`
    /// - the newest committed version older than `min_visible_lsn` as a base
    ///
    /// Returns the number of removed versions.
    pub fn vacuum(&mut self, min_visible_lsn: LogSequenceNumber) -> usize {
        fn retain_obsolete_versions(
            node: &VersionChain,
            min_visible_lsn: LogSequenceNumber,
            keep_obsolete_budget: &mut usize,
        ) -> VersionChain {
            let rebuilt_prev = node.prev_version.as_ref().map(|prev| {
                Box::new(retain_obsolete_versions(
                    prev,
                    min_visible_lsn,
                    keep_obsolete_budget,
                ))
            });

            let keep_this_obsolete = matches!(node.commit_lsn, Some(lsn) if lsn < min_visible_lsn)
                && *keep_obsolete_budget > 0;

            if keep_this_obsolete {
                *keep_obsolete_budget -= 1;
            }

            let prev_version = if matches!(node.commit_lsn, Some(lsn) if lsn < min_visible_lsn)
                && !keep_this_obsolete
            {
                rebuilt_prev.and_then(|prev| prev.prev_version)
            } else {
                rebuilt_prev
            };

            VersionChain {
                value: node.value.clone(),
                created_by: node.created_by,
                commit_lsn: node.commit_lsn,
                prev_version,
            }
        }

        fn count_removable_obsolete_versions(
            node: &VersionChain,
            min_visible_lsn: LogSequenceNumber,
        ) -> usize {
            let current = usize::from(matches!(
                node.commit_lsn,
                Some(lsn) if lsn < min_visible_lsn
            ));

            current
                + node
                    .prev_version
                    .as_deref()
                    .map(|prev| count_removable_obsolete_versions(prev, min_visible_lsn))
                    .unwrap_or(0)
        }

        let obsolete_count = count_removable_obsolete_versions(self, min_visible_lsn);
        let removed = obsolete_count.saturating_sub(1);

        if removed == 0 {
            return 0;
        }

        let mut keep_obsolete_budget = 1;
        *self = retain_obsolete_versions(self, min_visible_lsn, &mut keep_obsolete_budget);
        removed
    }
}

// Made with Bob
