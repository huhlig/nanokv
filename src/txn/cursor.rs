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

//! Cursor implementation for iterating over table entries.

use crate::txn::CursorResult;
use crate::wal::LogSequenceNumber;

/// Ordered cursor over table or index entries.
///
/// Cursors provide snapshot isolation: once opened, they read from a
/// stable view at a specific LSN and are not invalidated by concurrent writers.
pub struct Cursor {
    // TODO: Add internal fields
}

impl Cursor {
    /// Check if the cursor is positioned at a valid entry.
    ///
    /// Returns `false` when the cursor has moved past the end of the scan
    /// bounds or before the beginning.
    pub fn valid(&self) -> bool {
        todo!("Check if cursor is at a valid position")
    }

    /// Check if the cursor is still valid (not invalidated).
    ///
    /// Returns `false` if the cursor's snapshot has been released or an
    /// unrecoverable error has occurred.
    pub fn is_valid(&self) -> bool {
        todo!("Check if cursor itself is still usable")
    }

    /// Get the snapshot LSN at which this cursor reads.
    pub fn snapshot_lsn(&self) -> LogSequenceNumber {
        todo!("Return the snapshot LSN")
    }

    /// Get the key at the current cursor position.
    ///
    /// Returns `None` if the cursor is not positioned at a valid entry.
    /// The returned slice is borrowed and valid until the next cursor operation.
    pub fn key(&self) -> Option<&[u8]> {
        todo!("Return borrowed key slice")
    }

    /// Get the value at the current cursor position.
    ///
    /// Returns `None` if the cursor is not positioned at a valid entry.
    /// The returned slice is borrowed and valid until the next cursor operation.
    pub fn value(&self) -> Option<&[u8]> {
        todo!("Return borrowed value slice")
    }

    /// Move to the first entry in the scan bounds.
    pub fn first(&mut self) -> CursorResult<()> {
        todo!("Move to first entry")
    }

    /// Move to the last entry in the scan bounds.
    pub fn last(&mut self) -> CursorResult<()> {
        todo!("Move to last entry")
    }

    /// Seek to the first entry with a key greater than or equal to the supplied key.
    pub fn seek(&mut self, _key: &[u8]) -> CursorResult<()> {
        todo!("Seek to key >= supplied key")
    }

    /// Seek to the greatest key less than or equal to the supplied key.
    pub fn seek_prev(&mut self, _key: &[u8]) -> CursorResult<()> {
        todo!("Seek to key <= supplied key")
    }

    /// Move to the next entry.
    pub fn next(&mut self) -> CursorResult<()> {
        todo!("Move to next entry")
    }

    /// Move to the previous entry.
    pub fn prev(&mut self) -> CursorResult<()> {
        todo!("Move to previous entry")
    }
}
