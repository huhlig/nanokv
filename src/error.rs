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

//! Unified error type for NanoKV
//!
//! This module provides a unified error type that wraps all subsystem-specific
//! error types, enabling seamless error propagation across layer boundaries
//! without manual `map_err` conversions.

use crate::blob::BlobError;
use crate::index::{IndexError, IndexSourceError};
use crate::pager::PagerError;
use crate::table::TableError;
use crate::txn::{CursorError, TransactionError};
use crate::vfs::FileSystemError;
use crate::wal::WalError;
use thiserror::Error;

/// Result type for NanoKV operations
pub type NanoKvResult<T> = Result<T, NanoKvError>;

/// Unified error type for all NanoKV operations
///
/// This enum wraps all subsystem-specific error types, providing automatic
/// conversion via `From` implementations. This eliminates the need for manual
/// `map_err` calls when errors cross subsystem boundaries.
///
/// # Example
///
/// ```rust,ignore
/// fn operation() -> NanoKvResult<()> {
///     // Pager errors automatically convert to NanoKvError
///     let page = pager.read_page(page_id)?;
///     
///     // WAL errors also automatically convert
///     wal.write_record(record)?;
///     
///     Ok(())
/// }
/// ```
#[derive(Debug, Error)]
pub enum NanoKvError {
    /// Pager subsystem error
    #[error("Pager error: {0}")]
    Pager(#[from] PagerError),

    /// WAL subsystem error
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// Table subsystem error
    #[error("Table error: {0}")]
    Table(#[from] TableError),

    /// Transaction subsystem error
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),

    /// Cursor subsystem error
    #[error("Cursor error: {0}")]
    Cursor(#[from] CursorError),

    /// Blob storage subsystem error
    #[error("Blob error: {0}")]
    Blob(#[from] BlobError),

    /// Index subsystem error
    #[error("Index error: {0}")]
    Index(#[from] IndexError),

    /// Index source error (for rebuild operations)
    #[error("Index source error: {0}")]
    IndexSource(#[from] IndexSourceError),

    /// Virtual file system error
    #[error("VFS error: {0}")]
    Vfs(#[from] FileSystemError),

    /// Standard I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error for cases not covered by specific subsystems
    #[error("{0}")]
    Other(String),
}

impl NanoKvError {
    /// Create a generic error from a string
    pub fn other(msg: impl Into<String>) -> Self {
        NanoKvError::Other(msg.into())
    }

    /// Check if this error is a pager error
    pub fn is_pager(&self) -> bool {
        matches!(self, NanoKvError::Pager(_))
    }

    /// Check if this error is a WAL error
    pub fn is_wal(&self) -> bool {
        matches!(self, NanoKvError::Wal(_))
    }

    /// Check if this error is a table error
    pub fn is_table(&self) -> bool {
        matches!(self, NanoKvError::Table(_))
    }

    /// Check if this error is a transaction error
    pub fn is_transaction(&self) -> bool {
        matches!(self, NanoKvError::Transaction(_))
    }

    /// Check if this error is a cursor error
    pub fn is_cursor(&self) -> bool {
        matches!(self, NanoKvError::Cursor(_))
    }

    /// Check if this error is a blob error
    pub fn is_blob(&self) -> bool {
        matches!(self, NanoKvError::Blob(_))
    }

    /// Check if this error is an index error
    pub fn is_index(&self) -> bool {
        matches!(self, NanoKvError::Index(_))
    }

    /// Check if this error is an index source error
    pub fn is_index_source(&self) -> bool {
        matches!(self, NanoKvError::IndexSource(_))
    }

    /// Check if this error is a VFS error
    pub fn is_vfs(&self) -> bool {
        matches!(self, NanoKvError::Vfs(_))
    }

    /// Check if this error is an I/O error
    pub fn is_io(&self) -> bool {
        matches!(self, NanoKvError::Io(_))
    }

    /// Get the underlying pager error, if any
    pub fn as_pager(&self) -> Option<&PagerError> {
        match self {
            NanoKvError::Pager(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying WAL error, if any
    pub fn as_wal(&self) -> Option<&WalError> {
        match self {
            NanoKvError::Wal(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying table error, if any
    pub fn as_table(&self) -> Option<&TableError> {
        match self {
            NanoKvError::Table(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying transaction error, if any
    pub fn as_transaction(&self) -> Option<&TransactionError> {
        match self {
            NanoKvError::Transaction(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying cursor error, if any
    pub fn as_cursor(&self) -> Option<&CursorError> {
        match self {
            NanoKvError::Cursor(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying blob error, if any
    pub fn as_blob(&self) -> Option<&BlobError> {
        match self {
            NanoKvError::Blob(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying index error, if any
    pub fn as_index(&self) -> Option<&IndexError> {
        match self {
            NanoKvError::Index(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying index source error, if any
    pub fn as_index_source(&self) -> Option<&IndexSourceError> {
        match self {
            NanoKvError::IndexSource(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying VFS error, if any
    pub fn as_vfs(&self) -> Option<&FileSystemError> {
        match self {
            NanoKvError::Vfs(e) => Some(e),
            _ => None,
        }
    }

    /// Get the underlying I/O error, if any
    pub fn as_io(&self) -> Option<&std::io::Error> {
        match self {
            NanoKvError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion() {
        // Test that errors convert properly
        let pager_err = PagerError::DatabaseFull;
        let nanokv_err: NanoKvError = pager_err.into();
        assert!(nanokv_err.is_pager());
        assert!(nanokv_err.as_pager().is_some());
    }

    #[test]
    fn test_error_type_checks() {
        let err = NanoKvError::other("test error");
        assert!(!err.is_pager());
        assert!(!err.is_wal());
        assert!(!err.is_table());
        assert!(!err.is_transaction());
        assert!(!err.is_cursor());
        assert!(!err.is_blob());
        assert!(!err.is_index());
        assert!(!err.is_vfs());
        assert!(!err.is_io());
    }

    #[test]
    fn test_error_extraction() {
        let pager_err = PagerError::DatabaseFull;
        let nanokv_err: NanoKvError = pager_err.into();
        
        assert!(nanokv_err.as_pager().is_some());
        assert!(nanokv_err.as_wal().is_none());
        assert!(nanokv_err.as_table().is_none());
    }
}

// Made with Bob
