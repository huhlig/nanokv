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

use crate::index::IndexId;
use crate::table::TableError;

/// Index Result Type
pub type IndexResult<T> = Result<T, IndexError>;

/// Index error types with structured context for improved debuggability.
///
/// Each error variant includes relevant context such as index_id, keys, and
/// operation details to aid in debugging and operational visibility.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    /// Key not found in index
    #[error("Key not found in index {index_id}: {key:?}")]
    KeyNotFound {
        index_id: IndexId,
        key: Vec<u8>,
    },

    /// Duplicate key violation in unique index
    #[error("Duplicate key in unique index {index_id}: {key:?}")]
    DuplicateKey {
        index_id: IndexId,
        key: Vec<u8>,
    },

    /// Invalid key format or encoding
    #[error("Invalid key format in index {index_id}: {details}")]
    InvalidKey {
        index_id: IndexId,
        details: String,
    },

    /// Index is stale and needs rebuild
    #[error("Index {index_id} is stale: {details}")]
    Stale {
        index_id: IndexId,
        details: String,
    },

    /// Index corruption detected
    #[error("Corruption detected in index {index_id} at {location}: {corruption_type} - {details}")]
    Corrupted {
        index_id: IndexId,
        location: String,
        corruption_type: String,
        details: String,
    },

    /// Index operation failed
    #[error("Index operation '{operation}' failed for index {index_id}: {details}")]
    OperationFailed {
        index_id: IndexId,
        operation: String,
        details: String,
    },

    /// Index capacity exceeded
    #[error("Index {index_id} capacity exceeded: {details}")]
    CapacityExceeded {
        index_id: IndexId,
        details: String,
    },

    /// Unsupported index operation
    #[error("Unsupported operation '{operation}' for index {index_id} of type {index_type}")]
    UnsupportedOperation {
        index_id: IndexId,
        index_type: String,
        operation: String,
    },

    /// I/O error during index operation
    #[error("I/O error in index {index_id}: {source}")]
    Io {
        index_id: IndexId,
        #[source]
        source: std::io::Error,
    },

    /// Table error during index operation
    #[error("Table error in index {index_id}: {source}")]
    Table {
        index_id: IndexId,
        #[source]
        source: TableError,
    },

    /// Internal error
    #[error("Internal error in index {index_id}: {details}")]
    Internal {
        index_id: IndexId,
        details: String,
    },
}

impl IndexError {
    /// Create a key not found error
    pub fn key_not_found(index_id: IndexId, key: Vec<u8>) -> Self {
        Self::KeyNotFound { index_id, key }
    }

    /// Create a duplicate key error
    pub fn duplicate_key(index_id: IndexId, key: Vec<u8>) -> Self {
        Self::DuplicateKey { index_id, key }
    }

    /// Create an invalid key error
    pub fn invalid_key(index_id: IndexId, details: impl Into<String>) -> Self {
        Self::InvalidKey {
            index_id,
            details: details.into(),
        }
    }

    /// Create a stale index error
    pub fn stale(index_id: IndexId, details: impl Into<String>) -> Self {
        Self::Stale {
            index_id,
            details: details.into(),
        }
    }

    /// Create a corruption error
    pub fn corrupted(
        index_id: IndexId,
        location: impl Into<String>,
        corruption_type: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::Corrupted {
            index_id,
            location: location.into(),
            corruption_type: corruption_type.into(),
            details: details.into(),
        }
    }

    /// Create an operation failed error
    pub fn operation_failed(
        index_id: IndexId,
        operation: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::OperationFailed {
            index_id,
            operation: operation.into(),
            details: details.into(),
        }
    }

    /// Create a capacity exceeded error
    pub fn capacity_exceeded(index_id: IndexId, details: impl Into<String>) -> Self {
        Self::CapacityExceeded {
            index_id,
            details: details.into(),
        }
    }

    /// Create an unsupported operation error
    pub fn unsupported_operation(
        index_id: IndexId,
        index_type: impl Into<String>,
        operation: impl Into<String>,
    ) -> Self {
        Self::UnsupportedOperation {
            index_id,
            index_type: index_type.into(),
            operation: operation.into(),
        }
    }

    /// Create an I/O error
    pub fn io(index_id: IndexId, source: std::io::Error) -> Self {
        Self::Io { index_id, source }
    }

    /// Create a table error
    pub fn table(index_id: IndexId, source: TableError) -> Self {
        Self::Table { index_id, source }
    }

    /// Create an internal error
    pub fn internal(index_id: IndexId, details: impl Into<String>) -> Self {
        Self::Internal {
            index_id,
            details: details.into(),
        }
    }
}

/// Errors that can occur when scanning table data for index rebuilds.
///
/// This enum preserves the original error type information, enabling rebuild
/// logic to distinguish between transient I/O failures, corruption, and other
/// error categories for proper error handling and retry strategies.
#[derive(Debug, thiserror::Error)]
pub enum IndexSourceError {
    /// Table scan operation failed
    #[error("Table scan failed: {0}")]
    TableScan(#[from] TableError),

    /// I/O error during scan
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid data encountered during scan
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Scan was cancelled or interrupted
    #[error("Scan cancelled: {0}")]
    Cancelled(String),

    /// Other source error
    #[error("Source error: {0}")]
    Other(String),
}
