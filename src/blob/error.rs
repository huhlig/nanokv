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

use crate::blob::BlobRef;
use thiserror::Error;

/// Result type for blob operations.
pub type BlobResult<T> = Result<T, BlobError>;

/// Blob storage error types.
#[derive(Debug, Error)]
pub enum BlobError {
    #[error("Blob not found: {blob_ref:?}")]
    NotFound { blob_ref: BlobRef },

    #[error("Invalid blob reference: {details}")]
    InvalidReference { details: String },

    #[error("Stale blob reference {blob_ref:?}: checksum mismatch (expected: {expected:#x}, found: {found:#x})")]
    StaleReference {
        blob_ref: BlobRef,
        expected: u32,
        found: u32,
    },

    #[error("Blob too large: {size} bytes (max: {max}) for blob {blob_ref:?}")]
    TooLarge {
        blob_ref: BlobRef,
        size: u64,
        max: u64,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Pager error for blob {blob_ref:?}: {details}")]
    Pager { blob_ref: BlobRef, details: String },

    #[error("Corruption detected in blob {blob_ref:?} at {location}: {corruption_type} - {details}")]
    Corrupted {
        blob_ref: BlobRef,
        location: String,
        corruption_type: String,
        details: String,
    },

    #[error("Internal error: {0}")]
    Internal(String),
}

impl BlobError {
    /// Create a not found error
    pub fn not_found(blob_ref: BlobRef) -> Self {
        Self::NotFound { blob_ref }
    }

    /// Create an invalid reference error
    pub fn invalid_reference(details: impl Into<String>) -> Self {
        Self::InvalidReference {
            details: details.into(),
        }
    }

    /// Create a stale reference error
    pub fn stale_reference(blob_ref: BlobRef, expected: u32, found: u32) -> Self {
        Self::StaleReference {
            blob_ref,
            expected,
            found,
        }
    }

    /// Create a too large error
    pub fn too_large(blob_ref: BlobRef, size: u64, max: u64) -> Self {
        Self::TooLarge {
            blob_ref,
            size,
            max,
        }
    }

    /// Create a pager error
    pub fn pager(blob_ref: BlobRef, details: impl Into<String>) -> Self {
        Self::Pager {
            blob_ref,
            details: details.into(),
        }
    }

    /// Create a corruption error
    pub fn corrupted(
        blob_ref: BlobRef,
        location: impl Into<String>,
        corruption_type: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::Corrupted {
            blob_ref,
            location: location.into(),
            corruption_type: corruption_type.into(),
            details: details.into(),
        }
    }
}
