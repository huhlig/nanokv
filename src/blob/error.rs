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
    #[error("Blob not found: {0:?}")]
    NotFound(BlobRef),

    #[error("Invalid blob reference: {0}")]
    InvalidReference(String),

    #[error("Stale blob reference: checksum mismatch (expected: {expected:#x}, found: {found:#x})")]
    StaleReference { expected: u32, found: u32 },

    #[error("Blob too large: {size} bytes (max: {max})")]
    TooLarge { size: u64, max: u64 },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Pager error: {0}")]
    Pager(String),

    #[error("Corruption detected: {0}")]
    Corrupted(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
