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

//! Pager error types

use crate::pager::PageId;
use crate::vfs::FileSystemError;
use thiserror::Error;

/// Result type for pager operations
pub type PagerResult<T> = Result<T, PagerError>;

/// Pager error types
#[derive(Debug, Error)]
pub enum PagerError {
    /// VFS error
    #[error("VFS error: {0}")]
    VfsError(#[from] FileSystemError),

    /// Invalid page ID
    #[error("Invalid page ID: {0}")]
    InvalidPageId(PageId),

    /// Page not found
    #[error("Page not found: {0}")]
    PageNotFound(PageId),

    /// Checksum mismatch
    #[error("Checksum mismatch for page {0}")]
    ChecksumMismatch(PageId),

    /// Invalid page size
    #[error("Invalid page size: {0}")]
    InvalidPageSize(u32),

    /// Invalid file header
    #[error("Invalid file header: {0}")]
    InvalidFileHeader(String),

    /// Invalid superblock
    #[error("Invalid superblock: {0}")]
    InvalidSuperblock(String),

    /// Compression error
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// Decompression error
    #[error("Decompression error: {0}")]
    DecompressionError(String),

    /// Encryption error
    #[error("Encryption error: {0}")]
    EncryptionError(String),

    /// Decryption error
    #[error("Decryption error: {0}")]
    DecryptionError(String),

    /// Missing encryption key
    #[error("Encryption key required but not provided")]
    MissingEncryptionKey,

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Database is full (no free pages)
    #[error("Database is full (no free pages available)")]
    DatabaseFull,

    /// Page is already allocated
    #[error("Page {0} is already allocated")]
    PageAlreadyAllocated(PageId),

    /// Page is already free
    #[error("Page {0} is already free")]
    PageAlreadyFree(PageId),

    /// Page is pinned (cannot be freed while in use)
    #[error("Page {0} is pinned and cannot be freed")]
    PagePinned(PageId),

    /// Invalid page type
    #[error("Invalid page type: {0}")]
    InvalidPageType(u8),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),
}
