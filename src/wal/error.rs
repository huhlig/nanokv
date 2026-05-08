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

//! WAL error types

use crate::txn::TransactionId;
use crate::vfs::FileSystemError;
use crate::wal::LogSequenceNumber;
use thiserror::Error;

/// Result type for WAL operations
pub type WalResult<T> = Result<T, WalError>;

/// WAL error types
#[derive(Debug, Error)]
pub enum WalError {
    /// VFS error
    #[error("VFS error: {0}")]
    VfsError(#[from] FileSystemError),

    /// Invalid WAL record
    #[error("Invalid WAL record: {0}")]
    InvalidRecord(String),

    /// Checksum mismatch
    #[error("Checksum mismatch at offset {0}")]
    ChecksumMismatch(LogSequenceNumber),

    /// Corrupted WAL file
    #[error("Corrupted WAL file: {0}")]
    CorruptedWal(String),

    /// Transaction not found
    #[error("Transaction not found: {0}")]
    TransactionNotFound(TransactionId),

    /// Transaction already exists
    #[error("Transaction already exists: {0}")]
    TransactionAlreadyExists(TransactionId),

    /// Invalid transaction state
    #[error("Invalid transaction state: {0}")]
    InvalidTransactionState(String),

    /// WAL is full
    #[error("WAL is full (max size reached)")]
    WalFull,

    /// Recovery error
    #[error("Recovery error: {0}")]
    RecoveryError(String),

    /// Checkpoint error
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Encryption error
    #[error("Encryption error: {0}")]
    EncryptionError(String),

    /// Decryption error
    #[error("Decryption error: {0}")]
    DecryptionError(String),

    /// Missing encryption key
    #[error("Missing encryption key")]
    MissingEncryptionKey,

    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),
}


