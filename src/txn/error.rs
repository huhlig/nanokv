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

use crate::table::TableId;
use crate::txn::TransactionId;

/// Transaction Result Type
pub type TransactionResult<T> = Result<T, TransactionError>;

/// Transaction error type.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Transaction {0} has invalid state for this operation")]
    InvalidState(TransactionId),

    #[error("Write-write conflict: table {0}, key {1:?} already locked by transaction {2}")]
    WriteWriteConflict(TableId, Vec<u8>, TransactionId),

    #[error("Read-write conflict: table {0}, key {1:?} was modified after read")]
    ReadWriteConflict(TableId, Vec<u8>),

    #[error("Serialization conflict detected")]
    SerializationConflict,

    #[error("Transaction {0} not found")]
    TransactionNotFound(TransactionId),

    #[error("Deadlock detected involving transaction {0}")]
    Deadlock(TransactionId),

    /// Other error
    #[error("Transaction error: {0}")]
    Other(String),
}

/// Cursor Result Type
pub type CursorResult<T> = Result<T, CursorError>;

/// Cursor error type.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("Cursor error: {0}")]
    Other(String),
}
