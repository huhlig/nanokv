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
use crate::types::ObjectId;

/// Transaction Result Type
pub type TransactionResult<T> = Result<T, TransactionError>;

/// Transaction error type.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    /// Invalid state for the attempted operation
    #[error("Transaction {transaction_id} has invalid state '{current_state}' for operation '{attempted_operation}'")]
    InvalidState {
        transaction_id: TransactionId,
        current_state: String,
        attempted_operation: String,
    },

    #[error("Write-write conflict: object {0}, key {1:?} already locked by transaction {2}")]
    WriteWriteConflict(ObjectId, Vec<u8>, TransactionId),

    #[error("Read-write conflict: object {0}, key {1:?} was modified after read")]
    ReadWriteConflict(ObjectId, Vec<u8>),

    #[error("Serialization conflict detected")]
    SerializationConflict,

    #[error("Transaction {0} not found")]
    TransactionNotFound(TransactionId),

    /// Deadlock detected with cycle information
    #[error("Deadlock detected involving transaction {transaction_id}: {cycle_description}")]
    Deadlock {
        transaction_id: TransactionId,
        involved_transactions: Vec<TransactionId>,
        cycle_description: String,
    },

    /// Other error
    #[error("Transaction error: {0}")]
    Other(String),
}

impl TransactionError {
    /// Create an invalid state error with full context
    pub fn invalid_state(
        transaction_id: TransactionId,
        current_state: impl Into<String>,
        attempted_operation: impl Into<String>,
    ) -> Self {
        Self::InvalidState {
            transaction_id,
            current_state: current_state.into(),
            attempted_operation: attempted_operation.into(),
        }
    }

    /// Create a deadlock error with full context
    pub fn deadlock(
        transaction_id: TransactionId,
        involved_transactions: Vec<TransactionId>,
        cycle_description: impl Into<String>,
    ) -> Self {
        Self::Deadlock {
            transaction_id,
            involved_transactions,
            cycle_description: cycle_description.into(),
        }
    }
}

/// Cursor Result Type
pub type CursorResult<T> = Result<T, CursorError>;

/// Cursor error type.
#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("Cursor error: {0}")]
    Other(String),
}
