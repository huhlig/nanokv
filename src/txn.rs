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

use crate::wal::LogSequenceNumber;
use std::fmt::Formatter;

/// Transaction ID type
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TransactionId(u64);

impl TransactionId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

impl From<u64> for TransactionId {
    fn from(value: u64) -> Self {
        TransactionId(value)
    }
}

impl PartialEq<u64> for TransactionId {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialEq<TransactionId> for u64 {
    fn eq(&self, other: &TransactionId) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransactionId({})", self.0)
    }
}

/// Result of a successful commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitInfo {
    pub tx_id: TransactionId,
    pub commit_lsn: LogSequenceNumber,
    pub durable_lsn: Option<LogSequenceNumber>,
}
