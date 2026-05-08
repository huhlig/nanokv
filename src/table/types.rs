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


/// Owned key buffer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBuf(pub Vec<u8>);

impl AsRef<[u8]> for KeyBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Owned value buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueBuf(pub Vec<u8>);

impl AsRef<[u8]> for ValueBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A key-value entry returned by owned iterators or batch operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: KeyBuf,
    pub value: ValueBuf,
}
