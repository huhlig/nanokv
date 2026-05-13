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

//! Posting list for full-text search.
//!
//! A posting list stores the document IDs and positions where a term appears.

use serde::{Deserialize, Serialize};

/// A single position occurrence in a document.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostingEntry {
    /// Document ID
    pub doc_id: Vec<u8>,
    /// Field name where term appears
    pub field: String,
    /// Positions within the field
    pub positions: Vec<usize>,
    /// Field boost factor
    pub boost: f32,
}

/// Posting list for a single term.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PostingList {
    /// List of posting entries
    pub entries: Vec<PostingEntry>,
}

impl PostingList {
    /// Create a new empty posting list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a posting entry.
    pub fn add(&mut self, entry: PostingEntry) {
        self.entries.push(entry);
    }

    /// Remove all entries for a document.
    pub fn remove_document(&mut self, doc_id: &[u8]) {
        self.entries.retain(|e| e.doc_id != doc_id);
    }

    /// Get the document frequency (number of documents containing this term).
    pub fn doc_freq(&self) -> usize {
        self.entries.len()
    }

    /// Check if the posting list is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

/// Document store entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentEntry {
    /// Document ID
    pub doc_id: Vec<u8>,
    /// Stored fields (name -> value)
    pub fields: Vec<(String, String)>,
}

impl DocumentEntry {
    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Deserialize from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}
