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

//! Blob storage table implementations.
//!
//! This module provides blob storage as a specialty table type, following the
//! unified table architecture. Blobs are large binary objects that are stored
//! and retrieved by key, with three implementation strategies:
//!
//! - `MemoryBlob`: In-memory blob storage for ephemeral data
//! - `PagedBlob`: Disk-backed storage using linked pages in the pager
//! - `FileBlob`: Direct file-based storage for very large blobs
//!
//! All implementations follow the standard Table trait with MutableTable and
//! PointLookup for operations. Large values can be streamed using put_stream
//! and get_stream methods to avoid loading entire blobs into memory.

mod file;
mod memory;
mod paged;

pub use self::file::FileBlob;
pub use self::memory::MemoryBlob;
pub use self::paged::PagedBlob;

// Made with Bob
