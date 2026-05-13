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

//! Streaming interface for reading overflow page chains

use crate::pager::{OverflowPageHeader, PageId, PageType, Pager, PagerResult, calculate_crc32};
use crate::table::{TableResult, ValueStream};
use crate::vfs::FileSystem;

/// Stream for reading data from an overflow page chain
///
/// This struct implements the ValueStream trait to enable efficient
/// streaming of large values stored across multiple overflow pages.
pub struct OverflowChainStream<'a, FS: FileSystem> {
    /// Reference to the pager
    pager: &'a Pager<FS>,
    /// Current page ID being read
    current_page_id: Option<PageId>,
    /// Total length of the value across all pages
    total_length: u64,
    /// Current position in the stream
    position: u64,
    /// Buffer for current page data
    buffer: Vec<u8>,
    /// Position within the current buffer
    buffer_pos: usize,
}

impl<'a, FS: FileSystem> OverflowChainStream<'a, FS> {
    /// Create a new overflow chain stream
    pub fn new(pager: &'a Pager<FS>, first_page_id: PageId, total_length: u64) -> Self {
        Self {
            pager,
            current_page_id: Some(first_page_id),
            total_length,
            position: 0,
            buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    /// Load the next page in the chain into the buffer
    fn load_next_page(&mut self) -> PagerResult<()> {
        let page_id = match self.current_page_id {
            Some(id) => id,
            None => return Ok(()), // No more pages
        };

        // Read the page
        let page = self.pager.read_page(page_id)?;

        // Verify it's an overflow page
        if page.page_type() != PageType::Overflow {
            return Err(crate::pager::PagerError::InternalError(format!(
                "Expected overflow page, got {:?}",
                page.page_type()
            )));
        }

        // Parse header
        let header = OverflowPageHeader::from_bytes(page.data())?;

        // Extract data (skip header)
        let data_start = OverflowPageHeader::SIZE;
        let data_end = data_start + header.data_length as usize;

        if data_end > page.data().len() {
            return Err(crate::pager::PagerError::InternalError(format!(
                "Overflow page data length {} exceeds page size",
                header.data_length
            )));
        }

        let data = &page.data()[data_start..data_end];

        // Verify checksum
        let actual_checksum = calculate_crc32(data);
        if actual_checksum != header.checksum {
            return Err(crate::pager::PagerError::InternalError(format!(
                "Overflow page checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
                header.checksum, actual_checksum
            )));
        }

        // Update buffer
        self.buffer.clear();
        self.buffer.extend_from_slice(data);
        self.buffer_pos = 0;

        // Update current page ID for next iteration
        self.current_page_id = if header.is_last() {
            None
        } else {
            Some(PageId::from(header.next_page_id as u64))
        };

        Ok(())
    }
}

impl<'a, FS: FileSystem> ValueStream for OverflowChainStream<'a, FS> {
    fn read(&mut self, buf: &mut [u8]) -> TableResult<usize> {
        if self.position >= self.total_length {
            return Ok(0); // EOF
        }

        let mut total_read = 0;

        while total_read < buf.len() && self.position < self.total_length {
            // Refill buffer if needed
            if self.buffer_pos >= self.buffer.len() {
                if self.current_page_id.is_none() {
                    // No more pages to read
                    break;
                }
                self.load_next_page()
                    .map_err(crate::table::TableError::Pager)?;
            }

            // If buffer is still empty after load, we're done
            if self.buffer.is_empty() {
                break;
            }

            // Copy from buffer
            let to_copy = (buf.len() - total_read)
                .min(self.buffer.len() - self.buffer_pos)
                .min((self.total_length - self.position) as usize);

            buf[total_read..total_read + to_copy]
                .copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);

            total_read += to_copy;
            self.buffer_pos += to_copy;
            self.position += to_copy as u64;
        }

        Ok(total_read)
    }

    fn size_hint(&self) -> Option<u64> {
        Some(self.total_length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::PagerConfig;
    use crate::vfs::MemoryFileSystem;

    #[test]
    fn test_overflow_chain_stream_single_page() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        // Create test data
        let test_data = b"Hello, World!";

        // Allocate overflow chain
        let page_ids = pager.allocate_overflow_chain(test_data).unwrap();
        assert_eq!(page_ids.len(), 1);

        // Create stream
        let mut stream = OverflowChainStream::new(&pager, page_ids[0], test_data.len() as u64);

        // Read data
        let mut buffer = vec![0u8; test_data.len()];
        let n = stream.read(&mut buffer).unwrap();
        assert_eq!(n, test_data.len());
        assert_eq!(&buffer[..n], test_data);

        // EOF
        let n = stream.read(&mut buffer).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_overflow_chain_stream_multiple_pages() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        // Create large test data (multiple pages)
        let test_data = vec![0xAB; 10000];

        // Allocate overflow chain
        let page_ids = pager.allocate_overflow_chain(&test_data).unwrap();
        assert!(page_ids.len() > 1);

        // Create stream
        let mut stream = OverflowChainStream::new(&pager, page_ids[0], test_data.len() as u64);

        // Read data in chunks
        let mut result = Vec::new();
        let mut buffer = vec![0u8; 1024];
        loop {
            let n = stream.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            result.extend_from_slice(&buffer[..n]);
        }

        assert_eq!(result, test_data);
    }

    #[test]
    fn test_overflow_chain_stream_size_hint() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        let test_data = b"Test data";
        let page_ids = pager.allocate_overflow_chain(test_data).unwrap();

        let stream = OverflowChainStream::new(&pager, page_ids[0], test_data.len() as u64);
        assert_eq!(stream.size_hint(), Some(test_data.len() as u64));
    }
}

// Made with Bob
