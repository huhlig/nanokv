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

//! Pager implementation - Main page management logic

use crate::pager::{
    CompressionType, EncryptionType, FileHeader, FreeList, FreeListPage, Page, PageHeader,
    PageId, PageSize, PageType, PagerConfig, PagerError, PagerResult, Superblock,
};
use crate::vfs::{File, FileSystem};
use parking_lot::RwLock;
use std::sync::Arc;

/// Pager - Manages page-level storage operations
///
/// The Pager provides:
/// - Page allocation and deallocation
/// - Page reading and writing with checksums
/// - Free list management
/// - Optional compression and encryption
/// - Superblock management
pub struct Pager<FS: FileSystem> {
    /// VFS file handle
    file: Arc<RwLock<FS::File>>,
    /// Pager configuration
    config: PagerConfig,
    /// File header
    header: Arc<RwLock<FileHeader>>,
    /// Superblock
    superblock: Arc<RwLock<Superblock>>,
    /// Free list manager
    free_list: Arc<RwLock<FreeList>>,
}

impl<FS: FileSystem> Pager<FS> {
    /// Create a new database file with the given configuration
    pub fn create(fs: &FS, path: &str, config: PagerConfig) -> PagerResult<Self> {
        // Validate configuration
        config.validate().map_err(PagerError::ConfigError)?;

        // Create the file
        let mut file = fs.create_file(path)?;

        // Create file header
        let header = FileHeader::new(config.page_size, config.compression, config.encryption);

        // Write file header to page 0
        let header_bytes = header.to_bytes();
        let mut page0_data = vec![0u8; config.page_size.to_u32() as usize];
        page0_data[0..FileHeader::SIZE].copy_from_slice(&header_bytes);
        file.write_to_offset(0, &page0_data)?;

        // Create superblock
        let superblock = Superblock::new();

        // Write superblock to page 1
        let mut superblock_page = Page::new(1, PageType::Superblock, config.page_size.data_size());
        superblock_page.data_mut().extend_from_slice(&superblock.to_bytes());
        let page1_data = superblock_page.to_bytes(config.page_size.to_u32() as usize);
        file.write_to_offset(config.page_size.to_u32() as u64, &page1_data)?;

        // Sync to disk
        file.sync_all()?;

        // Create free list
        let free_list = FreeList::new();

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            header: Arc::new(RwLock::new(header)),
            superblock: Arc::new(RwLock::new(superblock)),
            free_list: Arc::new(RwLock::new(free_list)),
        })
    }

    /// Open an existing database file
    pub fn open(fs: &FS, path: &str) -> PagerResult<Self> {
        let mut file = fs.open_file(path)?;

        // Read and parse file header from page 0
        let page_size_guess = PageSize::Size4KB.to_u32() as usize;
        let mut header_data = vec![0u8; page_size_guess];
        file.read_at_offset(0, &mut header_data)?;
        let header = FileHeader::from_bytes(&header_data)?;

        // Now we know the actual page size
        let page_size = header.page_size.to_u32() as usize;

        // Read superblock from page 1
        let mut superblock_data = vec![0u8; page_size];
        file.read_at_offset(page_size as u64, &mut superblock_data)?;
        let superblock_page = Page::from_bytes(&superblock_data, true)?;
        let superblock = Superblock::from_bytes(superblock_page.data())?;

        // Create configuration from header
        let config = PagerConfig {
            page_size: header.page_size,
            compression: header.compression,
            encryption: header.encryption,
            encryption_key: None, // Will need to be provided separately for encrypted databases
            enable_checksums: true,
        };

        // Initialize free list from superblock
        let free_list = FreeList::from_state(
            superblock.first_free_list_page,
            superblock.last_free_list_page,
            superblock.free_pages,
        );

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            header: Arc::new(RwLock::new(header)),
            superblock: Arc::new(RwLock::new(superblock)),
            free_list: Arc::new(RwLock::new(free_list)),
        })
    }

    /// Get the page size
    pub fn page_size(&self) -> PageSize {
        self.config.page_size
    }

    /// Get the total number of pages
    pub fn total_pages(&self) -> u64 {
        self.superblock.read().total_pages
    }

    /// Get the number of free pages
    pub fn free_pages(&self) -> u64 {
        self.free_list.read().total_free()
    }

    /// Allocate a new page
    ///
    /// This will either:
    /// 1. Reuse a page from the free list, or
    /// 2. Grow the database by allocating a new page
    pub fn allocate_page(&self, page_type: PageType) -> PagerResult<PageId> {
        let mut free_list = self.free_list.write();
        let mut superblock = self.superblock.write();

        // Try to get a page from the free list first
        if !free_list.is_empty() {
            // Read the first free list page
            let first_page_id = free_list.first_page();
            let free_list_page = self.read_free_list_page(first_page_id)?;

            if let Some(page_id) = free_list_page.free_pages.last() {
                let allocated_page_id = *page_id;

                // Remove the page from the free list
                let mut updated_free_list_page = free_list_page;
                updated_free_list_page.pop_page();

                // If the free list page is now empty, move to the next one
                if updated_free_list_page.is_empty() {
                    if updated_free_list_page.next_page != 0 {
                        free_list.set_first_page(updated_free_list_page.next_page);
                    } else {
                        // No more free list pages
                        free_list.set_first_page(0);
                        free_list.set_last_page(0);
                    }
                    // TODO: Free the now-empty free list page itself
                } else {
                    // Write back the updated free list page
                    self.write_free_list_page(first_page_id, &updated_free_list_page)?;
                }

                free_list.decrement_free();
                superblock.mark_page_allocated();

                // Write updated superblock
                self.write_superblock(&superblock)?;

                return Ok(allocated_page_id);
            }
        }

        // No free pages available, allocate a new one
        let page_id = superblock.allocate_new_page();

        // Update header
        let mut header = self.header.write();
        header.total_pages = superblock.total_pages;
        header.update_modified_timestamp();
        self.write_header(&header)?;

        // Write updated superblock
        self.write_superblock(&superblock)?;

        Ok(page_id)
    }

    /// Free a page (add it to the free list)
    pub fn free_page(&self, page_id: PageId) -> PagerResult<()> {
        if page_id == 0 || page_id == 1 {
            return Err(PagerError::InvalidPageId(page_id));
        }

        let mut free_list = self.free_list.write();
        let mut superblock = self.superblock.write();

        // Get or create the last free list page
        let last_page_id = free_list.last_page();

        if last_page_id == 0 {
            // No free list pages exist, create the first one
            let new_free_list_page_id = superblock.allocate_new_page();
            let mut new_free_list_page = FreeListPage::new();
            new_free_list_page.add_page(page_id, self.config.page_size.data_size())?;

            self.write_free_list_page(new_free_list_page_id, &new_free_list_page)?;

            free_list.set_first_page(new_free_list_page_id);
            free_list.set_last_page(new_free_list_page_id);
        } else {
            // Add to existing free list page
            let mut last_free_list_page = self.read_free_list_page(last_page_id)?;

            if last_free_list_page.is_full(self.config.page_size.data_size()) {
                // Current page is full, create a new one
                let new_free_list_page_id = superblock.allocate_new_page();
                let mut new_free_list_page = FreeListPage::new();
                new_free_list_page.add_page(page_id, self.config.page_size.data_size())?;

                // Link the pages
                last_free_list_page.next_page = new_free_list_page_id;
                self.write_free_list_page(last_page_id, &last_free_list_page)?;
                self.write_free_list_page(new_free_list_page_id, &new_free_list_page)?;

                free_list.set_last_page(new_free_list_page_id);
            } else {
                // Add to current page
                last_free_list_page.add_page(page_id, self.config.page_size.data_size())?;
                self.write_free_list_page(last_page_id, &last_free_list_page)?;
            }
        }

        free_list.increment_free();
        superblock.mark_page_freed();

        // Update header
        let mut header = self.header.write();
        header.free_pages = free_list.total_free();
        header.update_modified_timestamp();
        self.write_header(&header)?;

        // Write updated superblock
        self.write_superblock(&superblock)?;

        Ok(())
    }

    /// Read a page from disk
    pub fn read_page(&self, page_id: PageId) -> PagerResult<Page> {
        if page_id >= self.total_pages() {
            return Err(PagerError::PageNotFound(page_id));
        }

        let page_size = self.config.page_size.to_u32() as usize;
        let offset = page_id * page_size as u64;

        let mut buffer = vec![0u8; page_size];
        let mut file = self.file.write();
        file.read_at_offset(offset, &mut buffer)?;

        Page::from_bytes(&buffer, self.config.enable_checksums)
    }

    /// Write a page to disk
    pub fn write_page(&self, page: &Page) -> PagerResult<()> {
        let page_size = self.config.page_size.to_u32() as usize;
        let offset = page.page_id() * page_size as u64;

        let buffer = page.to_bytes(page_size);
        let mut file = self.file.write();
        file.write_to_offset(offset, &buffer)?;

        Ok(())
    }

    /// Sync all changes to disk
    pub fn sync(&self) -> PagerResult<()> {
        let mut file = self.file.write();
        file.sync_all()?;
        Ok(())
    }

    /// Read a free list page
    fn read_free_list_page(&self, page_id: PageId) -> PagerResult<FreeListPage> {
        let page = self.read_page(page_id)?;
        FreeListPage::from_bytes(page.data())
    }

    /// Write a free list page
    fn write_free_list_page(&self, page_id: PageId, free_list_page: &FreeListPage) -> PagerResult<()> {
        let mut page = Page::new(page_id, PageType::FreeList, self.config.page_size.data_size());
        page.data_mut().extend_from_slice(&free_list_page.to_bytes());
        self.write_page(&page)
    }

    /// Write the file header
    fn write_header(&self, header: &FileHeader) -> PagerResult<()> {
        let header_bytes = header.to_bytes();
        let mut page0_data = vec![0u8; self.config.page_size.to_u32() as usize];
        page0_data[0..FileHeader::SIZE].copy_from_slice(&header_bytes);

        let mut file = self.file.write();
        file.write_to_offset(0, &page0_data)?;
        Ok(())
    }

    /// Write the superblock
    fn write_superblock(&self, superblock: &Superblock) -> PagerResult<()> {
        let mut page = Page::new(1, PageType::Superblock, self.config.page_size.data_size());
        page.data_mut().extend_from_slice(&superblock.to_bytes());
        self.write_page(&page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemoryFileSystem;

    #[test]
    fn test_pager_create() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();

        let pager = Pager::create(&fs, "test.db", config).unwrap();
        assert_eq!(pager.total_pages(), 2); // Header + Superblock
        assert_eq!(pager.free_pages(), 0);
    }

    #[test]
    fn test_pager_open() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();

        // Create database
        {
            let _pager = Pager::create(&fs, "test.db", config.clone()).unwrap();
        }

        // Open database
        let pager = Pager::open(&fs, "test.db").unwrap();
        assert_eq!(pager.total_pages(), 2);
    }

    #[test]
    fn test_page_allocation() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        // Allocate a new page
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pager.total_pages(), 3);
    }

    #[test]
    fn test_page_read_write() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        // Allocate and write a page
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        let mut page = Page::new(page_id, PageType::BTreeLeaf, pager.page_size().data_size());
        page.data_mut().extend_from_slice(b"test data");

        pager.write_page(&page).unwrap();

        // Read it back
        let read_page = pager.read_page(page_id).unwrap();
        assert_eq!(read_page.page_id(), page_id);
        assert_eq!(read_page.page_type(), PageType::BTreeLeaf);
        assert_eq!(&read_page.data()[0..9], b"test data");
    }

    #[test]
    fn test_page_free_and_reuse() {
        let fs = MemoryFileSystem::new();
        let config = PagerConfig::default();
        let pager = Pager::create(&fs, "test.db", config).unwrap();

        // Allocate a page
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(page_id, 2);
        assert_eq!(pager.free_pages(), 0);

        // Free the page
        pager.free_page(page_id).unwrap();
        assert_eq!(pager.free_pages(), 1);

        // Allocate again - should reuse the freed page
        let reused_page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(reused_page_id, page_id);
        assert_eq!(pager.free_pages(), 0);
    }
}

// Made with Bob
