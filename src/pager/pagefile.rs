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
    CacheConfig, FileHeader, FreeList, FreeListPage, Page, PageCache,
    PageId, PageSize, PageType, PagerConfig, PagerError, PagerResult, PinTable, Superblock,
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
/// - LRU page cache for performance
/// - Page pinning to prevent concurrent free/read corruption
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
    /// Page cache (optional)
    cache: Option<PageCache>,
    /// Pin table for reference counting
    pin_table: PinTable,
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
        let page1_data = superblock_page.to_bytes(
            config.page_size.to_u32() as usize,
            config.encryption_key.as_ref()
        )?;
        file.write_to_offset(config.page_size.to_u32() as u64, &page1_data)?;

        // Sync to disk
        file.sync_all()?;

        // Create free list
        let free_list = FreeList::new();

        // Create cache if enabled
        let cache = if config.cache_capacity > 0 {
            let cache_config = CacheConfig::new()
                .with_capacity(config.cache_capacity)
                .with_write_back(config.cache_write_back);
            Some(PageCache::new(cache_config))
        } else {
            None
        };

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            header: Arc::new(RwLock::new(header)),
            superblock: Arc::new(RwLock::new(superblock)),
            free_list: Arc::new(RwLock::new(free_list)),
            cache,
            pin_table: PinTable::new(),
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
        let superblock_page = Page::from_bytes(&superblock_data, true, None)?;
        let superblock = Superblock::from_bytes(superblock_page.data())?;

        // Create configuration from header
        let config = PagerConfig {
            page_size: header.page_size,
            compression: header.compression,
            encryption: header.encryption,
            encryption_key: None, // Will need to be provided separately for encrypted databases
            enable_checksums: true,
            cache_capacity: 1000, // Default cache capacity
            cache_write_back: true, // Default to write-back
        };

        // Initialize free list from persisted free-list pages
        let mut free_list = FreeList::from_state(
            superblock.first_free_list_page,
            superblock.last_free_list_page,
            superblock.free_pages,
        );

        if superblock.free_pages > 0 && superblock.first_free_list_page != 0 {
            let mut all_free_pages = Vec::new();
            let mut current_page_id = superblock.first_free_list_page;

            while current_page_id != 0 {
                let offset = current_page_id * page_size as u64;
                let mut free_list_page_data = vec![0u8; page_size];
                file.read_at_offset(offset, &mut free_list_page_data)?;

                let page = Page::from_bytes(&free_list_page_data, true, None)?;
                let free_list_page = FreeListPage::from_bytes(page.data())?;

                all_free_pages.extend(free_list_page.free_pages.iter().copied());
                current_page_id = free_list_page.next_page;
            }

            free_list.set_free_pages(all_free_pages);
        }

        // Create cache if enabled
        let cache = if config.cache_capacity > 0 {
            let cache_config = CacheConfig::new()
                .with_capacity(config.cache_capacity)
                .with_write_back(config.cache_write_back);
            Some(PageCache::new(cache_config))
        } else {
            None
        };

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            config,
            header: Arc::new(RwLock::new(header)),
            superblock: Arc::new(RwLock::new(superblock)),
            free_list: Arc::new(RwLock::new(free_list)),
            cache,
            pin_table: PinTable::new(),
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
        // CRITICAL: Hold both locks atomically to prevent race conditions
        // where multiple threads could get the same page ID
        let page_id = {
            let mut free_list = self.free_list.write();
            let mut superblock = self.superblock.write();
            
            if let Some(page_id) = free_list.pop_page() {
                superblock.mark_page_allocated();
                page_id
            } else {
                superblock.allocate_new_page()
            }
        };

        let mut page = Page::new(page_id, page_type, self.config.page_size.data_size());
        page.header.compression = self.config.compression;
        page.header.encryption = self.config.encryption;

        let page_size = self.config.page_size.to_u32() as usize;
        let page_bytes = page.to_bytes(page_size, self.config.encryption_key.as_ref())?;

        let (header_data, superblock_data) = {
            let free_pages = self.free_list.read().total_free();

            let superblock_data = {
                let superblock = self.superblock.read();
                superblock.clone()
            };

            let header_data = {
                let mut header = self.header.write();
                header.total_pages = superblock_data.total_pages;
                header.free_pages = free_pages;
                header.first_free_list_page_id = 0;
                header.update_modified_timestamp();
                header.clone()
            };

            (header_data, superblock_data)
        };

        {
            let mut file = self.file.write();
            file.write_to_offset(page_id * page_size as u64, &page_bytes)?;
            let header_bytes = header_data.to_bytes();
            let mut page0_data = vec![0u8; page_size];
            page0_data[0..FileHeader::SIZE].copy_from_slice(&header_bytes);
            file.write_to_offset(0, &page0_data)?;

            let mut superblock_page = Page::new(1, PageType::Superblock, self.config.page_size.data_size());
            superblock_page.header.compression = self.config.compression;
            superblock_page.header.encryption = self.config.encryption;
            superblock_page.data_mut().extend_from_slice(&superblock_data.to_bytes());
            let superblock_bytes = superblock_page.to_bytes(page_size, self.config.encryption_key.as_ref())?;
            file.write_to_offset(page_size as u64, &superblock_bytes)?;
        }

        Ok(page_id)
    }

    /// Free a page (add it to the free list)
    pub fn free_page(&self, page_id: PageId) -> PagerResult<()> {
        if page_id == 0 || page_id == 1 {
            return Err(PagerError::InvalidPageId(page_id));
        }

        // CRITICAL: Check if page is pinned before freeing
        // This prevents freeing pages that are currently being read
        if self.pin_table.is_pinned(page_id) {
            return Err(PagerError::PagePinned(page_id));
        }

        let page_size = self.config.page_size.to_u32() as usize;
        let offset = page_id * page_size as u64;

        {
            let mut file = self.file.write();
            let mut buffer = vec![0u8; page_size];
            file.read_at_offset(offset, &mut buffer)?;
            let page = Page::from_bytes(
                &buffer,
                self.config.enable_checksums,
                self.config.encryption_key.as_ref(),
            )?;
            if page.page_type() == PageType::Free || page.page_type() == PageType::FreeList {
                return Err(PagerError::PageAlreadyFree(page_id));
            }

            let mut free_page = Page::new(page_id, PageType::Free, self.config.page_size.data_size());
            free_page.header.compression = self.config.compression;
            free_page.header.encryption = self.config.encryption;
            let free_page_bytes = free_page.to_bytes(page_size, self.config.encryption_key.as_ref())?;
            file.write_to_offset(offset, &free_page_bytes)?;
        }

        // CRITICAL: Hold both locks atomically to prevent race conditions
        {
            let mut free_list = self.free_list.write();
            let mut superblock = self.superblock.write();
            free_list.push_page(page_id);
            superblock.mark_page_freed();
        }

        let (header_data, superblock_data) = {
            let free_pages = self.free_list.read().total_free();

            let superblock_data = {
                let superblock = self.superblock.read();
                superblock.clone()
            };

            let header_data = {
                let mut header = self.header.write();
                header.total_pages = superblock_data.total_pages;
                header.free_pages = free_pages;
                header.first_free_list_page_id = 0;
                header.update_modified_timestamp();
                header.clone()
            };

            (header_data, superblock_data)
        };

        {
            let mut file = self.file.write();
            let header_bytes = header_data.to_bytes();
            let mut page0_data = vec![0u8; page_size];
            page0_data[0..FileHeader::SIZE].copy_from_slice(&header_bytes);
            file.write_to_offset(0, &page0_data)?;

            let mut superblock_page = Page::new(1, PageType::Superblock, self.config.page_size.data_size());
            superblock_page.header.compression = self.config.compression;
            superblock_page.header.encryption = self.config.encryption;
            superblock_page.data_mut().extend_from_slice(&superblock_data.to_bytes());
            let superblock_bytes = superblock_page.to_bytes(page_size, self.config.encryption_key.as_ref())?;
            file.write_to_offset(page_size as u64, &superblock_bytes)?;
        }

        Ok(())
    }

    /// Read a page from disk (with caching)
    pub fn read_page(&self, page_id: PageId) -> PagerResult<Page> {
        if page_id >= self.total_pages() {
            return Err(PagerError::PageNotFound(page_id));
        }

        // Try cache first
        if let Some(cache) = &self.cache {
            if let Some(page) = cache.get(page_id) {
                return Ok(page);
            }
        }

        // CRITICAL: Pin the page before reading to prevent concurrent free
        // This ensures the page cannot be freed and reallocated while we're reading it
        self.pin_table.pin(page_id);

        // Cache miss - read from disk
        let page_size = self.config.page_size.to_u32() as usize;
        let offset = page_id * page_size as u64;

        let result = (|| {
            let mut buffer = vec![0u8; page_size];
            let mut file = self.file.write();
            file.read_at_offset(offset, &mut buffer)?;

            let page = Page::from_bytes(&buffer, self.config.enable_checksums, self.config.encryption_key.as_ref())?;

            // Add to cache
            if let Some(cache) = &self.cache {
                // If evicted page is dirty, write it to disk
                if let Some(evicted_page) = cache.put(page.clone(), false) {
                    self.write_page_to_disk(&evicted_page)?;
                }
            }

            Ok(page)
        })();

        // CRITICAL: Always unpin the page, even if an error occurred
        self.pin_table.unpin(page_id);

        result
    }

    /// Write a page to disk (with caching)
    pub fn write_page(&self, page: &Page) -> PagerResult<()> {
        if let Some(cache) = &self.cache {
            // If write-back mode, just update cache
            if self.config.cache_write_back {
                // If evicted page is dirty, write it to disk
                if let Some(evicted_page) = cache.put(page.clone(), true) {
                    self.write_page_to_disk(&evicted_page)?;
                }
                return Ok(());
            } else {
                // Write-through mode: write to disk and update cache
                self.write_page_to_disk(page)?;
                cache.put(page.clone(), false);
                return Ok(());
            }
        }

        // No cache - write directly to disk
        self.write_page_to_disk(page)
    }

    /// Write a page directly to disk (bypassing cache)
    fn write_page_to_disk(&self, page: &Page) -> PagerResult<()> {
        let page_size = self.config.page_size.to_u32() as usize;
        let offset = page.page_id() * page_size as u64;

        let buffer = page.to_bytes(page_size, self.config.encryption_key.as_ref())?;
        let mut file = self.file.write();
        file.write_to_offset(offset, &buffer)?;

        Ok(())
    }

    /// Flush all dirty pages from cache to disk
    pub fn flush_cache(&self) -> PagerResult<()> {
        if let Some(cache) = &self.cache {
            let dirty_pages = cache.get_dirty_pages();
            for (page_id, page) in dirty_pages {
                self.write_page_to_disk(&page)?;
                cache.mark_clean(page_id);
            }
        }
        Ok(())
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> Option<crate::pager::CacheStats> {
        self.cache.as_ref().map(|c| c.stats())
    }

    /// Clear the cache
    pub fn clear_cache(&self) -> PagerResult<()> {
        if let Some(cache) = &self.cache {
            let dirty_pages = cache.clear();
            // Write any dirty pages to disk
            for (_, page) in dirty_pages {
                self.write_page_to_disk(&page)?;
            }
        }
        Ok(())
    }

    /// Sync all changes to disk
    pub fn sync(&self) -> PagerResult<()> {
        // Flush cache first
        self.flush_cache()?;
        
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
        page.header.compression = self.config.compression;
        page.header.encryption = self.config.encryption;
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
        page.header.compression = self.config.compression;
        page.header.encryption = self.config.encryption;
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
