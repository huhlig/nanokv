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

//! LRU Page Cache
//!
//! Provides an LRU (Least Recently Used) cache for pages with:
//! - Configurable capacity
//! - Dirty page tracking for write-back policy
//! - Cache statistics (hit/miss rates, evictions)
//! - Thread-safe operations
//! - Integration with Pager layer

use crate::pager::{Page, PageId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Cache entry containing a page and metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    /// The cached page
    page: Page,
    /// Whether the page has been modified
    dirty: bool,
    /// Previous entry in LRU list (None if this is the head)
    prev: Option<PageId>,
    /// Next entry in LRU list (None if this is the tail)
    next: Option<PageId>,
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Total number of cache hits
    pub hits: u64,
    /// Total number of cache misses
    pub misses: u64,
    /// Total number of evictions
    pub evictions: u64,
    /// Total number of dirty page flushes
    pub flushes: u64,
    /// Current number of pages in cache
    pub current_size: usize,
    /// Current number of dirty pages
    pub dirty_pages: usize,
}

impl CacheStats {
    /// Calculate the cache hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Calculate the cache miss rate (0.0 to 1.0)
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        self.hits = 0;
        self.misses = 0;
        self.evictions = 0;
        self.flushes = 0;
    }
}

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of pages to cache
    pub capacity: usize,
    /// Whether to enable write-back (true) or write-through (false)
    pub write_back: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 1000, // Default to 1000 pages
            write_back: true, // Default to write-back for better performance
        }
    }
}

impl CacheConfig {
    /// Create a new cache configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the cache capacity
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Set write-back mode
    pub fn with_write_back(mut self, write_back: bool) -> Self {
        self.write_back = write_back;
        self
    }
}

/// LRU Page Cache
///
/// Thread-safe cache using LRU eviction policy with dirty page tracking.
/// The cache maintains a doubly-linked list for LRU ordering and a HashMap
/// for O(1) lookups.
pub struct PageCache {
    /// Cache configuration
    config: CacheConfig,
    /// Internal cache state
    inner: Arc<RwLock<CacheInner>>,
}

/// Internal cache state (protected by RwLock)
struct CacheInner {
    /// Map from page ID to cache entry
    entries: HashMap<PageId, CacheEntry>,
    /// Head of LRU list (most recently used)
    lru_head: Option<PageId>,
    /// Tail of LRU list (least recently used)
    lru_tail: Option<PageId>,
    /// Cache statistics
    stats: CacheStats,
    /// Maximum capacity
    capacity: usize,
    /// Write-back mode
    write_back: bool,
}

impl PageCache {
    /// Create a new page cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config: config.clone(),
            inner: Arc::new(RwLock::new(CacheInner {
                entries: HashMap::new(),
                lru_head: None,
                lru_tail: None,
                stats: CacheStats::default(),
                capacity: config.capacity,
                write_back: config.write_back,
            })),
        }
    }

    /// Get a page from the cache
    ///
    /// Returns Some(page) if found (cache hit), None if not found (cache miss).
    /// Updates LRU ordering on hit.
    pub fn get(&self, page_id: PageId) -> Option<Page> {
        let mut inner = self.inner.write();
        
        if inner.entries.contains_key(&page_id) {
            // Cache hit
            inner.stats.hits += 1;
            
            // Clone the page before moving to front
            let page = inner.entries.get(&page_id).unwrap().page.clone();
            
            // Move to front of LRU list
            inner.move_to_front(page_id);
            
            Some(page)
        } else {
            // Cache miss
            inner.stats.misses += 1;
            None
        }
    }

    /// Put a page into the cache
    ///
    /// If the cache is at capacity, evicts the least recently used page.
    /// Returns the evicted page if one was evicted and it was dirty.
    pub fn put(&self, page: Page, dirty: bool) -> Option<Page> {
        let mut inner = self.inner.write();
        let page_id = page.page_id();
        
        // Check if page already exists
        if inner.entries.contains_key(&page_id) {
            // Update existing entry
            if let Some(entry) = inner.entries.get_mut(&page_id) {
                entry.page = page;
                entry.dirty = entry.dirty || dirty; // Keep dirty flag if already dirty
            }
            inner.move_to_front(page_id);
            return None;
        }
        
        // Evict if at capacity
        let evicted = if inner.entries.len() >= inner.capacity {
            inner.evict_lru()
        } else {
            None
        };
        
        // Add new entry
        let entry = CacheEntry {
            page,
            dirty,
            prev: None,
            next: inner.lru_head,
        };
        
        inner.entries.insert(page_id, entry);
        
        // Update LRU list
        if let Some(old_head) = inner.lru_head {
            if let Some(head_entry) = inner.entries.get_mut(&old_head) {
                head_entry.prev = Some(page_id);
            }
        }
        
        inner.lru_head = Some(page_id);
        
        if inner.lru_tail.is_none() {
            inner.lru_tail = Some(page_id);
        }
        
        inner.stats.current_size = inner.entries.len();
        if dirty {
            inner.stats.dirty_pages += 1;
        }
        
        evicted
    }

    /// Check if a page is in the cache
    pub fn contains(&self, page_id: PageId) -> bool {
        let inner = self.inner.read();
        inner.entries.contains_key(&page_id)
    }

    /// Mark a page as dirty
    ///
    /// Returns true if the page was found and marked dirty, false otherwise.
    pub fn mark_dirty(&self, page_id: PageId) -> bool {
        let mut inner = self.inner.write();
        
        if let Some(entry) = inner.entries.get_mut(&page_id) {
            if !entry.dirty {
                entry.dirty = true;
                inner.stats.dirty_pages += 1;
            }
            true
        } else {
            false
        }
    }

    /// Mark a page as clean (after flushing to disk)
    ///
    /// Returns true if the page was found and marked clean, false otherwise.
    pub fn mark_clean(&self, page_id: PageId) -> bool {
        let mut inner = self.inner.write();
        
        if let Some(entry) = inner.entries.get_mut(&page_id) {
            if entry.dirty {
                entry.dirty = false;
                inner.stats.dirty_pages = inner.stats.dirty_pages.saturating_sub(1);
                inner.stats.flushes += 1;
            }
            true
        } else {
            false
        }
    }

    /// Check if a page is dirty
    pub fn is_dirty(&self, page_id: PageId) -> bool {
        let inner = self.inner.read();
        inner.entries.get(&page_id).map_or(false, |e| e.dirty)
    }

    /// Get all dirty pages
    ///
    /// Returns a vector of (page_id, page) tuples for all dirty pages.
    pub fn get_dirty_pages(&self) -> Vec<(PageId, Page)> {
        let inner = self.inner.read();
        inner
            .entries
            .iter()
            .filter(|(_, entry)| entry.dirty)
            .map(|(id, entry)| (*id, entry.page.clone()))
            .collect()
    }

    /// Remove a page from the cache
    ///
    /// Returns the page if it was in the cache and was dirty.
    pub fn remove(&self, page_id: PageId) -> Option<Page> {
        let mut inner = self.inner.write();
        inner.remove_entry(page_id)
    }

    /// Clear all pages from the cache
    ///
    /// Returns all dirty pages that were in the cache.
    pub fn clear(&self) -> Vec<(PageId, Page)> {
        let mut inner = self.inner.write();
        
        let dirty_pages: Vec<(PageId, Page)> = inner
            .entries
            .iter()
            .filter(|(_, entry)| entry.dirty)
            .map(|(id, entry)| (*id, entry.page.clone()))
            .collect();
        
        inner.entries.clear();
        inner.lru_head = None;
        inner.lru_tail = None;
        inner.stats.current_size = 0;
        inner.stats.dirty_pages = 0;
        
        dirty_pages
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read();
        inner.stats.clone()
    }

    /// Reset cache statistics
    pub fn reset_stats(&self) {
        let mut inner = self.inner.write();
        inner.stats.reset();
    }

    /// Get the current cache size
    pub fn size(&self) -> usize {
        let inner = self.inner.read();
        inner.entries.len()
    }

    /// Get the cache capacity
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Check if write-back mode is enabled
    pub fn is_write_back(&self) -> bool {
        self.config.write_back
    }
}

impl CacheInner {
    /// Move a page to the front of the LRU list (mark as most recently used)
    fn move_to_front(&mut self, page_id: PageId) {
        // If already at front, nothing to do
        if self.lru_head == Some(page_id) {
            return;
        }
        
        // Remove from current position
        if let Some(entry) = self.entries.get(&page_id) {
            let prev = entry.prev;
            let next = entry.next;
            
            // Update previous node's next pointer
            if let Some(prev_id) = prev {
                if let Some(prev_entry) = self.entries.get_mut(&prev_id) {
                    prev_entry.next = next;
                }
            }
            
            // Update next node's prev pointer
            if let Some(next_id) = next {
                if let Some(next_entry) = self.entries.get_mut(&next_id) {
                    next_entry.prev = prev;
                }
            }
            
            // Update tail if this was the tail
            if self.lru_tail == Some(page_id) {
                self.lru_tail = prev;
            }
        }
        
        // Add to front
        if let Some(entry) = self.entries.get_mut(&page_id) {
            entry.prev = None;
            entry.next = self.lru_head;
        }
        
        if let Some(old_head) = self.lru_head {
            if let Some(head_entry) = self.entries.get_mut(&old_head) {
                head_entry.prev = Some(page_id);
            }
        }
        
        self.lru_head = Some(page_id);
        
        if self.lru_tail.is_none() {
            self.lru_tail = Some(page_id);
        }
    }

    /// Evict the least recently used page
    ///
    /// Returns the evicted page if it was dirty.
    fn evict_lru(&mut self) -> Option<Page> {
        let tail_id = self.lru_tail?;
        self.stats.evictions += 1;
        self.remove_entry(tail_id)
    }

    /// Remove an entry from the cache
    ///
    /// Returns the page if it was dirty.
    fn remove_entry(&mut self, page_id: PageId) -> Option<Page> {
        let entry = self.entries.remove(&page_id)?;
        
        // Update LRU list
        if let Some(prev_id) = entry.prev {
            if let Some(prev_entry) = self.entries.get_mut(&prev_id) {
                prev_entry.next = entry.next;
            }
        } else {
            // This was the head
            self.lru_head = entry.next;
        }
        
        if let Some(next_id) = entry.next {
            if let Some(next_entry) = self.entries.get_mut(&next_id) {
                next_entry.prev = entry.prev;
            }
        } else {
            // This was the tail
            self.lru_tail = entry.prev;
        }
        
        self.stats.current_size = self.entries.len();
        
        if entry.dirty {
            self.stats.dirty_pages = self.stats.dirty_pages.saturating_sub(1);
            Some(entry.page)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::{PageType, PageSize};

    fn create_test_page(id: PageId) -> Page {
        let mut page = Page::new(id, PageType::BTreeLeaf, PageSize::Size4KB.data_size());
        page.data_mut().extend_from_slice(&format!("page {}", id).as_bytes());
        page
    }

    #[test]
    fn test_cache_basic_operations() {
        let config = CacheConfig::new().with_capacity(3);
        let cache = PageCache::new(config);

        // Put pages
        let page1 = create_test_page(1);
        let page2 = create_test_page(2);
        
        cache.put(page1.clone(), false);
        cache.put(page2.clone(), false);

        // Get pages
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_none());

        // Check contains
        assert!(cache.contains(1));
        assert!(cache.contains(2));
        assert!(!cache.contains(3));
    }

    #[test]
    fn test_cache_lru_eviction() {
        let config = CacheConfig::new().with_capacity(3);
        let cache = PageCache::new(config);

        // Fill cache
        cache.put(create_test_page(1), false);
        cache.put(create_test_page(2), false);
        cache.put(create_test_page(3), false);

        // Access page 1 (make it most recently used)
        cache.get(1);

        // Add page 4 (should evict page 2, the LRU)
        cache.put(create_test_page(4), false);

        assert!(cache.contains(1));
        assert!(!cache.contains(2)); // Evicted
        assert!(cache.contains(3));
        assert!(cache.contains(4));
    }

    #[test]
    fn test_cache_dirty_tracking() {
        let config = CacheConfig::new().with_capacity(5);
        let cache = PageCache::new(config);

        // Put clean page
        cache.put(create_test_page(1), false);
        assert!(!cache.is_dirty(1));

        // Mark as dirty
        cache.mark_dirty(1);
        assert!(cache.is_dirty(1));

        // Put dirty page
        cache.put(create_test_page(2), true);
        assert!(cache.is_dirty(2));

        // Mark as clean
        cache.mark_clean(2);
        assert!(!cache.is_dirty(2));

        // Get dirty pages
        cache.put(create_test_page(3), true);
        let dirty = cache.get_dirty_pages();
        assert_eq!(dirty.len(), 2); // Pages 1 and 3
    }

    #[test]
    fn test_cache_statistics() {
        let config = CacheConfig::new().with_capacity(3);
        let cache = PageCache::new(config);

        cache.put(create_test_page(1), false);
        cache.put(create_test_page(2), false);

        // Cache hits
        cache.get(1);
        cache.get(2);

        // Cache misses
        cache.get(3);
        cache.get(4);

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hit_rate(), 0.5);
        assert_eq!(stats.current_size, 2);
    }

    #[test]
    fn test_cache_eviction_dirty_page() {
        let config = CacheConfig::new().with_capacity(2);
        let cache = PageCache::new(config);

        // Fill cache with dirty pages
        cache.put(create_test_page(1), true);
        cache.put(create_test_page(2), true);

        // Add another page (should evict page 1)
        let evicted = cache.put(create_test_page(3), false);
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().page_id(), 1);

        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn test_cache_clear() {
        let config = CacheConfig::new().with_capacity(5);
        let cache = PageCache::new(config);

        cache.put(create_test_page(1), true);
        cache.put(create_test_page(2), false);
        cache.put(create_test_page(3), true);

        let dirty = cache.clear();
        assert_eq!(dirty.len(), 2); // Pages 1 and 3
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_cache_remove() {
        let config = CacheConfig::new().with_capacity(5);
        let cache = PageCache::new(config);

        cache.put(create_test_page(1), true);
        cache.put(create_test_page(2), false);

        // Remove dirty page
        let removed = cache.remove(1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().page_id(), 1);

        // Remove clean page
        let removed = cache.remove(2);
        assert!(removed.is_none());

        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_cache_update_existing() {
        let config = CacheConfig::new().with_capacity(3);
        let cache = PageCache::new(config);

        // Put initial page
        cache.put(create_test_page(1), false);
        assert!(!cache.is_dirty(1));

        // Update with dirty flag
        cache.put(create_test_page(1), true);
        assert!(cache.is_dirty(1));
        assert_eq!(cache.size(), 1); // Should not increase size
    }
}

// Made with Bob