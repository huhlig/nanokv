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

//! Page pinning mechanism for preventing concurrent free/read corruption
//!
//! This module provides a reference counting system to ensure pages cannot be
//! freed while they are being read by other threads. This prevents data corruption
//! that can occur when a page is freed and reallocated while another thread is
//! still reading it.

use crate::pager::PageId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Pin table for tracking page reference counts
///
/// The pin table maintains reference counts for pages that are currently being
/// accessed. When a page is pinned (ref count > 0), it cannot be freed.
#[derive(Debug)]
pub struct PinTable {
    /// Map from page ID to reference count
    pins: Arc<RwLock<HashMap<PageId, usize>>>,
}

impl PinTable {
    /// Create a new pin table
    pub fn new() -> Self {
        Self {
            pins: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Pin a page (increment reference count)
    ///
    /// Returns the new reference count.
    pub fn pin(&self, page_id: PageId) -> usize {
        let mut pins = self.pins.write();
        let count = pins.entry(page_id).or_insert(0);
        *count += 1;
        *count
    }

    /// Unpin a page (decrement reference count)
    ///
    /// Returns the new reference count, or None if the page was not pinned.
    pub fn unpin(&self, page_id: PageId) -> Option<usize> {
        let mut pins = self.pins.write();
        if let Some(count) = pins.get_mut(&page_id) {
            if *count > 0 {
                *count -= 1;
                let new_count = *count;
                // Remove entry if count reaches 0 to avoid memory leak
                if new_count == 0 {
                    pins.remove(&page_id);
                }
                return Some(new_count);
            }
        }
        None
    }

    /// Check if a page is pinned (ref count > 0)
    pub fn is_pinned(&self, page_id: PageId) -> bool {
        let pins = self.pins.read();
        pins.get(&page_id).map_or(false, |&count| count > 0)
    }

    /// Get the reference count for a page
    pub fn ref_count(&self, page_id: PageId) -> usize {
        let pins = self.pins.read();
        pins.get(&page_id).copied().unwrap_or(0)
    }

    /// Get the total number of pinned pages
    pub fn pinned_count(&self) -> usize {
        let pins = self.pins.read();
        pins.len()
    }

    /// Clear all pins (for testing/debugging only)
    #[cfg(test)]
    pub fn clear(&self) {
        let mut pins = self.pins.write();
        pins.clear();
    }
}

impl Default for PinTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PinTable {
    fn clone(&self) -> Self {
        Self {
            pins: Arc::clone(&self.pins),
        }
    }
}

/// RAII guard for automatic page unpinning
///
/// When the guard is dropped, the page is automatically unpinned.
/// This ensures pages are always unpinned even if an error occurs.
pub struct PinGuard {
    page_id: PageId,
    pin_table: PinTable,
}

impl PinGuard {
    /// Create a new pin guard
    ///
    /// The page is pinned when the guard is created.
    pub fn new(page_id: PageId, pin_table: PinTable) -> Self {
        pin_table.pin(page_id);
        Self { page_id, pin_table }
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.pin_table.unpin(self.page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pin_unpin() {
        let table = PinTable::new();

        // Pin a page
        assert_eq!(table.pin(PageId::from(1)), 1);
        assert!(table.is_pinned(PageId::from(1)));
        assert_eq!(table.ref_count(PageId::from(1)), 1);

        // Pin again (increment)
        assert_eq!(table.pin(PageId::from(1)), 2);
        assert_eq!(table.ref_count(PageId::from(1)), 2);

        // Unpin once
        assert_eq!(table.unpin(PageId::from(1)), Some(1));
        assert!(table.is_pinned(PageId::from(1)));

        // Unpin again (should reach 0)
        assert_eq!(table.unpin(PageId::from(1)), Some(0));
        assert!(!table.is_pinned(PageId::from(1)));
        assert_eq!(table.ref_count(PageId::from(1)), 0);
    }

    #[test]
    fn test_multiple_pages() {
        let table = PinTable::new();

        table.pin(PageId::from(1));
        table.pin(PageId::from(2));
        table.pin(PageId::from(3));

        assert_eq!(table.pinned_count(), 3);
        assert!(table.is_pinned(PageId::from(1)));
        assert!(table.is_pinned(PageId::from(2)));
        assert!(table.is_pinned(PageId::from(3)));

        table.unpin(PageId::from(2));
        assert_eq!(table.pinned_count(), 2);
        assert!(!table.is_pinned(PageId::from(2)));
    }

    #[test]
    fn test_unpin_unpinned_page() {
        let table = PinTable::new();

        // Unpinning a page that was never pinned returns None
        assert_eq!(table.unpin(PageId::from(1)), None);
    }

    #[test]
    fn test_pin_guard() {
        let table = PinTable::new();

        {
            let _guard = PinGuard::new(PageId::from(1), table.clone());
            assert!(table.is_pinned(PageId::from(1)));
            assert_eq!(table.ref_count(PageId::from(1)), 1);
        }

        // Guard dropped, page should be unpinned
        assert!(!table.is_pinned(PageId::from(1)));
        assert_eq!(table.ref_count(PageId::from(1)), 0);
    }

    #[test]
    fn test_multiple_guards() {
        let table = PinTable::new();

        let _guard1 = PinGuard::new(PageId::from(1), table.clone());
        assert_eq!(table.ref_count(PageId::from(1)), 1);

        let _guard2 = PinGuard::new(PageId::from(1), table.clone());
        assert_eq!(table.ref_count(PageId::from(1)), 2);

        drop(_guard1);
        assert_eq!(table.ref_count(PageId::from(1)), 1);

        drop(_guard2);
        assert_eq!(table.ref_count(PageId::from(1)), 0);
    }

    #[test]
    fn test_concurrent_pinning() {
        use std::sync::Barrier;
        use std::thread;

        let table = PinTable::new();
        let thread_count = 10;
        let barrier = Arc::new(Barrier::new(thread_count));

        let mut handles = vec![];

        for _ in 0..thread_count {
            let table_clone = table.clone();
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                // Pin and unpin multiple times
                for _ in 0..100 {
                    table_clone.pin(PageId::from(1));
                    table_clone.unpin(PageId::from(1));
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All pins should be balanced
        assert_eq!(table.ref_count(PageId::from(1)), 0);
    }
}
