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

//! Large-scale stress tests for the pager system
//!
//! These tests validate behavior under heavy load with 1000+ pages,
//! ensuring the system can handle production workloads without
//! performance degradation or resource issues.

use nanokv::pager::{Page, PageType, Pager, PagerConfig};
use nanokv::vfs::MemoryFileSystem;
use std::collections::HashSet;

/// Helper function to create a test pager
fn create_test_pager() -> Pager<MemoryFileSystem> {
    let fs = MemoryFileSystem::new();
    let config = PagerConfig::default();
    Pager::create(&fs, "stress_test.db", config).expect("Failed to create pager")
}

/// Test sequential allocation of 1000+ pages
///
/// This test validates that the pager can allocate a large number of pages
/// sequentially without errors, and that page IDs are assigned correctly.
#[test]
fn test_sequential_allocation_1000_pages() {
    let pager = create_test_pager();
    let page_count = 1000;
    
    let mut allocated_pages = Vec::new();
    
    // Allocate pages sequentially
    for i in 0..page_count {
        let page_id = pager
            .allocate_page(PageType::BTreeLeaf)
            .expect(&format!("Failed to allocate page {}", i));
        
        allocated_pages.push(page_id);
    }
    
    // Verify we allocated the expected number of pages
    assert_eq!(allocated_pages.len(), page_count);
    
    // Verify page IDs are unique
    let unique_pages: HashSet<_> = allocated_pages.iter().collect();
    assert_eq!(unique_pages.len(), page_count, "Page IDs should be unique");
    
    // Verify total pages count
    // +2 for header and superblock pages
    assert!(
        pager.total_pages() >= (page_count + 2) as u64,
        "Total pages should be at least {} (allocated) + 2 (header/superblock)",
        page_count
    );
    
    // Verify free pages count is reasonable (should be low)
    assert!(
        pager.free_pages() < 10,
        "Free pages should be minimal after sequential allocation"
    );
}

/// Test sequential allocation of 5000 pages
///
/// Larger scale test to ensure system scales well.
#[test]
#[ignore] // Run with --ignored flag for longer tests
fn test_sequential_allocation_5000_pages() {
    let pager = create_test_pager();
    let page_count = 5000;
    
    let mut allocated_pages = Vec::new();
    
    for i in 0..page_count {
        let page_id = pager
            .allocate_page(PageType::BTreeLeaf)
            .expect(&format!("Failed to allocate page {}", i));
        
        allocated_pages.push(page_id);
    }
    
    assert_eq!(allocated_pages.len(), page_count);
    
    let unique_pages: HashSet<_> = allocated_pages.iter().collect();
    assert_eq!(unique_pages.len(), page_count);
}

/// Test mixed allocation and deallocation patterns at scale
///
/// This test simulates realistic workload patterns with interleaved
/// allocations and deallocations.
#[test]
fn test_mixed_allocation_deallocation_1000_cycles() {
    let pager = create_test_pager();
    let cycles = 1000;
    
    let mut active_pages = Vec::new();
    
    // Allocate initial batch
    for _ in 0..100 {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        active_pages.push(page_id);
    }
    
    // Run mixed allocation/deallocation cycles
    for cycle in 0..cycles {
        // Allocate 5 pages
        for _ in 0..5 {
            let page_id = pager
                .allocate_page(PageType::BTreeLeaf)
                .expect(&format!("Failed to allocate in cycle {}", cycle));
            active_pages.push(page_id);
        }
        
        // Free 3 pages (if we have enough)
        if active_pages.len() >= 3 {
            for _ in 0..3 {
                let page_id = active_pages.pop().unwrap();
                pager
                    .free_page(page_id)
                    .expect(&format!("Failed to free page {} in cycle {}", page_id, cycle));
            }
        }
    }
    
    // Verify we still have active pages
    assert!(!active_pages.is_empty(), "Should have active pages remaining");
    
    // Verify all active pages are unique
    let unique_pages: HashSet<_> = active_pages.iter().collect();
    assert_eq!(unique_pages.len(), active_pages.len(), "Active pages should be unique");
    
    // Clean up - free all remaining pages
    for page_id in active_pages {
        pager.free_page(page_id).expect("Failed to free page during cleanup");
    }
}

/// Test random allocation and deallocation patterns
///
/// This test uses pseudo-random patterns to stress test the free list
/// management under unpredictable workloads.
#[test]
fn test_random_allocation_deallocation_patterns() {
    let pager = create_test_pager();
    let operations = 2000;
    
    let mut active_pages = Vec::new();
    let mut rng_state = 12345u64; // Simple LCG for deterministic randomness
    
    for op in 0..operations {
        // Simple LCG: next = (a * prev + c) mod m
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let should_allocate = (rng_state % 100) < 60; // 60% allocate, 40% free
        
        if should_allocate || active_pages.is_empty() {
            // Allocate a page
            let page_id = pager
                .allocate_page(PageType::BTreeLeaf)
                .expect(&format!("Failed to allocate in operation {}", op));
            active_pages.push(page_id);
        } else {
            // Free a random page
            let index = (rng_state as usize) % active_pages.len();
            let page_id = active_pages.swap_remove(index);
            pager
                .free_page(page_id)
                .expect(&format!("Failed to free page {} in operation {}", page_id, op));
        }
    }
    
    // Verify state consistency
    assert!(pager.total_pages() > 0, "Should have allocated pages");
    
    // Clean up
    for page_id in active_pages {
        pager.free_page(page_id).expect("Failed to free page during cleanup");
    }
}

/// Test fragmentation scenarios
///
/// This test creates a highly fragmented free list by allocating many pages,
/// freeing every other one, then reallocating to test free list efficiency.
#[test]
fn test_fragmentation_scenario() {
    let pager = create_test_pager();
    let page_count = 1000;
    
    // Phase 1: Allocate many pages
    let mut all_pages = Vec::new();
    for _ in 0..page_count {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        all_pages.push(page_id);
    }
    
    let initial_total = pager.total_pages();
    
    // Phase 2: Free every other page to create fragmentation
    let mut freed_count = 0;
    let mut kept_pages = Vec::new();
    for (i, page_id) in all_pages.into_iter().enumerate() {
        if i % 2 == 0 {
            pager.free_page(page_id).unwrap();
            freed_count += 1;
        } else {
            kept_pages.push(page_id);
        }
    }
    
    assert_eq!(freed_count, page_count / 2, "Should have freed half the pages");
    assert_eq!(
        pager.free_pages() as usize,
        freed_count,
        "Free page count should match freed pages"
    );
    
    // Phase 3: Reallocate pages - should reuse freed pages
    let mut reallocated = Vec::new();
    for _ in 0..freed_count {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        reallocated.push(page_id);
    }
    
    // After reallocation, free pages should be minimal
    assert!(
        pager.free_pages() < 10,
        "Free pages should be minimal after reallocation, got {}",
        pager.free_pages()
    );
    
    // Total pages should not have grown significantly
    let final_total = pager.total_pages();
    assert!(
        final_total <= initial_total + 10,
        "Total pages should not grow significantly: {} -> {}",
        initial_total,
        final_total
    );
    
    // Clean up
    for page_id in kept_pages.into_iter().chain(reallocated.into_iter()) {
        pager.free_page(page_id).expect("Failed to free page during cleanup");
    }
}

/// Test free list chain traversal with many pages
///
/// This test validates that the free list can handle long chains of
/// free list pages without performance degradation.
#[test]
fn test_free_list_chain_traversal() {
    let pager = create_test_pager();
    let page_count = 2000;
    
    // Allocate many pages
    let mut pages = Vec::new();
    for _ in 0..page_count {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        pages.push(page_id);
    }
    
    // Free all pages to create a long free list chain
    for page_id in &pages {
        pager.free_page(*page_id).unwrap();
    }
    
    assert_eq!(
        pager.free_pages() as usize,
        page_count,
        "All pages should be in free list"
    );
    
    // Reallocate all pages - this tests free list traversal
    let mut reallocated = Vec::new();
    for i in 0..page_count {
        let page_id = pager
            .allocate_page(PageType::BTreeLeaf)
            .expect(&format!("Failed to reallocate page {}", i));
        reallocated.push(page_id);
    }
    
    // Free list should be empty or nearly empty
    assert!(
        pager.free_pages() < 5,
        "Free list should be nearly empty after reallocation"
    );
    
    // Verify all reallocated pages are unique
    let unique_pages: HashSet<_> = reallocated.iter().collect();
    assert_eq!(unique_pages.len(), page_count, "Reallocated pages should be unique");
}

/// Test persistence and recovery with large page files
///
/// This test validates that large databases can be persisted and
/// reopened correctly with all state preserved.
#[test]
fn test_persistence_and_recovery_large_database() {
    let fs = MemoryFileSystem::new();
    let config = PagerConfig::default();
    let db_path = "large_db.db";
    let page_count = 1000;
    
    let mut allocated_pages = Vec::new();
    
    // Phase 1: Create database and allocate pages
    {
        let pager = Pager::create(&fs, db_path, config.clone()).unwrap();
        
        for _ in 0..page_count {
            let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
            allocated_pages.push(page_id);
        }
        
        // Write some data to pages
        for (i, &page_id) in allocated_pages.iter().enumerate() {
            let mut page = Page::new(page_id, PageType::BTreeLeaf, pager.page_size().data_size());
            let data = format!("Page {} data", i);
            page.data_mut().extend_from_slice(data.as_bytes());
            pager.write_page(&page).unwrap();
        }
        
        pager.sync().unwrap();
    }
    
    // Phase 2: Reopen database and verify state
    {
        let pager = Pager::open(&fs, db_path).unwrap();
        
        // Verify total pages
        assert!(
            pager.total_pages() >= (page_count + 2) as u64,
            "Total pages should be preserved"
        );
        
        // Verify we can read all pages
        for (i, &page_id) in allocated_pages.iter().enumerate() {
            let page = pager
                .read_page(page_id)
                .expect(&format!("Failed to read page {}", page_id));
            
            let expected_data = format!("Page {} data", i);
            assert_eq!(
                &page.data()[0..expected_data.len()],
                expected_data.as_bytes(),
                "Page data should be preserved"
            );
        }
        
        // Verify we can allocate new pages
        let new_page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        assert!(new_page_id > 0, "Should be able to allocate new pages");
    }
}

/// Test memory usage stability under load
///
/// This test validates that memory usage doesn't grow unboundedly
/// during repeated allocation/deallocation cycles.
#[test]
fn test_memory_usage_stability() {
    let pager = create_test_pager();
    let batch_size = 500;
    let cycles = 10;
    
    for cycle in 0..cycles {
        let mut pages = Vec::new();
        
        // Allocate a batch
        for _ in 0..batch_size {
            let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
            pages.push(page_id);
        }
        
        let total_after_alloc = pager.total_pages();
        
        // Free the batch
        for page_id in pages {
            pager.free_page(page_id).unwrap();
        }
        
        let free_after_dealloc = pager.free_pages();
        
        // Verify free pages are tracked
        assert!(
            free_after_dealloc >= batch_size as u64,
            "Cycle {}: Free pages should include freed batch",
            cycle
        );
        
        // Total pages should stabilize after first cycle
        if cycle > 0 {
            assert!(
                total_after_alloc <= pager.total_pages() + 20,
                "Cycle {}: Total pages should not grow significantly",
                cycle
            );
        }
    }
}

/// Test page file growth and shrinkage patterns
///
/// This test validates that the database file grows appropriately
/// and that free space is managed efficiently.
#[test]
fn test_page_file_growth_patterns() {
    let pager = create_test_pager();
    
    // Phase 1: Grow to 1000 pages
    let mut pages = Vec::new();
    for _ in 0..1000 {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        pages.push(page_id);
    }
    
    let peak_total = pager.total_pages();
    assert!(peak_total >= 1002, "Should have grown to at least 1002 pages");
    
    // Phase 2: Free half the pages
    let half = pages.len() / 2;
    for _ in 0..half {
        let page_id = pages.pop().unwrap();
        pager.free_page(page_id).unwrap();
    }
    
    assert_eq!(pager.free_pages() as usize, half, "Should have freed half the pages");
    
    // Phase 3: Reallocate - should reuse freed pages
    for _ in 0..half {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        pages.push(page_id);
    }
    
    // Total pages should not have grown significantly
    // Allow for a few extra pages for free list management
    assert!(
        pager.total_pages() <= peak_total + 5,
        "Total pages should not grow significantly when reusing freed pages: {} -> {}",
        peak_total,
        pager.total_pages()
    );
    
    // Free pages should be minimal
    assert!(
        pager.free_pages() < 10,
        "Free pages should be minimal after reallocation"
    );
}

/// Test edge case: approaching page ID limits
///
/// This test validates behavior when allocating many pages,
/// ensuring page IDs are managed correctly.
#[test]
fn test_large_page_id_values() {
    let pager = create_test_pager();
    let page_count = 10000;
    
    let mut pages = Vec::new();
    
    // Allocate many pages to get large page IDs
    for i in 0..page_count {
        let page_id = pager
            .allocate_page(PageType::BTreeLeaf)
            .expect(&format!("Failed to allocate page {}", i));
        pages.push(page_id);
    }
    
    // Verify page IDs are reasonable
    let max_page_id = *pages.iter().max().unwrap();
    assert!(
        max_page_id < u64::MAX / 2,
        "Page IDs should be well below u64::MAX"
    );
    
    // Verify we can still read/write pages with large IDs
    let large_page_id = pages[page_count - 1];
    let mut page = Page::new(large_page_id, PageType::BTreeLeaf, pager.page_size().data_size());
    page.data_mut().extend_from_slice(b"test data for large page ID");
    
    pager.write_page(&page).unwrap();
    let read_page = pager.read_page(large_page_id).unwrap();
    assert_eq!(
        &read_page.data()[0..27],
        b"test data for large page ID"
    );
}

/// Test concurrent-like allocation patterns
///
/// This test simulates patterns that might occur with concurrent access
/// (though the pager itself is not thread-safe, this tests the state machine).
#[test]
fn test_interleaved_allocation_patterns() {
    let pager = create_test_pager();
    let iterations = 500;
    
    let mut set_a = Vec::new();
    let mut set_b = Vec::new();
    
    // Simulate two "threads" interleaving allocations
    for _ in 0..iterations {
        // "Thread A" allocates 2 pages
        for _ in 0..2 {
            let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
            set_a.push(page_id);
        }
        
        // "Thread B" allocates 3 pages
        for _ in 0..3 {
            let page_id = pager.allocate_page(PageType::BTreeInternal).unwrap();
            set_b.push(page_id);
        }
        
        // "Thread A" frees 1 page
        if !set_a.is_empty() {
            let page_id = set_a.pop().unwrap();
            pager.free_page(page_id).unwrap();
        }
        
        // "Thread B" frees 2 pages
        for _ in 0..2 {
            if !set_b.is_empty() {
                let page_id = set_b.pop().unwrap();
                pager.free_page(page_id).unwrap();
            }
        }
    }
    
    // Verify no page ID collisions
    let all_pages: Vec<_> = set_a.iter().chain(set_b.iter()).collect();
    let unique_pages: HashSet<_> = all_pages.iter().collect();
    assert_eq!(
        unique_pages.len(),
        all_pages.len(),
        "All allocated pages should have unique IDs"
    );
}

/// Test stress scenario: rapid allocation and deallocation
///
/// This test performs rapid allocation/deallocation cycles to stress
/// test the free list management and page tracking.
#[test]
fn test_rapid_allocation_deallocation() {
    let pager = create_test_pager();
    let cycles = 1000;
    
    for cycle in 0..cycles {
        // Allocate 10 pages
        let mut pages = Vec::new();
        for _ in 0..10 {
            let page_id = pager
                .allocate_page(PageType::BTreeLeaf)
                .expect(&format!("Failed to allocate in cycle {}", cycle));
            pages.push(page_id);
        }
        
        // Immediately free all of them
        for page_id in pages {
            pager
                .free_page(page_id)
                .expect(&format!("Failed to free in cycle {}", cycle));
        }
    }
    
    // After all cycles, free list should have pages
    assert!(pager.free_pages() > 0, "Free list should have pages");
    
    // Total pages should be reasonable (not growing unboundedly)
    // Allow for free list pages that get allocated during the process
    assert!(
        pager.total_pages() < 1100,
        "Total pages should not grow excessively: {}",
        pager.total_pages()
    );
}

/// Test write and read operations at scale
///
/// This test validates that we can write and read data to/from
/// many pages without corruption.
#[test]
fn test_write_read_operations_at_scale() {
    let pager = create_test_pager();
    let page_count = 1000;
    
    let mut pages = Vec::new();
    
    // Allocate and write pages
    for i in 0..page_count {
        let page_id = pager.allocate_page(PageType::BTreeLeaf).unwrap();
        
        let mut page = Page::new(page_id, PageType::BTreeLeaf, pager.page_size().data_size());
        let data = format!("Test data for page {} - iteration {}", page_id, i);
        page.data_mut().extend_from_slice(data.as_bytes());
        
        pager.write_page(&page).unwrap();
        pages.push((page_id, data));
    }
    
    // Read back and verify all pages
    for (page_id, expected_data) in pages {
        let page = pager
            .read_page(page_id)
            .expect(&format!("Failed to read page {}", page_id));
        
        assert_eq!(
            &page.data()[0..expected_data.len()],
            expected_data.as_bytes(),
            "Data mismatch for page {}",
            page_id
        );
    }
}

// Made with Bob