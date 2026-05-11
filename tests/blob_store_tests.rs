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

//! Integration tests for the Blob Store.
//!
//! These tests verify blob storage, retrieval, deletion, and reference management.
//! Note: Many tests are marked as #[ignore] because the blob store implementation
//! is not yet complete (methods return todo!()).

use nanokv::blob::{BlobRef, BlobStore, FreedPages, PagedBlobStore};
use nanokv::pager::PageId;

/// Test blob store creation
#[test]
fn test_blob_store_creation() {
    let page_size = 4096;
    let store = PagedBlobStore::new(page_size);
    
    // Verify max inline size is 1/4 of page size
    assert_eq!(store.max_inline_size(), page_size / 4);
    
    // Verify max blob size
    assert_eq!(store.max_blob_size(), 1024 * 1024 * 1024); // 1GB
}

/// Test blob reference creation
#[test]
fn test_blob_ref_creation() {
    let blob_ref = BlobRef::new(PageId::from(42), 1024, 0x12345678);
    
    assert_eq!(blob_ref.first_page(), PageId::from(42));
    assert_eq!(blob_ref.size(), 1024);
    assert_eq!(blob_ref.checksum(), 0x12345678);
}

/// Test blob reference equality
#[test]
fn test_blob_ref_equality() {
    let ref1 = BlobRef::new(PageId::from(1), 100, 0xABCD);
    let ref2 = BlobRef::new(PageId::from(1), 100, 0xABCD);
    let ref3 = BlobRef::new(PageId::from(2), 100, 0xABCD);
    
    assert_eq!(ref1, ref2);
    assert_ne!(ref1, ref3);
}

/// Test freed pages list creation
#[test]
fn test_freed_pages_creation() {
    let freed = FreedPages::new();
    assert!(freed.is_empty());
    assert_eq!(freed.len(), 0);
}

/// Test freed pages single page
#[test]
fn test_freed_pages_single() {
    let freed = FreedPages::single(PageId::from(42));
    assert!(!freed.is_empty());
    assert_eq!(freed.len(), 1);
}

/// Test freed pages from vector
#[test]
fn test_freed_pages_from_vec() {
    let pages = vec![
        PageId::from(1),
        PageId::from(2),
        PageId::from(3),
    ];
    let freed = FreedPages::from_pages(pages);
    assert_eq!(freed.len(), 3);
}

/// Test freed pages push
#[test]
fn test_freed_pages_push() {
    let mut freed = FreedPages::new();
    freed.push(PageId::from(1));
    freed.push(PageId::from(2));
    freed.push(PageId::from(3));
    
    assert_eq!(freed.len(), 3);
}

/// Test freed pages extend
#[test]
fn test_freed_pages_extend() {
    let mut freed1 = FreedPages::from_pages(vec![
        PageId::from(1),
        PageId::from(2),
    ]);
    
    let freed2 = FreedPages::from_pages(vec![
        PageId::from(3),
        PageId::from(4),
    ]);
    
    freed1.extend(freed2);
    assert_eq!(freed1.len(), 4);
}

/// Test max inline size calculation
#[test]
fn test_max_inline_size() {
    let store_4k = PagedBlobStore::new(4096);
    assert_eq!(store_4k.max_inline_size(), 1024);
    
    let store_8k = PagedBlobStore::new(8192);
    assert_eq!(store_8k.max_inline_size(), 2048);
    
    let store_16k = PagedBlobStore::new(16384);
    assert_eq!(store_16k.max_inline_size(), 4096);
}

/// Test put blob (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_put_blob_small() {
    let mut store = PagedBlobStore::new(4096);
    let data = b"Hello, World!";
    
    let result = store.put_blob(data);
    assert!(result.is_ok());
    
    let blob_ref = result.unwrap();
    assert_eq!(blob_ref.size(), data.len() as u64);
}

/// Test put blob large (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_put_blob_large() {
    let mut store = PagedBlobStore::new(4096);
    
    // Create a blob larger than one page
    let data = vec![0xAB; 10000];
    
    let result = store.put_blob(&data);
    assert!(result.is_ok());
    
    let blob_ref = result.unwrap();
    assert_eq!(blob_ref.size(), data.len() as u64);
}

/// Test get blob (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_get_blob() {
    let mut store = PagedBlobStore::new(4096);
    let data = b"Test data for blob storage";
    
    let blob_ref = store.put_blob(data).unwrap();
    let retrieved = store.get_blob(blob_ref).unwrap();
    
    assert_eq!(retrieved.0, data);
}

/// Test delete blob (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_delete_blob() {
    let mut store = PagedBlobStore::new(4096);
    let data = b"Data to be deleted";
    
    let blob_ref = store.put_blob(data).unwrap();
    let freed = store.delete_blob(blob_ref).unwrap();
    
    assert!(!freed.is_empty());
}

/// Test blob round-trip (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_blob_round_trip() {
    let mut store = PagedBlobStore::new(4096);
    let original_data = b"Round trip test data";
    
    // Put blob
    let blob_ref = store.put_blob(original_data).unwrap();
    
    // Get blob
    let retrieved_data = store.get_blob(blob_ref).unwrap();
    assert_eq!(retrieved_data.0, original_data);
    
    // Delete blob
    let freed = store.delete_blob(blob_ref).unwrap();
    assert!(!freed.is_empty());
}

/// Test multiple blobs (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_multiple_blobs() {
    let mut store = PagedBlobStore::new(4096);
    
    let data1 = b"First blob";
    let data2 = b"Second blob";
    let data3 = b"Third blob";
    
    let ref1 = store.put_blob(data1).unwrap();
    let ref2 = store.put_blob(data2).unwrap();
    let ref3 = store.put_blob(data3).unwrap();
    
    // All references should be different
    assert_ne!(ref1, ref2);
    assert_ne!(ref2, ref3);
    assert_ne!(ref1, ref3);
    
    // Should be able to retrieve all blobs
    assert_eq!(store.get_blob(ref1).unwrap().0, data1);
    assert_eq!(store.get_blob(ref2).unwrap().0, data2);
    assert_eq!(store.get_blob(ref3).unwrap().0, data3);
}

/// Test blob at inline threshold (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_blob_at_inline_threshold() {
    let page_size = 4096;
    let mut store = PagedBlobStore::new(page_size);
    let threshold = store.max_inline_size();
    
    // Create blob exactly at threshold
    let data = vec![0x42; threshold];
    
    let blob_ref = store.put_blob(&data).unwrap();
    let retrieved = store.get_blob(blob_ref).unwrap();
    
    assert_eq!(retrieved.0, data);
}

/// Test blob just over inline threshold (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_blob_over_inline_threshold() {
    let page_size = 4096;
    let mut store = PagedBlobStore::new(page_size);
    let threshold = store.max_inline_size();
    
    // Create blob just over threshold
    let data = vec![0x42; threshold + 1];
    
    let blob_ref = store.put_blob(&data).unwrap();
    let retrieved = store.get_blob(blob_ref).unwrap();
    
    assert_eq!(retrieved.0, data);
}

/// Test empty blob (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_empty_blob() {
    let mut store = PagedBlobStore::new(4096);
    let data = b"";
    
    let blob_ref = store.put_blob(data).unwrap();
    let retrieved = store.get_blob(blob_ref).unwrap();
    
    assert_eq!(retrieved.0, data);
}

/// Test very large blob (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_very_large_blob() {
    let mut store = PagedBlobStore::new(4096);
    
    // Create 1MB blob
    let data = vec![0x55; 1024 * 1024];
    
    let blob_ref = store.put_blob(&data).unwrap();
    assert_eq!(blob_ref.size(), data.len() as u64);
    
    let retrieved = store.get_blob(blob_ref).unwrap();
    assert_eq!(retrieved.0, data);
}

/// Test blob with all byte values (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_blob_all_byte_values() {
    let mut store = PagedBlobStore::new(4096);
    
    // Create blob with all possible byte values
    let data: Vec<u8> = (0..=255).collect();
    
    let blob_ref = store.put_blob(&data).unwrap();
    let retrieved = store.get_blob(blob_ref).unwrap();
    
    assert_eq!(retrieved.0, data);
}

/// Test blob checksum validation (currently unimplemented)
#[test]
#[ignore = "Blob store implementation not complete"]
fn test_blob_checksum_validation() {
    let mut store = PagedBlobStore::new(4096);
    let data = b"Data with checksum";
    
    let blob_ref = store.put_blob(data).unwrap();
    
    // Checksum should be non-zero
    assert_ne!(blob_ref.checksum(), 0);
    
    // Should be able to retrieve with valid checksum
    let retrieved = store.get_blob(blob_ref).unwrap();
    assert_eq!(retrieved.0, data);
}

/// Test blob reference clone and copy
#[test]
fn test_blob_ref_clone_copy() {
    let ref1 = BlobRef::new(PageId::from(1), 100, 0xABCD);
    let ref2 = ref1; // Copy
    let ref3 = ref1.clone(); // Clone
    
    assert_eq!(ref1, ref2);
    assert_eq!(ref1, ref3);
    assert_eq!(ref2, ref3);
}

/// Test freed pages clone
#[test]
fn test_freed_pages_clone() {
    let freed1 = FreedPages::from_pages(vec![
        PageId::from(1),
        PageId::from(2),
        PageId::from(3),
    ]);
    
    let freed2 = freed1.clone();
    assert_eq!(freed1, freed2);
}

/// Test blob store trait object
#[test]
fn test_blob_store_trait_object() {
    let store: Box<dyn BlobStore> = Box::new(PagedBlobStore::new(4096));
    
    assert_eq!(store.max_inline_size(), 1024);
    assert_eq!(store.max_blob_size(), 1024 * 1024 * 1024);
}

// Made with Bob
