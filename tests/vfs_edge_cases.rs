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

//! Edge case and error handling tests for VFS implementations

use nanokv::vfs::{File, FileLockMode, FileSystem, FileSystemError, LocalFileSystem, MemoryFileSystem};
use std::io::{Read, Seek, SeekFrom, Write};

// ============================================================================
// Memory FileSystem Edge Cases
// ============================================================================

#[test]
fn test_memory_fs_path_normalization() {
    let fs = MemoryFileSystem::new();
    
    // Test various path formats
    let paths = [
        "/test.txt",
        "//test.txt",
        "/./test.txt",
    ];
    
    // Only the first path should work correctly
    let mut file = fs.create_file(paths[0]).unwrap();
    file.write_all(b"test").unwrap();
    drop(file);
    
    assert!(fs.exists(paths[0]).unwrap());
}

#[test]
fn test_memory_fs_concurrent_access() {
    let fs = MemoryFileSystem::new();
    let path = "/concurrent.txt";
    
    // Create file with first handle
    let mut file1 = fs.create_file(path).unwrap();
    file1.write_all(b"Hello").unwrap();
    
    // Open second handle
    let mut file2 = fs.open_file(path).unwrap();
    
    // Write more with first handle
    file1.write_all(b" World").unwrap();
    
    // Read with second handle should see all data
    let mut buf = Vec::new();
    file2.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"Hello World");
}

#[test]
fn test_memory_fs_write_beyond_size() {
    let fs = MemoryFileSystem::new();
    let path = "/expand.txt";
    
    let mut file = fs.create_file(path).unwrap();
    
    // Write at offset beyond current size
    file.write_to_offset(100, b"test").unwrap();
    
    // File should expand
    assert!(file.get_size().unwrap() >= 104);
    
    // Read at that offset
    let mut buf = [0u8; 4];
    file.read_at_offset(100, &mut buf).unwrap();
    assert_eq!(&buf, b"test");
}

#[test]
fn test_memory_fs_seek_beyond_end() {
    let fs = MemoryFileSystem::new();
    let path = "/seek_beyond.txt";
    
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"0123456789").unwrap();
    
    // Seek beyond end
    let pos = file.seek(SeekFrom::Start(100)).unwrap();
    assert_eq!(pos, 100);
    
    // Writing here should expand the file
    file.write_all(b"X").unwrap();
    assert!(file.get_size().unwrap() >= 101);
}

#[test]
fn test_memory_fs_negative_seek() {
    let fs = MemoryFileSystem::new();
    let path = "/negative_seek.txt";
    
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"0123456789").unwrap();
    
    // Seek to end
    file.seek(SeekFrom::End(0)).unwrap();
    
    // Seek backward from end
    let pos = file.seek(SeekFrom::End(-5)).unwrap();
    assert_eq!(pos, 5);
    
    // Read from that position
    let mut buf = [0u8; 5];
    file.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"56789");
}

#[test]
fn test_memory_fs_directory_listing() {
    let fs = MemoryFileSystem::new();
    
    // Create directory with metadata
    fs.create_directory("/testdir").unwrap();
    
    // List should be empty initially
    let entries = fs.list_directory("/testdir").unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn test_memory_fs_remove_nonexistent() {
    let fs = MemoryFileSystem::new();
    
    // Try to remove non-existent file
    match fs.remove_file("/nonexistent.txt") {
        Err(FileSystemError::PathMissing) => {},
        _ => panic!("Should fail with PathMissing"),
    }
    
    // Try to remove non-existent directory
    match fs.remove_directory("/nonexistent_dir") {
        Err(FileSystemError::PathMissing) => {},
        _ => panic!("Should fail with PathMissing"),
    }
}

#[test]
fn test_memory_fs_create_duplicate() {
    let fs = MemoryFileSystem::new();
    let path = "/duplicate.txt";
    
    // Create first file
    fs.create_file(path).unwrap();
    
    // Try to create again
    match fs.create_file(path) {
        Err(FileSystemError::PathExists) => {},
        _ => panic!("Should fail with PathExists"),
    }
}

#[test]
fn test_memory_fs_lock_transitions() {
    let fs = MemoryFileSystem::new();
    let path = "/lock_test.txt";
    
    let mut file = fs.create_file(path).unwrap();
    
    // Test all lock transitions
    assert_eq!(file.get_lock_status().unwrap(), FileLockMode::Unlocked);
    
    file.set_lock_status(FileLockMode::Shared).unwrap();
    assert_eq!(file.get_lock_status().unwrap(), FileLockMode::Shared);
    
    file.set_lock_status(FileLockMode::Exclusive).unwrap();
    assert_eq!(file.get_lock_status().unwrap(), FileLockMode::Exclusive);
    
    file.set_lock_status(FileLockMode::Shared).unwrap();
    assert_eq!(file.get_lock_status().unwrap(), FileLockMode::Shared);
    
    file.set_lock_status(FileLockMode::Unlocked).unwrap();
    assert_eq!(file.get_lock_status().unwrap(), FileLockMode::Unlocked);
}

#[test]
fn test_memory_fs_zero_byte_operations() {
    let fs = MemoryFileSystem::new();
    let path = "/zero.txt";
    
    let mut file = fs.create_file(path).unwrap();
    
    // Write zero bytes
    file.write_all(&[]).unwrap();
    assert_eq!(file.get_size().unwrap(), 0);
    
    // Read zero bytes
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf.len(), 0);
    
    // Write at offset 0 with zero bytes
    file.write_to_offset(0, &[]).unwrap();
    assert_eq!(file.get_size().unwrap(), 0);
}

#[test]
fn test_memory_fs_flush_operations() {
    let fs = MemoryFileSystem::new();
    let path = "/flush.txt";
    
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"test").unwrap();
    
    // Flush should succeed (even though it's a no-op for memory)
    file.flush().unwrap();
    file.sync_data().unwrap();
    file.sync_all().unwrap();
    
    // Data should still be there
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"test");
}

// ============================================================================
// Local FileSystem Edge Cases
// ============================================================================

#[test]
fn test_local_fs_absolute_path_handling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    // Test with leading slash (should be relative to root)
    let path = "/test.txt";
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"test").unwrap();
    drop(file);
    
    assert!(fs.exists(path).unwrap());
    fs.remove_file(path).unwrap();
}

#[test]
fn test_local_fs_nested_directory_creation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    // Create deeply nested directories
    let path = "/a/b/c/d/e";
    fs.create_directory_all(path).unwrap();
    
    assert!(fs.exists(path).unwrap());
    assert!(fs.is_directory(path).unwrap());
    
    // Verify all parent directories exist
    assert!(fs.exists("/a").unwrap());
    assert!(fs.exists("/a/b").unwrap());
    assert!(fs.exists("/a/b/c").unwrap());
    assert!(fs.exists("/a/b/c/d").unwrap());
}

#[test]
fn test_local_fs_file_in_nested_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    // Create nested directories
    fs.create_directory_all("/dir1/dir2").unwrap();
    
    // Create file in nested directory
    let path = "/dir1/dir2/file.txt";
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"nested").unwrap();
    drop(file);
    
    assert!(fs.exists(path).unwrap());
    assert!(fs.is_file(path).unwrap());
}

#[test]
fn test_local_fs_remove_directory_with_contents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    // Create directory with file
    fs.create_directory("/testdir").unwrap();
    let mut file = fs.create_file("/testdir/file.txt").unwrap();
    file.write_all(b"content").unwrap();
    drop(file);
    
    // Remove directory and all contents
    fs.remove_directory_all("/testdir").unwrap();
    assert!(!fs.exists("/testdir").unwrap());
}

#[test]
fn test_local_fs_file_permissions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    let path = "/perms.txt";
    let mut file = fs.create_file(path).unwrap();
    file.write_all(b"test").unwrap();
    
    // File should be readable and writable
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"test");
    
    file.write_all(b" more").unwrap();
}

#[test]
fn test_local_fs_large_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let fs = LocalFileSystem::new(temp_dir.path());
    
    let path = "/large.bin";
    let mut file = fs.create_file(path).unwrap();
    
    // Write 10MB
    let chunk = vec![0xAB; 1024 * 1024];
    for _ in 0..10 {
        file.write_all(&chunk).unwrap();
    }
    
    assert_eq!(file.get_size().unwrap(), 10 * 1024 * 1024);
    
    // Clean up
    drop(file);
    fs.remove_file(path).unwrap();
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_error_display() {
    // Test that errors can be displayed
    let err = FileSystemError::PathMissing;
    let display = format!("{}", err);
    assert!(!display.is_empty());
    
    let err = FileSystemError::PathExists;
    let display = format!("{}", err);
    assert!(!display.is_empty());
    
    let err = FileSystemError::InvalidPath("/bad/path".to_string());
    let display = format!("{}", err);
    assert!(!display.is_empty());
}

#[test]
fn test_error_debug() {
    // Test that errors can be debugged
    let err = FileSystemError::PermissionDenied;
    let debug = format!("{:?}", err);
    assert!(!debug.is_empty());
}

// ============================================================================
// FileLockMode Tests
// ============================================================================

#[test]
fn test_file_lock_mode_equality() {
    assert_eq!(FileLockMode::Unlocked, FileLockMode::Unlocked);
    assert_eq!(FileLockMode::Shared, FileLockMode::Shared);
    assert_eq!(FileLockMode::Exclusive, FileLockMode::Exclusive);
    
    assert_ne!(FileLockMode::Unlocked, FileLockMode::Shared);
    assert_ne!(FileLockMode::Shared, FileLockMode::Exclusive);
    assert_ne!(FileLockMode::Unlocked, FileLockMode::Exclusive);
}

#[test]
fn test_file_lock_mode_debug() {
    let modes = [
        FileLockMode::Unlocked,
        FileLockMode::Shared,
        FileLockMode::Exclusive,
    ];
    
    for mode in &modes {
        let debug = format!("{:?}", mode);
        assert!(!debug.is_empty());
    }
}

#[test]
fn test_file_lock_mode_copy_clone() {
    let mode = FileLockMode::Shared;
    let copied = mode;
    let cloned = mode.clone();
    
    assert_eq!(mode, copied);
    assert_eq!(mode, cloned);
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_many_small_files() {
    let fs = MemoryFileSystem::new();
    
    // Create 1000 small files
    for i in 0..1000 {
        let path = format!("/file_{}.txt", i);
        let mut file = fs.create_file(&path).unwrap();
        file.write_all(format!("Content {}", i).as_bytes()).unwrap();
    }
    
    // Verify all exist
    for i in 0..1000 {
        let path = format!("/file_{}.txt", i);
        assert!(fs.exists(&path).unwrap());
    }
    
    // Clean up
    for i in 0..1000 {
        let path = format!("/file_{}.txt", i);
        fs.remove_file(&path).unwrap();
    }
}

#[test]
fn test_repeated_create_remove() {
    let fs = MemoryFileSystem::new();
    let path = "/repeated.txt";
    
    // Create and remove 100 times
    for i in 0..100 {
        let mut file = fs.create_file(path).unwrap();
        file.write_all(format!("Iteration {}", i).as_bytes()).unwrap();
        drop(file);
        
        assert!(fs.exists(path).unwrap());
        fs.remove_file(path).unwrap();
        assert!(!fs.exists(path).unwrap());
    }
}

#[test]
fn test_interleaved_operations() {
    let fs = MemoryFileSystem::new();
    
    // Create multiple files
    let paths = ["/file1.txt", "/file2.txt", "/file3.txt"];
    let mut files = Vec::new();
    
    for path in &paths {
        files.push(fs.create_file(path).unwrap());
    }
    
    // Write to them in interleaved fashion
    for i in 0..10 {
        for (idx, file) in files.iter_mut().enumerate() {
            file.write_all(format!("{}:{} ", idx, i).as_bytes()).unwrap();
        }
    }
    
    // Verify content
    drop(files);
    for (idx, path) in paths.iter().enumerate() {
        let mut file = fs.open_file(path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        
        let content = String::from_utf8(buf).unwrap();
        for i in 0..10 {
            assert!(content.contains(&format!("{}:{}", idx, i)));
        }
    }
}

// Made with Bob
