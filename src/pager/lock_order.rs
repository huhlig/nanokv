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

//! Lock ordering enforcement for deadlock prevention
//!
//! This module provides debug-only assertions to enforce the lock hierarchy
//! documented in docs/PAGER_LOCK_ORDERING.md.
//!
//! # Lock Hierarchy (must acquire in this order)
//!
//! 1. pin_table (level 1)
//! 2. superblock (level 2)
//! 3. header (level 3)
//! 4. page_table (level 4)
//! 5. cache (level 5)
//! 6. file (level 6)
//!
//! Note: free_list uses lock-free atomics and has no ordering constraints.

#[cfg(debug_assertions)]
use std::cell::RefCell;

/// Lock types in the pager module
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockType {
    /// Pin table lock (level 1)
    PinTable = 1,
    /// Superblock lock (level 2)
    Superblock = 2,
    /// File header lock (level 3)
    Header = 3,
    /// Page table lock (level 4)
    PageTable = 4,
    /// Cache lock (level 5)
    Cache = 5,
    /// File lock (level 6)
    File = 6,
}

impl LockType {
    /// Get the lock level (lower numbers must be acquired first)
    #[inline]
    pub const fn level(self) -> u8 {
        self as u8
    }

    /// Get the lock name for error messages
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            LockType::PinTable => "pin_table",
            LockType::Superblock => "superblock",
            LockType::Header => "header",
            LockType::PageTable => "page_table",
            LockType::Cache => "cache",
            LockType::File => "file",
        }
    }
}

#[cfg(debug_assertions)]
thread_local! {
    /// Thread-local stack of currently held locks
    static LOCK_STACK: RefCell<Vec<LockType>> = const { RefCell::new(Vec::new()) };
}

/// Assert that acquiring a lock follows the lock ordering rules
///
/// This function should be called before acquiring any lock in the pager module.
/// In debug builds, it checks that the lock being acquired has a level >= all
/// currently held locks. In release builds, this is a no-op.
///
/// # Panics
///
/// Panics in debug builds if the lock ordering is violated.
///
/// # Examples
///
/// ```ignore
/// // Correct ordering
/// assert_lock_order(LockType::Superblock);
/// let _superblock = self.superblock.write();
///
/// assert_lock_order(LockType::PageTable);
/// let _page_lock = self.page_table.write_lock(page_id);
/// ```
#[inline]
pub fn assert_lock_order(lock_type: LockType) {
    #[cfg(debug_assertions)]
    {
        LOCK_STACK.with(|stack| {
            let stack = stack.borrow();
            if let Some(&last_lock) = stack.last()
                && lock_type.level() < last_lock.level() {
                    panic!(
                        "Lock ordering violation: attempted to acquire {} (level {}) \
                         while holding {} (level {}). Locks must be acquired in order: \
                         pin_table(1) → superblock(2) → header(3) → page_table(4) → cache(5) → file(6)",
                        lock_type.name(),
                        lock_type.level(),
                        last_lock.name(),
                        last_lock.level()
                    );
                }
        });
    }

    #[cfg(not(debug_assertions))]
    {
        let _ = lock_type; // Suppress unused variable warning
    }
}

/// Mark that a lock has been acquired
///
/// This should be called immediately after acquiring a lock in debug builds.
/// In release builds, this is a no-op.
#[inline]
pub fn mark_lock_acquired(lock_type: LockType) {
    #[cfg(debug_assertions)]
    {
        LOCK_STACK.with(|stack| {
            stack.borrow_mut().push(lock_type);
        });
    }

    #[cfg(not(debug_assertions))]
    {
        let _ = lock_type;
    }
}

/// Mark that a lock has been released
///
/// This should be called when a lock is explicitly dropped in debug builds.
/// In release builds, this is a no-op.
#[inline]
pub fn mark_lock_released(lock_type: LockType) {
    #[cfg(debug_assertions)]
    {
        LOCK_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if let Some(pos) = stack.iter().rposition(|&t| t == lock_type) {
                stack.remove(pos);
            }
        });
    }

    #[cfg(not(debug_assertions))]
    {
        let _ = lock_type;
    }
}

/// RAII guard that automatically tracks lock acquisition and release
///
/// This guard should wrap lock guards to automatically track lock ordering.
/// When the guard is dropped, it automatically marks the lock as released.
#[cfg(debug_assertions)]
pub struct LockOrderGuard<T> {
    lock_type: LockType,
    inner: T,
}

#[cfg(debug_assertions)]
impl<T> LockOrderGuard<T> {
    /// Create a new lock order guard
    ///
    /// This asserts the lock ordering and marks the lock as acquired.
    #[inline]
    pub fn new(lock_type: LockType, inner: T) -> Self {
        assert_lock_order(lock_type);
        mark_lock_acquired(lock_type);
        Self { lock_type, inner }
    }

    /// Get a reference to the inner guard
    #[inline]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the inner guard
    #[inline]
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

#[cfg(debug_assertions)]
impl<T> Drop for LockOrderGuard<T> {
    fn drop(&mut self) {
        mark_lock_released(self.lock_type);
    }
}

#[cfg(debug_assertions)]
impl<T> std::ops::Deref for LockOrderGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(debug_assertions)]
impl<T> std::ops::DerefMut for LockOrderGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correct_lock_order() {
        // This should not panic
        assert_lock_order(LockType::PinTable);
        mark_lock_acquired(LockType::PinTable);

        assert_lock_order(LockType::Superblock);
        mark_lock_acquired(LockType::Superblock);

        assert_lock_order(LockType::Header);
        mark_lock_acquired(LockType::Header);

        assert_lock_order(LockType::PageTable);
        mark_lock_acquired(LockType::PageTable);

        assert_lock_order(LockType::Cache);
        mark_lock_acquired(LockType::Cache);

        assert_lock_order(LockType::File);
        mark_lock_acquired(LockType::File);

        // Clean up
        mark_lock_released(LockType::File);
        mark_lock_released(LockType::Cache);
        mark_lock_released(LockType::PageTable);
        mark_lock_released(LockType::Header);
        mark_lock_released(LockType::Superblock);
        mark_lock_released(LockType::PinTable);
    }

    #[test]
    #[should_panic(expected = "Lock ordering violation")]
    #[cfg(debug_assertions)]
    fn test_incorrect_lock_order() {
        assert_lock_order(LockType::File);
        mark_lock_acquired(LockType::File);

        // This should panic - trying to acquire superblock after file
        assert_lock_order(LockType::Superblock);
    }

    #[test]
    fn test_same_level_allowed() {
        // Acquiring locks at the same level is allowed (e.g., multiple page locks)
        assert_lock_order(LockType::PageTable);
        mark_lock_acquired(LockType::PageTable);

        assert_lock_order(LockType::PageTable);
        mark_lock_acquired(LockType::PageTable);

        mark_lock_released(LockType::PageTable);
        mark_lock_released(LockType::PageTable);
    }

    #[test]
    fn test_lock_release() {
        assert_lock_order(LockType::Superblock);
        mark_lock_acquired(LockType::Superblock);

        assert_lock_order(LockType::File);
        mark_lock_acquired(LockType::File);

        // Release file lock
        mark_lock_released(LockType::File);

        // Now we can acquire header (which is between superblock and file)
        assert_lock_order(LockType::Header);
        mark_lock_acquired(LockType::Header);

        mark_lock_released(LockType::Header);
        mark_lock_released(LockType::Superblock);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn test_lock_order_guard() {
        {
            let _guard1 = LockOrderGuard::new(LockType::Superblock, ());
            {
                let _guard2 = LockOrderGuard::new(LockType::PageTable, ());
                // Both locks held
            }
            // guard2 dropped, only guard1 held
        }
        // All locks released
    }
}

// Made with Bob
