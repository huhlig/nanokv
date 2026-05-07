//
// Copyright 2019-2026 Hans W. Uhlig. All Rights Reserved.
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

/// Result Type for VFS Library
pub type FileSystemResult<T> = Result<T, FileSystemError>;

/// Error Type for VFS Library
#[derive(Debug)]
pub enum FileSystemError {
    /// Path is not valid in this FileSystem
    InvalidPath(String),
    /// Attempt to create an object that already exists.
    PathExists,
    /// Path doesn't exist
    PathMissing,
    /// Parent directory missing
    ParentMissing,
    /// File Already Locked
    FileAlreadyLocked,
    /// Operation Disallowed
    PermissionDenied,
    /// Already Locked
    AlreadyLocked,
    /// Operation Not supported on Path
    InvalidOperation,
    /// Virtual File System doesn't support an operation.
    UnsupportedOperation,
    /// FileSystemError Error
    InternalError(String),
    /// IO Error
    IOError(std::io::Error),
    /// Wrapped Error
    WrappedError(Box<dyn std::error::Error>),
}

impl FileSystemError {
    /// Create a new IO Error from an IO Error
    #[must_use]
    pub fn io_error(err: std::io::Error) -> FileSystemError {
        FileSystemError::IOError(err)
    }

    /// Create a new Internal Error from a string
    #[must_use]
    pub fn internal_error(err: &str) -> FileSystemError {
        FileSystemError::InternalError(err.to_string())
    }

    /// Create a new Internal Error from a string
    #[must_use]
    pub fn invalid_path(path: &str) -> FileSystemError {
        FileSystemError::InvalidPath(path.to_string())
    }

    /// Create a new Wrapper Error from an Error
    #[must_use]
    pub fn wrap_error<E: std::error::Error + 'static>(err: E) -> FileSystemError {
        FileSystemError::WrappedError(Box::new(err))
    }
}

impl std::fmt::Display for FileSystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for FileSystemError {}
