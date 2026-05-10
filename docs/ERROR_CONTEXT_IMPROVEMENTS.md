# Error Context Preservation Improvements

## Analysis Summary

After reviewing all error types across NanoKV modules, I've identified several areas where error context can be improved for better debuggability and operational visibility.

## Current State

### Strengths
1. **Good ID preservation in some areas**:
   - `PagerError::ChecksumMismatch(PageId)` - includes page ID
   - `WalError::ChecksumMismatch(LogSequenceNumber)` - includes LSN
   - `TransactionError::WriteWriteConflict` - includes object ID, key, and conflicting transaction ID
   - `TableError::ChecksumMismatch` - includes location, expected, and actual checksums

2. **Comprehensive error types**: Each module has well-defined error variants

3. **Good error propagation**: `From` implementations enable seamless error conversion

### Gaps Identified

#### 1. VFS Module - Minimal Context
**Current**: Most errors lack path information
```rust
PathMissing,           // Which path?
PermissionDenied,      // On which file?
AlreadyLocked,         // Which file is locked?
InvalidOperation,      // What operation on which path?
```

**Impact**: When VFS errors propagate up, we lose critical information about which file operation failed.

#### 2. Pager Module - Missing Operation Context
**Current**: Some errors lack context about what was being attempted
```rust
CompressionError(String),      // Which page? What compression type?
DecompressionError(String),    // Which page? What was expected?
EncryptionError(String),       // Which page? What encryption type?
DecryptionError(String),       // Which page?
InvalidFileHeader(String),     // What was the actual header?
InvalidSuperblock(String),     // What field was invalid?
```

#### 3. Transaction Module - Limited Conflict Details
**Current**: Some errors could provide more context
```rust
InvalidState(TransactionId),   // What state? What operation was attempted?
Deadlock(TransactionId),       // What other transactions are involved?
```

#### 4. Table Module - Generic String Errors
**Current**: Many errors use generic strings without structured context
```rust
Corruption(String),            // Where? What type of corruption?
CompactionFailed(String),      // Which level? Which files?
FlushFailed(String),           // Which memtable? What was the error?
EvictionFailed(String),        // Which SSTable? Why?
```

#### 5. WAL Module - Missing Transaction Context
**Current**: Some errors lack transaction context
```rust
InvalidRecord(String),         // At what LSN? For which transaction?
RecoveryError(String),         // At what point? Which transaction?
CheckpointError(String),       // What LSN range? What failed?
```

#### 6. Blob Module - Limited Reference Context
**Current**: Errors could include more blob metadata
```rust
Corrupted(String),             // Which blob? What type of corruption?
Internal(String),              // What operation? What blob?
```

## Proposed Improvements

### Phase 1: Add Context Fields to Existing Errors

#### VFS Errors
```rust
pub enum FileSystemError {
    InvalidPath { path: String, reason: String },
    PathMissing { path: String },
    ParentMissing { path: String },
    FileAlreadyLocked { path: String },
    PermissionDenied { path: String, operation: String },
    AlreadyLocked { path: String },
    InvalidOperation { path: String, operation: String },
    // ... existing variants
}
```

#### Pager Errors
```rust
pub enum PagerError {
    CompressionError { 
        page_id: PageId, 
        compression_type: CompressionType, 
        details: String 
    },
    DecompressionError { 
        page_id: PageId, 
        compression_type: CompressionType, 
        details: String 
    },
    EncryptionError { 
        page_id: PageId, 
        encryption_type: EncryptionType, 
        details: String 
    },
    DecryptionError { 
        page_id: PageId, 
        encryption_type: EncryptionType, 
        details: String 
    },
    InvalidFileHeader { 
        expected_magic: u32, 
        found_magic: u32, 
        details: String 
    },
    InvalidSuperblock { 
        field: String, 
        expected: String, 
        found: String 
    },
    // ... existing variants
}
```

#### Transaction Errors
```rust
pub enum TransactionError {
    InvalidState { 
        transaction_id: TransactionId, 
        current_state: String, 
        attempted_operation: String 
    },
    Deadlock { 
        transaction_id: TransactionId, 
        involved_transactions: Vec<TransactionId>,
        cycle_description: String 
    },
    // ... existing variants
}
```

#### Table Errors
```rust
pub enum TableError {
    Corruption { 
        location: String, 
        corruption_type: String, 
        details: String 
    },
    CompactionFailed { 
        level: u32, 
        input_files: Vec<String>, 
        error: String 
    },
    FlushFailed { 
        memtable_size: usize, 
        target_level: u32, 
        error: String 
    },
    EvictionFailed { 
        sstable_id: String, 
        level: u32, 
        reason: String 
    },
    // ... existing variants
}
```

#### WAL Errors
```rust
pub enum WalError {
    InvalidRecord { 
        lsn: LogSequenceNumber, 
        transaction_id: Option<TransactionId>, 
        details: String 
    },
    RecoveryError { 
        lsn: LogSequenceNumber, 
        transaction_id: Option<TransactionId>, 
        phase: String, 
        error: String 
    },
    CheckpointError { 
        start_lsn: LogSequenceNumber, 
        end_lsn: LogSequenceNumber, 
        error: String 
    },
    // ... existing variants
}
```

#### Blob Errors
```rust
pub enum BlobError {
    Corrupted { 
        blob_ref: BlobRef, 
        corruption_type: String, 
        details: String 
    },
    Internal { 
        blob_ref: Option<BlobRef>, 
        operation: String, 
        error: String 
    },
    // ... existing variants
}
```

### Phase 2: Add Error Metrics and Logging

#### Error Metrics Structure
```rust
pub struct ErrorMetrics {
    // Error counts by type
    pager_errors: AtomicU64,
    wal_errors: AtomicU64,
    table_errors: AtomicU64,
    transaction_errors: AtomicU64,
    vfs_errors: AtomicU64,
    
    // Specific error categories
    checksum_mismatches: AtomicU64,
    corruption_detected: AtomicU64,
    resource_exhaustion: AtomicU64,
    transaction_conflicts: AtomicU64,
    
    // Last error timestamp
    last_error_time: AtomicU64,
}
```

#### Error Logging Strategy
1. **Structured logging**: Use `tracing` crate for structured error logging
2. **Error context**: Include full context in log messages
3. **Error severity**: Categorize errors by severity (critical, error, warning)
4. **Error correlation**: Include correlation IDs for tracking related errors

### Phase 3: Error Recovery Strategies

#### Retry Logic
```rust
pub struct RetryConfig {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: f64,
}

pub trait Retryable {
    fn is_retryable(&self) -> bool;
    fn retry_delay(&self, attempt: u32) -> Duration;
}
```

#### Error Recovery Handlers
```rust
pub enum RecoveryAction {
    Retry,
    Abort,
    Fallback(Box<dyn Fn() -> Result<(), NanoKvError>>),
    Ignore,
}

pub trait RecoverableError {
    fn recovery_action(&self) -> RecoveryAction;
    fn can_recover(&self) -> bool;
}
```

## Implementation Plan

### Step 1: VFS Error Context (Highest Impact)
- Add path and operation context to all VFS errors
- Update all VFS implementations (local, memory)
- Update error propagation in pager

### Step 2: Pager Error Context
- Add page ID and type context to compression/encryption errors
- Add structured context to header/superblock errors
- Update error creation sites

### Step 3: Transaction Error Context
- Add state and operation context to InvalidState
- Add involved transactions to Deadlock
- Update conflict detector error reporting

### Step 4: Table Error Context
- Convert string-based errors to structured variants
- Add level/file context to compaction errors
- Add size/target context to flush errors

### Step 5: WAL Error Context
- Add LSN and transaction ID to all relevant errors
- Add phase information to recovery errors
- Add range information to checkpoint errors

### Step 6: Blob Error Context
- Add blob reference to all errors
- Add operation context to internal errors
- Add corruption type to corrupted errors

### Step 7: Error Metrics Infrastructure
- Implement ErrorMetrics structure
- Add metrics collection to error creation
- Add metrics reporting API

### Step 8: Error Logging
- Integrate `tracing` crate
- Add structured logging to all error creation sites
- Add correlation IDs for error tracking

### Step 9: Error Recovery
- Implement retry logic for transient errors
- Add recovery handlers for specific error types
- Document recovery strategies

### Step 10: Testing
- Update existing error tests with new context
- Add tests for error metrics
- Add tests for error recovery
- Add integration tests for error propagation

## Benefits

1. **Better Debuggability**: Errors include full context about what failed
2. **Operational Visibility**: Metrics enable monitoring and alerting
3. **Improved Recovery**: Structured errors enable better recovery strategies
4. **Better User Experience**: More informative error messages
5. **Easier Troubleshooting**: Full context helps diagnose issues faster

## Backward Compatibility

Most changes are backward compatible because:
1. New error variants can coexist with old ones
2. Error display implementations remain compatible
3. Error conversion (`From` implementations) continue to work

Breaking changes:
1. Changing existing error variants from `String` to structured fields
2. These will require updating error creation sites

## Migration Strategy

1. Add new error variants alongside old ones
2. Deprecate old variants with `#[deprecated]`
3. Update error creation sites incrementally
4. Remove deprecated variants in next major version

## Success Metrics

1. **Context Coverage**: 100% of errors include relevant IDs (page, transaction, etc.)
2. **Error Metrics**: All error types tracked in metrics
3. **Recovery Rate**: Measure successful error recovery attempts
4. **MTTR**: Reduce mean time to resolution for production issues
5. **Test Coverage**: 100% coverage of new error context fields