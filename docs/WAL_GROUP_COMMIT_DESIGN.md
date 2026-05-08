# WAL Group Commit Design

## Overview

Group commit is a performance optimization that batches multiple transaction commits into a single fsync operation, significantly improving throughput under high concurrency.

## Current Implementation Analysis

### Commit Flow (Before Group Commit)
1. Transaction calls `write_commit(txn_id)`
2. Writer creates COMMIT record
3. Record is serialized and added to buffer
4. If `sync_on_write` is true (default), buffer is flushed immediately
5. `file.sync_data()` is called for each commit
6. Transaction is removed from active set

### Performance Issues
- Each commit triggers an independent fsync
- Under high load, fsync becomes a bottleneck
- No opportunity to amortize fsync cost across multiple commits
- Latency increases linearly with commit rate

## Group Commit Architecture

### Design Goals
1. **Maintain Durability**: All committed transactions must survive crashes
2. **Preserve Isolation**: Transaction ordering must be maintained
3. **Configurable**: Allow tuning for different workloads
4. **Optional**: Can be disabled for low-latency scenarios
5. **Metrics**: Track effectiveness of group commit

### Components

#### 1. Commit Queue
```rust
struct CommitRequest {
    txn_id: TransactionId,
    lsn: Lsn,
    notifier: oneshot::Sender<WalResult<()>>,
}

struct CommitQueue {
    pending: VecDeque<CommitRequest>,
    max_batch_size: usize,
    max_wait_time: Duration,
}
```

#### 2. Group Commit Coordinator
```rust
struct GroupCommitCoordinator {
    queue: Arc<Mutex<CommitQueue>>,
    worker_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}
```

The coordinator runs a background thread that:
1. Waits for commits to accumulate in the queue
2. Batches commits based on time or size threshold
3. Performs a single fsync for the entire batch
4. Notifies all waiting transactions

#### 3. Configuration
```rust
pub struct GroupCommitConfig {
    /// Enable group commit optimization
    pub enabled: bool,
    /// Maximum number of commits to batch together
    pub max_batch_size: usize,
    /// Maximum time to wait for batching (microseconds)
    pub max_wait_micros: u64,
    /// Minimum batch size to trigger early flush
    pub min_batch_size: usize,
}
```

### Commit Flow (With Group Commit)

#### Fast Path (Group Commit Enabled)
1. Transaction calls `write_commit(txn_id)`
2. Writer creates COMMIT record and writes to buffer
3. Record is added to commit queue with a oneshot channel
4. Thread waits on the oneshot channel
5. Background coordinator batches commits
6. Single fsync for entire batch
7. All waiting transactions are notified
8. Transactions complete

#### Slow Path (Group Commit Disabled or Low Latency Mode)
1. Same as current implementation
2. Immediate fsync after each commit

### Batching Strategy

The coordinator uses a hybrid approach:

```rust
fn should_flush(queue: &CommitQueue, last_flush: Instant) -> bool {
    // Flush if batch is full
    if queue.pending.len() >= queue.max_batch_size {
        return true;
    }
    
    // Flush if minimum batch size reached and timeout expired
    if queue.pending.len() >= queue.min_batch_size 
        && last_flush.elapsed() >= queue.max_wait_time {
        return true;
    }
    
    // Flush if timeout expired and queue is not empty
    if !queue.pending.is_empty() 
        && last_flush.elapsed() >= queue.max_wait_time {
        return true;
    }
    
    false
}
```

### Metrics

Track the following metrics:
```rust
pub struct GroupCommitMetrics {
    /// Total number of commits processed
    pub total_commits: AtomicU64,
    /// Total number of fsync operations
    pub total_fsyncs: AtomicU64,
    /// Total number of batches
    pub total_batches: AtomicU64,
    /// Average batch size
    pub avg_batch_size: AtomicU64,
    /// Maximum batch size seen
    pub max_batch_size: AtomicU64,
    /// Total wait time (microseconds)
    pub total_wait_time_us: AtomicU64,
}
```

## Implementation Plan

### Phase 1: Core Infrastructure
1. Add `GroupCommitConfig` to `WalWriterConfig`
2. Implement `CommitQueue` structure
3. Implement `GroupCommitCoordinator` with background thread
4. Add metrics tracking

### Phase 2: Integration
1. Modify `write_commit()` to use group commit when enabled
2. Add fallback to direct fsync when disabled
3. Ensure proper shutdown and cleanup
4. Handle error cases (coordinator failure, queue full, etc.)

### Phase 3: Testing
1. Unit tests for commit queue operations
2. Unit tests for batching logic
3. Integration tests for concurrent commits
4. Stress tests for high load scenarios
5. Correctness tests (durability, ordering)

### Phase 4: Benchmarking
1. Benchmark single-threaded commit throughput
2. Benchmark multi-threaded commit throughput
3. Measure latency distribution
4. Compare with/without group commit
5. Test different configuration parameters

## Configuration Recommendations

### High Throughput (Default)
```rust
GroupCommitConfig {
    enabled: true,
    max_batch_size: 100,
    max_wait_micros: 1000,  // 1ms
    min_batch_size: 10,
}
```

### Low Latency
```rust
GroupCommitConfig {
    enabled: false,
    // ... other fields ignored
}
```

### Balanced
```rust
GroupCommitConfig {
    enabled: true,
    max_batch_size: 50,
    max_wait_micros: 500,  // 0.5ms
    min_batch_size: 5,
}
```

## Error Handling

### Coordinator Failure
- If coordinator thread panics, fall back to direct fsync
- Log error and continue operation
- Optionally restart coordinator

### Queue Full
- If queue reaches capacity, block or return error
- Configurable behavior via `QueueFullPolicy`

### Fsync Failure
- Notify all pending commits with error
- Clear queue
- Maintain consistency

## Compatibility

### Backward Compatibility
- Group commit is opt-in via configuration
- Default behavior unchanged (sync_on_write=true)
- No changes to WAL file format
- No changes to recovery logic

### Forward Compatibility
- Metrics can be extended without breaking changes
- Configuration can be extended with new fields
- Batching strategy can be improved

## Performance Expectations

Based on typical database implementations:

- **Throughput**: 5-10x improvement under high concurrency
- **Latency**: Slight increase (1-2ms) due to batching delay
- **Fsync Reduction**: 10-100x fewer fsync calls
- **CPU Usage**: Minimal overhead from coordinator thread

## References

- PostgreSQL group commit implementation
- MySQL InnoDB group commit
- "The Design and Implementation of Modern Column-Oriented Database Systems" (Section on WAL)
- "Transaction Processing: Concepts and Techniques" by Gray & Reuter