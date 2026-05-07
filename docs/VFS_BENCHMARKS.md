# VFS Library Benchmarks

This document describes the benchmark suite for the Virtual File System (VFS) library and provides performance analysis.

## Running Benchmarks

### Run All Benchmarks
```bash
cargo bench --bench vfs_benchmarks
```

### Run Quick Benchmarks (fewer samples)
```bash
cargo bench --bench vfs_benchmarks -- --quick
```

### Run Specific Benchmark Group
```bash
cargo bench --bench vfs_benchmarks -- file_creation
cargo bench --bench vfs_benchmarks -- sequential_write
cargo bench --bench vfs_benchmarks -- random_access
```

### Save Baseline for Comparison
```bash
cargo bench --bench vfs_benchmarks -- --save-baseline main
```

### Compare Against Baseline
```bash
cargo bench --bench vfs_benchmarks -- --baseline main
```

## Benchmark Categories

### 1. File Creation (`file_creation`)
Measures the time to create a new file.

**Operations Tested:**
- `create_file()` for MemoryFileSystem
- `create_file()` for LocalFileSystem

**Typical Results:**
- MemoryFS: ~390 ns per file
- LocalFS: ~280 µs per file (700x slower due to OS overhead)

### 2. Sequential Write (`sequential_write`)
Measures throughput for sequential write operations at various buffer sizes.

**Buffer Sizes:** 1KB, 4KB, 16KB, 64KB

**Operations Tested:**
- Create file, write data, remove file

**Typical Results (64KB writes):**
- MemoryFS: ~28 GiB/s
- LocalFS: ~120 MiB/s (230x slower)

**Key Insights:**
- MemoryFS throughput scales well with buffer size
- LocalFS limited by disk I/O and OS overhead
- Larger buffers improve throughput for both implementations

### 3. Sequential Read (`sequential_read`)
Measures throughput for sequential read operations at various buffer sizes.

**Buffer Sizes:** 1KB, 4KB, 16KB, 64KB

**Operations Tested:**
- Open file, read all data

**Typical Results (64KB reads):**
- MemoryFS: ~32 GiB/s
- LocalFS: ~1.7 GiB/s (19x slower)

**Key Insights:**
- MemoryFS reads are extremely fast (memory bandwidth limited)
- LocalFS reads benefit from OS page cache
- Read performance generally better than write performance

### 4. Random Access (`random_access`)
Measures performance of offset-based read/write operations.

**Operations Tested:**
- `read_at_offset()` - Read without moving cursor
- `write_to_offset()` - Write without moving cursor

**Typical Results:**
- MemoryFS read_at_offset: ~49 ns
- LocalFS read_at_offset: ~6.3 µs (128x slower)
- MemoryFS write_to_offset: ~13 ns
- LocalFS write_to_offset: ~2.7 µs (208x slower)

**Key Insights:**
- Offset operations are very efficient in MemoryFS
- LocalFS requires system calls for each operation
- Write operations slightly slower than reads

### 5. Seek Operations (`seek_operations`)
Measures performance of cursor positioning operations.

**Operations Tested:**
- `seek(SeekFrom::Start)` - Seek from beginning
- `seek(SeekFrom::End)` - Seek from end

**Typical Results:**
- MemoryFS seek_start: ~4.3 ns
- LocalFS seek_start: ~222 ns (52x slower)
- MemoryFS seek_end: ~3.8 ns
- LocalFS seek_end: ~1.5 µs (395x slower)

**Key Insights:**
- Seek operations are nearly free in MemoryFS
- LocalFS seek_end requires file size query
- Seek_start faster than seek_end for LocalFS

### 6. File Resize (`file_resize`)
Measures performance of file size modification operations.

**Operations Tested:**
- Growing files (0 → 64KB)
- Shrinking files (64KB → 1KB)

**Typical Results:**
- MemoryFS grow: ~1.2 µs
- LocalFS grow: ~500 µs (417x slower)
- MemoryFS shrink: ~2.1 µs
- LocalFS shrink: ~616 µs (293x slower)

**Key Insights:**
- Growing files requires memory allocation
- Shrinking slightly slower than growing (data copy)
- LocalFS resize involves filesystem metadata updates

### 7. Directory Operations (`directory_operations`)
Measures performance of directory creation operations.

**Operations Tested:**
- `create_directory()` - Single directory
- `create_directory_all()` - Nested directories (5 levels)

**Typical Results:**
- MemoryFS create_dir: ~379 ns
- LocalFS create_dir: ~279 µs (736x slower)
- MemoryFS create_dir_all: ~1.2 µs
- LocalFS create_dir_all: ~265 µs (221x slower)

**Key Insights:**
- Directory operations similar cost to file creation
- Nested creation scales linearly with depth
- LocalFS overhead dominates for simple operations

### 8. Metadata Operations (`metadata_operations`)
Measures performance of metadata query operations.

**Operations Tested:**
- `exists()` - Check path existence
- `filesize()` - Get file size

**Typical Results:**
- MemoryFS exists: ~6.3 ns
- LocalFS exists: ~24.5 µs (3,889x slower)
- MemoryFS filesize: ~9.2 ns
- LocalFS filesize: ~27.2 µs (2,957x slower)

**Key Insights:**
- Metadata queries extremely fast in MemoryFS
- LocalFS requires system calls and filesystem access
- Exists check slightly faster than filesize query

### 9. Mixed Workload (`mixed_workload`)
Measures performance of realistic usage patterns.

**Operations Tested:**
- Create file → Write 4KB → Read 4KB → Delete file

**Typical Results:**
- MemoryFS: ~456 ns per cycle
- LocalFS: ~1.02 ms per cycle (2,237x slower)

**Key Insights:**
- Real-world workloads show cumulative overhead
- MemoryFS suitable for high-frequency operations
- LocalFS overhead dominated by file creation/deletion

## Performance Summary

### MemoryFileSystem Characteristics
- **Strengths:**
  - Extremely fast for all operations (nanosecond scale)
  - No I/O overhead
  - Consistent performance
  - Ideal for testing and caching
  
- **Limitations:**
  - Limited by available RAM
  - Data lost on process termination
  - No persistence

### LocalFileSystem Characteristics
- **Strengths:**
  - Persistent storage
  - Leverages OS page cache
  - Handles large files
  - Real filesystem semantics
  
- **Limitations:**
  - 100-1000x slower than MemoryFS
  - Subject to disk I/O bottlenecks
  - OS overhead for each operation
  - Performance varies by storage device

## Performance Comparison Table

| Operation | MemoryFS | LocalFS | Ratio |
|-----------|----------|---------|-------|
| File Creation | 390 ns | 280 µs | 718x |
| Sequential Write (64KB) | 28 GiB/s | 120 MiB/s | 232x |
| Sequential Read (64KB) | 32 GiB/s | 1.7 GiB/s | 19x |
| Random Read | 49 ns | 6.3 µs | 128x |
| Random Write | 13 ns | 2.7 µs | 208x |
| Seek Start | 4.3 ns | 222 ns | 52x |
| Seek End | 3.8 ns | 1.5 µs | 395x |
| File Grow | 1.2 µs | 500 µs | 417x |
| File Shrink | 2.1 µs | 616 µs | 293x |
| Create Directory | 379 ns | 279 µs | 736x |
| Exists Check | 6.3 ns | 24.5 µs | 3,889x |
| File Size Query | 9.2 ns | 27.2 µs | 2,957x |
| Mixed Workload | 456 ns | 1.02 ms | 2,237x |

## Optimization Recommendations

### For MemoryFileSystem
1. Already highly optimized
2. Consider pre-allocation for known file sizes
3. Use for temporary data and testing
4. Ideal for caching frequently accessed data

### For LocalFileSystem
1. Use larger buffer sizes for sequential I/O
2. Batch file operations when possible
3. Leverage OS page cache by keeping files open
4. Consider using MemoryFS for temporary files
5. Use buffered I/O for small operations

### General Best Practices
1. Choose appropriate buffer sizes (4KB-64KB optimal)
2. Minimize file open/close cycles
3. Use offset operations for random access
4. Batch metadata queries
5. Consider hybrid approach (MemoryFS + LocalFS)

## Benchmark Methodology

- **Tool:** Criterion.rs v0.8
- **Measurement:** Wall-clock time
- **Samples:** 100 iterations per benchmark (default)
- **Warmup:** Automatic warmup phase
- **Statistical Analysis:** Outlier detection and confidence intervals
- **Platform:** Results may vary by OS and hardware

## Interpreting Results

### Time Measurements
- **ns (nanoseconds):** 10⁻⁹ seconds
- **µs (microseconds):** 10⁻⁶ seconds
- **ms (milliseconds):** 10⁻³ seconds

### Throughput Measurements
- **GiB/s:** Gibibytes per second (2³⁰ bytes/s)
- **MiB/s:** Mebibytes per second (2²⁰ bytes/s)

### Confidence Intervals
- Criterion reports mean time with confidence intervals
- Smaller intervals indicate more consistent performance
- Large intervals suggest high variance

## Future Benchmark Additions

1. **Concurrent Access:** Multi-threaded benchmarks
2. **Large Files:** Performance with files >1GB
3. **Many Files:** Scalability with 10,000+ files
4. **Fragmentation:** Performance with fragmented files
5. **Platform Comparison:** Linux vs Windows vs macOS
6. **Storage Types:** SSD vs HDD vs NVMe
7. **Network FS:** Performance over network filesystems

## Continuous Performance Monitoring

To track performance regressions:

```bash
# Save baseline before changes
cargo bench --bench vfs_benchmarks -- --save-baseline before

# Make changes...

# Compare after changes
cargo bench --bench vfs_benchmarks -- --baseline before
```

Criterion will highlight any significant performance changes.

## Conclusion

The VFS benchmark suite provides comprehensive performance analysis of both MemoryFileSystem and LocalFileSystem implementations. MemoryFileSystem offers exceptional performance for in-memory operations, while LocalFileSystem provides persistent storage with reasonable performance for disk-based operations. Choose the appropriate implementation based on your use case requirements for speed vs. persistence.