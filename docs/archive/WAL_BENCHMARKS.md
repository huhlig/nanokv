# WAL Compression and Encryption Benchmarks

## Overview

This document describes the compression and encryption benchmarks added to the WAL (Write-Ahead Log) implementation in `benches/wal_benchmarks.rs`.

## Benchmark Categories

### 1. Compression Benchmarks (`bench_compression`)

Tests WAL performance with different compression algorithms:

- **None**: No compression (baseline)
- **LZ4**: Fast compression with moderate compression ratio
- **Zstd**: Slower compression with better compression ratio

**Test Scenarios:**
- Write operations with compressible data (repeated patterns)
- Read operations (recovery) with compressed data
- Value sizes: 256, 1024, 4096 bytes

**Key Metrics:**
- Write throughput (MiB/s or GiB/s)
- Read throughput during recovery
- Compression overhead vs baseline

### 2. Encryption Benchmarks (`bench_encryption`)

Tests WAL performance with AES-256-GCM encryption:

- **Unencrypted**: No encryption (baseline)
- **Encrypted**: AES-256-GCM encryption

**Test Scenarios:**
- Write operations with encryption enabled
- Read operations (recovery) with encrypted data
- Value sizes: 256, 1024, 4096 bytes

**Key Metrics:**
- Write throughput with encryption
- Read throughput with decryption
- Encryption overhead vs baseline

### 3. Combined Compression + Encryption Benchmarks (`bench_compression_and_encryption`)

Tests WAL performance with both compression and encryption enabled:

- **Compression types**: None, LZ4, Zstd
- **Encryption**: AES-256-GCM (always enabled)
- **Value size**: 4096 bytes (compressible data)

**Test Scenarios:**
- Write operations with compression then encryption
- Read operations with decryption then decompression

**Key Metrics:**
- Combined overhead
- Interaction between compression and encryption
- Optimal compression algorithm for encrypted data

## Running the Benchmarks

### Run All WAL Benchmarks
```bash
cargo bench --bench wal_benchmarks
```

### Run Specific Benchmark Groups

**Compression only:**
```bash
cargo bench --bench wal_benchmarks -- compression
```

**Encryption only:**
```bash
cargo bench --bench wal_benchmarks -- encryption
```

**Combined compression and encryption:**
```bash
cargo bench --bench wal_benchmarks -- compression_and_encryption
```

### Run Specific Test Cases

**Test LZ4 compression write performance:**
```bash
cargo bench --bench wal_benchmarks -- compression/memory_fs_write_lz4
```

**Test encrypted write performance:**
```bash
cargo bench --bench wal_benchmarks -- encryption/memory_fs_write_encrypted
```

**Test LZ4 + encryption:**
```bash
cargo bench --bench wal_benchmarks -- compression_and_encryption/memory_fs_write_lz4_encrypted
```

## Sample Results

### Compression Benchmarks (256 bytes)
```
compression/memory_fs_write_none/256
    time:   [854.85 ns 861.65 ns 872.18 ns]
    thrpt:  [279.92 MiB/s 283.34 MiB/s 285.59 MiB/s]
```

### Encryption Benchmarks (256 bytes)
```
encryption/memory_fs_write_encrypted/256
    time:   [1.2820 µs 1.3104 µs 1.3432 µs]
    thrpt:  [181.77 MiB/s 186.31 MiB/s 190.43 MiB/s]
```

### Combined Benchmarks (4096 bytes)
```
compression_and_encryption/memory_fs_write_lz4_encrypted
    time:   [1.5922 µs 1.5969 µs 1.6041 µs]
    thrpt:  [2.3780 GiB/s 2.3888 GiB/s 2.3958 GiB/s]
```

## Performance Analysis

### Compression Performance

- **LZ4**: Minimal overhead, excellent for write-heavy workloads
- **Zstd**: Better compression ratio, suitable when storage is a concern
- **None**: Baseline performance, fastest but no space savings

### Encryption Performance

- **AES-256-GCM**: ~50% overhead compared to unencrypted (256 bytes)
- Overhead decreases with larger value sizes due to fixed per-record costs
- Provides strong security with authenticated encryption

### Combined Performance

- Compression before encryption is optimal
- LZ4 + encryption provides good balance of speed and security
- Zstd + encryption maximizes compression at cost of throughput

## Implementation Details

### Configuration

Benchmarks use `WalWriterConfig` to enable features:

```rust
let mut config = WalWriterConfig::default();
config.compression = CompressionType::Lz4;
config.encryption = EncryptionType::Aes256Gcm;
config.encryption_key = Some([0x42u8; 32]);
```

### Test Data

- **Compressible data**: Repeated patterns (e.g., "The quick brown fox...")
- **Random data**: For encryption benchmarks (less compressible)
- **Multiple sizes**: 256, 1024, 4096 bytes to test scaling

### Benchmark Structure

Each benchmark follows this pattern:
1. Create WAL writer with specific configuration
2. Write test data (10 records for read benchmarks)
3. Measure operation time
4. Calculate throughput

## Recommendations

### For Production Use

1. **High-throughput systems**: Use LZ4 compression or no compression
2. **Storage-constrained systems**: Use Zstd compression
3. **Security-required systems**: Enable AES-256-GCM encryption
4. **Balanced approach**: LZ4 + AES-256-GCM provides good performance with security

### Benchmark Interpretation

- Focus on throughput (MiB/s or GiB/s) for write-heavy workloads
- Consider both write and read performance for recovery scenarios
- Test with your actual data patterns (compression ratios vary)

## Future Work

- [ ] Add benchmarks for different buffer sizes
- [ ] Test with real-world data patterns
- [ ] Benchmark checkpoint performance with compression/encryption
- [ ] Add benchmarks for concurrent writes
- [ ] Test recovery performance with large WAL files

## Related Documentation

- [WAL Implementation](WAL_IMPLEMENTATION.md)
- [Pager Compression/Encryption Benchmarks](PAGER_COMPRESSION_ENCRYPTION_BENCHMARKS.md)
- [Compression/Encryption Summary](COMPRESSION_ENCRYPTION_SUMMARY.md)