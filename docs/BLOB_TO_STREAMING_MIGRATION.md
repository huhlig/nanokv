# BlobTable to Streaming API Migration Guide

## Overview

The `BlobTable` trait has been deprecated in favor of the unified `MutableTable` and `PointLookup` traits with streaming support. This guide helps you migrate existing blob storage code to the new API.

## Why Migrate?

### Problems with BlobTable

1. **Architectural Inconsistency**: Separate trait hierarchy violates ADR-012's unified table architecture
2. **API Duplication**: `put_blob` vs `put`, `get_blob` vs `get` - same operations, different names
3. **Memory Inefficiency**: All values must be loaded entirely into memory
4. **No MVCC Support**: Blob values don't participate in transactions
5. **Stub Implementations**: Regular tables must implement BlobTable just to return errors

### Benefits of Streaming API

1. **Unified Architecture**: Single trait hierarchy for all table types
2. **Memory Efficient**: Stream large values in chunks without full memory load
3. **MVCC Integration**: Blob values participate in snapshot isolation
4. **Consistent API**: Same methods for all value sizes
5. **Backward Compatible**: Existing slice-based API still works

## Migration Steps

### Step 1: Replace BlobTable with MutableTable

**Before:**
```rust
use crate::table::BlobTable;

fn store_blob(table: &mut dyn BlobTable, key: &[u8], data: &[u8]) -> Result<u64, Error> {
    table.put_blob(key, data)
}
```

**After:**
```rust
use crate::table::MutableTable;

fn store_blob(table: &mut dyn MutableTable, key: &[u8], data: &[u8]) -> Result<u64, Error> {
    table.put(key, data)
}
```

### Step 2: Use Streaming for Large Values

**Before (loads entire blob into memory):**
```rust
let large_data = vec![0u8; 100_000_000]; // 100MB
table.put_blob(key, &large_data)?;
```

**After (streams in chunks):**
```rust
use crate::table::{MutableTable, ValueStream};

struct FileValueStream {
    file: File,
    size: u64,
}

impl ValueStream for FileValueStream {
    fn read(&mut self, buf: &mut [u8]) -> TableResult<usize> {
        self.file.read(buf).map_err(|e| TableError::Io(e))
    }
    
    fn size_hint(&self) -> Option<u64> {
        Some(self.size)
    }
}

let mut stream = FileValueStream {
    file: File::open("large_file.dat")?,
    size: metadata.len(),
};

table.put_stream(key, &mut stream)?;
```

### Step 3: Stream Reads for Large Values

**Before (loads entire blob into memory):**
```rust
let data = table.get_blob(key)?.ok_or(Error::NotFound)?;
process_data(&data);
```

**After (streams in chunks):**
```rust
use crate::table::PointLookup;

let reader: &dyn PointLookup = ...;
if let Some(mut stream) = reader.get_stream(key, snapshot_lsn)? {
    let mut buffer = vec![0u8; 8192]; // 8KB chunks
    loop {
        let n = stream.read(&mut buffer)?;
        if n == 0 { break; }
        process_chunk(&buffer[..n]);
    }
} else {
    return Err(Error::NotFound);
}
```

### Step 4: Configure Value Storage Thresholds

Use `TableOptions` to control when values are stored inline vs. externally:

```rust
use crate::table::TableOptions;

let options = TableOptions {
    engine: TableEngineKind::BTree,
    key_encoding: KeyEncoding::RawBytes,
    compression: None,
    encryption: None,
    page_size: None,
    format_version: 1,
    max_inline_size: Some(4096),      // Store values < 4KB inline
    max_value_size: Some(1_000_000_000), // Max 1GB values
};

let table_id = db.create_table("large_values", options)?;
```

## API Comparison

### Writing Values

| BlobTable (Old) | MutableTable (New) | Notes |
|-----------------|-------------------|-------|
| `put_blob(key, data)` | `put(key, data)` | Returns bytes written |
| N/A | `put_stream(key, stream)` | For large values |
| `delete_blob(key)` | `delete(key)` | Same semantics |

### Reading Values

| BlobTable (Old) | PointLookup (New) | Notes |
|-----------------|-------------------|-------|
| `get_blob(key)` | `get(key, lsn)` | Requires snapshot LSN |
| N/A | `get_stream(key, lsn)` | For large values |
| `contains_blob(key)` | `contains(key, lsn)` | Requires snapshot LSN |
| `blob_size(key)` | Use `get_stream().size_hint()` | Via stream metadata |

### Metadata

| BlobTable (Old) | TableOptions (New) | Notes |
|-----------------|-------------------|-------|
| `max_inline_size()` | `options.max_inline_size` | Configuration |
| `max_blob_size()` | `options.max_value_size` | Configuration |
| `list_keys()` | Use `OrderedScan` trait | Standard iteration |

## Common Patterns

### Pattern 1: Small Values (< 4KB)

No changes needed - use `put()` and `get()` directly:

```rust
// Works the same as before
writer.put(key, small_value)?;
let value = reader.get(key, snapshot_lsn)?;
```

### Pattern 2: Medium Values (4KB - 1MB)

Use slice-based API, but consider streaming for very large values:

```rust
if value.len() > 100_000 {
    // Use streaming for values > 100KB
    let mut stream = SliceValueStream::new(value.to_vec());
    writer.put_stream(key, &mut stream)?;
} else {
    // Use direct put for smaller values
    writer.put(key, value)?;
}
```

### Pattern 3: Large Values (> 1MB)

Always use streaming to avoid memory pressure:

```rust
// Writing
let mut stream = create_stream_from_source(source);
writer.put_stream(key, &mut stream)?;

// Reading
if let Some(mut stream) = reader.get_stream(key, snapshot_lsn)? {
    let mut output = File::create("output.dat")?;
    let mut buffer = vec![0u8; 65536]; // 64KB chunks
    loop {
        let n = stream.read(&mut buffer)?;
        if n == 0 { break; }
        output.write_all(&buffer[..n])?;
    }
}
```

### Pattern 4: Streaming from Network

```rust
use crate::table::ValueSink;

struct NetworkValueSink {
    socket: TcpStream,
    total_written: u64,
}

impl ValueSink for NetworkValueSink {
    fn write(&mut self, buf: &[u8]) -> TableResult<usize> {
        let n = self.socket.write(buf)
            .map_err(|e| TableError::Io(e))?;
        self.total_written += n as u64;
        Ok(n)
    }
    
    fn finish(self) -> TableResult<u64> {
        Ok(self.total_written)
    }
}

// Stream value directly to network without buffering
if let Some(mut stream) = reader.get_stream(key, snapshot_lsn)? {
    let mut sink = NetworkValueSink {
        socket: connect_to_client()?,
        total_written: 0,
    };
    
    let mut buffer = vec![0u8; 8192];
    loop {
        let n = stream.read(&mut buffer)?;
        if n == 0 { break; }
        sink.write(&buffer[..n])?;
    }
    let total = sink.finish()?;
    println!("Streamed {} bytes", total);
}
```

## Testing Migration

### Unit Tests

Update tests to use new API:

```rust
#[test]
fn test_large_value_storage() {
    let mut table = create_test_table();
    let large_value = vec![42u8; 10_000_000]; // 10MB
    
    // Old way (deprecated)
    // table.put_blob(b"key", &large_value).unwrap();
    
    // New way
    let bytes_written = table.put(b"key", &large_value).unwrap();
    assert_eq!(bytes_written, large_value.len() as u64 + 16); // +16 for overhead
    
    // Verify with streaming read
    let reader = table.reader();
    let mut stream = reader.get_stream(b"key", LogSequenceNumber::MAX)
        .unwrap()
        .unwrap();
    
    let mut read_back = Vec::new();
    let mut buffer = vec![0u8; 8192];
    loop {
        let n = stream.read(&mut buffer).unwrap();
        if n == 0 { break; }
        read_back.extend_from_slice(&buffer[..n]);
    }
    
    assert_eq!(read_back, large_value);
}
```

### Integration Tests

Test streaming with real workloads:

```rust
#[test]
fn test_streaming_large_files() {
    let db = Database::open("test.db").unwrap();
    let table_id = db.create_table("files", TableOptions {
        engine: TableEngineKind::BTree,
        max_inline_size: Some(4096),
        max_value_size: Some(100_000_000),
        ..Default::default()
    }).unwrap();
    
    // Write large file using streaming
    let mut tx = db.begin_transaction(IsolationLevel::Serializable).unwrap();
    let mut writer = tx.writer(table_id).unwrap();
    
    let mut stream = FileValueStream::open("large_file.dat").unwrap();
    let bytes_written = writer.put_stream(b"file1", &mut stream).unwrap();
    
    tx.commit().unwrap();
    
    // Read back using streaming
    let tx = db.begin_transaction(IsolationLevel::Serializable).unwrap();
    let reader = tx.reader(table_id).unwrap();
    
    let mut stream = reader.get_stream(b"file1", tx.snapshot_lsn())
        .unwrap()
        .unwrap();
    
    let mut output = File::create("output.dat").unwrap();
    let mut buffer = vec![0u8; 65536];
    let mut total_read = 0u64;
    
    loop {
        let n = stream.read(&mut buffer).unwrap();
        if n == 0 { break; }
        output.write_all(&buffer[..n]).unwrap();
        total_read += n as u64;
    }
    
    assert_eq!(total_read, bytes_written);
}
```

## Performance Considerations

### Memory Usage

**Before (BlobTable):**
- 100MB blob = 100MB memory usage
- Multiple concurrent operations = N × 100MB

**After (Streaming):**
- 100MB blob = 8KB buffer × concurrent operations
- 10 concurrent operations = 80KB memory usage

### Throughput

Streaming API provides similar or better throughput:
- Small values (< 4KB): Same performance (direct put/get)
- Medium values (4KB - 1MB): Slightly better (reduced allocations)
- Large values (> 1MB): Significantly better (no memory pressure)

### Latency

- First byte latency: Similar for both APIs
- Total operation time: Better for streaming (no full buffer allocation)

## Troubleshooting

### Issue: "Blob tables do not support put"

**Cause:** Trying to use `put()` on a BlobTable implementation.

**Solution:** Migrate to MutableTable or use `put_blob()` temporarily.

### Issue: Out of memory with large values

**Cause:** Using `get()` instead of `get_stream()` for large values.

**Solution:** Use `get_stream()` and process in chunks:

```rust
// Bad: Loads entire value into memory
let value = reader.get(key, lsn)?.unwrap();

// Good: Streams in chunks
let mut stream = reader.get_stream(key, lsn)?.unwrap();
let mut buffer = vec![0u8; 8192];
while stream.read(&mut buffer)? > 0 {
    // Process chunk
}
```

### Issue: Snapshot LSN required

**Cause:** PointLookup requires snapshot LSN for MVCC.

**Solution:** Get LSN from transaction or use MAX for latest:

```rust
// In transaction
let lsn = tx.snapshot_lsn();
let value = reader.get(key, lsn)?;

// Outside transaction (latest committed)
let value = reader.get(key, LogSequenceNumber::MAX)?;
```

## Timeline

- **v0.1.0**: BlobTable deprecated, streaming API added
- **v0.2.0**: BlobTable implementations marked as deprecated
- **v0.3.0**: BlobTable trait and implementations removed

## See Also

- [Streaming API Implementation](STREAMING_API_IMPLEMENTATION.md)
- [ADR-012: Unified Table Architecture](adrs/012-unified-table-architecture.md)
- [Architecture Overview](ARCHITECTURE.md)