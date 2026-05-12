# Archived Tests

This directory contains tests for deprecated functionality that has been superseded by newer implementations.

## blob_table_tests.rs

**Archived**: 2026-05-12  
**Reason**: BlobTable trait deprecated in favor of unified MutableTable/PointLookup with streaming support

### Migration

The BlobTable trait has been deprecated and replaced with the standard table traits:

- `BlobTable::put_blob()` → `MutableTable::put()` or `MutableTable::put_stream()`
- `BlobTable::get_blob()` → `PointLookup::get()` or `PointLookup::get_stream()`
- `BlobTable::delete_blob()` → `MutableTable::delete()`
- `BlobTable::contains_blob()` → `PointLookup::contains()`
- `BlobTable::blob_size()` → Use `get_stream().size_hint()`
- `BlobTable::max_inline_size()` → `MutableTable::max_inline_size()`
- `BlobTable::max_blob_size()` → `MutableTable::max_value_size()`

See `docs/BLOB_TO_STREAMING_MIGRATION.md` for detailed migration guide.

### Why Keep These Tests?

These tests are kept in archive for:
1. Reference for migration
2. Verification that old API behavior is preserved
3. Historical documentation

The blob implementations (`src/table/blob/`) remain in the codebase but are deprecated.
They will be removed in a future version once all users have migrated.