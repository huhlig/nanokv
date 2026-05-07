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

//! WAL reader - Handles reading records from the WAL file

use crate::vfs::{File, FileSystem};
use crate::wal::{Lsn, WalError, WalRecord, WalResult};

/// WAL reader - Reads records from the WAL file
pub struct WalReader<FS: FileSystem> {
    /// VFS file handle
    file: FS::File,
    /// Current read offset
    offset: u64,
    /// File size
    file_size: u64,
}

impl<FS: FileSystem> WalReader<FS> {
    /// Open a WAL file for reading
    pub fn open(fs: &FS, path: &str) -> WalResult<Self> {
        let file = fs.open_file(path)?;
        let file_size = file.get_size()?;

        Ok(Self {
            file,
            offset: 0,
            file_size,
        })
    }

    /// Read the next record from the WAL
    pub fn read_next(&mut self) -> WalResult<Option<WalRecord>> {
        // Check if we've reached the end
        if self.offset >= self.file_size {
            return Ok(None);
        }

        // Read the record header to determine size
        // Magic (4) + LSN (8) + Timestamp (8) + Type (1) + Length (4) = 25 bytes minimum
        let mut header_buf = vec![0u8; 25];
        let bytes_read = self.file.read_at_offset(self.offset, &mut header_buf)?;

        if bytes_read < 25 {
            // Incomplete record at end of file
            return Ok(None);
        }

        // Check magic number
        if &header_buf[0..4] != b"WALR" {
            return Err(WalError::CorruptedWal(format!(
                "Invalid magic at offset {}",
                self.offset
            )));
        }

        // Extract data length
        let data_len = u32::from_le_bytes(header_buf[21..25].try_into().unwrap()) as usize;

        // Calculate total record size: header (25) + data + checksum (32)
        let total_size = 25 + data_len + 32;

        // Read the complete record
        let mut record_buf = vec![0u8; total_size];
        let bytes_read = self.file.read_at_offset(self.offset, &mut record_buf)?;

        if bytes_read < total_size {
            // Incomplete record
            return Err(WalError::CorruptedWal(format!(
                "Incomplete record at offset {}",
                self.offset
            )));
        }

        // Deserialize the record
        let record = WalRecord::from_bytes(&record_buf)?;

        // Update offset
        self.offset += total_size as u64;

        Ok(Some(record))
    }

    /// Read all records from the WAL
    pub fn read_all(&mut self) -> WalResult<Vec<WalRecord>> {
        let mut records = Vec::new();

        while let Some(record) = self.read_next()? {
            records.push(record);
        }

        Ok(records)
    }

    /// Seek to a specific LSN
    pub fn seek_to_lsn(&mut self, target_lsn: Lsn) -> WalResult<()> {
        // Reset to beginning
        self.offset = 0;

        // Scan until we find the target LSN
        while let Some(record) = self.read_next()? {
            if record.lsn == target_lsn {
                // Move back to the start of this record
                let record_bytes = record.to_bytes()?;
                self.offset -= record_bytes.len() as u64;
                return Ok(());
            }
        }

        Err(WalError::InvalidRecord(format!(
            "LSN {} not found",
            target_lsn
        )))
    }

    /// Get current read offset
    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    /// Get file size
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Check if we've reached the end of the file
    pub fn is_eof(&self) -> bool {
        self.offset >= self.file_size
    }

    /// Reset to the beginning of the file
    pub fn reset(&mut self) {
        self.offset = 0;
    }
}

/// Iterator over WAL records
pub struct WalRecordIterator<FS: FileSystem> {
    reader: WalReader<FS>,
}

impl<FS: FileSystem> WalRecordIterator<FS> {
    /// Create a new iterator
    pub fn new(reader: WalReader<FS>) -> Self {
        Self { reader }
    }
}

impl<FS: FileSystem> Iterator for WalRecordIterator<FS> {
    type Item = WalResult<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_next() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemoryFileSystem;
    use crate::wal::{RecordData, WalWriter, WalWriterConfig, WriteOpType};

    fn create_test_wal() -> (MemoryFileSystem, String) {
        let fs = MemoryFileSystem::new();
        let path = "test.wal".to_string();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, &path, config).unwrap();

        // Write some test records
        writer.write_begin(1).unwrap();
        writer
            .write_operation(
                1,
                "table1".to_string(),
                WriteOpType::Put,
                b"key1".to_vec(),
                b"value1".to_vec(),
            )
            .unwrap();
        writer.write_commit(1).unwrap();

        writer.write_begin(2).unwrap();
        writer
            .write_operation(
                2,
                "table2".to_string(),
                WriteOpType::Delete,
                b"key2".to_vec(),
                vec![],
            )
            .unwrap();
        writer.write_rollback(2).unwrap();

        writer.flush().unwrap();

        (fs, path)
    }

    #[test]
    fn test_read_records() {
        let (fs, path) = create_test_wal();
        let mut reader = WalReader::open(&fs, &path).unwrap();

        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 6); // 2 begin, 2 write, 1 commit, 1 rollback

        // Check first record
        assert_eq!(records[0].lsn, 1);
        assert!(matches!(records[0].data, RecordData::Begin { txn_id: 1 }));

        // Check second record
        assert_eq!(records[1].lsn, 2);
        if let RecordData::Write {
            txn_id,
            table,
            op_type,
            key,
            value,
        } = &records[1].data
        {
            assert_eq!(*txn_id, 1);
            assert_eq!(table, "table1");
            assert_eq!(*op_type, WriteOpType::Put);
            assert_eq!(key, b"key1");
            assert_eq!(value, b"value1");
        } else {
            panic!("Expected Write record");
        }

        // Check third record
        assert_eq!(records[2].lsn, 3);
        assert!(matches!(records[2].data, RecordData::Commit { txn_id: 1 }));
    }

    #[test]
    fn test_read_next() {
        let (fs, path) = create_test_wal();
        let mut reader = WalReader::open(&fs, &path).unwrap();

        let record1 = reader.read_next().unwrap().unwrap();
        assert_eq!(record1.lsn, 1);

        let record2 = reader.read_next().unwrap().unwrap();
        assert_eq!(record2.lsn, 2);

        let record3 = reader.read_next().unwrap().unwrap();
        assert_eq!(record3.lsn, 3);
    }

    #[test]
    fn test_seek_to_lsn() {
        let (fs, path) = create_test_wal();
        let mut reader = WalReader::open(&fs, &path).unwrap();

        reader.seek_to_lsn(3).unwrap();
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.lsn, 3);
    }

    #[test]
    fn test_reset() {
        let (fs, path) = create_test_wal();
        let mut reader = WalReader::open(&fs, &path).unwrap();

        // Read some records
        reader.read_next().unwrap();
        reader.read_next().unwrap();

        // Reset and read again
        reader.reset();
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.lsn, 1);
    }

    #[test]
    fn test_is_eof() {
        let (fs, path) = create_test_wal();
        let mut reader = WalReader::open(&fs, &path).unwrap();

        assert!(!reader.is_eof());

        // Read all records
        while reader.read_next().unwrap().is_some() {}

        assert!(reader.is_eof());
    }

    #[test]
    fn test_iterator() {
        let (fs, path) = create_test_wal();
        let reader = WalReader::open(&fs, &path).unwrap();
        let iter = WalRecordIterator::new(reader);

        let records: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 6);
    }
}

// Made with Bob
