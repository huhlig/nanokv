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

//! Benchmarks for WAL (Write-Ahead Log) implementation

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nanokv::vfs::{LocalFileSystem, MemoryFileSystem};
use nanokv::wal::{
    WalReader, WalRecordIterator, WalRecovery, WalWriter, WalWriterConfig, WriteOpType,
};
use std::hint::black_box;

// ============================================================================
// WAL Writer Creation Benchmarks
// ============================================================================

fn bench_wal_writer_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_writer_creation");

    group.bench_function("memory_fs", |b| {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let mut counter = 0;
        b.iter(|| {
            let path = format!("/bench_{}.wal", counter);
            counter += 1;
            let writer = WalWriter::create(&fs, &path, config.clone()).unwrap();
            black_box(writer);
        });
    });

    group.bench_function("local_fs", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path());
        let config = WalWriterConfig::default();
        let mut counter = 0;
        b.iter(|| {
            let path = format!("/bench_{}.wal", counter);
            counter += 1;
            let writer = WalWriter::create(&fs, &path, config.clone()).unwrap();
            black_box(writer);
        });
    });

    group.finish();
}

// ============================================================================
// Transaction Benchmarks
// ============================================================================

fn bench_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    group.bench_function("memory_fs_begin_commit", |b| {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
        let mut txn_id = 1;

        b.iter(|| {
            writer.write_begin(txn_id).unwrap();
            writer.write_commit(txn_id).unwrap();
            txn_id += 1;
        });
    });

    group.bench_function("local_fs_begin_commit", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path());
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
        let mut txn_id = 1;

        b.iter(|| {
            writer.write_begin(txn_id).unwrap();
            writer.write_commit(txn_id).unwrap();
            txn_id += 1;
        });
    });

    group.bench_function("memory_fs_begin_rollback", |b| {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
        let mut txn_id = 1;

        b.iter(|| {
            writer.write_begin(txn_id).unwrap();
            writer.write_rollback(txn_id).unwrap();
            txn_id += 1;
        });
    });

    group.finish();
}

// ============================================================================
// Write Operation Benchmarks
// ============================================================================

fn bench_write_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_operations");

    for value_size in [64, 256, 1024, 4096].iter() {
        group.throughput(Throughput::Bytes(*value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("memory_fs_put", value_size),
            value_size,
            |b, &value_size| {
                let fs = MemoryFileSystem::new();
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
                let value = vec![0xAB; value_size];
                let mut counter = 0;

                // Begin a transaction for the benchmark
                writer.write_begin(1).unwrap();

                b.iter(|| {
                    let key = format!("key_{}", counter).into_bytes();
                    counter += 1;
                    writer
                        .write_operation(
                            1,
                            "test".to_string(),
                            WriteOpType::Put,
                            key,
                            value.clone(),
                        )
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("local_fs_put", value_size),
            value_size,
            |b, &value_size| {
                let temp_dir = tempfile::tempdir().unwrap();
                let fs = LocalFileSystem::new(temp_dir.path());
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
                let value = vec![0xAB; value_size];
                let mut counter = 0;

                // Begin a transaction for the benchmark
                writer.write_begin(1).unwrap();

                b.iter(|| {
                    let key = format!("key_{}", counter).into_bytes();
                    counter += 1;
                    writer
                        .write_operation(
                            1,
                            "test".to_string(),
                            WriteOpType::Put,
                            key,
                            value.clone(),
                        )
                        .unwrap();
                });
            },
        );
    }

    group.bench_function("memory_fs_delete", |b| {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
        let mut counter = 0;

        // Begin a transaction for the benchmark
        writer.write_begin(1).unwrap();

        b.iter(|| {
            let key = format!("key_{}", counter).into_bytes();
            counter += 1;
            writer
                .write_operation(1, "test".to_string(), WriteOpType::Delete, key, vec![])
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Flush Benchmarks
// ============================================================================

fn bench_flush_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_operations");

    for num_writes in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("memory_fs_flush_after_writes", num_writes),
            num_writes,
            |b, &num_writes| {
                let fs = MemoryFileSystem::new();
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                b.iter(|| {
                    writer.write_begin(1).unwrap();
                    for i in 0..num_writes {
                        writer
                            .write_operation(
                                1,
                                "test".to_string(),
                                WriteOpType::Put,
                                format!("key_{}", i).into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap();
                    }
                    writer.write_commit(1).unwrap();
                    writer.flush().unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("local_fs_flush_after_writes", num_writes),
            num_writes,
            |b, &num_writes| {
                let temp_dir = tempfile::tempdir().unwrap();
                let fs = LocalFileSystem::new(temp_dir.path());
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                b.iter(|| {
                    writer.write_begin(1).unwrap();
                    for i in 0..num_writes {
                        writer
                            .write_operation(
                                1,
                                "test".to_string(),
                                WriteOpType::Put,
                                format!("key_{}", i).into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap();
                    }
                    writer.write_commit(1).unwrap();
                    writer.flush().unwrap();
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// WAL Reader Benchmarks
// ============================================================================

fn bench_wal_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_reader");

    for num_records in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("memory_fs_read_all", num_records),
            num_records,
            |b, &num_records| {
                let fs = MemoryFileSystem::new();
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                // Write test data
                for i in 0..num_records {
                    writer.write_begin(i as u64).unwrap();
                    writer
                        .write_operation(
                            i as u64,
                            "test".to_string(),
                            WriteOpType::Put,
                            format!("key_{}", i).into_bytes(),
                            b"value".to_vec(),
                        )
                        .unwrap();
                    writer.write_commit(i as u64).unwrap();
                }
                writer.flush().unwrap();
                drop(writer);

                b.iter(|| {
                    let reader = WalReader::open(&fs, "/bench.wal", None).unwrap();
                    let iter = WalRecordIterator::new(reader);
                    let records: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
                    black_box(records);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("local_fs_read_all", num_records),
            num_records,
            |b, &num_records| {
                let temp_dir = tempfile::tempdir().unwrap();
                let fs = LocalFileSystem::new(temp_dir.path());
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                // Write test data
                for i in 0..num_records {
                    writer.write_begin(i as u64).unwrap();
                    writer
                        .write_operation(
                            i as u64,
                            "test".to_string(),
                            WriteOpType::Put,
                            format!("key_{}", i).into_bytes(),
                            b"value".to_vec(),
                        )
                        .unwrap();
                    writer.write_commit(i as u64).unwrap();
                }
                writer.flush().unwrap();
                drop(writer);

                b.iter(|| {
                    let reader = WalReader::open(&fs, "/bench.wal", None).unwrap();
                    let iter = WalRecordIterator::new(reader);
                    let records: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
                    black_box(records);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Recovery Benchmarks
// ============================================================================

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");

    for num_transactions in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("memory_fs_recover", num_transactions),
            num_transactions,
            |b, &num_transactions| {
                let fs = MemoryFileSystem::new();
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                // Write test data
                for i in 0..num_transactions {
                    writer.write_begin(i as u64).unwrap();
                    writer
                        .write_operation(
                            i as u64,
                            "test".to_string(),
                            WriteOpType::Put,
                            format!("key_{}", i).into_bytes(),
                            b"value".to_vec(),
                        )
                        .unwrap();
                    writer.write_commit(i as u64).unwrap();
                }
                writer.flush().unwrap();
                drop(writer);

                b.iter(|| {
                    let result = WalRecovery::recover(&fs, "/bench.wal").unwrap();
                    black_box(result);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("local_fs_recover", num_transactions),
            num_transactions,
            |b, &num_transactions| {
                let temp_dir = tempfile::tempdir().unwrap();
                let fs = LocalFileSystem::new(temp_dir.path());
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

                // Write test data
                for i in 0..num_transactions {
                    writer.write_begin(i as u64).unwrap();
                    writer
                        .write_operation(
                            i as u64,
                            "test".to_string(),
                            WriteOpType::Put,
                            format!("key_{}", i).into_bytes(),
                            b"value".to_vec(),
                        )
                        .unwrap();
                    writer.write_commit(i as u64).unwrap();
                }
                writer.flush().unwrap();
                drop(writer);

                b.iter(|| {
                    let result = WalRecovery::recover(&fs, "/bench.wal").unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Checkpoint Benchmarks
// ============================================================================

fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint");

    group.bench_function("memory_fs_checkpoint", |b| {
        let fs = MemoryFileSystem::new();
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

        b.iter(|| {
            let lsn = writer.write_checkpoint().unwrap();
            black_box(lsn);
        });
    });

    group.bench_function("local_fs_checkpoint", |b| {
        let temp_dir = tempfile::tempdir().unwrap();
        let fs = LocalFileSystem::new(temp_dir.path());
        let config = WalWriterConfig::default();
        let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();

        b.iter(|| {
            let lsn = writer.write_checkpoint().unwrap();
            black_box(lsn);
        });
    });

    group.finish();
}

// ============================================================================
// Complete Transaction Benchmarks
// ============================================================================

fn bench_complete_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("complete_transactions");

    for num_writes in [1, 5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::new("memory_fs_txn_with_writes", num_writes),
            num_writes,
            |b, &num_writes| {
                let fs = MemoryFileSystem::new();
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
                let mut txn_id = 1;

                b.iter(|| {
                    writer.write_begin(txn_id).unwrap();
                    for i in 0..num_writes {
                        writer
                            .write_operation(
                                txn_id,
                                "test".to_string(),
                                WriteOpType::Put,
                                format!("key_{}", i).into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap();
                    }
                    writer.write_commit(txn_id).unwrap();
                    txn_id += 1;
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("local_fs_txn_with_writes", num_writes),
            num_writes,
            |b, &num_writes| {
                let temp_dir = tempfile::tempdir().unwrap();
                let fs = LocalFileSystem::new(temp_dir.path());
                let config = WalWriterConfig::default();
                let writer = WalWriter::create(&fs, "/bench.wal", config).unwrap();
                let mut txn_id = 1;

                b.iter(|| {
                    writer.write_begin(txn_id).unwrap();
                    for i in 0..num_writes {
                        writer
                            .write_operation(
                                txn_id,
                                "test".to_string(),
                                WriteOpType::Put,
                                format!("key_{}", i).into_bytes(),
                                b"value".to_vec(),
                            )
                            .unwrap();
                    }
                    writer.write_commit(txn_id).unwrap();
                    txn_id += 1;
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_writer_creation,
    bench_transactions,
    bench_write_operations,
    bench_flush_operations,
    bench_wal_reader,
    bench_recovery,
    bench_checkpoint,
    bench_complete_transactions,
);

criterion_main!(benches);

// Made with Bob
