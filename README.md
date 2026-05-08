# Agentic Memory Nonsense

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Actions Status](https://github.com/huhlig/nanokv/workflows/rust/badge.svg)](https://github.com/huhlig/nanokv/actions)

> NanoKV is a lightweight embeddable single file key value table service.

## Project Structure

```
nanokv/
├── docs/            # Documentation, Architecture, ADRs
├── api/             # Embeddable API
├── cli/             # CLI Utilities
├── net/             # Network Service
├── table/           # Key Value Tables
├── pager/           # File Pager
└── vfs/             # Virtual File System
```

---

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test module
cargo test --test pager_stress_tests
```

### Large-Scale Stress Tests

NanoKV includes comprehensive stress tests for 100K-200K pages that are marked with `#[ignore]` to avoid slowing down regular test runs.

```bash
# Run all large-scale stress tests
cargo test --test pager_stress_tests -- --ignored --nocapture

# Run specific stress test
cargo test --test pager_stress_tests test_sequential_allocation_100k_pages -- --ignored --nocapture
cargo test --test pager_stress_tests test_fragmentation_100k_pages -- --ignored --nocapture
cargo test --test pager_stress_tests test_memory_usage_200k_pages -- --ignored --nocapture
```

Available stress tests:
- `test_sequential_allocation_100k_pages` - Sequential allocation of 100K pages
- `test_mixed_allocation_deallocation_100k_pages` - Realistic mixed workload patterns
- `test_fragmentation_100k_pages` - Extreme fragmentation scenarios
- `test_memory_usage_200k_pages` - Scalability validation to 200K pages
- `test_free_list_chain_100k_pages` - Long free list chain performance
- `test_persistence_recovery_50k_pages` - Large database persistence

See [docs/PAGER_STRESS_TEST_RESULTS.md](docs/PAGER_STRESS_TEST_RESULTS.md) for detailed performance metrics.

### Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark suite
cargo bench --bench pager_benchmarks
cargo bench --bench vfs_benchmarks
cargo bench --bench wal_benchmarks
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes with tests
4. Submit a pull request

---

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0) or the
[MIT License](https://opensource.org/licenses/MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work
by you shall be dual-licensed as above, without any additional terms or conditions.

Copyright 2025–2026 Hans W. Uhlig. All Rights Reserved.