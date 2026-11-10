# IntervalLock — High-Performance Serializability with Interval Locking

This is the reproducibility package for the paper **"High-Performance Serializability with Interval Locking"**, under submission to VLDB'27.

## Compiling

Install dependencies:

### Core

`sudo apt-get install autoconf automake libtool curl make cmake g++ unzip libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev liburing-dev libzstd-dev libcurl4-openssl-dev libbenchmark-dev`

### Third-party libraries

`sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

## How to build

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## Repository layout

- `src/include/` — public headers, mirroring the modules under `src/`:
  - `bin/` — runtime (worker pool, execute/commit/abort entry points), gflags, statistics
  - `common/` — typedefs, utilities, random, worker pool
  - `buffer/` — buffer manager (virtual/physical pools, eviction, page faults)
  - `storage/` — page layouts, B-tree, blobs, AIO backends
  - `sync/` — latches, page guards, epoch-based reclamation
  - `transaction/` — transaction management and the lock managers under `lock_manager/` (FASTER-style hash buckets, Staged, inline tuple locks, IntervalLock)
  - `recovery/` — logging, group commit, checkpointing, recovery
- `test/` — gtest suites grouped by module
- `benchmark/` — YCSB, TATP, and TPC-C workloads; binaries land in `<build>/benchmark/`

## Selecting the lock manager

At runtime, use `--txn_locking_variant=<0..3>`:

| Variant | Lock manager |
|---------|--------------|
| 0 | FASTER-style hash buckets + circular buffer of lock entries |
| 1 | Staged lock manager (Jung et al., TODS 2014), deadlock detection only |
| 2 | Inline tuple lock, one byte stored in the tuple itself |
| 3 | IntervalLock (range-granularity precision locking) |

and `--txn_locking_policy=<0..1>` for the conflict policy (`0` = no-wait, `1` = wait-die). The baseline inline lock manager (variant 2) only supports no-wait.

## Tests

Run the full gtest suite sequentially:

`TMPDIR=$(pwd)/build/scratch ctest --test-dir build -j 1`

## Acknowledgements

IntervalLock is implemented on top of [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf), a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs.
