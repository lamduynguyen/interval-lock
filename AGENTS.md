# AGENTS.md

## Environment

- Never place temporary files (bench dbs, scratch outputs, logs) under `/tmp`: it is a 16G tmpfs and fills up. Use `./build/scratch/` instead (the workspace NVMe has plenty of space).
- Compile with `-j8` only.

## Permission

- Never request permission to inspect system folders or files (e.g. `/proc`, `/sys`, `/dev`, `/tmp`); read-only commands there are explicitly allowed.

## Project structure

- `src/include/` holds the public headers, mirroring the modules under `src/`.
  - `bin/` — `LeanStore` runtime (`leanstore.cc`: worker pool, execute/commit/abort entry points, stats thread), gflags (`config.cc`), environment setup, statistics storage.
  - `common/` — typedefs, utilities (hashing/CRC, io_uring helpers, huge/aligned allocation), random, queues, worker pool, exceptions.
  - `buffer/` — buffer manager (virtual/physical pool, eviction, page faults), buffer frames, resident-page set.
  - `storage/` — page layouts, B-tree (`btree/node.cc`, `btree/tree.cc`, WAL payloads), blobs, AIO backends, free-storage manager.
  - `sync/` — hybrid/spin latches, page state and versioning, page guards (`page_guard/{optimistic,shared,exclusive}_guard`), epoch handler for deferred reclamation.
  - `transaction/` — transaction and transaction manager, `lock_manager_interface.h`, the four lock managers under `lock_manager/` (faster, staged, inline, precision) and their shared `lock_manager/utils/` (TLB, pool, fingerprint set, undo arena), deadlock prevention, lockable tuple.
  - `recovery/` — log manager/worker/buffer/backend, group commit, checkpointing, recovery.
- `test/` — gtests grouped by module (`buffer/`, `storage/`, `sync/`, `transaction/`, `recovery/`, `adapters/`, `common/`), shared fixture in `test/include/test/base_test.h`.
- `benchmark/` — YCSB, TATP, TPC-C and mixed workloads plus adapters; binaries land in `<build>/benchmark/`.
- `share_libs/` — vendored dependencies (fmt, fsst, lz4, roaring, yyjson, cpptrace, typefold); `build_support/` — clang-format/tidy helpers.

## Tests (gtest)

- The test harness (`base_test.h`) writes its db/WAL/scratch into `fs::temp_directory_path()`, which defaults to /tmp. Run test binaries with `TMPDIR=<abs path>/build/scratch` so all test artifacts stay inside build/ (same rule as Environment above).
- Variant-dependent runs: pass `--txn_locking_variant=<0..3>`; BASELINE_INLINE additionally needs `--txn_locking_policy=0` (its manager asserts NO_WAIT).
- Test binaries need `timeout -s KILL 90-300`; gtest runs must not be run with ctest -j8 (flaky) — use `ctest --test-dir build -j 1`.

## Code Style & Conventions

- Naming: let names capture the semantics so comments are not needed/minimal (e.g. `TryLockExclusiveInPlace` says claim mode + latch state, `ReclaimIfFree` says latch-free via callback). Use the semantic typedefs from `common/typedefs.h` (`pageid_t`, `timestamp_t`, `fingerprint_t`, ...) instead of raw width types where the meaning exists.
- Write explanations (comments and answers) in plain technical terms: state what the thing is, what it does, and what problem it solves. No flowery or literary phrasing.
- Comments: 1-2 lines that explain the why at the use site, not paraphrase the code. No excessive doc blocks on trivial APIs; a rule only one caller needs lives at that caller, not in the class doc. 
- Never weaken a test to pass (e.g. `EXPECT_GT` instead of the exact value because a layout differs): adapt the expectation per variant (pinned per-variant constants) instead, and prefer strengthening tests to cover edge cases.
- APIs: add only when necessary; every method must have a production or test caller. Test-only observation hooks are private members granted access via `FRIEND_TEST` (include `gtest/gtest_prod.h`), with the tests declared inside the same namespace as the manager (`leanstore::transaction`) so the friended test classes resolve.
- Header layout: public lock/API surface first, then category blocks among privates (`// Types`, `// Table`, `// Pool`, lifecycle, state members last; class constants at the top of the class). Includes minimal: fwd-declare types used only as pointers (`buffer::BufferManager`, `BufferFrame`), and impl-only includes (`lock_tlb.h`, `leanstore/config.h`, ...) belong in the .cc.
- Construction: ctor/dtor implementations live in the .cc; the header keeps only constants-based NSDMIs — member initializers that depend on gflags go into the .cc ctor init list.
- Edit surgically: never rewrite a whole file or a large span. The user hand-edits files between turns, and a rewrite resets those updates. Re-read the exact region right before editing it.
