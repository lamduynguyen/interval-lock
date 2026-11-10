#include "common/constants.h"

#include "gflags/gflags.h"

// -----------------------------------------------------------------------------------
/* System in general */
DEFINE_string(db_path, "/tmp/whatever.db", "Default db path, supporting both normal and block file");
DEFINE_uint32(worker_count, 16, "The number of workers");
DEFINE_bool(worker_pin_thread, false, "Pin worker to a specific thread");
DEFINE_uint32(txn_rate, 0, "Limit transaction rate in second for latency checking -- Disabled by default");
// -----------------------------------------------------------------------------------
/* Buffer manager */
DEFINE_uint64(bm_virtual_gb, 4, "Size of virtual memory in GB");
DEFINE_uint64(bm_physical_gb, 1, "Size of physical memory in GB");
DEFINE_uint64(bm_alias_block_mb, 1024, "Size of worker-local aliasing area in MB");
DEFINE_uint64(bm_evict_batch_size, 64, "Expected number of pages to be evicted during each eviction");
DEFINE_bool(bm_enable_fair_eviction, true,
            "Whether to use fair extent eviction policy or not"
            "Fair eviction policy: Large extents are more likely to be evicted than small extents/pages");
// -----------------------------------------------------------------------------------
/* Write-ahead logging */
DEFINE_bool(wal_enable, true, "Whether to enable WAL logging or not");
DEFINE_uint64(wal_size_gb, 30, "Bounding on-disk WAL size, used for checkpointing");
DEFINE_bool(wal_debug, false, "Enable debugging for WAL ops");
DEFINE_bool(wal_fsync, true, "Force FSync for WAL");
DEFINE_uint64(wal_buffer_size_mb, 10, "Size of WAL log buffer in MB");

/* Configuration for commit protocols */
DEFINE_uint32(wal_batch_write_kb, 16,
              "Workers only write WAL logs when the amount of unwritten logs are larger than this flag");
DEFINE_uint32(wal_block_size_mb, 10, "The write granularity for workers to avoid contention");
DEFINE_uint32(wal_max_idle_time_us, 30,
              "Force write when the avg idle time (from sampling) is larger than this number");

/* Recovery */
DEFINE_bool(wal_enable_recovery, false, "Whether to run recovery before 'boot up' the system");
DEFINE_uint32(wal_recovery_threads, 1, "Number of threads used for recovery");

/* Checkpointing */
DEFINE_bool(wal_enable_checkpoint, true, "Whether to enable checkpointing or not");
DEFINE_uint64(wal_checkpoint_shards, 128,
              "Number of shards dividing the WAL limit: a checkpoint increment "
              "fires for every 1/S of the WAL limit written to disk. "
              "0 = use FLAGS_worker_count. Coupled to WAL volume.");
// -----------------------------------------------------------------------------------
/* Transaction */
DEFINE_bool(txn_debug, false, "Enable debugging for transaction ops, including commit latency info");
DEFINE_string(txn_default_isolation_level, "ru",
              "The serializable mode used in LeanStore, only support two following modes"
              "(ru: READ_UNCOMMITTED, ser: SERIALIZABLE)");

/* Configuration for the lock manager */
DEFINE_uint32(txn_locking_variant, 0,
              "Which variant of 2PL lock manager is used"
              "0. FASTER hash buckets + FASTER-style circular buffer of lock entries"
              "1. STAGED lock manager (Jung et al., TODS 2014) - deadlock detection only"
              "2. Inline tuple lock, one byte stored in the tuple itself"
              "3. Range-granularity wait-die precision lock");
DEFINE_uint32(txn_locking_policy, 1,
              "The conflict policy the 2PL lock manager uses"
              "0. No-wait: abort as soon as the lock is not immediately available"
              "1. Wait-die: wait if every owner is older, abort otherwise");
DEFINE_uint64(txn_lock_pool_bytes, 10 * 1024 * 1024,
              "Per-worker pre-allocated lock memory in bytes:"
              "ring entries for FASTER, pool nodes for STAGED, pooled range entries for precision "
              "(a bigger entry size simply yields fewer slots); 0 = no pre-allocation at all");
DEFINE_uint32(txn_lock_tlb_slots, 256,
              "Per-worker direct-mapped lock TLB slots;"
              "0 = no TLB (every lock request pays the table lookup)");

/* Configuration for commit processing subsystem */
DEFINE_uint32(txn_commit_group_size, 4,
              "The size (number of workers) of the commit group; workers can trigger commit ack for the whole group");
DEFINE_uint32(txn_queue_size_mb, 10, "The transaction queue size in MB");
// -----------------------------------------------------------------------------------
/* BLOB */
DEFINE_bool(blob_enable, false, "Whether to enable Blob functionalities");
DEFINE_bool(blob_normal_buffer_pool, false,
            "Extra overheads to emulate normal buffer pool, should be enabled during development"
            "1. *IMPORTANT* Extra hashtable lookup on Buffer's ToPtr & Read op"
            "2. *IMPORTANT* Require chunked processing on large object operations"
            "3. GroupCommit::PrepareLargePageWrite: Write on 4KB granularity instead of extent granularity");
DEFINE_uint64(blob_db_region_gb, 1,
              "Fixed size of the on-disk blob region of the database file in GBs"
              "0. Same value with FLAGS_bm_virtual_gb"
              "> 0. Fixed value");
