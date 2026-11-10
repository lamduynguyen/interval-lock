#include "leanstore/leanstore.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "storage/btree/tree.h"

#include "share_headers/logger.h"
#include "share_headers/mem_usage.h"
#include "share_headers/time.h"

#include <sys/sysinfo.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <ranges>
#include <span>

namespace leanstore {

InternalCatalog catalog;
u32 LeanStore::signature = 0;
std::vector<buffer::BufferManager *> LeanStore::all_buffer_pools;
thread_local wid_t LeanStore::worker_thread_id = -1;  // The ID of current worker (min wid is 0)

LeanStore::LeanStore()
    : buffer_pool(std::make_unique<buffer::BufferManager>(is_running)),
      log_manager(std::make_unique<recovery::LogManager>(is_running)),
      transaction_manager(std::make_unique<transaction::TransactionManager>(buffer_pool.get(), log_manager.get())),
      blob_manager(std::make_unique<storage::blob::BlobManager>(buffer_pool.get())),
      recovery(std::make_unique<recovery::RecoveryManager>(buffer_pool.get())),
      worker_pool(is_running,
                  [&]() {
                    buffer_pool->ConstructLocalRing();
                    log_manager->LocalLogWorker().backend.Connect();
                  }),
      gen(2, FLAGS_wal_max_idle_time_us * 1000) {
  // Validate and fix flags
  Ensure(FLAGS_txn_commit_group_size > 0);
  Ensure(FLAGS_worker_count % FLAGS_txn_commit_group_size == 0);
  Ensure(FLAGS_txn_commit_group_size <= FLAGS_worker_count);

#ifdef DEBUG
  (void)LOG_LEVEL_DEBUG;
#endif

  // Necessary dirty works
  WarningMessage("Current TSC_PER_NS setting: " + std::to_string(tsctime::TSC_PER_NS));
  log_manager->InitializeCommitExecutor(buffer_pool.get());
  StartCheckpointerThread();
  worker_pool.ScheduleSyncJob(0, [&]() { buffer_pool->AllocMetadataPage(); });
  LeanStore::all_buffer_pools.push_back(buffer_pool.get());

  // If recovery is required, initialize the recovery manager and then execute it
  if (FLAGS_wal_enable_recovery) {
    // HACK: disable WAL subsystem temporarily during recovery
    FLAGS_wal_enable = false;

    // Start recovery
    auto recovery_start = tsctime::ReadTSC();
    buffer_pool->SetRecoveryMode(true);
    recovery->RecoveryPreparation();
    LeanStore::signature       = recovery->SignatureOfPrevSession();
    FLAGS_wal_recovery_threads = std::min(FLAGS_wal_recovery_threads, FLAGS_worker_count);

    /**
     * @brief Three-phase recovery
     * - 1st: Log materialization
     * - 2nd: Analysis: determine winner and loser
     * - 3rd: Redo logs
     * Not support UNDO at the moment
     *
     * After recovery, continue appending log with previous session, i.e., reuse the same MASTER_RECORD block
     */
    // 1st phase
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
      worker_pool.ScheduleAsyncJob(idx, [&]() {
        auto last_empty_offset = recovery->MaterializeLogs();
        UpdateMin(log_manager->WALOffset(), last_empty_offset);
      });
    }
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }
    auto recovery_p1 = tsctime::ReadTSC();
    LOG_INFO("Last valid offset: {} MB", log_manager->WALOffset().load() / MB);

    // 2nd phase:
    // - Apply the correct GSNs to the WAL Block ring registry
    // - Analyze the WAL logs to detect winners/losers
    recovery->ApplyRecoveredBlocks(log_manager->WALBlocks());
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
      worker_pool.ScheduleAsyncJob(idx, [&]() { recovery->Analysis(); });
    }
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }
    auto recovery_p2 = tsctime::ReadTSC();

    // 3rd phase
    worker_pool.ScheduleSyncJob(0, [&]() { recovery->PrepareRedoEnv(); });
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) {
      worker_pool.ScheduleAsyncJob(idx, [&]() {
        // Trick: Run in RU to prevent holding new locks
        transaction_manager->StartTransaction(Transaction::Type::SYSTEM, 0,
                                              transaction::IsolationLevel::READ_UNCOMMITTED);
        recovery->Redo();
        CommitTransaction();
      });
    }
    for (auto idx = 0U; idx < FLAGS_wal_recovery_threads; idx++) { worker_pool.JoinWorker(idx); }

    // recovery end
    auto recovery_end = tsctime::ReadTSC();
    buffer_pool->SetRecoveryMode(false);
    LOG_INFO("Recovery time: phase 1 {:.4f} us, phase 2 {:.4f} us, phase 3 {:.4f} us",
             tsctime::TscDifferenceNs(recovery_start, recovery_p1) / 1000.0L,
             tsctime::TscDifferenceNs(recovery_p1, recovery_p2) / 1000.0L,
             tsctime::TscDifferenceNs(recovery_p2, recovery_end) / 1000.0L);
    FLAGS_wal_enable = true;

    /* Bootstrap the checkpoint from the prior session's on-disk images and
     * recycle the recovered WAL blocks whose records are already covered. */
    log_manager->BootstrapCheckpoint();
  } else {
    // Start a fresh session
    log_manager->WriteMasterRecord();
  }
}

LeanStore::~LeanStore() {
  Shutdown();
  start_profiling = false;
}

void LeanStore::Shutdown() {
  worker_pool.Stop();
  Ensure(is_running == false);
  if (checkpointer.joinable()) { checkpointer.join(); }
  if (stat_collector.joinable()) {
    stat_collector.join();
    // Print transaction latency
    if (FLAGS_txn_debug) {
      LOG_INFO("Start measuring latency");
      auto wcnt = FLAGS_worker_count;
      for (auto idx = 1U; idx < wcnt; idx++) {
        std::ranges::copy(statistics::txn_latency[idx], std::back_inserter(statistics::txn_latency[0]));
        std::ranges::copy(statistics::txn_queue[idx], std::back_inserter(statistics::txn_queue[0]));
        std::ranges::copy(statistics::txn_exec[idx], std::back_inserter(statistics::txn_exec[0]));
        std::ranges::copy(statistics::io_latency[idx], std::back_inserter(statistics::io_latency[0]));
        std::ranges::copy(statistics::txn_per_round[idx], std::back_inserter(statistics::txn_per_round[0]));
      }
      LOG_INFO("# data points: {}", statistics::txn_latency[0].size());
      LOG_INFO("Start evaluating latency data");
      std::sort(statistics::txn_latency[0].begin(), statistics::txn_latency[0].end());
      if (!statistics::txn_latency[0].empty()) {
        WriteSequenceToFile(statistics::txn_latency[0], 1000, "latency.txt");
        auto nth = statistics::txn_latency[0].size() * 999 / 1000;
        LOG_INFO("Transaction statistics:\n\tAvgLatency({:.4f} us)\n\t99.9thLatency({} us)",
                 Average(statistics::txn_latency[0]) / 1000UL,
                 static_cast<float>(statistics::txn_latency[0][nth - 1]) / 1000UL);
      }
      LOG_INFO(
        "Statistics:\n\tAvgExecTime({:.4f} us)\n\tAvgQueue({:.4f} us)\n\t"
        "AvgIOLatency({:.4f} us)\n\t"
        "AvgTxnPerCommitRound({:.4f} txns)\n\t99.9thTxnPerRound({} txns)\n\t99.99thTxnPerRound({} txns)",
        Average(statistics::txn_exec[0]) / 1000UL, Average(statistics::txn_queue[0]) / 1000UL,
        Average(statistics::io_latency[0]) / 1000UL, Average(statistics::txn_per_round[0]),
        Percentile(statistics::txn_per_round[0], 99.9), Percentile(statistics::txn_per_round[0], 99.99));
    }
  }
}

void LeanStore::CheckDuringIdle() {
  assert(FLAGS_wal_max_idle_time_us > 0);
  /**
   * @brief A probabilistic model to decide whether to write logs during IDLE
   *
   * With original WILO, there are two scenarios that it can't handle:
   * - 1 write txn then a sequence of read-only transactions
   * - 1 write txn then system workers become idle for a long time
   *
   * To tackle these two issues, we can force WILO to write logs during system idle --
   *  see PoissonScheduler::Wait() for your information
   * However, if we force write every time, it's possible that:
   * - the free time is not enough for 1 write, i.e., idle for 5us, but 1 write causes at least 20us
   * - we write log excessively, worsening write amplification and decrease throughput
   *
   * To fix this, we probabilistically trigger force write according to the maximum allowed idle time
   */
  auto avg_idle_time = Average(statistics::worker_idle_ns[worker_thread_id]);
  auto eval_value    = gen.NoElements() - gen.Rand();
  if (avg_idle_time >= eval_value) {
    auto &logger = log_manager->LocalLogWorker();
    logger.WorkerWritesLog();
    log_manager->TriggerGroupCommit(worker_thread_id / FLAGS_txn_commit_group_size);
  }
}

// -------------------------------------------------------------------------------------
void LeanStore::RegisterTable(const std::type_index &relation, uint32_t relation_idx) {
  assert(indexes.find(relation) == indexes.end());
  assert(FLAGS_worker_count > 0);
  if (FLAGS_wal_enable_recovery) { Ensure(recovery->HasRecovered(METADATA_PAGE_ID)); }
  worker_pool.ScheduleSyncJob(0, [&]() {
    transaction_manager->StartTransaction(leanstore::transaction::Transaction::Type::SYSTEM);
    indexes.try_emplace(relation, std::make_unique<storage::BTree>(buffer_pool.get(), relation_idx));
    if (relation_idx >= catalog.size()) { catalog.resize(relation_idx + 1); }
    catalog[relation_idx] = indexes.at(relation).get();
    CommitTransaction();
  });
}

auto LeanStore::RetrieveIndex(const std::type_index &relation) -> KVInterface * {
  assert(indexes.find(relation) != indexes.end());
  return indexes.at(relation).get();
}

void LeanStore::StartTransaction(timestamp_t txn_arrival_time, Transaction::Mode tx_mode,
                                 const std::string &tx_isolation_level, timestamp_t predefined_start_ts) {
  transaction_manager->StartTransaction(Transaction::Type::USER, txn_arrival_time,
                                        transaction::TransactionManager::ParseIsolationLevel(tx_isolation_level),
                                        tx_mode, predefined_start_ts);
}

void LeanStore::CommitTransaction() {
  transaction_manager->CommitTransaction();
  blob_manager->UnloadAllBlobs();
}

void LeanStore::AbortTransaction() {
  transaction_manager->AbortTransaction();
  blob_manager->UnloadAllBlobs();
}

auto LeanStore::ExecuteTransaction(const std::function<void()> &body, timestamp_t txn_arrival_time) -> bool {
  timestamp_t predefined_start_ts = 0;  // 0 = first attempt takes a fresh global-clock ts
  while (is_running.load()) {
    StartTransaction(txn_arrival_time, Transaction::Mode::OLTP, FLAGS_txn_default_isolation_level, predefined_start_ts);
    try {
      body();
      CommitTransaction();
      return true;
    } catch (const ex::AbortTransaction &) {
      predefined_start_ts = transaction::Transaction::active_txn.start_ts;
      AbortTransaction();
    }
  }
  return false;
}

// -------------------------------------------------------------------------------------
auto LeanStore::AllocatedSize() -> float {
  return static_cast<float>(buffer_pool->GetMetadataPtr()->alloc_cnt.load() * PAGE_SIZE) / GB;
}

auto LeanStore::WALSize() -> float {
  auto wal_capacity       = StorageCapacity(FLAGS_db_path.c_str()) - BLK_BLOCK_SIZE;
  auto current_wal_offset = log_manager->WALOffset().load();
  return static_cast<float>(wal_capacity - current_wal_offset) / GB;
}

auto LeanStore::DBSize() -> float {
  return static_cast<float>((buffer_pool->GetMetadataPtr()->alloc_cnt.load() - statistics::storage::free_size) *
                            PAGE_SIZE) /
         GB;
}

void LeanStore::DropCache() {
  worker_pool.ScheduleSyncJob(0, [&]() {
    LOG_INFO("Dropping the cache");
    while (buffer_pool->physical_used_cnt_ > 0) { buffer_pool->Evict(); }
    LOG_INFO("Complete cleaning the cache. Buffer size: {}", buffer_pool->physical_used_cnt_.load());
  });
}

// -------------------------------------------------------------------------------------
void LeanStore::StartCheckpointerThread() {
  if (!FLAGS_wal_enable_checkpoint) { return; }
  checkpointer = std::thread([&]() {
    pthread_setname_np(pthread_self(), "checkpointer");
    /* Dedicated thread: owns its own AIO ring, parked at the last interface slot. */
    LeanStore::worker_thread_id = FLAGS_worker_count;
    buffer_pool->ConstructLocalRing();
    /* Waiting for system bootstraping */
    while (log_manager->CheckpointIsBootstraping()) { std::this_thread::yield(); }
    /* Main loop */
    try {
      auto &blocks = log_manager->WALBlocks();
      /* Volume-coupled trigger: one increment per 1/S of the WAL limit, in block granularity.
       * Keep cycling until the number of log blocks being used is >= 1/S, then start checkpointing */
      const u64 interval = std::max<u64>(1, blocks.Capacity() / FLAGS_wal_checkpoint_shards);
      while (is_running.load()) {
        auto cur_wal_size = blocks.Capacity() - blocks.BlockCycleCnt();
        if (cur_wal_size >= interval) { log_manager->TriggerCheckpoint(); }
        std::this_thread::yield();
      }
    } catch (...) {
      // Similar reason to GroupCommitExecutor::StartExecution()
    }
  });
}

void LeanStore::StartProfilingThread() {
  start_profiling = true;
  stat_collector  = std::thread([&]() {
    pthread_setname_np(pthread_self(), "stats_collector");
    std::printf(
      "ts,committed_tx,aborted_tx,precommited_tx,commit_rounds,bm_rmb,bm_wmb,bm_evict,log_sz_mb,logio_mb,log_flush_cnt,"
       "gct_time_us,db_size\n");
    auto cnt           = 0UL;
    auto commit_exec   = 0UL;
    auto commit_rounds = 0UL;

    while (is_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (!start_profiling_latency) { start_profiling_latency = true; }
      // Progress stats
      auto rounds         = 0UL;
      auto committed_tx   = 0UL;
      auto aborted_tx     = 0UL;
      auto precommited_tx = 0UL;
      for (auto idx = 0U; idx <= FLAGS_worker_count; idx++) {
        rounds += statistics::commit_rounds[idx].exchange(0);
        committed_tx += statistics::committed_txn[idx].exchange(0);
        aborted_tx += statistics::aborted_txn[idx].exchange(0);
        precommited_tx += statistics::precommited_txn[idx].exchange(0);
      }
      if (!FLAGS_wal_enable) { committed_tx = precommited_tx; }
      statistics::total_committed_txn += committed_tx;
      commit_rounds += rounds;
      // System stats
      auto r_mb  = static_cast<float>(statistics::buffer::read_cnt.exchange(0) * PAGE_SIZE) / MB;
      auto w_mb  = static_cast<float>(statistics::buffer::write_cnt.exchange(0) * PAGE_SIZE) / MB;
      auto e_cnt = statistics::buffer::evict_cnt.exchange(0);
      auto db_sz = DBSize();
      // Group commit stats
      auto log_sz        = 0.0F;
      auto log_write     = 0.0F;
      auto log_flush_cnt = 0UL;
      auto gct_us        = 0UL;
      for (auto idx = 0U; idx <= FLAGS_worker_count; idx++) {
        log_sz += static_cast<float>(statistics::recovery::real_log_bytes[idx].exchange(0)) / MB;
        log_write += static_cast<float>(statistics::recovery::written_log_bytes[idx].exchange(0)) / MB;
        log_flush_cnt += statistics::log_flush_cnt[idx].exchange(0);
        gct_us += statistics::recovery::gct_time_ns[idx].exchange(0) / 1000;
      }
      commit_exec += gct_us;
      // Output
      std::printf("%lu,%lu,%lu,%lu,%lu,%.4f,%.4f,%lu,%.4f,%.4f,%lu,%lu,%.4f\n", cnt++, committed_tx, aborted_tx,
                   precommited_tx, rounds, r_mb, w_mb, e_cnt, log_sz, log_write, log_flush_cnt, gct_us, db_sz);
    }
    LOG_INFO("Transaction statistics: # committed txns: {}", statistics::total_committed_txn.load());
    LOG_INFO("AvgGroupCommitTime: {:.4f}us - No rounds {} - Txn per round {:.4f}",
              static_cast<double>(commit_exec) / commit_rounds, commit_rounds,
              static_cast<double>(statistics::total_committed_txn) / commit_rounds);
    LOG_INFO("Halt LeanStore's Profiling thread");
  });
}

// -------------------------------------------------------------------------------------
auto LeanStore::CreateNewBlob(std::span<const u8> blob_payload) -> std::span<const u8> {
  Ensure(FLAGS_blob_enable);
  if (blob_payload.empty()) { throw leanstore::ex::GenericException("Blob payload shouldn't be empty"); }
  auto blob_hd = blob_manager->AllocateBlob(blob_payload);
  return std::span{reinterpret_cast<u8 *>(blob_hd), blob_hd->MallocSize()};
}

void LeanStore::LoadBlob(const BlobState *blob_t, const storage::blob::BlobCallbackFunc &read_cb, bool partial_load) {
  Ensure(FLAGS_blob_enable);
  if (partial_load) {
    blob_manager->LoadBlob(blob_t, PAGE_SIZE, read_cb);
  } else {
    blob_manager->LoadBlob(blob_t, blob_t->blob_size, read_cb);
  }
}

void LeanStore::RemoveBlob(BlobState *blob_t) {
  Ensure(FLAGS_blob_enable);
  blob_manager->RemoveBlob(blob_t);
}

auto LeanStore::RetrieveComparisonFunc(ComparisonOperator cmp_op) -> ComparisonLambda {
  switch (cmp_op) {
    case ComparisonOperator::BLOB_LOOKUP:
      return {cmp_op, [blob_man = blob_manager.get()](const void *a, const void *b, [[maybe_unused]] size_t) {
                return blob_man->BlobStateCompareWithString(a, b);
              }};
    case ComparisonOperator::BLOB_HANDLER:
      return {cmp_op, [blob_man = blob_manager.get()](const void *a, const void *b, [[maybe_unused]] size_t) {
                return blob_man->BlobStateComparison(a, b);
              }};
    default: return {cmp_op, std::memcmp};
  }
}

}  // namespace leanstore
