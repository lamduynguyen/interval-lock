#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"

#include <atomic>
#include <limits>

namespace leanstore::recovery {

struct LogEntry {
  enum class Type : u8 {
    MASTER_RECORD   = 0,
    WRITE_METADATA  = 1,
    CARRIAGE_RETURN = 2,
    TX_START        = 3,
    TX_COMMIT       = 4,
    TX_ABORT        = 5,
    DATA_ENTRY      = 6,
    FREE_EXTENT     = 9,
    REUSE_EXTENT    = 10,
    ALLOC_NEW_PAGE  = 12,
  };

  u32 chksum = 99;
  Type type : 4;
  u32 size : 28;

  void ComputeChksum() { chksum = ComputeCRC(reinterpret_cast<u8 *>(this) + sizeof(chksum), size - sizeof(chksum)); }

  void ValidateChksum() const {
    if (type == Type::WRITE_METADATA) { return; }  // TODO(XXX): We should validate chksum of all types
    Ensure(chksum == ComputeCRC(reinterpret_cast<const u8 *>(this) + sizeof(chksum), size - sizeof(chksum)));
  }
};

/* Must be on top of all log records */
struct RecoveryInfo {
  /**
   * @brief Worker-ID is required for recovery
   * This helps evalute the durable GSNs of all workers after the crash,
   *  which is used to determine which txns are winners/losers, even if their TX_COMMIT are already durable
   * E.g.:
   *   HARDENED
   *      |
   *      v
   * W0 ----- INS(P1) ------ COMMIT(TX0) ------->
   *               \
   *                \ Dependency edge
   *                 \
   * W1 -- INS(P0) -- UP(P1) --- COMMIT(TX1) --->
   *                                 ^
   *                                 |
   *                             HARDENED
   * In this case, even if TX1 of W1 is already durable, it accesses dirty data in P1 from TX0.
   * If the DBMS crashes now, TX1 should be listed as loser.
   *
   * This requires TX_COMMIT log entry (i.e., TxnCommitEntry) to contain a GSN vector of all workers,
   *  which determines the dependency info of the current transaction
   * This information is already stored as Transaction::gsn_vector_ - see transaction.h
   */
  wid_t w_id;
  timestamp_t gsn;  // Used for all log types except WRITE_METADATA

  union {
    txnid_t txn;  // Used for normal log entry, i.e., not WRITE_METADATA

    /* Used only for WRITE_METADATA log entry */
    struct {
      u32 chunk_size;
      u32 signature;
    };
  };
};

// ----------------------------------------------------------------------------------------

struct DataEntry : LogEntry {
  RecoveryInfo rc;       // metadata required for recovery process
  pageid_t pid;          // the page id which contains the modification stored in this log
  timestamp_t prev_gsn;  // the prev GSN of operator that modifies this page
};

struct LogMetaEntry : LogEntry {
  RecoveryInfo rc;  // metadata required for recovery process
};

/**
 * @brief Session-config entry written at the end of the WAL file as the
 *        master record.  Carries everything recovery needs to read the
 *        prior session's WAL regardless of the new session's configuration.
 */
struct MasterRecordEntry : LogEntry {
  RecoveryInfo rc;  // signature, chunk_size, etc.

  /* Session configuration captured at startup. */
  u32 num_workers;        // FLAGS_worker_count of the prior session, used for determining # retries upon recovery
  u32 wal_size_gb;        // FLAGS_wal_size_gb of the prior session
  u32 wal_block_size_mb;  // FLAGS_wal_block_size_mb of the prior session
};

/* Only used when FLAGS_wal_variant == LoggingVariant::VECTOR */
struct TxnCommitEntry : LogEntry {
  RecoveryInfo rc;
  size_t vector_size;
  u8 payload[];
};

/**
 * @brief Required for BLOB operations
 */
struct FreeExtentEntry : LogEntry {
  RecoveryInfo rc;     // metadata required for recovery process
  pageid_t start_pid;  // the start pid of the large page to be freed
  pageid_t lp_size;    // number of pages to be freed, starting from `start_pid`
};

/**
 * @brief Records a page allocation. Unlike DataEntry, it carries no RecoveryInfo:
 *  allocation is not tied to a transaction, its effect (a max on alloc_cnt) is
 *  order-independent and idempotent, and it needs no GSN for recovery.
 */
struct AllocNewPageEntry : LogEntry {
  pageid_t pid;          // always METADATA_PAGE_ID (0)
  pageid_t new_page_id;  // the pid of the newly allocated page
};

static_assert(sizeof(LogEntry) == 8);

}  // namespace leanstore::recovery