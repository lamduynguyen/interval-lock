#include "leanstore/config.h"

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "share_headers/logger.h"

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_worker_count                = 1;
  FLAGS_txn_default_isolation_level = "nl";

  google::ParseCommandLineFlags(&argc, &argv, true);
  (void)LOG_LEVEL_DEBUG;
  return RUN_ALL_TESTS();
}
