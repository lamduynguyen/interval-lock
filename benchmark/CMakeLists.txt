# ---------------------------------------------------------------------------
# Benchmark utilities
# ---------------------------------------------------------------------------
file(GLOB_RECURSE BENCHMARK_LIB_FILES src/**/**.cc)
add_library(leanstore_bm STATIC ${BENCHMARK_LIB_FILES})
target_link_libraries(leanstore_bm PUBLIC leanstore ${THIRD_PARTY_DEPENDENCIES})
target_include_directories(leanstore_bm PUBLIC ${SRC_INCLUDE_DIR}
                                               ${THIRD_PARTY_INCLUDE_DIR})

# ---------------------------------------------------------------------------
# Add benchmark helper
# ---------------------------------------------------------------------------
set(BM_REQUIRED_LIBS leanstore leanstore_bm)
set(BM_REQUIRED_HEADERS "")
function(ADD_BENCHMARK BENCHMARK_NAME)
  add_executable(${ARGV})
  target_link_libraries(${BENCHMARK_NAME} PUBLIC ${BM_REQUIRED_LIBS})
  target_include_directories(${BENCHMARK_NAME} PUBLIC ${BM_REQUIRED_HEADERS})
endfunction()

# ---------------------------------------------------------------------------
# All benchmarks
# ---------------------------------------------------------------------------
add_benchmark(LeanStore_TPCC src/bin/leanstore_tpcc.cc)
add_benchmark(LeanStore_TATP src/bin/leanstore_tatp.cc)
add_benchmark(LeanStore_MixWorkloads src/bin/leanstore_mixed.cc)
add_benchmark(LeanStore_YCSB src/bin/ycsb/leanstore_ycsb.cc)
add_benchmark(SQLite_YCSB src/bin/ycsb/sqlite3_ycsb.cc)

# ---------------------------------------------------------------------------
# Tests for custom prototypes
# ---------------------------------------------------------------------------
if(ENABLE_TESTING)
  set(PROTOTYPE_TEST_REQUIRED_LIBS gtest ${THIRD_PARTY_DEPENDENCIES})
  set(PROTOTYPE_TEST_REQUIRED_HEADERS ${GTEST_INCLUDE_DIR})
  function(PROTOTYPE_ADD_TEST TEST_NAME)
    add_executable(${ARGV})
    target_link_libraries(${TEST_NAME} PUBLIC ${PROTOTYPE_TEST_REQUIRED_LIBS})
    target_include_directories(${TEST_NAME} PUBLIC ${PROTOTYPE_TEST_REQUIRED_HEADERS})
  endfunction()
endif()
