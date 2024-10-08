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
add_benchmark(LeanStore_TPCC main/leanstore_tpcc.cc)
add_benchmark(LeanStore_TATP main/leanstore_tatp.cc)
add_benchmark(LeanStore_MixWorkloads main/leanstore_mixed.cc)
add_benchmark(LeanStore_YCSB main/ycsb/leanstore_ycsb.cc)
add_benchmark(LeanStore_FB_WebServer main/leanstore_webserver.cc)
add_benchmark(LeanStore_Aging main/misc/leanstore_aging.cc)
add_benchmark(Filesystem_YCSB main/ycsb/filesystem_ycsb.cc)
add_benchmark(Filesystem_Aging main/misc/filesystem_aging.cc)
add_benchmark(SQLite_YCSB main/ycsb/sqlite3_ycsb.cc)
add_benchmark(WiredTiger_YCSB main/ycsb/wiredtiger_ycsb.cc)
add_benchmark(RocksDB_YCSB main/ycsb/rocksdb_ycsb.cc)
add_benchmark(Postgres_YCSB main/ycsb/postgres_ycsb.cc)
add_benchmark(MySQL_YCSB main/ycsb/mysql_ycsb.cc)
add_benchmark(MicroSparsePTE micro/sparse_pte.cc)
add_benchmark(MicroRDTSC micro/rdtsc.cc)

# ---------------------------------------------------------------------------
# Tests for custom prototypes
# ---------------------------------------------------------------------------
if(ENABLE_TESTING)
  set(PROTOTYPE_TEST_REQUIRED_LIBS gtest ${THIRD_PARTY_DEPENDENCIES})
  set(PROTOTYPE_TEST_REQUIRED_HEADERS ${GTEST_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/micro)
  function(PROTOTYPE_ADD_TEST TEST_NAME)
    add_executable(${ARGV})
    target_link_libraries(${TEST_NAME} PUBLIC ${PROTOTYPE_TEST_REQUIRED_LIBS})
    target_include_directories(${TEST_NAME} PUBLIC ${PROTOTYPE_TEST_REQUIRED_HEADERS})
  endfunction()

  prototype_add_test(TestFreePagePrototype micro/prototype/free_page/test.cc)
endif()
