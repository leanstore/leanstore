# ---------------------------------------------------------------------------
# Benchmark config templates
# ---------------------------------------------------------------------------
set(WIKI_WORKLOAD_CSV ${CMAKE_CURRENT_SOURCE_DIR}/src/wikipedia/summary.csv)
set(WIKI_ARTICLES_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/wikipedia/articles)
set(GIT_WORKLOAD_CSV ${CMAKE_CURRENT_SOURCE_DIR}/src/gitclone/traces/main.csv)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/wikipedia/config.cc.in
               ${CMAKE_CURRENT_SOURCE_DIR}/src/wikipedia/config.cc)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/gitclone/config.cc.in
               ${CMAKE_CURRENT_SOURCE_DIR}/src/gitclone/config.cc)

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
add_benchmark(LeanStore_YCSB main/ycsb/leanstore_ycsb.cc)
add_benchmark(LeanStore_FB_WebServer main/leanstore_webserver.cc)
add_benchmark(LeanStore_Wiki main/wikipedia/leanstore_wikipedia.cc)
add_benchmark(LeanStore_Gitclone main/gitclone/leanstore_gitclone.cc)
add_benchmark(LeanStore_IndexWiki main/misc/leanstore_wiki_index.cc)
add_benchmark(LeanStore_Aging main/misc/leanstore_aging.cc)
add_benchmark(Filesystem_YCSB main/ycsb/filesystem_ycsb.cc)
add_benchmark(Filesystem_Wiki main/wikipedia/filesystem_wikipedia.cc)
add_benchmark(Filesystem_Gitclone main/gitclone/filesystem_gitclone.cc)
add_benchmark(Filesystem_Aging main/misc/filesystem_aging.cc)
add_benchmark(SQLite_YCSB main/ycsb/sqlite3_ycsb.cc)
add_benchmark(Postgres_YCSB main/ycsb/postgres_ycsb.cc)
add_benchmark(MySQL_YCSB main/ycsb/mysql_ycsb.cc)
add_benchmark(MicroSHA2 micro/sha2.cc)
add_benchmark(MicroCompression micro/compress.cc)
add_benchmark(MicroFreePages micro/free_pages.cc)
add_benchmark(MicroQueue micro/queue.cc)
add_benchmark(MicroBitmap micro/bitmap.cc)
add_benchmark(MicroSearch micro/search.cc)

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
