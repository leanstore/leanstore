cmake_minimum_required(VERSION 3.10)

# ---------------------------------------------------------------------------
# Leanstore DB
# ---------------------------------------------------------------------------
project(
  LeanStore
  VERSION 0.1
  LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Detect Architecture
# ---------------------------------------------------------------------------
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4 -msha")
else()
  add_compile_options(-Wall -Wextra)
endif()

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20) # Compile as C++20.
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Require C++20 support.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-stringop-overflow -fasynchronous-unwind-tables")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For clang-tidy.

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to `Debug` as none was specified.")
  set(CMAKE_BUILD_TYPE "Debug")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
  add_compile_options(-fstandalone-debug)
endif()

# Expected directory structure.
set(BUILD_SUPPORT_DIR ${CMAKE_SOURCE_DIR}/build_support)
set(SRC_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src/include ${CMAKE_SOURCE_DIR}/benchmark/src/include)
set(THIRD_PARTY_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/share_libs/include)

# Directory to format source code
string(CONCAT BUILD_FORMAT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
              "${CMAKE_CURRENT_SOURCE_DIR}/src," "${CMAKE_CURRENT_SOURCE_DIR}/test,")

# ---------------------------------------------------------------------------
# Third-Parties & Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)
find_package(TBB REQUIRED)
find_package(GTest REQUIRED)
find_package(gflags REQUIRED)
find_package(benchmark REQUIRED)
add_subdirectory(share_libs)

set(DB_CLIENT_LIBS wiredtiger sqlite3 pqxx mysqlcppconn rocksdb)
set(COMPRESS_LIBS zstd lz4 fsst)
set(CORE_LIBS
    Threads::Threads
    uring
    gflags
    benchmark::benchmark
    tbb
    fmt
    yyjson
    typefold
    croaring
    fuse
    atomic
    cpptrace::cpptrace)

set(THIRD_PARTY_DEPENDENCIES "${CORE_LIBS}" "${DB_CLIENT_LIBS}" "${COMPRESS_LIBS}")

# clang-format
find_program(CLANG_FORMAT_BIN NAMES clang-format clang-format-18)
if("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
  message(WARNING "LeanStore couldn't find clang-format.")
else()
  message(STATUS "LeanStore found clang-format at ${CLANG_FORMAT_BIN}")
endif()

# clang-tidy
find_program(CLANG_TIDY_BIN NAMES clang-tidy clang-tidy-18)

if("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
  message(WARNING "LeanStore couldn't find clang-tidy.")
else()
  message(STATUS "LeanStore found clang-tidy at ${CLANG_TIDY_BIN}")
endif()

# ---------------------------------------------------------------------------
# Make Targets
# ---------------------------------------------------------------------------

# Runs clang format and updates files in place.
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN} --source_dirs
                         ${BUILD_FORMAT_DIRS} --fix --quiet)

# Runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py ${CLANG_FORMAT_BIN}
                               --source_dirs ${BUILD_FORMAT_DIRS} --quiet)

# Clang-tidy
add_custom_target(check-tidy ${BUILD_SUPPORT_DIR}/run_clang_tidy.py -clang-tidy-binary
                             ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR} -only-diff)

add_custom_target(check-tidy-full ${BUILD_SUPPORT_DIR}/run_clang_tidy.py -clang-tidy-binary
                                  ${CLANG_TIDY_BIN} -p ${CMAKE_BINARY_DIR})

# ---------------------------------------------------------------------------
# Sources & Benchmark
# ---------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_SOURCE_FILES src/**.cc src/**/**.cc src/**/**.h)
add_library(leanstore STATIC ${LEANSTORE_SOURCE_FILES})
target_link_libraries(leanstore PUBLIC ${THIRD_PARTY_DEPENDENCIES})
target_include_directories(leanstore PUBLIC ${SRC_INCLUDE_DIR} ${THIRD_PARTY_INCLUDE_DIR})

add_subdirectory(benchmark)

# ---------------------------------------------------------------------------
# Testing & Sanitizer
# ---------------------------------------------------------------------------
option(ENABLE_TESTING "Build tests" OFF)
if(ENABLE_TESTING)
  enable_testing()
  set(CMAKE_BUILD_TYPE "DEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DENABLE_TESTING -D_FILE_OFFSET_BITS=64")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_TESTING -D_FILE_OFFSET_BITS=64")
  add_subdirectory(test)
endif()

option(SANITIZER "Compile leanstore with sanitizer" OFF)
if(SANITIZER)
  message(STATUS "LeanStore enable sanitizer")
  target_compile_options(leanstore PUBLIC -fsanitize=address)
  target_link_libraries(leanstore PUBLIC asan)
endif()
