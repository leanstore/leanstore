# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.7)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
add_compile_options(-Wall -Wextra)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
    add_compile_options(-fstandalone-debug)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (APPLE)
    list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/bison)
    list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/flex)
endif (APPLE)

if (CYGWIN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
endif (CYGWIN)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)

include("${CMAKE_SOURCE_DIR}/vendor/benchmark.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/rapidjson.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/yaml-cpp.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/clang-tidy.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/fastpfor.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/spdlog.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/tbb.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/croaring.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/psql.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/gdouble.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/turbo.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/lz4.cmake")

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/shared-headers/local.cmake")
include_directories(
        ${CMAKE_SOURCE_DIR}/include
        ${FLEX_INCLUDE_DIRS}
        ${GTEST_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

#include("${CMAKE_SOURCE_DIR}/playground/local.cmake")
#include("${CMAKE_SOURCE_DIR}/cengine/local.cmake")
#include("${CMAKE_SOURCE_DIR}/tools/local.cmake")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

#include("${CMAKE_SOURCE_DIR}/test/local.cmake")

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

#include("${CMAKE_SOURCE_DIR}/benchmark/local.cmake")

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------

add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore psql)
target_include_directories(harbook PRIVATE ${CENGINE_INCLUDE_DIR})
# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------


#todo

list(APPEND lint_targets lint_src)
add_custom_target(lint)
add_dependencies(lint ${lint_targets})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

message(STATUS "[cengine] settings")
message(STATUS "    GFLAGS_INCLUDE_DIR          = ${GFLAGS_INCLUDE_DIR}")
message(STATUS "    GFLAGS_LIBRARY_PATH         = ${GFLAGS_LIBRARY_PATH}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
