# ---------------------------------------------------------------------------
# Leanstore DB
# ---------------------------------------------------------------------------

project(leanstoredb)
cmake_minimum_required(VERSION 3.7)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
enable_language(ASM)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -rdynamic")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64) # -fno-elide-constructors no need for now
else()
  add_compile_options(-Wall -Wextra -march=native)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3") # because we had weird behavior on aws ARM

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
    add_compile_options(-fstandalone-debug)
endif ()

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

include("${CMAKE_SOURCE_DIR}/libs/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/libs/tabluate.cmake")
include("${CMAKE_SOURCE_DIR}/libs/googletest.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/benchmark.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/rapidjson.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/yaml-cpp.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/fastpfor.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/spdlog.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/tbb.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/psql.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/gdouble.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/turbo.cmake")
#nclude("${CMAKE_SOURCE_DIR}/libs/lz4.cmake")

# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/shared-headers/local.cmake")

include_directories(
        ${CMAKE_SOURCE_DIR}/include
        ${GTEST_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

add_subdirectory("backend")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

add_subdirectory("tests")

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------

add_subdirectory("frontend")
add_subdirectory("tools")

# ---------------------------------------------------------------------------
# Linting: TODO
# ---------------------------------------------------------------------------

#include("${CMAKE_SOURCE_DIR}/cmake/clang-tidy.cmake")
#list(APPEND lint_targets lint_src)
#add_custom_target(lint)
#add_dependencies(lint ${lint_targets})
