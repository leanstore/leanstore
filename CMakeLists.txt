# ---------------------------------------------------------------------------
# Leanstore DB
# ---------------------------------------------------------------------------
cmake_minimum_required(VERSION 3.7)
project(leanstoredb)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
enable_language(ASM)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -rdynamic")

#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -mavx2 -mcx16 -m64) # -fno-elide-constructors no need for now
else()
  add_compile_options(-Wall -march=native)
endif()

add_compile_options(-Werror -Wno-error=unused-parameter -Wno-error=unused-variable -Wno-error=unused-local-typedefs -Wno-error=unused-but-set-variable -Wno-error=maybe-uninitialized -Wno-error=sign-compare -Wno-error=unused-function -Wno-error=missing-field-initializers) 

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3")
#set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=address")
#set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=undefined")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3") # because we had behavior on aws arm without

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

option(LEANSTORE_INCLUDE_XNVME "include xnvme, be carefull as this overrides the used spdk binary to use the one given by xnvme.." OFF) #OFF by default
if(LEANSTORE_INCLUDE_XNVME)
   message("Including XNVME")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEANSTORE_INCLUDE_XNVME")
else()
	message("NOT Including XNVME")
   # do nothing
endif(LEANSTORE_INCLUDE_XNVME)

option(LEANSTORE_INCLUDE_SPDK "include SPDK" OFF) #OFF by default
if(LEANSTORE_INCLUDE_SPDK)
   message("Including SPDK")
   set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEANSTORE_INCLUDE_SPDK")
else()
	message("NOT Including SPDK")
   # do nothing
endif(LEANSTORE_INCLUDE_SPDK)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)

set(THREADS_PREFER_PTHREAD_FLAG ON)

include("${CMAKE_SOURCE_DIR}/libs/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/libs/tabluate.cmake")
if(LEANSTORE_INCLUDE_SPDK)
	include("${CMAKE_SOURCE_DIR}/libs/spdk.cmake")
endif()
include("${CMAKE_SOURCE_DIR}/libs/uring.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/libnvme.cmake")
if(LEANSTORE_INCLUDE_XNVME)
   include("${CMAKE_SOURCE_DIR}/libs/xnvme.cmake")
endif()
include("${CMAKE_SOURCE_DIR}/libs/boost.cmake")
#include("${CMAKE_SOURCE_DIR}/libs/googletest.cmake")
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
        ${FLEX_INCLUDE_DIRS}
        ${GTEST_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

add_subdirectory("backend")

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------

add_subdirectory("frontend")

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------


#todo

#include("${CMAKE_SOURCE_DIR}/cmake/clang-tidy.cmake")
#list(APPEND lint_targets lint_src)
#add_custom_target(lint)
#add_dependencies(lint ${lint_targets})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

message(STATUS "[cengine] settings")
message(STATUS "    GFLAGS_INCLUDE_DIR          = ${GFLAGS_INCLUDE_DIR}")
message(STATUS "    GFLAGS_LIBRARY_PATH         = ${GFLAGS_LIBRARY_PATH}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
