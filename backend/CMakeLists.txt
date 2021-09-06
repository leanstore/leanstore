# ---------------------------------------------------------------------------
# Leanstore
# ---------------------------------------------------------------------------
project(leanstore)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(leanstore STATIC ${LEANSTORE_CC})

OPTION(SANI "Compile leanstore with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    target_compile_options(leanstore PUBLIC -fsanitize=address)
    target_link_libraries(leanstore asan)
  endif ()
ENDIF(SANI)

target_link_libraries(leanstore gflags Threads::Threads aio tbb atomic tabluate rapidjson) #tbb

# ---------------------------------------------------------------------------
SET(COUNTERS_LEVEL "all" CACHE STRING "Which counters to leave in leanstore build")
IF(COUNTERS_LEVEL STREQUAL "all")
  target_compile_definitions(leanstore PUBLIC MACRO_COUNTERS_ALL)
ENDIF()

SET(CHECKS_LEVEL "default" CACHE STRING "Which checks to leave in leanstore build")
IF(CHECKS_LEVEL STREQUAL "default")
  IF(CMAKE_BUILD_TYPE MATCHES Debug)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
  ELSEIF(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
  ENDIF()
ELSEIF(CHECKS_LEVEL STREQUAL "debug")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
ELSEIF(CHECKS_LEVEL STREQUAL "release")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
ELSEIF(CHECKS_LEVEL STREQUAL "benchmark")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_BENCHMARK)
ENDIF()
# ---------------------------------------------------------------------------
target_include_directories(leanstore PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})
