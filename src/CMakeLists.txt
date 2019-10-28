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

add_library(leanstore STATIC ${LEANSTORE_CC} )

target_link_libraries(leanstore gflags Threads::Threads yaml aio tbb) #tbb
if (CMAKE_BUILD_TYPE MATCHES Debug)
    # CMAKE_<LANG>_STANDARD_LIBRARIES
    target_compile_options(leanstore PUBLIC -fsanitize=address)
    target_link_libraries(leanstore asan) #tbb
endif ()

target_include_directories(leanstore PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})

set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})
