# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE TEST_CASES **/**.cpp **/**.hpp)
# ---------------------------------------------------------------------------
# Tester
# ---------------------------------------------------------------------------

add_executable(tester tester.cpp ${TEST_CASES})
target_link_libraries(tester leanstore gtest gmock Threads::Threads)
target_include_directories(tester PRIVATE ${SHARED_INCLUDE_DIRECTORY})

enable_testing()
add_test(leanstore tester)
