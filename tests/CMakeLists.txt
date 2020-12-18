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

add_executable(tester tester.cpp ${TEST_CASES} leanstore/BTreeTest.cpp)
target_link_libraries(tester leanstore gtest Threads::Threads)
target_include_directories(tester PRIVATE ${SHARED_INCLUDE_DIRECTORY})
#
#target_compile_options(tester PUBLIC -fsanitize=address)
#target_link_libraries(tester asan) #tbb

enable_testing()
add_test(leanstore tester)
