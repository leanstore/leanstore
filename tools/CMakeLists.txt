project(tools)

add_executable(btree_benchmark btree_benchmark.cpp)
target_link_libraries(btree_benchmark leanstore)