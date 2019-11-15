project(tools)
set(ENV{LD_RUN_PATH} "/home/adnan/gcc92/lib/../lib64")
add_executable(btree_benchmark btree_benchmark.cpp)
target_link_libraries(btree_benchmark leanstore)


add_executable(caught_exceptions caught_exceptions.cpp fast-uncaught-exception/fast-uncaught-exception.cpp)
