project(tools)
set(ENV{LD_RUN_PATH} "/home/adnan/gcc92/lib/../lib64")

add_executable(btree_benchmark btree-implementations/btree_benchmark.cpp)
target_link_libraries(btree_benchmark leanstore)

add_executable(random_benchmark random_benchmark.cpp)
target_link_libraries(random_benchmark leanstore)

add_executable(io_benchmark io_benchmark.cpp)
target_link_libraries(io_benchmark leanstore)
target_include_directories(io_benchmark PRIVATE ${SHARED_INCLUDE_DIRECTORY})


add_executable(caught_exceptions caught_exceptions.cpp fast-uncaught-exception/fast-uncaught-exception.cpp)
target_include_directories(caught_exceptions PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(mutex_benchmark BenchmarkMutex.cpp)
target_link_libraries(mutex_benchmark gbenchmark)
target_include_directories(mutex_benchmark PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(txt_to_vector txt_to_vector.cpp)
target_link_libraries(txt_to_vector leanstore)
target_include_directories(txt_to_vector PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(futex futex.cpp)
target_link_libraries(futex Threads::Threads tbb gflags)
target_include_directories(futex PRIVATE ${SHARED_INCLUDE_DIRECTORY})
