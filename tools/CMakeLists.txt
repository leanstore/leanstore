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

add_executable(txt_to_vector txt_to_vector.cpp)
target_link_libraries(txt_to_vector leanstore)
target_include_directories(txt_to_vector PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(futex futex.cpp)
target_link_libraries(futex Threads::Threads tbb gflags)
target_include_directories(futex PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tx tx.cpp)
target_link_libraries(tx leanstore Threads::Threads tbb gflags)
target_include_directories(tx PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tcp tcp.cpp)
target_link_libraries(tcp Threads::Threads gflags)
target_include_directories(tcp PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(contention_effects micro-benchmarks/contention_effects.cpp)
target_link_libraries(contention_effects leanstore Threads::Threads gflags)
target_include_directories(contention_effects PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(signal micro-benchmarks/signal.cpp)
target_link_libraries(signal leanstore Threads::Threads gflags)
target_include_directories(signal PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(perf_test micro-benchmarks/perf_test.cpp)
target_link_libraries(perf_test Threads::Threads gflags)
target_include_directories(perf_test PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(aio_mb micro-benchmarks/aio.cpp)
target_link_libraries(aio_mb leanstore Threads::Threads gflags aio)
target_include_directories(aio_mb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(morsel morsel/morsel.cpp)
target_link_libraries(morsel Threads::Threads gflags)
target_include_directories(morsel PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(cli cli.cpp)
target_link_libraries(cli Threads::Threads gflags)
target_include_directories(cli PRIVATE ${SHARED_INCLUDE_DIRECTORY})
