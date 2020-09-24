add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)


add_executable(ycsb ycsb/ycsb.cpp)
target_link_libraries(ycsb leanstore Threads::Threads)
target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(exp ycsb/exp.cpp)
target_link_libraries(exp leanstore)
target_include_directories(exp PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads)
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})


add_executable(random_io misc-benchmarks/random_io.cpp)
target_link_libraries(random_io leanstore Threads::Threads)
target_include_directories(random_io PRIVATE ${SHARED_INCLUDE_DIRECTORY})


add_executable(scan misc-benchmarks/scan.cpp)
target_link_libraries(scan leanstore Threads::Threads)
target_include_directories(scan PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(zipf misc-benchmarks/zipf.cpp)
target_link_libraries(zipf leanstore Threads::Threads)
target_include_directories(zipf PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(latest misc-benchmarks/latest.cpp)
target_link_libraries(latest leanstore Threads::Threads)
target_include_directories(latest PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(contention misc-benchmarks/contention.cpp)
target_link_libraries(contention leanstore Threads::Threads)
target_include_directories(contention PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(merge misc-benchmarks/merge.cpp)
target_link_libraries(merge leanstore Threads::Threads)
target_include_directories(merge PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(urls misc-benchmarks/urls.cpp)
target_link_libraries(urls leanstore Threads::Threads)
target_include_directories(urls PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(debug misc-benchmarks/debug.cpp)
target_link_libraries(debug leanstore Threads::Threads)
target_include_directories(debug PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(seq misc-benchmarks/seq.cpp)
target_link_libraries(seq leanstore Threads::Threads)
target_include_directories(seq PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(cx16 misc-benchmarks/cx16.cpp)
target_link_libraries(cx16 leanstore Threads::Threads)
target_include_directories(cx16 PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(replacement_strategy_test misc-benchmarks/replacement_strategy_test.cpp)
target_link_libraries(replacement_strategy_test leanstore Threads::Threads)
target_include_directories(replacement_strategy_test PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(fairness misc-benchmarks/fairness.cpp)
target_link_libraries(fairness leanstore Threads::Threads)
target_include_directories(fairness PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(scheduler misc-benchmarks/scheduler.cpp)
target_link_libraries(scheduler leanstore Threads::Threads)
target_include_directories(scheduler PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(ut misc-benchmarks/ut.cpp)
target_link_libraries(ut leanstore Threads::Threads)
target_include_directories(ut PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(wal misc-benchmarks/wal.cpp)
target_link_libraries(wal leanstore Threads::Threads)
target_include_directories(wal PRIVATE ${SHARED_INCLUDE_DIRECTORY})
