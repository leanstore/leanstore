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
