add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)


add_executable(ycsb ycsb/ycsb.cpp)
target_link_libraries(ycsb leanstore Threads::Threads)
target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads )
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(iob iob/iob.cpp)
target_link_libraries(iob leanstore Threads::Threads)
target_include_directories(iob PRIVATE ${SHARED_INCLUDE_DIRECTORY})


add_executable(memalloc memalloc/memalloc.cpp)
target_link_libraries(memalloc leanstore Threads::Threads )
target_include_directories(memalloc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(spdkmicro spdkmicro/spdkmicro.cpp)
target_link_libraries(spdkmicro leanstore Threads::Threads )
target_include_directories(spdkmicro PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(rdtsc rdtsc/rdtsc.cpp)
target_link_libraries(rdtsc Threads::Threads )
target_include_directories(rdtsc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(smalllist smalllist/smalllist.cpp)
target_link_libraries(smalllist leanstore Threads::Threads )
target_include_directories(smalllist PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(ringbuffer ringbuffer/ringbuffer.cpp)
target_link_libraries(ringbuffer leanstore Threads::Threads )
target_include_directories(ringbuffer PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(znsmicro znsmicro/znsmicro.cpp)
target_link_libraries(znsmicro leanstore Threads::Threads )
target_include_directories(znsmicro PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(dsmdiscard dsmdiscard/dsmdiscard.cpp)
target_link_libraries(dsmdiscard leanstore Threads::Threads )
target_include_directories(dsmdiscard PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(contextmicro contextmicro/contextmicro.cpp)
target_link_libraries(contextmicro leanstore Threads::Threads )
target_include_directories(contextmicro PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(iotrace iotrace/iotrace.cpp)
target_link_libraries(iotrace leanstore Threads::Threads )
target_include_directories(iotrace PRIVATE ${SHARED_INCLUDE_DIRECTORY})
