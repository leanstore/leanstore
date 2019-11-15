add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)


add_executable(ycsb ycsb/ycsb.cpp)
target_link_libraries(ycsb leanstore gtest gmock Threads::Threads)
target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})