project(playground)

add_executable(btree_sloted btree_sloted.cpp BTreeSlotted.cpp)
target_link_libraries(btree_sloted leanstore)

add_executable(playground playground.cpp)
target_link_libraries(playground tbb)
target_include_directories(playground PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(libaio libaio.cpp)
target_link_libraries(libaio PRIVATE aio)
