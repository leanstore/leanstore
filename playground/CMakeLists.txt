project(playground)

add_executable(btree_sloted btree_sloted.cpp)

add_executable(playground playground.cpp)

add_executable(libaio libaio.cpp)
target_link_libraries(libaio PRIVATE aio)