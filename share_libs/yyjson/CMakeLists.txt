include(ExternalProject)
set(BUILD_COMMAND $(MAKE))

ExternalProject_Add(
  Libyyjson
  GIT_REPOSITORY https://github.com/ibireme/yyjson.git
  GIT_TAG 0.7.0
  BUILD_COMMAND ${BUILD_COMMAND}
  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/Libyyjson
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
)

ExternalProject_Get_Property(Libyyjson INSTALL_DIR)

add_library(yyjson INTERFACE)
target_link_libraries(yyjson INTERFACE ${INSTALL_DIR}/lib/libyyjson.a)
target_include_directories(yyjson INTERFACE ${INSTALL_DIR}/include)
add_dependencies(yyjson Libyyjson)
