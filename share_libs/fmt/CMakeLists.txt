include(ExternalProject)
set(BUILD_COMMAND $(MAKE))

ExternalProject_Add(
  Libfmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 11.0.2
  BUILD_COMMAND ${BUILD_COMMAND}
  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/Libfmt
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
)

ExternalProject_Get_Property(Libfmt INSTALL_DIR)

add_library(fmt INTERFACE)
target_link_libraries(fmt INTERFACE ${INSTALL_DIR}/lib/libfmt.a)
target_include_directories(fmt INTERFACE ${INSTALL_DIR}/include)
add_dependencies(fmt Libfmt)
