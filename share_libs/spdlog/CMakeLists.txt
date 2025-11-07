include(ExternalProject)
set(BUILD_COMMAND $(MAKE))

ExternalProject_Add(
  Libspdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.15.0
  BUILD_COMMAND ${BUILD_COMMAND}
  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/Libspdlog
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>  -DCMAKE_INSTALL_LIBDIR=lib  
)

ExternalProject_Get_Property(Libspdlog INSTALL_DIR)

add_library(spdlog INTERFACE)
target_link_libraries(spdlog INTERFACE ${INSTALL_DIR}/lib/libspdlog.a)
target_include_directories(spdlog INTERFACE ${INSTALL_DIR}/include)
add_dependencies(spdlog Libspdlog fmt)
