include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# SPDK
#add_subdirectory(extern/spdk)
ExternalProject_Add(
  spdk-ext
  PREFIX "vendor/spdk"
  GIT_REPOSITORY "https://github.com/spdk/spdk"
  GIT_TAG "v22.09"
  #SOURCE_DIR "${PROJECT_SOURCE_DIR}/extern/spdk"
  #CONFIGURE_COMMAND ${PROJECT_SOURCE_DIR}/extern/spdk/configure --enable-debug 
  CONFIGURE_COMMAND ./configure --without-shared
  BUILD_COMMAND ${make_cmd} 
  BUILD_IN_SOURCE 1
  UPDATE_COMMAND ""
  INSTALL_COMMAND "true"
)

ExternalProject_Get_Property(spdk-ext source_dir)
message(${source_dir})
set(SPDK_INCLUDE_DIR ${source_dir}/build/include)
set(SPDK_LIBRARY_PATH ${source_dir}/build/lib)
set(DPDK_INCLUDE_DIR ${source_dir}/dpdk/build/include)
set(DPDK_LIBRARY_PATH ${source_dir}/dpdk/build/lib)
#set(ISAL_INCLUDE_DIR ${source_dir}/dpdk/build/include)
set(ISAL_LIBRARY_PATH ${source_dir}/isa-l/.libs/)

# lib
add_library(spdk STATIC IMPORTED)
set_property(TARGET spdk PROPERTY IMPORTED_LOCATION ${SPDK_LIBRARY_PATH} ${DPDK_LIBRARY_PATH})
set_property(TARGET spdk APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${SPDK_INCLUDE_DIR} ${DPDK_INCLUDE_DIR})

include_directories(${SPDK_INCLUDE_DIR})
link_directories(${SPDK_LIBRARY_DIR})

# Dependency
add_dependencies(spdk spdk-ext)
