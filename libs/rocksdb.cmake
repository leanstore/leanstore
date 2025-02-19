# ---------------------------------------------------------------------------
# leanstore
# ---------------------------------------------------------------------------
# Note: RocksDB has CMake build only for windows 64-bit, therefore we use make after cloning their repo
# Not used atm, use sudo apt install -y librocksdb-dev instead

include(ExternalProject)
find_package(Git REQUIRED)

# Get rocksdb
ExternalProject_Add(
        rocksdb_src
        PREFIX "vendor/rocksdb"
        GIT_REPOSITORY "https://github.com/facebook/rocksdb.git"
        GIT_TAG 5f003e4a22d2e48e37c98d9620241237cd30dd24
        GIT_SHALLOW TRUE
        TIMEOUT 10
        CONFIGURE_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        BUILD_COMMAND $(MAKE) static_lib USE_RTTI=1
        BUILD_IN_SOURCE TRUE
)

# Prepare rocksdb
ExternalProject_Get_Property(rocksdb_src source_dir)
set(ROCKSDB_INCLUDE_DIR ${source_dir}/include)
set(ROCKSDB_LIBRARY_PATH ${source_dir}/librocksdb.a)
#set(ROCKSDB_LIBRARY_PATH ${source_dir}/librocksdb.so)
file(MAKE_DIRECTORY ${ROCKSDB_INCLUDE_DIR})
add_library(rocksdb STATIC IMPORTED)
set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${ROCKSDB_LIBRARY_PATH})
set_property(TARGET rocksdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ROCKSDB_INCLUDE_DIR})

# Dependencies
add_dependencies(rocksdb rocksdb_src)
