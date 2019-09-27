# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# Get lz4
ExternalProject_Add(
    lz4_src
    PREFIX "vendor/lz4"
    GIT_REPOSITORY "https://github.com/lz4/lz4.git"
    GIT_TAG 798301b4e144fab5d25fc34566c1419685f5f1eb
    TIMEOUT 10
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND make
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
)

# Prepare lz4
ExternalProject_Get_Property(lz4_src source_dir)
set(lz4_INCLUDE_DIR ${source_dir}/lib)
set(lz4_LIBRARY_PATH ${source_dir}/lib/liblz4.a)
file(MAKE_DIRECTORY ${lz4_INCLUDE_DIR})
add_library(lz4 STATIC IMPORTED)

set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION ${lz4_LIBRARY_PATH})
set_property(TARGET lz4 APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${lz4_INCLUDE_DIR})

# Dependencies
add_dependencies(lz4 lz4_src)
