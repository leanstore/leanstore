# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get gflags
ExternalProject_Add(
    gflags_src
    PREFIX "vendor/gflags"
    GIT_REPOSITORY "https://github.com/gflags/gflags.git"
    GIT_TAG f8a0efe03aa69b3336d8e228b37d4ccb17324b88
    TIMEOUT 10
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/gflags
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    UPDATE_COMMAND ""
)

# Prepare gflags
ExternalProject_Get_Property(gflags_src install_dir)
set(GFLAGS_INCLUDE_DIR ${install_dir}/include)
set(GFLAGS_LIBRARY_PATH ${install_dir}/lib/libgflags.a)
file(MAKE_DIRECTORY ${GFLAGS_INCLUDE_DIR})
add_library(gflags STATIC IMPORTED)
set_property(TARGET gflags PROPERTY IMPORTED_LOCATION ${GFLAGS_LIBRARY_PATH})
set_property(TARGET gflags APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${GFLAGS_INCLUDE_DIR})

# Dependencies
add_dependencies(gflags gflags_src)
