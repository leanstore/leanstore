# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get psql
ExternalProject_Add(
        psql_src
        PREFIX "vendor/psql"
        GIT_REPOSITORY "https://github.com/jtv/libpqxx.git"
        GIT_TAG 520113bc790d5f44726830c23018668cb5241e67
        TIMEOUT 10
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/psql
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        UPDATE_COMMAND ""
)

# Prepare psql
ExternalProject_Get_Property(psql_src install_dir)
set(psql_INCLUDE_DIR ${install_dir}/include)
set(psql_LIBRARY_PATH ${install_dir}/lib/libpqxx.so)
file(MAKE_DIRECTORY ${psql_INCLUDE_DIR})
add_library(psql SHARED IMPORTED)
set_property(TARGET psql PROPERTY IMPORTED_LOCATION ${psql_LIBRARY_PATH})
set_property(TARGET psql APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${psql_INCLUDE_DIR})

# Dependencies
add_dependencies(psql psql_src)
