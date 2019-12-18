# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
        fastpfor_src
        PREFIX "vendor/lemire/fastpfor"
        GIT_REPOSITORY "https://github.com/lemire/FastPFor.git"
        GIT_TAG dfc88a9f775958f325982a2699db08324fca0568
        TIMEOUT 10
        BUILD_COMMAND make FastPFor
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        INSTALL_COMMAND ""
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/fastpfor_cpp
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

# Prepare json
ExternalProject_Get_Property(fastpfor_src source_dir)
ExternalProject_Get_Property(fastpfor_src binary_dir)

set(FASTPFOR_INCLUDE_DIR ${source_dir}/)
set(FASTPFOR_LIBRARY_PATH ${binary_dir}/libFastPFor.a)

file(MAKE_DIRECTORY ${FASTPFOR_INCLUDE_DIR})

add_library(fastpfor STATIC IMPORTED)
add_dependencies(fastpfor fastpfor_src)

set_property(TARGET fastpfor PROPERTY IMPORTED_LOCATION ${FASTPFOR_LIBRARY_PATH})
set_property(TARGET fastpfor APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${FASTPFOR_INCLUDE_DIR})
