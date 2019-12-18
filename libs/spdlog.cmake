# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
        spdlog_src
        PREFIX "vendor/spdlog"
        GIT_REPOSITORY "https://github.com/gabime/spdlog.git"
        GIT_TAG 366935142780e3754b358542f0fd5ed83793b263
        TIMEOUT 10
        BUILD_COMMAND ""
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        INSTALL_COMMAND ""
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/spdlog_cpp
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

# Prepare json
ExternalProject_Get_Property(spdlog_src source_dir)
ExternalProject_Get_Property(spdlog_src binary_dir)
set(SPDLOG_INCLUDE_DIR ${source_dir}/include/)
file(MAKE_DIRECTORY ${SPDLOG_INCLUDE_DIR})
