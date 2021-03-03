# ---------------------------------------------------------------------------
# leanstore
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
        rapidjson_src
        PREFIX "vendor/rapidjson"
        GIT_REPOSITORY "https://github.com/tencent/rapidjson"
        GIT_TAG 1c2c8e085a8b2561dff17bedb689d2eb0609b689
        TIMEOUT 10
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        UPDATE_COMMAND ""
)

# Prepare json
ExternalProject_Get_Property(rapidjson_src source_dir)
set(RAPIDJSON_INCLUDE_DIR ${source_dir}/include)
file(MAKE_DIRECTORY ${RAPIDJSON_INCLUDE_DIR})
add_library(rapidjson INTERFACE)
set_property(TARGET rapidjson APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${RAPIDJSON_INCLUDE_DIR})

# Dependencies
add_dependencies(rapidjson rapidjson_src)
