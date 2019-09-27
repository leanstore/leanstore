# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get rapidjson
ExternalProject_Add(
    yaml_src
    PREFIX "vendor/yaml_cpp"
    GIT_REPOSITORY "https://github.com/jbeder/yaml-cpp.git"
    GIT_TAG bd7f8c60c82614bb0bd1c526db2cbc39dac02fec
    TIMEOUT 10
    BUILD_COMMAND  make
    UPDATE_COMMAND "" # to prevent rebuilding everytime
    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/yaml_cpp
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

# Prepare json
ExternalProject_Get_Property(yaml_src source_dir)
ExternalProject_Get_Property(yaml_src binary_dir)

set(YAML_INCLUDE_DIR ${source_dir}/include)
set(YAML_LIBRARY_PATH ${binary_dir}/libyaml-cpp.a)

file(MAKE_DIRECTORY ${YAML_INCLUDE_DIR})

add_library(yaml STATIC IMPORTED)
add_dependencies(yaml yaml_src)

set_property(TARGET yaml PROPERTY IMPORTED_LOCATION ${YAML_LIBRARY_PATH})
set_property(TARGET yaml APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${YAML_INCLUDE_DIR})
