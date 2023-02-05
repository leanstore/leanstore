include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# Liburing
ExternalProject_Add(
        liburing-ext
        PREFIX "vendor/liburing"
        GIT_REPOSITORY "https://github.com/axboe/liburing.git"
        GIT_TAG master
        TIMEOUT 10
        CONFIGURE_COMMAND "./configure"
        BUILD_IN_SOURCE TRUE
        BUILD_COMMAND make
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
)

ExternalProject_Get_Property(liburing-ext source_dir)
set(liburing_INCLUDE_DIR ${source_dir}/src/include)
set(liburing_LIBRARY_PATH ${source_dir}/src/)
file(MAKE_DIRECTORY ${liburing_INCLUDE_DIR})

# lib
add_library(liburing STATIC IMPORTED)
set_property(TARGET liburing PROPERTY IMPORTED_LOCATION ${liburing_LIBRARY_PATH})
set_property(TARGET liburing APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${liburing_INCLUDE_DIR})

include_directories(${liburing_INCLUDE_DIR})

# Dependency
add_dependencies(liburing liburing-ext)
