include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# xnvme
ExternalProject_Add(
        xnvme-ext
        PREFIX "vendor/xnvme"
        GIT_REPOSITORY "https://github.com/OpenMPDK/xNVMe.git"
        GIT_TAG main
        TIMEOUT 10
        CONFIGURE_COMMAND meson setup builddir
        BUILD_IN_SOURCE TRUE
        BUILD_COMMAND meson compile -C builddir
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
)

ExternalProject_Get_Property(xnvme-ext source_dir)
set(XNVME_INCLUDE_DIR ${source_dir}/include)
set(XNVME_LIBRARY_PATH ${source_dir}/builddir/lib)
file(MAKE_DIRECTORY ${XNVME_INCLUDE_DIR})

# lib
add_library(xnvme STATIC IMPORTED)
set_property(TARGET xnvme PROPERTY IMPORTED_LOCATION ${XNMVE_LIBRARY_PATH})
set_property(TARGET xnvme APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${XNVME_INCLUDE_DIR})

include_directories(${XNVME_INCLUDE_DIR})
link_directories(${XNVME_LIBRARY_DIR})

# Dependency
add_dependencies(xnvme xnvme-ext)
