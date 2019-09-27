# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# Get turbo
ExternalProject_Add(
    turbo_src
    PREFIX "vendor/turbo"
    GIT_REPOSITORY "https://github.com/powturbo/TurboPFor"
    GIT_TAG 58f49912603d85b60b176b8e5e905f69c5a259a8
    TIMEOUT 10
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND AVX2=1 make -j20 COMMAND ar rcs libic.a bitpack_avx2.o  bitpack_sse.o     bitunpack.o      bitutil.o    fp.o     icbench.o  idxqry.o  plugins.o         transpose.o      vint.o       vp4c.o      vp4d_avx2.o  vp4d_sse.o  bitpack.o       bitunpack_avx2.o  bitunpack_sse.o  eliasfano.o  icapp.o  idxcr.o    idxseg.o  transpose_avx2.o  transpose_sse.o  vp4c_avx2.o  vp4c_sse.o  vp4d.o       vsimple.o
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
)

# Prepare turbo
ExternalProject_Get_Property(turbo_src source_dir)
set(TURBO_INCLUDE_DIR ${source_dir}/)
set(TURBO_LIBRARY_PATH ${source_dir}/libic.a)
file(MAKE_DIRECTORY ${TURBO_INCLUDE_DIR})
add_library(turbo STATIC IMPORTED)

set_property(TARGET turbo PROPERTY IMPORTED_LOCATION ${TURBO_LIBRARY_PATH})
set_property(TARGET turbo APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TURBO_INCLUDE_DIR})

# Dependencies
add_dependencies(turbo turbo_src)
