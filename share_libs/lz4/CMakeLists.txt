include(ExternalProject)

if(MSVC)
  set(BUILD_COMMAND ${CMAKE_VS_MSBUILD_COMMAND} /property:PlatformToolset=${CMAKE_VS_PLATFORM_TOOLSET} /p:Configuration=$<CONFIG> /p:Platform=${CMAKE_VS_PLATFORM_NAME} "visual/VS2010/lz4.sln")
else()
  set(BUILD_COMMAND $(MAKE))
endif()

ExternalProject_Add(
  LibLz4
  GIT_REPOSITORY https://github.com/lz4/lz4.git
  GIT_TAG v1.8.3
  BUILD_COMMAND ${BUILD_COMMAND}
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ""
  CONFIGURE_COMMAND ""
)

ExternalProject_Get_property(LibLz4 SOURCE_DIR)
ExternalProject_Get_property(LibLz4 BINARY_DIR)

add_library(lz4 INTERFACE)

if(MSVC)
  target_link_libraries(lz4 INTERFACE ${BINARY_DIR}/visual/VS2010/bin/${CMAKE_VS_PLATFORM_NAME}_$<CONFIG>/liblz4_static.lib)
else()
  target_link_libraries(lz4 INTERFACE ${BINARY_DIR}/lib/liblz4.a)
endif()

target_include_directories(lz4 INTERFACE ${SOURCE_DIR}/lib)

add_dependencies(lz4 LibLz4)