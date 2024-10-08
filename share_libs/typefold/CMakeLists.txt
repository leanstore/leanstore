set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -fPIC")
add_library(typefold STATIC typefold.cc)
target_include_directories(
  typefold PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                  ${THIRD_PARTY_INCLUDE_DIR})
set_target_properties(typefold PROPERTIES EXPORT_NAME typefold)

install(
  TARGETS typefold
  EXPORT "${EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
