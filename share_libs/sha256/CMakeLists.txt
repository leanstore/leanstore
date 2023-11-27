# AVX512 instrinsics are disabled by default, uncomment this to enable
#add_definitions(-DFSST_ENABLE_INTRINSINCS)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
if(CMAKE_APPLE_SILICON_PROCESSOR AND CMAKE_SYSTEM_PROCESSOR MATCHES "(aarch64)")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc+crypto")
  set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -fPIC")
  add_library(sha256 STATIC sha256_arm.cc)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4 -msha")
  set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -fPIC")
  add_library(sha256 STATIC sha256_x86.cc)
endif()

target_include_directories(sha256 PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(sha256 PROPERTIES EXPORT_NAME sha256)

install(TARGETS sha256
        EXPORT "${EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
