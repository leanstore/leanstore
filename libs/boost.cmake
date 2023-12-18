#include(ExternalProject)

#set(Boost_Git_Tag "boost-1.82.0") # Set the desired Boost version tag here
#set(Boost_URL "https://github.com/boostorg/boost.git")

#ExternalProject_Add(
	#boost
  #PREFIX "vendor/boost"
  #GIT_REPOSITORY ${Boost_URL}
  #GIT_TAG ${Boost_Git_Tag}
  #GIT_SUBMODULES "libs/context tools libs/config libs/headers"
  #UPDATE_COMMAND ""
  #CONFIGURE_COMMAND ./bootstrap.sh --with-libraries=context
  #BUILD_COMMAND ./b2 
  ##BUILD_COMMAND ./b2 context-impl=ucontext
  ## use cxx flags: -DBOOST_USE_ASAN -DBOOST_USE_UCONTEXT
  #INSTALL_COMMAND "" #./b2 install --prefix=bla
  #BUILD_IN_SOURCE 1
  #)
#ExternalProject_Get_Property(boost source_dir)
#set(Boost_INCLUDE_DIR ${source_dir}/include)
#set(Boost_LIBRARY_DIR ${source_dir}/stage/lib)
#set(Boost_USE_STATIC_LIBS   ON)

#include_directories(${Boost_INCLUDE_DIR})
#link_directories(${Boost_LIBRARY})

# sudo apt install libboost-context-dev
set(Boost_USE_STATIC_LIBS   ON)
find_package(Boost REQUIRED COMPONENTS context )

if (${Boost_FOUND})
       INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
       link_directories(${Boost_LIBRARY_DIR})
       message("Boost found.")
else()
       message("Boost not found.")
endif()

