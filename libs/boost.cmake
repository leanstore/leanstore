# Find Boost

set(Boost_USE_STATIC_LIBS   ON)

# sudo apt install libboost-context-dev
find_package(Boost  COMPONENTS context )

if (${Boost_FOUND})
	#INCLUDE_DIRECTORIES( ${Boost_INCLUDE_DIR} )
	#link_directories(${Boost_LIBRARY_DIR})
	message("Boost found.")
else()
	message("Boost not found.")
endif()
