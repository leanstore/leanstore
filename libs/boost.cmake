# Find Boost

set(Boost_USE_STATIC_LIBS   OFF)

# sudo apt install libboost-context-dev libboost-container-dev
find_package(Boost  COMPONENTS context container )

if (${Boost_FOUND})
	message("Boost found. ${Boost_LIBRARIES} ")
else()
	message("Boost not found.")
endif()
