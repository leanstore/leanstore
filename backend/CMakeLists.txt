# ---------------------------------------------------------------------------
# Leanstore
# ---------------------------------------------------------------------------
project(leanstore)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(leanstore STATIC ${LEANSTORE_CC})
target_link_libraries(leanstore gflags Threads::Threads aio tbb atomic tabluate nvme) #tbb


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
add_dependencies(leanstore liburing)

# xnvme stuff
if(LEANSTORE_INCLUDE_XNVME)
   add_dependencies(leanstore xnvme)
   message("Including XNVME")
   target_link_libraries(leanstore 
      -L${XNVME_LIBRARY_PATH}
      -Wl,--whole-archive -Wl,--no-as-needed
      -lxnvme -lssl
      -Wl,--no-whole-archive -Wl,--as-needed
	  -pthread -lrt -lm -lnuma -luuid -laio
   )
endif()

if(LEANSTORE_INCLUDE_SPDK)
	add_dependencies(leanstore spdk)
	# all the SPDK stuff
	target_link_libraries(leanstore -L${SPDK_LIBRARY_PATH})
	target_link_libraries(leanstore  -lspdk_vmd -Wl,--whole-archive -lspdk_nvme -Wl,--no-whole-archive -lspdk_trace -lspdk_sock -lspdk_log -lspdk_json -lspdk_env_dpdk -lspdk_util -lspdk_vfio_user)
	target_link_libraries(leanstore
		-Wl,--whole-archive
		${DPDK_LIBRARY_PATH}/librte_eal.a
		${DPDK_LIBRARY_PATH}/librte_telemetry.a
		${DPDK_LIBRARY_PATH}/librte_mempool.a
		${DPDK_LIBRARY_PATH}/librte_kvargs.a
		${DPDK_LIBRARY_PATH}/librte_bus_pci.a
		${DPDK_LIBRARY_PATH}/librte_pci.a
		${DPDK_LIBRARY_PATH}/librte_ring.a
		-Wl,--no-whole-archive
	)
	target_link_libraries(leanstore -luuid -lnuma -lrt -lcrypto -laio)
	target_link_libraries(leanstore -L${ISAL_LIBRARY_PATH} -lisal )
endif()

target_link_libraries(leanstore ${TBB_LIBRARIES})

#add_dependencies(leanstore boost)
#target_link_libraries(leanstore -L${Boost_LIBRARY_DIR} ${Boost_LIBRARY_DIR}/libboost_context.a)
#target_include_directories(leanstore PRIVATE ${Boost_INCLUDE_DIR})
#target_link_libraries(leanstore -L~/boost_1_73_0/stage/lib/ -lboost_context)
#target_include_directories(leanstore PUBLIC ~/boost_1_73_0/boost/context/) 
# use it with cmake ... -DCMAKE_CXX_FLAGS='-Dleanstore_USE_TASKING -DBOOST_USE_UCONTEXT'
if(Boost_FOUND)
    target_link_libraries(leanstore ${Boost_LIBRARIES})
endif()


target_link_libraries(leanstore -L${liburing_LIBRARY_PATH})
target_link_libraries(leanstore -luring )

target_link_libraries(leanstore ${CMAKE_DL_LIBS})


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------



#if (CMAKE_BUILD_TYPE MATCHES Debug)
#    target_compile_options(leanstore PUBLIC -fsanitize=address)
#    target_link_libraries(leanstore asan)
#endif ()
OPTION(LEANSTORE_COUNTERS "Compile leanstore with debugging/performance counters" ON)
IF(LEANSTORE_COUNTERS)
  target_compile_definitions(leanstore PUBLIC MACRO_FLAG_COUNTERS=1)
ENDIF(LEANSTORE_COUNTERS)
# ---------------------------------------------------------------------------
target_include_directories(leanstore PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})
