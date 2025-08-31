aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COMMON_TEST_LIB_SRCS)

SET (TEST_MEMORY_POOL_SRCS
    ${PROJECT_SOURCE_DIR}/test/common_test/test_mem_pool.cpp
)

include_directories (  
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${PROJECT_SOURCE_DIR}/src/include/strings
    ${PROJECT_SOURCE_DIR}/src/include/common
    ${PROJECT_SOURCE_DIR}/src/include/vio
)

link_directories (
    ${PROJECT_SOURCE_DIR}/third_lib/securec/lib/linux
    ${PROJECT_SOURCE_DIR}/build/src/strings
    ${PROJECT_SOURCE_DIR}/build/src/common
    ${PROJECT_SOURCE_DIR}/build/src/vio
)

#link_libraries(libcommon.so libvio.so m rt pthread dl)

add_executable(test_memory_pool ${TEST_MEMORY_POOL_SRCS})
# dynamic dll
#target_link_libraries(test_memory_pool libvio.so libcommon.so libstrings.so libsecurec.so m rt pthread dl)
target_link_libraries(test_memory_pool libvio.a libcommon.a libstrings.a libsecurec.a m rt pthread dl)

install (TARGETS test_memory_pool RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
