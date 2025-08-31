aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COMMON_TEST_LIB_SRCS)

SET (MEMORY_TEST_LIB_SRCS
    ${PROJECT_SOURCE_DIR}/test/test_memory.cpp
)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include
)

link_directories (
    ${PROJECT_SOURCE_DIR}/build/src
)

add_executable(test_memory ${MEMORY_TEST_LIB_SRCS})
# dynamic dll
#target_link_libraries(test_memory libsecurec.so m rt pthread dl)
target_link_libraries(test_memory libsecurec.a m rt pthread dl)

install (TARGETS test_memory RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
