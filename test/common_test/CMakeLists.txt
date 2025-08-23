aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COMMON_TEST_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${PROJECT_SOURCE_DIR}/src/include/strings
    ${PROJECT_SOURCE_DIR}/src/include/common
    ${PROJECT_SOURCE_DIR}/src/include/vio
)

link_directories (
    ${PROJECT_SOURCE_DIR}/build/src/securec
    ${PROJECT_SOURCE_DIR}/build/src/strings
    ${PROJECT_SOURCE_DIR}/build/src/common
    ${PROJECT_SOURCE_DIR}/build/src/vio
)

#link_libraries(libcommon.so libvio.so m rt pthread dl)

add_executable(common_test ${COMMON_TEST_LIB_SRCS})
# dynamic dll
#target_link_libraries(common_test libstrings.so libsecurec.so libcommon.so libvio.so m rt pthread dl)
target_link_libraries(common_test libstrings.a libsecurec.a libcommon.a libvio.a m rt pthread dl)

install (TARGETS common_test RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
