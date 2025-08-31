aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COMMON_LIB_SRCS)

include_directories (
    #${PROJECT_SOURCE_DIR}/third_lib/openssl-1.1.1/include/openssl
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${PROJECT_SOURCE_DIR}/src/include/strings
    ${PROJECT_SOURCE_DIR}/src/include/common
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/third_lib/securec/lib/linux
    ${PROJECT_SOURCE_DIR}/build/src/strings
)

#
add_library (libcommon.lib STATIC ${COMMON_LIB_SRCS})
add_library (libcommon.dll SHARED ${COMMON_LIB_SRCS})

#target_link_libraries(libcommon.lib libsecurec.a libstrings.a m rt pthread dl)
#target_link_libraries(libcommon.dll libsecurec.so libstrings.so m rt pthread dl)

#
set_target_properties (libcommon.lib PROPERTIES OUTPUT_NAME "common")
set_target_properties (libcommon.dll PROPERTIES OUTPUT_NAME "common")

set_target_properties (libcommon.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libcommon.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libcommon.lib libcommon.dll
         RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin
         LIBRARY DESTINATION ${CMAKE_OUTPUT_DIR}/lib
         ARCHIVE DESTINATION ${CMAKE_OUTPUT_DIR}/lib
)
