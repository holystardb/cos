aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} VIO_LIB_SRCS)

include_directories (
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${PROJECT_SOURCE_DIR}/src/include/common
    ${PROJECT_SOURCE_DIR}/src/include/vio
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/third_lib/securec/lib/linux
    ${PROJECT_SOURCE_DIR}/build/src/strings
    ${PROJECT_SOURCE_DIR}/build/src/common
)

#link_libraries (m rt pthread dl)

#
add_library (libvio.lib STATIC ${VIO_LIB_SRCS})
add_library (libvio.dll SHARED ${VIO_LIB_SRCS})

target_link_libraries(libvio.lib libsecurec.a libstrings.a libcommon.a m rt pthread dl)
target_link_libraries(libvio.dll libsecurec.so libstrings.so libcommon.so m rt pthread dl)

#
set_target_properties (libvio.lib PROPERTIES OUTPUT_NAME "vio")
set_target_properties (libvio.dll PROPERTIES OUTPUT_NAME "vio")

set_target_properties (libvio.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libvio.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libvio.lib libvio.dll
         RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin
         LIBRARY DESTINATION ${CMAKE_OUTPUT_DIR}/lib
         ARCHIVE DESTINATION ${CMAKE_OUTPUT_DIR}/lib
)
