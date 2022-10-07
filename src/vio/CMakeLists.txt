aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} VIO_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include/common
    ${PROJECT_SOURCE_DIR}/include/vio
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/lib
)

link_libraries (m rt pthread dl)

#
add_library (libvio.lib STATIC ${VIO_LIB_SRCS})
add_library (libvio.dll SHARED ${VIO_LIB_SRCS})

#
set_target_properties (libvio.lib PROPERTIES OUTPUT_NAME "libvio_lib")
set_target_properties (libvio.dll PROPERTIES OUTPUT_NAME "libvio")

set_target_properties (libvio.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libvio.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libcommon.lib
         RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
         LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
         ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib
)

install (FILES ${COS_HEADER_FILES} DESTINATION ${PROJECT_SOURCE_DIR}/include)
