aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} VIO_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/src/include/common
    ${PROJECT_SOURCE_DIR}/src/include/vio
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${CMAKE_OUTPUT_DIR}/lib
)

link_libraries (m rt pthread dl)

#
add_library (libvio.lib STATIC ${VIO_LIB_SRCS})
add_library (libvio.dll SHARED ${VIO_LIB_SRCS})

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

install (DIRECTORY ${PROJECT_SOURCE_DIR}/src/include/vio DESTINATION ${CMAKE_OUTPUT_DIR}/include)