aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COMMON_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include/common
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/lib
)

link_libraries (m rt pthread dl)

#
add_library (libcommon.lib STATIC ${COMMON_LIB_SRCS})
add_library (libcommon.dll SHARED ${COMMON_LIB_SRCS})

#
set_target_properties (libcommon.lib PROPERTIES OUTPUT_NAME "libcommon_lib")
set_target_properties (libcommon.dll PROPERTIES OUTPUT_NAME "libcommon")

set_target_properties (libcommon.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libcommon.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libcommon.lib
         RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
         LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
         ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib
)

install (FILES ${COS_HEADER_FILES} DESTINATION ${PROJECT_SOURCE_DIR}/include)
