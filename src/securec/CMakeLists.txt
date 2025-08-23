aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} SECUREC_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${CMAKE_OUTPUT_DIR}/lib
)

link_libraries (m rt pthread dl)

#
add_library (libsecurec.lib STATIC ${SECUREC_LIB_SRCS})
add_library (libsecurec.dll SHARED ${SECUREC_LIB_SRCS})

#
set_target_properties (libsecurec.lib PROPERTIES OUTPUT_NAME "securec")
set_target_properties (libsecurec.dll PROPERTIES OUTPUT_NAME "securec")

set_target_properties (libsecurec.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libsecurec.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libsecurec.lib libsecurec.dll
         RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin
         LIBRARY DESTINATION ${CMAKE_OUTPUT_DIR}/lib
         ARCHIVE DESTINATION ${CMAKE_OUTPUT_DIR}/lib
)

install (DIRECTORY ${PROJECT_SOURCE_DIR}/src/include/securec DESTINATION ${CMAKE_OUTPUT_DIR}/include)
