
aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} STRINGS_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/src/include/securec
    ${PROJECT_SOURCE_DIR}/src/include/strings
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/third_lib/securec/lib/linux
)

#link_libraries (m rt pthread dl)

#
add_library (libstrings.lib STATIC ${STRINGS_LIB_SRCS})
add_library (libstrings.dll SHARED ${STRINGS_LIB_SRCS})

target_link_libraries(libstrings.lib libsecurec.a m rt pthread dl)
target_link_libraries(libstrings.dll libsecurec.so m rt pthread dl)

#
set_target_properties (libstrings.lib PROPERTIES OUTPUT_NAME "strings")
set_target_properties (libstrings.dll PROPERTIES OUTPUT_NAME "strings")

set_target_properties (libstrings.lib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (libstrings.dll PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS libstrings.lib libstrings.dll
         RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin
         LIBRARY DESTINATION ${CMAKE_OUTPUT_DIR}/lib
         ARCHIVE DESTINATION ${CMAKE_OUTPUT_DIR}/lib
)
