aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} SOCKS_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include
)

link_directories (
    ${PROJECT_SOURCE_DIR}/lib
)

link_libraries (cos m rt pthread dl)

add_executable(socks ${SOCKS_LIB_SRCS})

install (TARGETS socks RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin)
