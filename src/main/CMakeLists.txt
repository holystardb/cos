aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} MAIN_LIB_SRCS)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include/common
    ${PROJECT_SOURCE_DIR}/include/vio
)

link_directories (
    ${PROJECT_SOURCE_DIR}/lib
)

link_libraries (libcommon libvio m rt pthread dl)

add_executable(cosdb ${MAIN_LIB_SRCS})

install (TARGETS socks RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin)
