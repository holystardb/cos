aux_source_directory (${CMAKE_CURRENT_SOURCE_DIR} COS_LIB_SRCS)

set (COS_HEADER_FILES 
    os_base64.h
    os_biqueue.h
    os_config.h
    os_date.h
    os_epoll.h
    os_epoll_reactor.h
    os_file.h
    os_getopt.h
    os_list.h
    os_log.h
    os_mach.h
    os_md5.h
    os_mutex.h
    os_pat.h
    os_queue.h
    os_rbt.h
    os_rijndael.h
    os_thread.h
    os_thread_group.h
    os_thread_pool.h
    os_timewheel.h
    os_type.h
    os_vio_socket.h)

include_directories (  
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories (
    ${PROJECT_SOURCE_DIR}/lib
)

link_libraries (m rt pthread dl)

#
add_library (cos_static STATIC ${COS_LIB_SRCS})
add_library (cos SHARED ${COS_LIB_SRCS})

#
set_target_properties (cos_static PROPERTIES OUTPUT_NAME "cos")
set_target_properties (cos PROPERTIES OUTPUT_NAME "cos")

set_target_properties (cos_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties (cos PROPERTIES VERSION 1.0 SOVERSION 1)

install (TARGETS cos
         RUNTIME DESTINATION ${PROJECT_SOURCE_DIR}/bin
         LIBRARY DESTINATION ${PROJECT_SOURCE_DIR}/lib
         ARCHIVE DESTINATION ${PROJECT_SOURCE_DIR}/lib
)

install (FILES ${COS_HEADER_FILES} DESTINATION ${PROJECT_SOURCE_DIR}/include)
