
set (SAFEC_MEMORY_TEST_SRCS 
    test_safec_memory.cpp
)

set (SAFEC_STR_TEST_SRCS 
    test_safec_str.cpp
)

set (SAFEC_OTHER_TEST_SRCS 
    test_safec_other.cpp
)

include_directories (  
    ${PROJECT_SOURCE_DIR}/third_lib/safec/include
)

link_directories (
    ${PROJECT_SOURCE_DIR}/third_lib/safec/lib
)

add_executable(safec_memory_test ${SAFEC_MEMORY_TEST_SRCS})
target_link_libraries(safec_memory_test libsafec.a m rt pthread dl)

add_executable(safec_str_test ${SAFEC_STR_TEST_SRCS})
target_link_libraries(safec_str_test libsafec.a m rt pthread dl)

add_executable(safec_other_test ${SAFEC_OTHER_TEST_SRCS})
target_link_libraries(safec_other_test libsafec.a m rt pthread dl)

install (TARGETS safec_memory_test RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
install (TARGETS safec_str_test RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
install (TARGETS safec_other_test RUNTIME DESTINATION ${CMAKE_OUTPUT_DIR}/bin)
