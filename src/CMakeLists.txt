#include_directories(include/securec)
include_directories(include/strings)
include_directories(include/common)
include_directories(include/vio)

add_subdirectory(strings)
add_subdirectory(common)
add_subdirectory(vio)

install (DIRECTORY ${PROJECT_SOURCE_DIR}/src/include DESTINATION ${CMAKE_OUTPUT_DIR})
