# Install script for directory: /home/cos/cos/third_lib/securec/source/test

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/home/cos")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "0")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory")
    file(RPATH_CHECK
         FILE "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory"
         RPATH "")
  endif()
  list(APPEND CMAKE_ABSOLUTE_DESTINATION_FILES
   "/home/cos/cos/third_lib/securec/source/output/bin/test_memory")
  if(CMAKE_WARN_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(WARNING "ABSOLUTE path INSTALL DESTINATION : ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
  if(CMAKE_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION)
    message(FATAL_ERROR "ABSOLUTE path INSTALL DESTINATION forbidden (by caller): ${CMAKE_ABSOLUTE_DESTINATION_FILES}")
  endif()
file(INSTALL DESTINATION "/home/cos/cos/third_lib/securec/source/output/bin" TYPE EXECUTABLE FILES "/home/cos/cos/third_lib/securec/source/build/test/test_memory")
  if(EXISTS "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory")
    file(RPATH_CHANGE
         FILE "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory"
         OLD_RPATH "/home/cos/cos/third_lib/securec/source/build/src:"
         NEW_RPATH "")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/bin/strip" "$ENV{DESTDIR}/home/cos/cos/third_lib/securec/source/output/bin/test_memory")
    endif()
  endif()
endif()

