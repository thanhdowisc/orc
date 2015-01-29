# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required (VERSION 2.6)

project(ORC)

set (ORC_VERSION "1.0.0")
set (LIBUV_VERSION "0.11.29")

include_directories(${CMAKE_SOURCE_DIR}/libs/libuv-${LIBUV_VERSION}/include)
set (LIBUV_LIB "${CMAKE_BINARY_DIR}/libs/libuv-${LIBUV_VERSION}/libuv.a")
if (NOT APPLE)
  list (APPEND LIBUV_LIB pthread rt)
endif (NOT APPLE)

set (GMOCK_VERSION "1.7.0")
include_directories(
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/include
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/gtest/include
  )

if (MSVC)
  set (GMOCK_LIB "${CMAKE_BINARY_DIR}/libs/gmock-${GMOCK_VERSION}/gmock_main")
  set (PROTOBUF_VERSION "2.6.0")
  set (ZLIB_VERSION "1.2.8")

  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")

  # set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH};${CMAKE_SOURCE_DIR}/libs/binary/x86)
  # set(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH};${CMAKE_SOURCE_DIR}/include)
  # set(PATH ${PATH};${CMAKE_SOURCE_DIR}/bin)
  
  find_package (Protobuf)
  
  # message(STATUS "PROTOBUF_LIBRARIES = ${PROTOBUF_LIBRARIES}")
  # message(STATUS "PROTOBUF_LITE_LIBRARIES = ${PROTOBUF_LITE_LIBRARIES}")
  # message(STATUS "PROTOBUF_LIBRARY = ${PROTOBUF_LIBRARY}")
  # message(STATUS "PROTOBUF_LITE_LIBRARY = ${PROTOBUF_LITE_LIBRARY}")
  
  if (NOT Protobuf_FOUND)
    include_directories(
    ${CMAKE_SOURCE_DIR}/libs/protobuf-${PROTOBUF_VERSION}/include
    )
    set (PROTOBUF_PROTOC_EXECUTABLE ${CMAKE_BINARY_DIR}/libs/protobuf-${PROTOBUF_VERSION}/${CMAKE_CFG_INTDIR}/a_protoc.exe)
    set (PROTOBUF_LITE_LIBRARIES "${CMAKE_CFG_INTDIR};${CMAKE_BINARY_DIR}/libs/protobuf-${PROTOBUF_VERSION}/${CMAKE_CFG_INTDIR}/libprotobuf-lite.lib")
    set (PROTOBUF_LIBRARIES "${CMAKE_CFG_INTDIR};${CMAKE_BINARY_DIR}/libs/protobuf-${PROTOBUF_VERSION}/${CMAKE_CFG_INTDIR}/libprotobuf.lib")
  else (NOT Protobuf_FOUND)
    message(STATUS "PROTOBUF_LIBRARIES = ${PROTOBUF_LIBRARIES}")
    message(STATUS "PROTOBUF_LITE_LIBRARIES = ${PROTOBUF_LITE_LIBRARIES}")
    message(STATUS "PROTOBUF_LIBRARY = ${PROTOBUF_LIBRARY}")
    message(STATUS "PROTOBUF_LITE_LIBRARY = ${PROTOBUF_LITE_LIBRARY}")
  endif (NOT Protobuf_FOUND)
else(MSVC)
  set (GMOCK_LIB "${CMAKE_BINARY_DIR}/libs/gmock-${GMOCK_VERSION}/libgmock_main.a")
  find_package (Protobuf REQUIRED)
  set (CXX11_FLAGS "-std=c++11")
endif(MSVC)

if(NOT APPLE AND NOT MSVC)
  list (APPEND GMOCK_LIB pthread)
endif(NOT APPLE AND NOT MSVC)

enable_testing()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  #  -stdlib=libc++
  set (COVERAGE_FLAGS "--coverage -g -O0")
  set (CXX11_FLAGS "${CXX11_FLAGS}")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set (WARN_FLAGS "-Wall")
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_FLAGS}")

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(libs)
add_subdirectory(src)
# add_subdirectory(test)
