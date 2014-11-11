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
if(NOT APPLE)
  list (APPEND LIBUV_LIB pthread rt)
endif(NOT APPLE)

set (GTEST_VERSION "1.7.0")
include_directories(${CMAKE_SOURCE_DIR}/libs/gtest-${GTEST_VERSION}/include)
set (GTEST_LIB "${CMAKE_BINARY_DIR}/libs/gtest-${GTEST_VERSION}/libgtest.a")
if(NOT APPLE)
  list (APPEND GTEST_LIB pthread)
endif(NOT APPLE)
enable_testing()

set (CXX11_FLAGS "-std=gnu++11")
if(APPLE)
  set (CXX11_FLAGS "${CXX11_FLAGS} -stdlib=libc++")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
else(APPLE)
  set (WARN_FLAGS "-Wall")
endif(APPLE)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

add_subdirectory(libs)
add_subdirectory(src)
