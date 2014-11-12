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

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/orc/orc-config.hh.in"
  "${CMAKE_BINARY_DIR}/include/orc/orc-config.hh"
)
include_directories(${CMAKE_BINARY_DIR}/include)

find_package(Protobuf REQUIRED)
if( NOT PROTOBUF_FOUND)
# hack: if can't find protobuf, set path explicitly ..
set(PROTOBUF_INCLUDE_DIRS $ENV{PROTOBUF_INCLUDE_DIRS})
set(PROTOBUF_LITE_LIBRARIES $ENV{PROTOBUF_LITE_LIBRARIES})
endif( NOT PROTOBUF_FOUND)
include_directories(${PROTOBUF_INCLUDE_DIRS})

set(PROTOC_GENERATED ${CMAKE_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${PROTOC_GENERATED})

add_custom_command(
  OUTPUT ${CMAKE_BINARY_DIR}/generated/orc_proto.pb.h
  COMMAND protoc -I ${CMAKE_CURRENT_SOURCE_DIR}
                 --cpp_out=${CMAKE_BINARY_DIR}/generated 
                 ${CMAKE_CURRENT_SOURCE_DIR}/orc_proto.proto 
  DEPENDS ${PROTOC_GENERATED}
)
include_directories(${PROTOC_GENERATED})

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_library(orc STATIC 
  ${PROTOC_GENERATED}/orc_proto.pb.h
  Compression.cc
  RLE.cc
  Vector.cc
  wrap/orc-proto-wrapper.cc
)

add_executable(test-rle 
  TestRLE.cc
)

target_link_libraries(test-rle 
  ${CMAKE_BINARY_DIR}/src/liborc.a
  ${PROTOBUF_LITE_LIBRARIES}
  ${GTEST_LIB}
)

add_test(test-rle test-rle)

# Add another target called test-out that prints the results on failure
if (CMAKE_CONFIGURATION_TYPES)
  add_custom_target(test-out COMMAND ${CMAKE_CTEST_COMMAND}
    --force-new-ctest-process --output-on-failure
    --build-config "$<CONFIGURATION>")
else()
  add_custom_target(test-out COMMAND ${CMAKE_CTEST_COMMAND}
    --force-new-ctest-process --output-on-failure)
endif()
