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

include_directories(${PROTOBUF_INCLUDE_DIRS})

include_directories(${LZO_INCLUDE_DIR})

include_directories(${ZLIB_INCLUDE_DIR})

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

set (CMAKE_CXX_FLAGS "-O0 ${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_library(orc STATIC
  ${PROTOC_GENERATED}/orc_proto.pb.h
  ByteRLE.cc
  Compression.cc
  Reader.cc
  RLE.cc
  Vector.cc
  wrap/orc-proto-wrapper.cc
)
