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
  "${CMAKE_CURRENT_SOURCE_DIR}/orc-config.hh.in"
  "${CMAKE_BINARY_DIR}/include/orc-config.hh"
)
include_directories(${CMAKE_BINARY_DIR}/include)

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
include_directories(/opt/local/include)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable(test-orc 
  ${PROTOC_GENERATED}/orc_proto.pb.h
  orc-proto-wrapper.cc
  test-orc.cc 
)

target_link_libraries(test-orc /opt/local/lib/libprotobuf.a)
