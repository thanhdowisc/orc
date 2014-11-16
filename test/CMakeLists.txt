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

include_directories(
  ${CMAKE_BINARY_DIR}/include
  ${CMAKE_SOURCE_DIR}/src
  ${PROTOBUF_INCLUDE_DIRS}
)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable(test-orc
  TestByteRle.cc
  TestDriver.cc
  TestRle.cc
)

target_link_libraries(test-orc
  ${CMAKE_BINARY_DIR}/src/liborc.a
  ${PROTOBUF_LITE_LIBRARIES}
  ${GTEST_LIB}
)

add_test(test-orc test-orc)

# Add another target called test-out that prints the results on failure
if (CMAKE_CONFIGURATION_TYPES)
  add_custom_target(test-out COMMAND ${CMAKE_CTEST_COMMAND}
    --force-new-ctest-process --output-on-failure
    --build-config "$<CONFIGURATION>")
else()
  add_custom_target(test-out COMMAND ${CMAKE_CTEST_COMMAND}
    --force-new-ctest-process --output-on-failure)
endif()
