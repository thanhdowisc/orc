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

# common files
set(LIBUV_SOURCES src/fs-poll.c
                  src/inet.c
                  src/threadpool.c
                  src/uv-common.c
                  src/version.c)

# unix files
list(APPEND LIBUV_SOURCES src/unix/async.c 
                   src/unix/core.c
                   src/unix/dl.c
                   src/unix/fs.c
                   src/unix/getaddrinfo.c
                   src/unix/getnameinfo.c
                   src/unix/loop-watcher.c
                   src/unix/loop.c
                   src/unix/pipe.c
                   src/unix/poll.c
                   src/unix/process.c
                   src/unix/signal.c
                   src/unix/stream.c
                   src/unix/tcp.c
                   src/unix/thread.c
                   src/unix/timer.c
                   src/unix/tty.c
                   src/unix/udp.c)

if(APPLE)
  list(APPEND LIBUV_SOURCES src/unix/darwin.c
                    src/unix/darwin-proctitle.c
                    src/unix/fsevents.c
                    src/unix/kqueue.c
                    src/unix/proctitle.c)
else(APPLE)
  list(APPEND LIBUV_SOURCES src/unix/linux-core.c
                    src/unix/linux-inotify.c
                    src/unix/linux-syscalls.c
                    src/unix/proctitle.c)
endif(APPLE)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src
                    ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library (uv STATIC ${LIBUV_SOURCES})
