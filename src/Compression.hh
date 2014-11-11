/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_COMPRESSION_HH
#define ORC_COMPRESSION_HH

#include <google/protobuf/io/zero_copy_stream.h>
#include <list>

namespace orc {

  class PositionProvider {
  private:
    std::list<long> positions;
  public:
    PositionProvider(const std::list<long>& positions);
    long next();
  };

  class SeekableInputStream: public google::protobuf::io::ZeroCopyInputStream {
    virtual void seek(PositionProvider& position) = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream: public SeekableInputStream {
  private:
    const void* data;
    long offset;
    long length;
    long position;
    long blockSize;

  public:
    SeekableArrayInputStream(const void* data, long offset, long length,
			     long block_size = -1);
    virtual bool Next(const void** data, int*size);
    virtual void BackUp(int count);
    virtual bool Skip(int count);
    virtual google::protobuf::int64 ByteCount() const;
    virtual void seek(PositionProvider& position);
  };
}

#endif
