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

#include <initializer_list>
#include <list>
#include <memory>

#include "wrap/zero-copy-stream-wrapper.h"

namespace orc {

  class PositionProvider {
  private:
    std::list<unsigned long>::const_iterator position;
  public:
    PositionProvider(const std::list<unsigned long>& positions);
    unsigned long next();
  };

  /**
   * A subclass of Google's ZeroCopyInputStream that supports seek.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf readers.
   */
  class SeekableInputStream: public google::protobuf::io::ZeroCopyInputStream {
  public:
    virtual ~SeekableInputStream();
    virtual void seek(PositionProvider& position) = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream: public SeekableInputStream {
  private:
    std::unique_ptr<char[]> data;
    unsigned long length;
    unsigned long position;
    unsigned long blockSize;

  public:
    SeekableArrayInputStream(std::initializer_list<unsigned char> list,
                             long block_size = -1);
    virtual ~SeekableArrayInputStream();
    virtual bool Next(const void** data, int*size);
    virtual void BackUp(int count);
    virtual bool Skip(int count);
    virtual google::protobuf::int64 ByteCount() const;
    virtual void seek(PositionProvider& position);
  };
}

#endif
