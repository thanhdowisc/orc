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

#include <algorithm>
#include <iostream>

#include "Compression.hh"

namespace orc {

  PositionProvider::PositionProvider(const std::list<long>& posns) {
    position = posns.cbegin();
  }

  long PositionProvider::next() {
    long result = *position;
    ++position;
    return result;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream
     (std::initializer_list<unsigned char> values,
      long blkSize) {
    length = static_cast<long>(values.size());
    data = std::unique_ptr<char[]>(new char[length]);
    char *ptr = data.get();
    for(unsigned char ch: values) {
      *(ptr++) = static_cast<char>(ch);
    }
    position = 0;
    this->blockSize = blkSize == -1 ? length : blkSize;
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int*size) {
    long currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = data.get() + position;
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count <= blockSize && count + position >= 0) {
      position -= count;
    }
  }
  
  bool SeekableArrayInputStream::Skip(int count) {
    if (count + position <= length) {
      position += count;
      return true;
    }
    return false;
  }
  
  google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return position;
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
  }
}
