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
#include "Compression.hh"

namespace orc {

  PositionProvider::PositionProvider(const std::list<long>& positions) {
    this->positions = positions;
  }

  long PositionProvider::next() {
    long result = positions.front();
    positions.pop_front();
    return result;
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const void* data, 
						     long offset, 
						     long length,
						     long blockSize) {
    this->data = data;
    this->offset = offset;
    this->length = length;
    this->position = 0;
    this->blockSize = blockSize == -1 ? length : blockSize;
  }

  bool SeekableArrayInputStream::Next(const void** data, int*size) {
    int currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *data = static_cast<const char*>(this->data) + position;
      *size = currentSize;
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

  void SeekableArrayInputStream::seek(PositionProvider& position) {
    throw new std::string("not implemented yet");
  }
}
