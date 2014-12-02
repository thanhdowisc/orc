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

#include "orc/Vector.hh"

namespace orc {

  ColumnVectorBatch::ColumnVectorBatch(unsigned long cap
                                       ): notNull(new char[cap]) {
    capacity = cap;
    numElements = 0;
    hasNulls = false;
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  LongVectorBatch::~LongVectorBatch() {
    // PASS
  }

  DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
  }

  ByteVectorBatch::~ByteVectorBatch() {
    // PASS
  }

  StructVectorBatch::~StructVectorBatch() {
    // PASS
  }

  LongVectorBatch::LongVectorBatch(unsigned long capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(std::unique_ptr<long[]>
                                           (new long[capacity])){
    // PASS
  }

  DoubleVectorBatch::DoubleVectorBatch(unsigned long capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(std::unique_ptr<double[]>
                                           (new double[capacity])){
    // PASS
  }

  ByteVectorBatch::ByteVectorBatch(unsigned long capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(std::unique_ptr<char*[]>
                                           (new char *[capacity])),
                                      length(std::unique_ptr<long[]>
                                             (new long[capacity])) {
    // PASS
  }

  StructVectorBatch::StructVectorBatch(unsigned long capacity
                                       ): ColumnVectorBatch(capacity) {
    // PASS
  }
}
