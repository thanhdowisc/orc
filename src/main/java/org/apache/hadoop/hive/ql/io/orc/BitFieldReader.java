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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;

class BitFieldReader {
  private RunLengthByteReader input;
  private final int bitSize;
  private int current = 0;
  private int bitsLeft = 0;

  BitFieldReader(InStream input,
                 int bitSize) throws IOException {
    this.input = new RunLengthByteReader(input);
    this.bitSize = bitSize;
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    }
  }

  int next() throws IOException {
    int result = 0;
    int bitsLeftToRead = bitSize;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= current >> (8 - bitsLeft);
      bitsLeftToRead -= bitsLeft;
      readByte();
    }
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= bitsLeftToRead;
      result |= current >>> bitsLeft;
      current &= (1 << bitsLeft) - 1;
    }
    return result;
  }
}
