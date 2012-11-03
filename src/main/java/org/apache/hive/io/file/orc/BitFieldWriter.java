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
package org.apache.hive.io.file.orc;

import java.io.IOException;
import java.io.OutputStream;

class BitFieldWriter {
  private OutputStream output;
  private final int bitSize;
  private byte current = 0;
  private int bitsLeft = 8;

  BitFieldWriter(OutputStream output, int bitSize) {
    this.output = output;
    this.bitSize = bitSize;
  }

  private void writeByte() throws IOException {
    output.write(current);
    current = 0;
    bitsLeft = 8;
  }

  void flush() throws IOException {
    if (bitsLeft != 8) {
      writeByte();
    }
    output.flush();
  }

  void append(int value) throws IOException {
    if (bitSize > bitsLeft) {
      int bitsToWrite = bitSize;
      while (bitsToWrite > 0) {
        // add the bits to the bottom of the current word
        current |= value >>> (bitsToWrite - bitsLeft);
        // subtract out the bits we just added
        bitsToWrite -= bitsLeft;
        // zero out the bits above bitsToWrite
        value &= (1 << bitsToWrite) - 1;
        writeByte();
      }
    } else {
      bitsLeft -= bitSize;
      current |= value << bitsLeft;
      if (bitsLeft == 0) {
        writeByte();
      }
    }
  }

  /*
  int get(int location) {
    int bitLocation = location * bitSize;
    int byteOffset = bitLocation / 8;
    int bitOffset = bitLocation % 8;
    // get the low bits of the first byte
    int b = buffer.get(byteOffset) & ((1 << (8-bitOffset)) - 1);
    if (bitOffset + bitSize > 8) {
      int remaining = bitSize - (8 - bitOffset);
      int result = b;
      byteOffset += 1;
      while (remaining > 0) {
        b = buffer.get(byteOffset++) & 0xff;
        if (remaining >= 8) {
          result = (result << 8) | b;
          remaining -= 8;
        } else {
          // shift the new part to the bottom
          b >>= (8-remaining);
          // shift the old part up and add the new part
          result = (result << remaining) | b;
          remaining = 0;
        }
      }
      return result;
    } else {
      return b >> (8 - bitOffset - bitSize);
    }
  }
*/
}
