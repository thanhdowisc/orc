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
import java.nio.ByteBuffer;

class SerializationUtils {

  static int getSerializedSize(int value, boolean signed) {
    if (signed) {
      value = (value << 1) ^ (value >> 31);
    }
    int size = 0;
    while (value != 0) {
      if ((value & ~0x7f) == 0) {
        return 1 + size;
      } else {
        size += 1;
        value >>>= 7;
      }
    }
    return 5;
  }

  static void writeVuint(OutputStream output, int value) throws IOException {
    while (true) {
      if ((value & ~0x7f) == 0) {
        output.write((byte) value);
        return;
      } else {
        output.write((byte) (value & 0x7f));
        value >>>= 7;
      }
    }
  }

  static void writeVsint(OutputStream output, int value) throws IOException {
    writeVuint(output, (value << 1) ^ (value >> 31));
  }

  static void writeVulong(OutputStream output, long value) throws IOException {
    while (true) {
      if ((value & ~0x7f) == 0) {
        output.write((byte) value);
        return;
      } else {
        output.write((byte) (0x80 | (value & 0x7f)));
        value >>>= 7;
      }
    }
  }

  static void writeVslong(OutputStream output, long value) throws IOException {
    writeVulong(output, (value << 1) ^ (value >> 63));
  }

  static void writeFloat(OutputStream output, float value) throws IOException {
    int ser = Float.floatToIntBits(value);
    output.write(ser & 0xff);
    output.write((ser >> 8) & 0xff);
    output.write((ser >> 16) & 0xff);
    output.write((ser >> 24) & 0xff);
  }

  static void writeDouble(OutputStream output, double value) throws IOException {
    long ser = Double.doubleToLongBits(value);
    output.write(((int) ser) & 0xff);
    output.write(((int) (ser >> 16)) & 0xff);
    output.write(((int) (ser >> 24)) & 0xff);
    output.write(((int) (ser >> 32)) & 0xff);
    output.write(((int) (ser >> 40)) & 0xff);
    output.write(((int) (ser >> 48)) & 0xff);
    output.write(((int) (ser >> 56)) & 0xff);
  }

  static class ByteBufferWrapper extends OutputStream {
    private final ByteBuffer buffer;

    ByteBufferWrapper(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void write(int i) throws IOException {
      buffer.put((byte) i);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      buffer.put(bytes, offset, length);
    }
  }
}