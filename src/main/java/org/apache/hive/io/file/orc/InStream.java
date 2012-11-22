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
import java.io.InputStream;
import java.nio.ByteBuffer;

public class InStream {

  private static class UncompressedStream extends InputStream {
    private final byte[] array;
    private int offset;
    private int limit;

    public UncompressedStream(ByteBuffer input) {
      this.array = input.array();
      offset = input.arrayOffset() + input.position();
      limit = input.arrayOffset() + input.limit();
    }

    @Override
    public int read() {
      if (offset == limit) {
        return -1;
      }
      return 0xff & array[offset++];
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (this.offset == limit) {
        return -1;
      }
      int actualLength = Math.min(length, limit - offset);
      System.arraycopy(array, this.offset, data, offset, actualLength);
      this.offset += actualLength;
      return actualLength;
    }
  }

  private static class CompressedStream extends InputStream {
    private final byte[] array;
    private final int bufferSize;
    private ByteBuffer uncompressed = null;
    private final CompressionCodec codec;
    private int offset;
    private int limit;
    private boolean isUncompressedOriginal;

    public CompressedStream(ByteBuffer input, CompressionCodec codec,
                            int bufferSize) throws IOException {
      this.array = input.array();
      this.codec = codec;
      this.bufferSize = bufferSize;
      offset = input.arrayOffset() + input.position();
      limit = input.arrayOffset() + input.limit();
      if (limit != offset) {
        readHeader();
      }
    }

    private void readHeader() throws IOException {
      if (limit - offset > OutStream.HEADER_SIZE) {
        int chunkLength = ((0xff & array[offset+2]) << 15) |
          ((0xff & array[offset+1]) << 7) | (0xff & (array[offset] >> 1));
        boolean isOriginal = (array[offset] & 0x01) == 1;
        offset += OutStream.HEADER_SIZE;
        if (isOriginal) {
          isUncompressedOriginal = true;
          uncompressed = ByteBuffer.wrap(array, offset, chunkLength);
        } else {
          if (isUncompressedOriginal) {
            uncompressed = ByteBuffer.allocate(bufferSize);
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            uncompressed = ByteBuffer.allocate(bufferSize);
          } else {
            uncompressed.clear();
          }
          codec.decompress(ByteBuffer.wrap(array, offset, chunkLength),
            uncompressed);
        }
        offset += chunkLength;
      } else {
        throw new IllegalStateException("Can't read header");
      }
    }

    @Override
    public int read() throws IOException {
      if (uncompressed.remaining() == 0) {
        if (offset == limit) {
          return -1;
        }
        readHeader();
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (uncompressed.remaining() == 0) {
        if (this.offset == this.limit) {
          return -1;
        }
        readHeader();
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      System.arraycopy(uncompressed.array(),
        uncompressed.arrayOffset() + uncompressed.position(), data,
        offset, actualLength);
      uncompressed.position(uncompressed.position() + actualLength);
      return actualLength;
    }
  }

  public static InputStream create(ByteBuffer input,
                                   CompressionCodec codec,
                                   int bufferSize) throws IOException {
    if (codec == null) {
      return new UncompressedStream(input);
    } else {
      return new CompressedStream(input, codec, bufferSize);
    }
  }
}
