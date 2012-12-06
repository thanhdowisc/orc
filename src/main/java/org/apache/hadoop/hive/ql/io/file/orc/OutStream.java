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
package org.apache.hadoop.hive.ql.io.file.orc;

import java.io.IOException;
import java.nio.ByteBuffer;

class OutStream extends PositionedOutputStream {

  interface OutputReceiver {
    void output(ByteBuffer buffer) throws IOException;
    long getPosition() throws IOException;
  }

  static final int HEADER_SIZE = 3;
  private final String name;
  private final OutputReceiver receiver;
  private ByteBuffer compressed = null;
  private ByteBuffer overflow = null;
  private ByteBuffer current;
  private final int bufferSize;
  private final CompressionCodec codec;

  OutStream(String name,
            int bufferSize,
            CompressionCodec codec,
            OutputReceiver receiver) throws IOException {
    this.name = name;
    this.bufferSize = bufferSize;
    this.codec = codec;
    this.receiver = receiver;
    getNewInputBuffer();
  }

  public void clear() throws IOException {
    current.position(codec == null ? 0 : HEADER_SIZE);
    if (compressed != null) {
      compressed.clear();
    }
    if (overflow != null) {
      overflow.clear();
    }
  }

  /**
   * Write the length of the compressed bytes. Life is much easier if the
   * header is constant length, so just use 3 bytes. Considering most of the
   * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
   * be plenty. We also use the low bit for whether it is the original or
   * compressed bytes.
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to write at
   * @param val the size in the file
   * @param original is it uncompressed
   */
  private static void writeHeader(ByteBuffer buffer,
                                  int position,
                                  int val,
                                  boolean original) {
    buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));
    buffer.put(position+1, (byte) (val >> 7));
    buffer.put(position+2, (byte) (val >> 15));
  }

  private void getNewInputBuffer() throws IOException {
    if (codec == null) {
      current = ByteBuffer.allocate(bufferSize);
    } else {
      current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      writeHeader(current, 0, bufferSize, true);
      current.position(HEADER_SIZE);
    }
  }

  private ByteBuffer getNewOutputBuffer() throws IOException {
    return ByteBuffer.allocate(bufferSize +
      (codec == null ? 0 : HEADER_SIZE));
  }

  private void flip() throws IOException {
    current.limit(current.position());
    current.position(codec == null ? 0 : HEADER_SIZE);
  }

  @Override
  public void write(int i) throws IOException {
    if (current.remaining() < 1) {
      spill();
    }
    current.put((byte) i);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    int remaining = Math.min(current.remaining(), length);
    current.put(bytes, offset, remaining);
    length -= remaining;
    while (length != 0) {
      spill();
      offset += remaining;
      remaining = Math.min(current.remaining(), length);
      current.put(bytes, offset, remaining);
      length -= remaining;
    }
  }

  void printStatus(String msg) throws IOException {
    StringBuilder builder = new StringBuilder(name);
    builder.append(" ");
    builder.append(msg);
    builder.append(" current: ");
    if (current == null) {
      builder.append("null");
    } else {
      builder.append(current.position());
      builder.append(", ");
      builder.append(current.limit());
      builder.append(", ");
      builder.append(current.capacity());
    }
    builder.append("; compressed: ");
    if (compressed == null) {
      builder.append("null");
    } else {
      builder.append(compressed.position());
      builder.append(", ");
      builder.append(compressed.limit());
      builder.append(", ");
      builder.append(compressed.capacity());
    }
    builder.append("; overflow: ");
    if (overflow == null) {
      builder.append("null");
    } else {
      builder.append(overflow.position());
      builder.append(", ");
      builder.append(overflow.limit());
      builder.append(", ");
      builder.append(overflow.capacity());
    }
    System.out.println(builder.toString());
  }

  private void spill() throws java.io.IOException {
    // if there isn't anything in the current buffer, don't spill
    if (current.position() == (codec == null ? 0 : HEADER_SIZE)) {
      return;
    }
    flip();
    if (codec == null) {
      receiver.output(current);
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        compressed = getNewOutputBuffer();
      } else if (overflow == null) {
        overflow = getNewOutputBuffer();
      }
      int sizePosn = compressed.position();
      compressed.position(compressed.position()+HEADER_SIZE);
      if (codec.compress(current, compressed, overflow)) {
        // move position back to after the header
        current.position(HEADER_SIZE);
        current.limit(current.capacity());
        // find the total bytes in the chunk
        int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;
        if (overflow != null) {
          totalBytes += overflow.position();
        }
        writeHeader(compressed, sizePosn, totalBytes, false);
        // if we have less than the next header left, spill it.
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          receiver.output(compressed);
          compressed = overflow;
          overflow = null;
        }
      } else {
        // we are using the original, but need to spill the current
        // compressed buffer first. So back up to where we started,
        // flip it and add it to done.
        if (sizePosn != 0) {
          compressed.position(sizePosn);
          compressed.flip();
          receiver.output(compressed);
          compressed = null;
          // if we have an overflow, clear it and make it the new compress
          // buffer
          if (overflow != null) {
            overflow.clear();
            compressed = overflow;
          }
        } else {
          compressed.clear();
          if (overflow != null) {
            overflow.clear();
          }
        }

        // now add the current buffer into the done list and get a new one.
        current.position(0);
        // update the header with the current length
        writeHeader(current, 0, current.limit() - HEADER_SIZE, true);
        receiver.output(current);
        getNewInputBuffer();
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    long doneBytes = receiver.getPosition();
    if (codec == null) {
      recorder.addPosition(doneBytes + current.position());
    } else {
      recorder.addPosition(doneBytes);
      recorder.addPosition(current.position() - HEADER_SIZE);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();
    if (compressed != null && compressed.position() != 0) {
      compressed.flip();
      receiver.output(compressed);
      compressed = null;
    }
    if (overflow != null && overflow.position() != 0) {
      overflow.flip();
      receiver.output(overflow);
      overflow = null;
    }
  }
}

