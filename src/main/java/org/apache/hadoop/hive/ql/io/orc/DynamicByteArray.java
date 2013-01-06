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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A class that is a growable array of bytes. Growth is managed in terms of
 * chunks that are allocated when needed.
 */
class DynamicByteArray {
  final static int DEFAULT_CHUNKSIZE = 32 * 1024;
  final static int DEFAULT_NUM_CHUNKS = 128;

  final int chunkSize;  /** our allocation sizes */
  byte[][] data;   /** the real data */
  int length;     /** max set element index +1 */
  int initializedChunks = 0;

  public DynamicByteArray() {
    this(DEFAULT_NUM_CHUNKS, DEFAULT_CHUNKSIZE);
  }

  public DynamicByteArray(int numChunks, int chunkSize) {
    this.chunkSize = chunkSize;

    data = new byte[numChunks][];
  }

  /**
   * Ensure that the given index is valid.
   */
  private void grow(int chunkIndex) {
    if (chunkIndex >= initializedChunks) {
      if (chunkIndex >= data.length) {
        int new_size = Math.max(chunkIndex, 2 * data.length);
        byte[][] newChunk = new byte[new_size][];
        System.arraycopy(data, 0, newChunk, 0, data.length);
        data = newChunk;
      }
      for(int i=initializedChunks; i <= chunkIndex; ++i) {
        data[i] = new byte[chunkSize];
      }
      initializedChunks = chunkIndex + 1;
    }
  }

  public byte get (int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }
    int i = index / chunkSize;
    int j = index % chunkSize;
    return data[i][j];
  }

  public void set (int index, byte value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
      length = index + 1;
    }
    data[i][j] = value;
  }

  public int add(byte value) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow(i);
    data[i][j] = value;
    int result = length;
    length += 1;
    return result;
  }

  public int add(byte[] value, int offset, int newLength) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow((length + newLength) / chunkSize);
    int remaining = newLength;
    while (remaining > 0) {
      int size = Math.min(remaining, chunkSize - j);
      System.arraycopy(value, offset, data[i], j, size);
      remaining -= chunkSize - size;
      i += 1;
      j = 0;
    }
    int result = length;
    length += newLength;
    return result;
  }

  /**
   * Read the entire stream into this array
   * @param in the stream to read from
   * @throws IOException
   */
  public void readAll(InputStream in) throws IOException {
    int currentChunk = length / chunkSize;
    int currentOffset = length % chunkSize;
    int currentLength = in.read(data[currentChunk], currentOffset,
      chunkSize - currentOffset);
    while (currentLength > 0) {
      length += currentLength;
      currentOffset = length % chunkSize;
      if (currentOffset == 0) {
        currentChunk = length / chunkSize;
        grow(currentChunk);
      }
      currentLength = in.read(data[currentChunk], currentOffset,
        chunkSize - currentOffset);
    }
  }

  public int compare(byte[] other, int otherOffset, int otherLength,
                     int ourOffset, int ourLength) {
    int currentChunk = ourOffset / chunkSize;
    int currentOffset = ourOffset % chunkSize;
    int maxLength = Math.min(otherLength, ourLength);
    while (maxLength > 0 &&
      other[otherOffset++] == data[currentChunk][currentOffset++]) {
      if (currentOffset == chunkSize) {
        currentChunk += 1;
        currentOffset = 0;
      }
      maxLength -= 1;
    }
    byte otherByte = other[otherOffset - 1];
    byte ourByte = data[currentChunk][currentOffset - 1];
    if (otherByte == ourByte) {
      if (otherLength == ourLength) {
        return 0;
      } else {
        return otherLength > ourLength ? 1 : -1;
      }
    } else {
      return otherByte > ourByte ? 1 : -1;
    }
  }

  public int size() {
    return length;
  }

  public void clear() {
    length = 0;
    for(int i=0; i < data.length; ++i) {
      data[i] = null;
    }
  }

  public void setText(Text result, int offset, int length) {
    result.clear();
    int currentChunk = offset / chunkSize;
    int currentOffset = offset % chunkSize;
    int currentLength = Math.min(length, chunkSize - currentOffset);
    while (length > 0) {
      result.append(data[currentChunk], currentOffset, currentLength);
      length -= currentLength;
      currentChunk += 1;
      currentOffset = 0;
      currentLength = Math.min(length, chunkSize - currentOffset);
    }
  }

  public void write(OutputStream out, int offset,
                    int length) throws IOException {
    int currentChunk = offset / chunkSize;
    int currentOffset = offset % chunkSize;
    while (length > 0) {
      int currentLength = Math.min(length, chunkSize - currentOffset);
      out.write(data[currentChunk], currentOffset, currentLength);
      length -= currentLength;
      currentChunk += 1;
      currentOffset = 0;
    }
  }

  public String toString() {
    int i;
    StringBuilder sb = new StringBuilder(length*3);

    sb.append('{');
    int l = length-1;
    for (i=0; i<l; i++) {
      sb.append(Integer.toHexString(get(i)));
      sb.append(',');
    }
    sb.append(get(i));
    sb.append('}');

    return sb.toString();
  }

}

