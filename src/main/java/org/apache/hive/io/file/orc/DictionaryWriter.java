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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Encode a set of strings by replace each string with a unique integer. The
 * bytes of the string are stored in a buffer. Each entry in the dictionary is
 * encoded as the number of bytes in each string. The rows are encoded as a
 * vector of positive integers. Finally, the number of repetitions of each
 * dictionary entry is given in the count outStream.
 */
class DictionaryWriter {
  private final PositionedOutputStream stringOutput;
  private final RunLengthIntegerWriter lengthOutput;
  private final RunLengthIntegerWriter rowOutput;
  private final RunLengthIntegerWriter countOutput;
  private int nextId = 0;
  private final Map<String, Entry> dictionary = new HashMap<String, Entry>();

  private static class Entry {
    int repetitions = 0;
    int id;
  }

  DictionaryWriter(PositionedOutputStream stringOutput,
                   PositionedOutputStream lengthOutput,
                   PositionedOutputStream rowOutput,
                   PositionedOutputStream countOutput) throws IOException {
    this.stringOutput = stringOutput;
    this.lengthOutput = new RunLengthIntegerWriter(lengthOutput, false);
    this.rowOutput = new RunLengthIntegerWriter(rowOutput, false);
    this.countOutput = new RunLengthIntegerWriter(countOutput, false);
  }

  /**
   * Write out a string to the file.
   * @param value the string to write
   * @return true if the word hasn't been seen before in this block
   * @throws IOException
   */
  boolean write(String value) throws IOException {
    boolean result = false;
    Entry entry = dictionary.get(value);
    // if we haven't seen this word before, add it to the dictionary.
    if (entry == null) {
      entry = new Entry();
      entry.id = nextId++;
      dictionary.put(value, entry);
      ByteBuffer bytes = Text.encode(value, true);
      lengthOutput.write(bytes.limit());
      stringOutput.write(bytes.array(), bytes.arrayOffset(), bytes.limit());
      result = true;
    }
    entry.repetitions += 1;
    rowOutput.write(entry.id);
    return result;
  }

  void flush() throws IOException {
    stringOutput.flush();
    lengthOutput.flush();
    rowOutput.flush();
    // create a section with the replication counts for each dictionary entry
    int[] counts = new int[nextId];
    for(Entry entry: dictionary.values()) {
      counts[entry.id] = entry.repetitions;
    }
    for(int i=0; i < counts.length; ++i) {
      countOutput.write(counts[i]);
    }
    countOutput.flush();
    nextId = 0;
    dictionary.clear();
  }

  void recordPosition(PositionRecorder recorder) throws IOException {
    rowOutput.getPosition(recorder);
  }
}
