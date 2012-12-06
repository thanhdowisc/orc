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

/**
 * A reader that reads a sequence of integers.
 * */
class RunLengthIntegerReader {
  private final InStream input;
  private final boolean signed;
  private final int[] literals =
    new int[RunLengthIntegerWriter.MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private int delta = 0;
  private int used = 0;
  private boolean done = false;
  private boolean repeat = false;

  RunLengthIntegerReader(InStream input,
                         boolean signed) throws IOException {
    this.input = input;
    this.signed = signed;
  }

  private void readValues() throws IOException {
    int control = input.read();
    if (control == -1) {
      done = true;
      return;
    } else if (control < 0x80) {
      numLiterals = control + 3;
      used = 0;
      repeat = true;
      if (signed) {
        literals[0] = SerializationUtils.readVsint(input);
      } else {
        literals[0] = SerializationUtils.readVuint(input);
      }
      delta = SerializationUtils.readVsint(input);
    } else {
      repeat = false;
      numLiterals = 0x100 - control;
      used = 0;
      for(int i=0; i < numLiterals; ++i) {
        if (signed) {
          literals[i] = SerializationUtils.readVsint(input);
        } else {
          literals[i] = SerializationUtils.readVuint(input);
        }
      }
    }
  }

  boolean hasNext() throws IOException {
    if (used == numLiterals && !done) {
      readValues();
    }
    return !done && used != numLiterals;
  }

  int next() throws IOException {
    if (repeat) {
      return literals[0] + (used++) * delta;
    } else {
      return literals[used++];
    }
  }

}
