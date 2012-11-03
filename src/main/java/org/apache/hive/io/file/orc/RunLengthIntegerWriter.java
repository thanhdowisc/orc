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

/**
 * A writer that writes a sequence of integers. A control byte is written before
 * each run with positive values 0 to 127 meaning 2 to 129 repetitions. If the
 * bytes is -1 to -128, 1 to 128 literal vint values follow.
 */
class RunLengthIntegerWriter {
  private static final int MAX_LITERAL_SIZE=128;
  private static final int MAX_REPEAT_SIZE=129;
  private final OutputStream output;
  private final boolean signed;
  private final int[] literals = new int[MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private boolean repeat = false;

  RunLengthIntegerWriter(OutputStream output,
                         boolean signed) throws IOException {
    this.output = output;
    this.signed = signed;
  }

  void flush() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - 2);
        if (signed) {
          SerializationUtils.writeVsint(output, literals[0]);
        } else {
          SerializationUtils.writeVuint(output, literals[0]);
        }
      } else {
        output.write(-numLiterals);
        for(int i=0; i < numLiterals; ++i) {
          if (signed) {
            SerializationUtils.writeVsint(output, literals[i]);
          } else {
            SerializationUtils.writeVuint(output, literals[i]);
          }
        }
      }
      repeat = false;
      numLiterals = 0;
    }
    output.flush();
  }

  void write(int value) throws IOException {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
    } else if (repeat) {
      if (literals[0] == value) {
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {
          flush();
        }
      } else {
        flush();
        literals[numLiterals++] = value;
      }
    } else {
      literals[numLiterals++] = value;
      if (numLiterals == MAX_LITERAL_SIZE) {
        flush();
      }
    }
  }

}
