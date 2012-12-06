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
 * A writer that writes a sequence of bytes. A control byte is written before
 * each run with positive values 0 to 127 meaning 2 to 129 repetitions. If the
 * bytes is -1 to -128, 1 to 128 literal byte values follow.
 */
class RunLengthByteWriter {
  static final int MAX_LITERAL_SIZE=128;
  static final int MAX_REPEAT_SIZE=130;
  private final PositionedOutputStream output;
  private final byte[] literals = new byte[MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private boolean repeat = false;
  private boolean lastTwoSame = false;

  RunLengthByteWriter(PositionedOutputStream output) throws IOException {
    this.output = output;
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - 3);
        output.write(literals, 0, 1);
     } else {
        output.write(-numLiterals);
        output.write(literals, 0, numLiterals);
      }
      repeat = false;
      lastTwoSame = false;
      numLiterals = 0;
    }
  }

  void flush() throws IOException {
    writeValues();
    output.flush();
  }

  void write(byte value) throws IOException {
    if (numLiterals == 0) {
      literals[numLiterals++] = value;
    } else if (numLiterals == 1) {
      lastTwoSame = value == literals[numLiterals - 1];
      literals[numLiterals++] = value;
    } else if (numLiterals == 2) {
      if (value == literals[1] && lastTwoSame) {
        repeat = true;
      } else {
        lastTwoSame = value == literals[numLiterals - 1];
        literals[numLiterals] = value;
      }
      numLiterals += 1;
    } else {
      if (repeat) {
        if (value == literals[0]) {
          numLiterals += 1;
          if (numLiterals == MAX_REPEAT_SIZE) {
            writeValues();
          }
        } else {
          writeValues();
          literals[numLiterals++] = value;
        }
      } else {
        // if the current number and the previous two form a repetition
        boolean currentSame = value == literals[numLiterals - 1];
        if (currentSame && lastTwoSame) {
          // record the base of the repetition
          byte base = literals[numLiterals - 2];
          // remove the last two values and flush
          numLiterals -= 2;
          writeValues();
          // set up the new repetition at size 3
          literals[0] = base;
          repeat = true;
          numLiterals = 3;
        } else {
          lastTwoSame = currentSame;
          literals[numLiterals++] = value;
          if (numLiterals == MAX_LITERAL_SIZE) {
            writeValues();
          }
        }
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }
}
