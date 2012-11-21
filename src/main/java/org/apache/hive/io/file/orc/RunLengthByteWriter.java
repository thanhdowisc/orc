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
 * A writer that writes a sequence of bytes. A control byte is written before
 * each run with positive values 0 to 127 meaning 2 to 129 repetitions. If the
 * bytes is -1 to -128, 1 to 128 literal byte values follow.
 */
class RunLengthByteWriter {
  private static final int MAX_LITERAL_SIZE=128;
  private static final int MAX_REPEAT_SIZE=129;
  private final PositionedOutputStream output;
  private final byte[] literals = new byte[MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private boolean repeat = false;

  RunLengthByteWriter(PositionedOutputStream output) throws IOException {
    this.output = output;
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - 2);
        output.write(literals, 0, 1);
     } else {
        output.write(-numLiterals);
        output.write(literals, 0, numLiterals);
      }
      repeat = false;
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
      repeat = true;
    } else if (repeat) {
      if (literals[0] == value) {
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {
          writeValues();
        }
      } else if (numLiterals == 1) {
        literals[numLiterals++] = value;
        repeat = false;
      } else {
        writeValues();
        literals[numLiterals++] = value;
      }
    } else {
      literals[numLiterals++] = value;
      if (numLiterals == MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals * 2 + (repeat ? 1 : 0));
  }
}
