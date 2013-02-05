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
package org.apache.hadoop.hive.ql.vector;

import java.io.IOException;

/**
 * The following interface delivers a sequence of vectorized
 * batches, to facilitate high-performance query execution.
 */

public interface VectorizedBatchReader {
  /**
   * Prepare the interface for the first call to next().
   * @throws java.io.IOException
   */
  void open() throws IOException;

  /**
   * Read the next batch of rows into the argument batch object.
   * This batch is re-used to mimimize memory allocation expense.
   * The end of the scan has been reached when batch.EOF is true.
   * @throws java.io.IOException
   */
  void next(VectorizedRowBatch batch) throws IOException;

  /**
   * Get the progress of the reader through the batches of rows.
   * @return a fraction between 0.0 and 1.0 of rows read
   * @throws java.io.IOException
   */
  float getProgress() throws IOException;

  /**
   * Release the resources associated with the given reader.
   * @throws java.io.IOException
   */
  void close() throws IOException;
}
