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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Reader {
  public interface FileInformation {
    /**
     * Get the number of rows in the file
     * @return the number of rows
     */
    long getNumberOfRows();

    /**
     * Get the user metadata keys
     * @return the set of metadata keys
     */
    Iterable<String> getMetadataKeys();

    /**
     * Get a user metadata value
     * @param key a key given by the user
     * @return the bytes associated with the given key
     */
    ByteBuffer getMetadataValue(String key);

    /**
     * Get the compression kind.
     * @return the kind of compression in the file
     */
    CompressionKind getCompression();

    /**
     * Get the list of stripes.
     * @return the information about the stripes in order
     */
    Iterable<StripeInformation> getStripes();

    /**
     * Get the type of the records in this file.
     * @return the type information
     */
    TypeInfo getSchema();

    /**
     * Get the length of the file.
     * @return the number of bytes in the file
     */
    long getLength();

    /**
     * Get the statistics about the given primitive column with in the file.
     * @param column the column of interest. It must be one of the objects
     *               reachable from getSchema().
     * @return the information about the column
     */
    ColumnStatistics getStatistics(TypeInfo column);
  }

  public interface StripeInformation extends Writable {
    /**
     * Get the byte offset of the start of the stripe.
     * @return the bytes from the start of the file
     */
    long getOffset();

    /**
     * Get the length of the stripe
     * @return the number of bytes in the stripe
     */
    long getLength();

    /**
     * Get the length of the stripe's tail section, which contains its index.
     * @return the number of bytes in the tail
     */
    long getTailLength();

    /**
     * Get the number of rows in the stripe.
     * @return a count of the number of rows
     */
    long getNumberOfRows();
  }

  public interface ColumnStatistics {
    /**
     * Get the minimum value for this column.
     * @return the minimum value
     */
    Object getMinimum();

    /**
     * Get the maximum value for this column
     * @return the maximum value
     */
    Object getMaximum();

    /**
     * Get the number of values in this column. It will differ from the number
     * of rows because of NULL values and repeated values.
     * @return the number of values
     */
    long getNumberOfValues();
  }

  public FileInformation getFileInformation() throws IOException;

  public RecordReader rows() throws IOException;

  public RecordReader rows(StripeInformation[] stripes) throws IOException;

  public ColumnStatistics getStatistics(StripeInformation stripe
                                        ) throws IOException;
}
