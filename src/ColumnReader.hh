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

#ifndef ORC_COLUMN_READER_HH
#define ORC_COLUMN_READER_HH

#include "orc/Vector.hh"
#include "Compression.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

  class StripeStreams {
  public:
    virtual ~StripeStreams();

    /**
     * Get the encoding for the given column for this stripe.
     */
    virtual proto::ColumnEncoding getEncoding(int columnId) = 0;

    /**
     * Get the stream for the given column/kind in this stripe.
     */
    virtual std::unique_ptr<SeekableInputStream> 
                    getStream(int columnId,
                              proto::Stream_Kind kind) = 0;
  };

  /**
   * The interface for reading ORC data types.
   */
  class ColumnReader {
  protected:
    std::unique_ptr<ByteRleDecoder> isNullDecoder;
    int columnId;

  public:
    ColumnReader(const Type& type, StripeStreams& stipe);

    virtual ~ColumnReader();

    /**
     * Skip number of specified rows.
     * @param numValues the number of values to skip
     * @return the number of non-null values skipped
     * @throws IOException
     */
    virtual unsigned long skip(unsigned long numValues);

    /**
     * Read the next group of values into this rowBatch.
     * @param rowBatch the memory to read into.
     * @throws IOException
     */
    virtual void next(ColumnVectorBatch& rowBatch, 
                      unsigned long numValues);
  };

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                            StripeStreams& stripe);
}

#endif
