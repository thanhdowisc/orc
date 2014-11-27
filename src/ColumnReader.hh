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

#include "orc/Reader.hh"
#include "orc/Vector.hh"
#include "Compression.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

  class DetailedStripeInformation {
  public:
    virtual ~DetailedStripeInformation();

    /**
     * Get the encoding for the given column for this stripe.
     */
    virtual proto::ColumnEncoding_Kind getEncoding(int columnId) = 0;

    /**
     * Get the stream for the given column/kind in this stripe.
     */
    virtual std::unique_ptr<SeekableInputStream> 
                    getStream(int columnId,
                              proto::Stream_Kind kind) = 0;
  };

  /**
   * The interface for reading ORC data types
   */
  class ColumnReader {
  public:
    ColumnReader(int columnId, DetailedStripeInformation& stipe);

    virtual ~ColumnReader();

    /**
     * Seek to the position provided by index.
     * @param index
     * @throws IOException
     */
    virtual void seek(PositionProvider& index) = 0;

    /**
     * Skip number of specified rows.
     * @param numValues
     * @throws IOException
     */
    virtual void skip(long numValues) = 0;

    /**
     * Read the next group of values into this rowBatch.
     * @param rowBatch the memory to read into.
     * @throws IOException
     */
    virtual void next(ColumnVectorBatch& rowBatch, long numValues) = 0;
  };

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Reader& reader,
                                            int rootColumn,
                                            DetailedStripeInformation& stripe);
}

#endif
