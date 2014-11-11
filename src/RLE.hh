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

#ifndef ORC_RLE_HH
#define ORC_RLE_HH

#include <google/protobuf/io/zero_copy_stream.h>
#include <memory>

#include "Compression.hh"
#include "orc/Vector.hh"

namespace orc {

  enum RleVersion {
    VERSION_1,
    VERSION_2
  };

  class RleDecoder {
  public:
    virtual ~RleDecoder();
    
    /**
     * Reset the run length decoder.
     */
    virtual void reset(std::unique_ptr<SeekableInputStream> stream) = 0;

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&) = 0;

    /**
     * Seek over a given number of values.
     */
    virtual void skip(long numValues) = 0;

    /**
     * Read a number of values into the batch.
     */
    virtual void next(LongVectorBatch& data, long numValues) = 0;
  };

  /**
   * Create an RLE decoder.
   * @param input the input stream to read from
   * @param isSigned is the number sequence signed?
   * @param version version of RLE decoding to do
   */
  std::unique_ptr<RleDecoder> createRleDecoder
                                  (std::unique_ptr<SeekableInputStream> input, 
				   bool isSigned,
				   RleVersion version);
}

#endif
