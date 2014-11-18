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

#ifndef ORC_DATA_READER_HH
#define ORC_DATA_READER_HH

#include <orc/Reader.hh>
#include <orc/OrcFile.hh>
#include <RLE.hh>
#include "orc_proto.pb.h"

namespace orc {

  /**
   * The interface for reading ORC data types
   */
  class DataReader {
  public:
    virtual ~DataReader();

    /**
     * Setup the reader with an encoded data stream
     * @return
     * @throws IOException
     */
    virtual void reset(SeekableArrayInputStream* stream, orc::proto::ColumnEncoding encoding) = 0;

    /**
     * Seek to the position provided by index.
     * @param index
     * @throws IOException
     */
//    virtual void seek(PositionProvider index) = 0;

    /**
     * Skip number of specified rows.
     * @param numValues
     * @throws IOException
     */
//    virtual void skip(long numValues) = 0;

    /**
     * Check if there are any more values left.
     * @return
     * @throws IOException
     */
//    virtual bool hasNext() = 0;

    /**
     * Return the next available value.
     * @return
     * @throws IOException
     */
    virtual boost::any next() = 0;

    /**
     * Return the next available vector for values.
     * @return
     * @throws IOException
     */
//     virtual void nextVector(LongColumnVector previous, long previousLen) = 0;
  };

  DataReader* createIntegerReader() ;
  DataReader* createStringReader() ;
  DataReader* createDummyReader() ;
}

#endif
