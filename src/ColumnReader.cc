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

#include "ColumnReader.hh"
#include "RLE.hh"

namespace orc {

  ColumnReader::~ColumnReader() {
    //PASS
  }

  class IntegerReader: public ColumnReader {
  private:
    std::unique_ptr<orc::RleDecoder> rle;

  public:
    IntegerReader(int columnId, DetailedStripeInformation& stipe);
    ~IntegerReader();

    void seek(PositionProvider& index) override;

    void skip(long numValues) override;

    void next(ColumnVectorBatch& rowBatch, long numValues) override;
  };

  IntegerReader::IntegerReader(int columnId, 
                               DetailedStripeInformation& stipe) {
  }

  IntegerReader::~IntegerReader() {
    // PASS
  }

  void IntegerReader::seek(PositionProvider& index) override {
    throw std::string("not implemented yet");
  }

  void IntegerReader::skip(long numValues) override {
    rle->skip(numValues);
  }

  void IntegerReader::next(ColumnVectorBatch& rowBatch, 
                           long numValues) override {
  }

  class StringReader: public ColumnReader {
  public:
    StringReader(int columnId, DetailedStripeInformation& stipe);
    ~StringReader();

    void seek(PositionProvider& index) override;

    void skip(long numValues) override;

    void next(ColumnVectorBatch& rowBatch, long numValues) override;
  };

  class StructReader: public ColumnReader {
  public:
    StructReader(int columnId, DetailedStripeInformation& stipe);
    ~StructReader();

    void seek(PositionProvider& index) override;

    void skip(long numValues) override;

    void next(ColumnVectorBatch& rowBatch, long numValues) override;
  };
}
