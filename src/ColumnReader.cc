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

#include "ByteRLE.hh"
#include "ColumnReader.hh"
#include "RLE.hh"

namespace orc {

  ColumnReader::ColumnReader(const Type& type,
                             StripeStreams& stripe
                             ): columnId(type.getColumnId()) {
    std::unique_ptr<SeekableInputStream> stream =
      stripe.getStream(columnId, proto::Stream_Kind_PRESENT);
    if (stream.get()) {
      isNullDecoder = createBooleanRleDecoder(std::move(stream));
    }
  }

  ColumnReader::~ColumnReader() {
    // PASS
  }

  unsigned long ColumnReader::skip(unsigned long numValues) {
    ByteRleDecoder* decoder = isNullDecoder.get();
    if (decoder) {
      decoder->skip(numValues);
      // TODO we need to get the number of non null values
    }
    return numValues;
  }

  void ColumnReader::next(ColumnVectorBatch& rowBatch, 
                          unsigned long numValues) {
    rowBatch.numElements = numValues;
    ByteRleDecoder* decoder = isNullDecoder.get();
    rowBatch.hasNulls = decoder != nullptr;
    if (decoder) {
      decoder->next(rowBatch.isNull.get(), numValues, 0);
    }
  }

  class IntegerColumnReader: public ColumnReader {
  private:
    std::unique_ptr<orc::RleDecoder> rle;

  public:
    IntegerColumnReader(const Type& type, StripeStreams& stipe);
    ~IntegerColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, unsigned long numValues) override;
  };

  IntegerColumnReader::IntegerColumnReader(const Type& type,
                                           StripeStreams& stripe
                                           ): ColumnReader(type, stripe) {
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DIRECT:
      rle = createRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA), 
                             false, VERSION_1);
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      rle = createRleDecoder(stripe.getStream(columnId,
                                              proto::Stream_Kind_DATA), 
                             false, VERSION_2);
      break;
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      throw std::string("Unknown encoding for IntegerColumnReader");
    }
  }

  IntegerColumnReader::~IntegerColumnReader() {
    // PASS
  }

  unsigned long IntegerColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void IntegerColumnReader::next(ColumnVectorBatch& rowBatch, 
                                 unsigned long numValues) {
    ColumnReader::next(rowBatch, numValues);
    rle->next(dynamic_cast<LongVectorBatch&>(rowBatch).data.get(),
              numValues, rowBatch.isNull.get());
  }

  class StringDictionaryColumnReader: public ColumnReader {
  private:
    std::unique_ptr<char[]> dictionaryBlob;
    std::unique_ptr<long[]> dictionaryOffset;
    std::unique_ptr<RleDecoder> rle;
    unsigned int dictionaryCount;
    
  public:
    StringDictionaryColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDictionaryColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, unsigned long numValues) override;
  };

  void readFully(char* buffer, long bufferSize, SeekableInputStream* stream) {
    long posn = 0;
    while (posn < bufferSize) {
      const void* chunk;
      int length;
      if (!stream->Next(&chunk, &length)) {
        throw std::string("bad read");
      }
      memcpy(buffer + posn, chunk, static_cast<size_t>(length));
      posn += length;
    }
  }

  StringDictionaryColumnReader::StringDictionaryColumnReader
      (const Type& type,
       StripeStreams& stripe
       ): ColumnReader(type, stripe) {
    RleVersion rleVersion;
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
      rleVersion = VERSION_1;
      break;
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      rleVersion = VERSION_2;
      break;
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DIRECT_V2:
      throw std::string("Unknown encoding for StringDictionaryColumnReader");
    }
    dictionaryCount = stripe.getEncoding(columnId).dictionarysize();
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_DATA), 
                           false, rleVersion);
    std::unique_ptr<RleDecoder> lengthDecoder = 
      createRleDecoder(stripe.getStream(columnId,
                                        proto::Stream_Kind_LENGTH),
                       false, rleVersion);
    dictionaryOffset = std::unique_ptr<long[]>(new long[dictionaryCount+1]);
    long* lengthArray = dictionaryOffset.get();
    lengthDecoder->next(lengthArray + 1, dictionaryCount, 0);
    lengthArray[0] = 0;
    for(unsigned int i=1; i < dictionaryCount + 1; ++i) {
      lengthArray[i] += lengthArray[i-1];
    }
    long blobSize = lengthArray[dictionaryCount];
    dictionaryBlob = std::unique_ptr<char[]>(new char[blobSize]);
    std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA);
    readFully(dictionaryBlob.get(), blobSize, blobStream.get());
  }

  StringDictionaryColumnReader::~StringDictionaryColumnReader() {
    // PASS
  }

  unsigned long StringDictionaryColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void StringDictionaryColumnReader::next(ColumnVectorBatch& rowBatch, 
                                          unsigned long numValues) {
    ColumnReader::next(rowBatch, numValues);
    rle->next(dynamic_cast<LongVectorBatch&>(rowBatch).data.get(),
              numValues, rowBatch.isNull.get());
  }

  class StructColumnReader: public ColumnReader {
  private:
    std::unique_ptr<std::unique_ptr<ColumnReader>[]> children;
    int subtypeCount;

  public:
    StructColumnReader(const Type& type,
                       StripeStreams& stipe);
    ~StructColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch, unsigned long numValues) override;
  };

  StructColumnReader::StructColumnReader(const Type& type, 
                                         StripeStreams& stripe
                                         ): ColumnReader(type, stripe) {
    subtypeCount = type.getSubtypeCount();
    children.reset(new std::unique_ptr<ColumnReader>[subtypeCount]);
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for(int i=0; i < subtypeCount; ++i) {
        children.get()[i].reset(buildReader(type.getSubtype(i), stripe
                                            ).release());
      }
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      throw std::string("Unknown encoding for StructColumnReader");
    }
  }

  StructColumnReader::~StructColumnReader() {
    // PASS
  }

  unsigned long StructColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    for(int i=0; i < subtypeCount; ++i) {
      children.get()[i].get()->skip(numValues);
    }
    return numValues;
  }

  void StructColumnReader::next(ColumnVectorBatch& rowBatch, 
                                unsigned long numValues) {
    ColumnReader::next(rowBatch, numValues);
    std::unique_ptr<ColumnVectorBatch> *childBatch = 
      dynamic_cast<StructVectorBatch&>(rowBatch).fields.get();
    for(int i=0; i < subtypeCount; ++i) {
      children.get()[i].get()->next(*(childBatch[i]), numValues);
    }
  }

}
