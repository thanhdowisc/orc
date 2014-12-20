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
#include "Exceptions.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"
#include "wrap/gmock.h"

#include <iostream>
#include <vector>

namespace orc {

  class MockStripeStreams : public StripeStreams {
  public:
    ~MockStripeStreams();
    std::unique_ptr<SeekableInputStream> getStream(int columnId,
                                                   proto::Stream_Kind kind
                                                   ) const override;
    MOCK_CONST_METHOD0(getSelectedColumns, bool*());
    MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding (int));
    MOCK_CONST_METHOD2(getStreamProxy, SeekableInputStream*
                       (int, proto::Stream_Kind));
  };

  MockStripeStreams::~MockStripeStreams() {
    // PASS
  }

  std::unique_ptr<SeekableInputStream>
       MockStripeStreams::getStream(int columnId,
                                    proto::Stream_Kind kind) const {
    return std::auto_ptr<SeekableInputStream>(getStreamProxy(columnId,
                                                             kind));
  }

  TEST(TestColumnReader, testIntegerWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool(2));
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x19, 0xf0})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x64, 0x01, 0x00})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(INT)}, {"myInt"});
    rowType->assignIds(0);


    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StructVectorBatch batch(1024);
    batch.numFields = 1;
    batch.fields = std::unique_ptr<std::unique_ptr<ColumnVectorBatch>[]>
      (new std::unique_ptr<ColumnVectorBatch>[1]);
    batch.fields.get()[0] = std::unique_ptr<ColumnVectorBatch>
      (new LongVectorBatch(1024));
    LongVectorBatch *longBatch =
      dynamic_cast<LongVectorBatch*>(batch.fields.get()[0].get());
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    long next = 0;
    for(size_t i=0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, longBatch->notNull.get()[i]);
      } else {
        EXPECT_EQ(1, longBatch->notNull.get()[i]);
        EXPECT_EQ(next++, longBatch->data.get()[i]);
      }
    }
  };

  TEST(TestColumnReader, testDictionaryWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool(2));
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(0))
      .WillRepeatedly(testing::Return(directEncoding));
    proto::ColumnEncoding dictionaryEncoding;
    dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionaryEncoding.set_dictionarysize(2);
    EXPECT_CALL(streams, getEncoding(1))
      .WillRepeatedly(testing::Return(dictionaryEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x19, 0xf0})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x2f, 0x00, 0x00,
                                          0x2f, 0x00, 0x01})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x4f, 0x52, 0x43, 0x4f, 0x77,
                                          0x65, 0x6e})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x02, 0x01, 0x03})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"myString"});
    rowType->assignIds(0);


    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StructVectorBatch batch(1024);
    batch.numFields = 1;
    batch.fields = std::unique_ptr<std::unique_ptr<ColumnVectorBatch>[]>
      (new std::unique_ptr<ColumnVectorBatch>[1]);
    batch.fields.get()[0] = std::unique_ptr<ColumnVectorBatch>
      (new StringVectorBatch(1024));
    StringVectorBatch *stringBatch =
      dynamic_cast<StringVectorBatch*>(batch.fields.get()[0].get());
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, stringBatch->numElements);
    ASSERT_EQ(true, stringBatch->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, stringBatch->notNull.get()[i]);
      } else {
        EXPECT_EQ(1, stringBatch->notNull.get()[i]);
        const char* expected = i < 98 ? "ORC" : "Owen";
        ASSERT_EQ(strlen(expected), stringBatch->length.get()[i])
            << "Wrong length at " << i;
        for(size_t letter = 0; letter < strlen(expected); ++letter) {
          EXPECT_EQ(expected[letter], stringBatch->data.get()[i][letter])
            << "Wrong contents at " << i << ", " << letter;
        }
      }
    }
  };

  TEST(TestColumnReader, testVarcharDictionaryWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool(4));
    memset(selectedColumns.get(), true, 3);
    selectedColumns.get()[3] = false;
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(0))
      .WillRepeatedly(testing::Return(directEncoding));

    proto::ColumnEncoding dictionary2Encoding;
    dictionary2Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionary2Encoding.set_dictionarysize(2);
    EXPECT_CALL(streams, getEncoding(1))
      .WillRepeatedly(testing::Return(dictionary2Encoding));

    proto::ColumnEncoding dictionary0Encoding;
    dictionary0Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictionary0Encoding.set_dictionarysize(0);
    EXPECT_CALL(streams, getEncoding(testing::Ge(2)))
      .WillRepeatedly(testing::Return(dictionary0Encoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0xff})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x01,
                                          0x61, 0x00, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x4f, 0x52, 0x43, 0x4f, 0x77,
                                          0x65, 0x6e})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x02, 0x01, 0x03})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(VARCHAR),
                        createPrimitiveType(CHAR),
                        createPrimitiveType(STRING)},
        {"col0", "col1", "col2"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StructVectorBatch batch(1024);
    batch.numFields = 2;
    batch.fields = std::unique_ptr<std::unique_ptr<ColumnVectorBatch>[]>
	(new std::unique_ptr<ColumnVectorBatch>[2]);
    for(size_t i=0; i < 2 ; ++i) {
      batch.fields.get()[i] = std::unique_ptr<ColumnVectorBatch>
	(new StringVectorBatch(1024));
    }
    StringVectorBatch *stringBatch =
      dynamic_cast<StringVectorBatch*>(batch.fields.get()[0].get());
    StringVectorBatch *nullBatch =
      dynamic_cast<StringVectorBatch*>(batch.fields.get()[1].get());
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, stringBatch->numElements);
    ASSERT_EQ(false, stringBatch->hasNulls);
    ASSERT_EQ(200, nullBatch->numElements);
    ASSERT_EQ(true, nullBatch->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(true, stringBatch->notNull.get()[i]);
      EXPECT_EQ(false, nullBatch->notNull.get()[i]);
      const char* expected = i < 100 ? "Owen" : "ORC";
      ASSERT_EQ(strlen(expected), stringBatch->length.get()[i])
	<< "Wrong length at " << i;
      for(size_t letter = 0; letter < strlen(expected); ++letter) {
	EXPECT_EQ(expected[letter], stringBatch->data.get()[i][letter])
	  << "Wrong contents at " << i << ", " << letter;
      }
    }
  };

  TEST(TestColumnReader, testUnimplementedTypes) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool(2));
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(FLOAT)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(DOUBLE)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(BINARY)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(BOOLEAN)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(TIMESTAMP)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(LIST)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(MAP)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(UNION)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(DECIMAL)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(DATE)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(VARCHAR)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(VARCHAR)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(CHAR)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);
  };

}  // namespace orc
