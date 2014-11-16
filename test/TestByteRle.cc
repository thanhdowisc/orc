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

#include <iostream>

#include "ByteRLE.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(ByteRle, simpleTest) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0x61, 0x00, 0xfd, 0x44, 0x45, 0x46});
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<SeekableInputStream>
				     (stream)));
    char* data = new char[103];
    rle->next(data, 103, 0);

    for(int i=0; i < 100; ++i) {
      EXPECT_EQ(0, data[i]) << "Output wrong at " << i;
    }
    EXPECT_EQ(0x44, data[100]);
    EXPECT_EQ(0x45, data[101]);
    EXPECT_EQ(0x46, data[102]);
    delete[] data;
  }

  TEST(ByteRle, simpleRuns) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0x0d, 0xff, 0x0d, 0xfe, 0x0d, 0xfd});
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<SeekableInputStream>
				     (stream)));
    char* data = new char[16];
    for(int i=0; i < 3; ++i) {
      rle->next(data, 16, 0);
      for(int j=0; j < 16; ++j) {
	EXPECT_EQ(static_cast<char>(-1 - i), data[j]) << "Output wrong at " 
						      << (16 * i + j);
      }
    }
    delete[] data;
  }

  TEST(ByteRle, splitHeader) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0x0, 0x01, 0xe0, 
	    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	    0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	    0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}, 1);
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream>
				     (stream)));
    char* data = new char[35];
    rle->next(data, 35, 0);
    for(int i=0; i < 3; ++i) {
      EXPECT_EQ(1, data[i]) << "Output wrong at " << i;
    }
    for(int i=3; i < 35; ++i) {
      EXPECT_EQ(i-2, data[i]) << "Output wrong at " << i;
    }
    delete[] data;
  }

  TEST(ByteRle, splitRuns) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0x0d, 0x02, 0xf0, 
	    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10});
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream>
				     (stream)));
    char* data = new char[5];
    for(int i=0; i < 3; ++i) {
      rle->next(data, 5, 0);
      for(int j=0; j < 5; ++j) {
	EXPECT_EQ(2, data[j]) << "Output wrong at " << (i * 5 + j);
      }
    }
    rle->next(data, 5, 0);
    EXPECT_EQ(2, data[0]) << "Output wrong at 15";
    for(int i=1; i < 5; ++i) {
      EXPECT_EQ(i, data[i]) << "Output wrong at " << (15 + i);
    }
    for(int i=0; i < 2; ++i) {
      rle->next(data, 5, 0);
      for(int j=0; j < 5; ++j) {
	EXPECT_EQ(5 * i + j + 5, data[j]) << "Output wrong at " 
					  << (20 + 5 * i + j);
      }
    }
    rle->next(data, 2, 0);
    EXPECT_EQ(15, data[0]) << "Output wrong at 30" ;
    EXPECT_EQ(16, data[1]) << "Output wrong at 31" ;
    delete[] data;
  }

  TEST(ByteRle, testNulls) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0xf0, 
	    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
	    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	    0x3d, 0xdc}, 3);
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream>
				     (stream)));
    char* data = new char[16];
    char* isNull = new char[16];
    for(int i=0; i < 16; ++i) {
      isNull[i] = i % 2;
      data[i] = -1;
    }
    for(int i=0; i < 2; ++i) {
      rle->next(data, 16, isNull);
      for(int j=0; j < 16; ++j) {
	if (j % 2 == 0) {
	  EXPECT_EQ((i*16 + j)/2, data[j]) << "Output wrong at " 
					   << (i * 16 + j);
	} else {
	  EXPECT_EQ(-1, data[j]) << "Output wrong at " 
				 << (i * 16 + j);
	}
      }
    }
    for(int i=0; i < 8; ++i) {
      rle->next(data, 16, isNull);
      for(int j=0; j < 16; ++j) {
	if (j % 2 == 0) {
	  EXPECT_EQ(-36, data[j]) << "Output wrong at " 
					   << (i * 16 + j + 32);
	} else {
	  EXPECT_EQ(-1, data[j]) << "Output wrong at " 
				 << (i * 16 + j + 32);
	}
      }
    }
    delete[] data;
    delete[] isNull;
  }

  TEST(ByteRle, testAllNulls) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0xf0, 
	    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
	    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	    0x3d, 0xdc});
    std::unique_ptr<ByteRleDecoder> rle = 
      createByteRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream>
				     (stream)));
    char* data = new char[16];
    char* allNull = new char[16];
    char* noNull = new char[16];
    for(int i=0; i < 16; ++i) {
      allNull[i] = 1;
      noNull[i] = 0;
      data[i] = -1;
    }
    rle->next(data, 16, allNull);
    for(int i=0; i < 16; ++i) {
      EXPECT_EQ(-1, data[i]) << "Output wrong at " << i;
    }
    rle->next(data, 16, noNull);
    for(int i=0; i < 16; ++i) {
      EXPECT_EQ(i, data[i]) << "Output wrong at " << i;
      data[i] = -1;
    }
    rle->next(data, 16, allNull);
    for(int i=0; i < 16; ++i) {
      EXPECT_EQ(-1, data[i]) << "Output wrong at " << i;
    }
    for(int i=0; i < 4; ++i) {
      rle->next(data, 16, noNull);
      for(int j=0; j < 16; ++j) {
	EXPECT_EQ(-36, data[j]) << "Output wrong at " << i;
      }
    }
    delete[] data;
    delete[] allNull;
    delete[] noNull;
  }

}
