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

#include "orc/orc-config.hh"
#include "RLE.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {
  TEST(RLEv1, simpleTest) {
    SeekableInputStream* stream = 
      new SeekableArrayInputStream({0x61, 0xff, 0x64, 
	    0xfb, 0x02, 0x03, 0x5, 0x7, 0xb});
    std::unique_ptr<orc::RleDecoder> rle = 
      orc::createRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream>
				      (stream)), 
			    false, orc::VERSION_1);
    long* data = new long[105];
    rle->next(data, 105, 0);

    for(int i=0; i < 100; ++i) {
      EXPECT_EQ(100 - i, data[i]) << "Output wrong at " << i;
    }
    EXPECT_EQ(2, data[100]);
    EXPECT_EQ(3, data[101]);
    EXPECT_EQ(5, data[102]);
    EXPECT_EQ(7, data[103]);
    EXPECT_EQ(11, data[104]);
  }

}

GTEST_API_ int main(int argc, char **argv) {
  std::cout << "ORC version: " << ORC_VERSION << "\n";
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
