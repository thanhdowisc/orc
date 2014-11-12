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

#include <algorithm>
#include <iostream>
#include <utility>

#include "RLE.hh"

namespace orc {

  const int MINIMUM_REPEAT = 3;
  const int BASE_128_MASK = 0x7f;

  inline long unZigZag(long value) {
    return value >> 1 ^ -(value & 1);
  }
  
  RleDecoder::~RleDecoder() {
    // PASS
  }

  class RleDecoderV1: public RleDecoder {
  public:
    RleDecoderV1(std::unique_ptr<SeekableInputStream> input,
		 bool isSigned);

    virtual ~RleDecoderV1();
    
    /**
     * Reset the run length decoder.
     */
    virtual void reset(std::unique_ptr<SeekableInputStream> stream);

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&);

    /**
     * Seek over a given number of values.
     */
    virtual void skip(long numValues);

    /**
     * Read a number of values into the batch.
     */
    virtual void next(long* data, long numValues, bool* isNull);

  private:
    inline signed char readByte();
    inline void readHeader();
    inline long readLong();
    inline void skipLongs(long numValues);

    std::unique_ptr<SeekableInputStream> inputStream;
    long remainingValues;
    long value;
    const char* bufferStart;
    const char* bufferEnd;
    int delta;
    const bool isSigned;
    bool repeating;
  };

  signed char RleDecoderV1::readByte() {
    if (bufferStart == bufferEnd) {
      int bufferLength;
      const void* bufferPointer;
      bool result = inputStream->Next(&bufferPointer, &bufferLength);
      if (!result) {
	throw std::string("bad read in readByte");
      }
      bufferStart = static_cast<const char*>(bufferPointer);
      bufferEnd = bufferStart + bufferLength;
    }
    return *(bufferStart++);
  }

  long RleDecoderV1::readLong() {
    long result = 0;
    int offset = 0;
    signed char ch = readByte();
    if (ch >= 0) {
      result = ch;
    } else {
      result = ch & BASE_128_MASK;
      while ((ch = readByte()) < 0) {
	offset += 7;
	result |= (ch & BASE_128_MASK) << offset;
      }
      result |= ch << (offset + 7);
    }
    return result;
  }

  void RleDecoderV1::skipLongs(long numValues) {
    while (numValues > 0) {
      if (readByte() >= 0) {
	numValues -= 1;
      }
    }
  }

  void RleDecoderV1::readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = - ch;
      repeating = false;
    } else {
      remainingValues = ch + MINIMUM_REPEAT;
      repeating = true;
      delta = readByte();
      if (isSigned) {
	value = unZigZag(readLong());
      } else {
	value = readLong();
      }
    }
  }

  RleDecoderV1::RleDecoderV1(std::unique_ptr<SeekableInputStream> input,
			     bool hasSigned) : isSigned(hasSigned) {
    reset(std::move(input));
  }

  RleDecoderV1::~RleDecoderV1() {
    // PASS
  }
    
  void RleDecoderV1::reset(std::unique_ptr<SeekableInputStream> stream) {
    inputStream = std::move(stream);
    repeating = false;
    remainingValues = 0;
    value = 0;
    delta = 0;
    bufferStart = 0;
    bufferEnd = 0;
  }

  void RleDecoderV1::seek(PositionProvider&) {
    throw new std::string("Not implemented yet!");
  }

  void RleDecoderV1::skip(long numValues) {
    while (numValues > 0) {
      if (remainingValues == 0) {
	readHeader();
      }
      long count = std::min(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      if (repeating) {
	value += delta * count;
      } else {
	skipLongs(count);
      }
    }
  }

  void RleDecoderV1::next(long* data, long numValues, bool* isNull) {
    int position = 0;
    while (position < numValues) {
      // if we are out of values, read more
      if (remainingValues == 0) {
	readHeader();
      }
      // how many do we read out of this block?
      long count = std::min(numValues - position, remainingValues);
      long consumed = 0;
      if (repeating) {
	if (isNull) {
	  for(int i=0; i < count; ++i) {
	    if (!isNull[position + i]) {
	      data[position + i] = value + consumed * delta;
	      consumed += 1;
	    }
	  }
	} else {
	  for(int i=0; i < count; ++i) {
	    data[position + i] = value + i * delta;
	  }
	  consumed = count;
	}
	value += consumed * delta;
      } else {
	if (isNull) {
	  for(int i=0; i < count; ++i) {
	    if (!isNull[i]) {
	      if (isSigned) {
		data[position + i] = unZigZag(readLong());
	      } else {
		data[position + i] = readLong();
	      }
	      consumed += 1;
	    }
	  }
	} else {
	  if (isSigned) {
	    for(int i=0; i < count; ++i) {
	      data[position + i] = unZigZag(readLong());
	    }
	  } else {
	    for(int i=0; i < count; ++i) {
	      data[position + i] = readLong();
	    }
	  }
	  consumed = count;
	}
      }
      remainingValues -= consumed;
      position += count;
    }
  }

  std::unique_ptr<RleDecoder> createRleDecoder
                                  (std::unique_ptr<SeekableInputStream> input, 
				   bool isSigned,
				   RleVersion version) {
    RleDecoder* result;
    if (version == VERSION_1) {
      result = new RleDecoderV1(std::move(input), isSigned);
    } else {
      throw std::string("Not implemented yet");
    }
    return std::unique_ptr<RleDecoder>(result);
  }
}
