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

  const unsigned long MINIMUM_REPEAT = 3;
  const unsigned long BASE_128_MASK = 0x7f;

  inline long unZigZag(unsigned long value) {
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
    virtual void skip(unsigned long numValues);

    /**
     * Read a number of values into the batch.
     */
    virtual void next(long* data, unsigned long numValues, bool* isNull);

  private:
    inline signed char readByte();
    inline void readHeader();
    inline unsigned long readLong();
    inline void skipLongs(unsigned long numValues);

    std::unique_ptr<SeekableInputStream> inputStream;
    unsigned long remainingValues;
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

  unsigned long RleDecoderV1::readLong() {
    unsigned long result = 0;
    int offset = 0;
    signed char ch = readByte();
    if (ch >= 0) {
      result = static_cast<unsigned long>(ch);
    } else {
      result = static_cast<unsigned long>(ch) & BASE_128_MASK;
      while ((ch = readByte()) < 0) {
	offset += 7;
	result |= (static_cast<unsigned long>(ch) & BASE_128_MASK) << offset;
      }
      result |= static_cast<unsigned long>(ch) << (offset + 7);
    }
    return result;
  }

  void RleDecoderV1::skipLongs(unsigned long numValues) {
    while (numValues > 0) {
      if (readByte() >= 0) {
	numValues -= 1;
      }
    }
  }

  void RleDecoderV1::readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = static_cast<unsigned long>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<unsigned long>(ch) + MINIMUM_REPEAT;
      repeating = true;
      delta = readByte();
      if (isSigned) {
	value = unZigZag(readLong());
      } else {
	value = static_cast<long>(readLong());
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

  void RleDecoderV1::seek(PositionProvider& location) {
    // move the input stream
    inputStream->seek(location);
    // force a re-read from the stream
    bufferEnd = bufferStart;
    // read a new header
    readHeader();
    // skip ahead the given number of records
    skip(location.next());
  }

  void RleDecoderV1::skip(unsigned long numValues) {
    while (numValues > 0) {
      if (remainingValues == 0) {
	readHeader();
      }
      unsigned long count = std::min(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      if (repeating) {
	value += delta * static_cast<long>(count);
      } else {
	skipLongs(count);
      }
    }
  }

  void RleDecoderV1::next(long* data, unsigned long numValues, bool* isNull) {
    unsigned long position = 0;
    while (position < numValues) {
      // if we are out of values, read more
      if (remainingValues == 0) {
	readHeader();
      }
      // how many do we read out of this block?
      unsigned long count = std::min(numValues - position, remainingValues);
      unsigned long consumed = 0;
      if (repeating) {
	if (isNull) {
	  for(unsigned long i=0; i < count; ++i) {
	    if (!isNull[position + i]) {
	      data[position + i] = value + static_cast<long>(consumed) * delta;
	      consumed += 1;
	    }
	  }
	} else {
	  for(unsigned long i=0; i < count; ++i) {
	    data[position + i] = value + static_cast<long>(i) * delta;
	  }
	  consumed = count;
	}
	value += static_cast<long>(consumed) * delta;
      } else {
	if (isNull) {
	  for(unsigned long i=0; i < count; ++i) {
	    if (!isNull[i]) {
	      if (isSigned) {
		data[position + i] = unZigZag(readLong());
	      } else {
		data[position + i] = static_cast<long>(readLong());
	      }
	      consumed += 1;
	    }
	  }
	} else {
	  if (isSigned) {
	    for(unsigned long i=0; i < count; ++i) {
	      data[position + i] = unZigZag(readLong());
	    }
	  } else {
	    for(unsigned long i=0; i < count; ++i) {
	      data[position + i] = static_cast<long>(readLong());
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
