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
#include <string.h>
#include <utility>

#include "ByteRLE.hh"

namespace orc {

  const size_t MINIMUM_REPEAT = 3;
  
  ByteRleDecoder::~ByteRleDecoder() {
    // PASS
  }

  class ByteRleDecoderImpl: public ByteRleDecoder {
  public:
    ByteRleDecoderImpl(std::unique_ptr<SeekableInputStream> input);

    virtual ~ByteRleDecoderImpl();
    
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
    virtual void next(char* data, unsigned long numValues, char* isNull);

  protected:
    inline void nextBuffer();
    inline signed char readByte();
    inline void readHeader();

    std::unique_ptr<SeekableInputStream> inputStream;
    size_t remainingValues;
    char value;
    const char* bufferStart;
    const char* bufferEnd;
    bool repeating;
  };

  void ByteRleDecoderImpl::nextBuffer() {
    int bufferLength;
    const void* bufferPointer;
    bool result = inputStream->Next(&bufferPointer, &bufferLength);
    if (!result) {
      throw std::string("bad read in nextBuffer");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  signed char ByteRleDecoderImpl::readByte() {
    if (bufferStart == bufferEnd) {
      nextBuffer();
    }
    return *(bufferStart++);
  }

  void ByteRleDecoderImpl::readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = static_cast<size_t>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<size_t>(ch) + MINIMUM_REPEAT;
      repeating = true;
      value = readByte();
    }
  }

  ByteRleDecoderImpl::ByteRleDecoderImpl(std::unique_ptr<SeekableInputStream> 
					 input) {
    inputStream = std::move(input);
    repeating = false;
    remainingValues = 0;
    value = 0;
    bufferStart = 0;
    bufferEnd = 0;
  }

  ByteRleDecoderImpl::~ByteRleDecoderImpl() {
    // PASS
  }
    
  void ByteRleDecoderImpl::seek(PositionProvider& location) {
    // move the input stream
    inputStream->seek(location);
    // force a re-read from the stream
    bufferEnd = bufferStart;
    // read a new header
    readHeader();
    // skip ahead the given number of records
    skip(location.next());
  }

  void ByteRleDecoderImpl::skip(unsigned long numValues) {
    while (numValues > 0) {
      if (remainingValues == 0) {
	readHeader();
      }
      size_t count = std::min(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      // for literals we need to skip over count bytes, which may involve 
      // reading from the underlying stream
      if (!repeating) {
	size_t consumedBytes = count;
	while (consumedBytes > 0) {
	  if (bufferStart == bufferEnd) {
	    nextBuffer();
	  }
	  unsigned long skipSize = std::min(consumedBytes, 
                          static_cast<unsigned long>(bufferEnd - bufferStart));
	  bufferStart += skipSize;
	  consumedBytes -= skipSize;
	}
      }
    }
  }

  void ByteRleDecoderImpl::next(char* data, unsigned long numValues, 
				char* isNull) {
    unsigned long position = 0;
    // skip over null values
    while (isNull && isNull[position]) {
      position += 1;
    }
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
	      data[position + i] = value;
	      consumed += 1;
	    }
	  }
	} else {
	  memset(data + position, value, count);
	  consumed = count;
	}
      } else {
	if (isNull) {
	  for(unsigned long i=0; i < count; ++i) {
	    if (!isNull[i]) {
	      data[position + i] = readByte();
	      consumed += 1;
	    }
	  }
	} else {
	  unsigned long i = 0;
	  while (i < count) {
	    if (bufferStart == bufferEnd) {
	      nextBuffer();
	    }
	    unsigned long copyBytes = std::min(count, 
		       static_cast<unsigned long>(bufferEnd - bufferStart));
	    memcpy(data + position + i, bufferStart, copyBytes);
	    bufferStart += copyBytes;
	    i += copyBytes;
	  }
	  consumed = count;
	}
      }
      remainingValues -= consumed;
      position += count;
      // skip over any null values
      while (isNull && isNull[position]) {
	position += 1;
      }
    }
  }

  class BooleanRleDecoderImpl: public ByteRleDecoder {
  private:
      std::unique_ptr<ByteRleDecoderImpl> rle ;
      char currentByte;
      const std::vector<char> masks {128,64,32,16,8,4,2,1} ;
      unsigned char maskIx ;

      void readNextByte() {
          char value;
          rle->next(&value, 1, 0);
          maskIx = 0;
      }

  public:
    ~BooleanRleDecoderImpl() {
        //PASS
    }

    BooleanRleDecoderImpl(std::unique_ptr<SeekableInputStream> input) {
        rle = createByteRleDecoder(std::move(std::unique_ptr<SeekableInputStream> (input)));
        currentByte = 0;
        maskIx = 8;
    }

    void seek(PositionProvider&) {
        // PASS
    }

    void skip(unsigned long numValues) {
        // PASS
    }

    void next(char* data, unsigned long numValues, char* isNull) {
        // TODO: check if we cannot read all numValues
        for (unsigned long i=0; i < numValues; i++) {
            if (maskIx > 7 || maskIx < 0)
                readNextByte();
            data[i] = (char)(currentByte && masks[maskIx]);
            maskIx++ ;
        }
    }
  };



  std::unique_ptr<ByteRleDecoder> createByteRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input) {
    return std::unique_ptr<ByteRleDecoder>
      (new ByteRleDecoderImpl(std::move(input)));
  }

  std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input) {
    return std::unique_ptr<ByteRleDecoder>
      (new BooleanRleDecoderImpl(std::move(input)));
  }
}
