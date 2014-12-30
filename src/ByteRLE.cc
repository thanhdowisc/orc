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
#include "Exceptions.hh"

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
    virtual void next(char* data, unsigned long numValues, char* notNull);

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
      throw ParseError("bad read in nextBuffer");
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
      size_t count = std::min(numValues, (unsigned long)remainingValues);
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
          unsigned long skipSize = std::min((unsigned long)consumedBytes,
                          static_cast<unsigned long>(bufferEnd - bufferStart));
          bufferStart += skipSize;
          consumedBytes -= skipSize;
        }
      }
    }
  }

  void ByteRleDecoderImpl::next(char* data, unsigned long numValues,
                                char* notNull) {
    unsigned long position = 0;
    // skip over null values
    while (notNull && position < numValues && !notNull[position]) {
      position += 1;
    }
    while (position < numValues) {
      // if we are out of values, read more
      if (remainingValues == 0) {
        readHeader();
      }
      // how many do we read out of this block?
      unsigned long count = std::min(numValues - position,
		  (unsigned long) remainingValues);
      unsigned long consumed = 0;
      if (repeating) {
        if (notNull) {
          for(unsigned long i=0; i < count; ++i) {
            if (notNull[position + i]) {
              data[position + i] = value;
              consumed += 1;
            }
          }
        } else {
          memset(data + position, value, count);
          consumed = count;
        }
      } else {
        if (notNull) {
          for(unsigned long i=0; i < count; ++i) {
            if (notNull[i]) {
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
      while (notNull && position < numValues && !notNull[position]) {
        position += 1;
      }
    }
  }

  std::unique_ptr<ByteRleDecoder> createByteRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input) {
    return std::unique_ptr<ByteRleDecoder>
      (new ByteRleDecoderImpl(std::move(input)));
  }

  class BooleanRleDecoderImpl: public ByteRleDecoderImpl {
  public:
    BooleanRleDecoderImpl(std::unique_ptr<SeekableInputStream> input);

    virtual ~BooleanRleDecoderImpl();

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
    virtual void next(char* data, unsigned long numValues, char* notNull);

  protected:
    size_t remainingBits;
    char lastByte;
  };

  BooleanRleDecoderImpl::BooleanRleDecoderImpl
                                (std::unique_ptr<SeekableInputStream> input
                                 ): ByteRleDecoderImpl(std::move(input)) {
    remainingBits = 0;
    lastByte = 0;
  }

  BooleanRleDecoderImpl::~BooleanRleDecoderImpl() {
    // PASS
  }

  void BooleanRleDecoderImpl::seek(PositionProvider& location) {
    ByteRleDecoderImpl::seek(location);
    unsigned long consumed = location.next();
    if (consumed > 8) {
      throw ParseError("bad position");
    }
    if (consumed != 0) {
      remainingBits = 8 - consumed;
      ByteRleDecoderImpl::next(&lastByte, 1, 0);
    }
  }

  void BooleanRleDecoderImpl::skip(unsigned long numValues) {
    if (numValues <= remainingBits) {
      remainingBits -= numValues;
    } else {
      numValues -= remainingBits;
      unsigned long bytesSkipped = numValues / 8;
      ByteRleDecoderImpl::skip(bytesSkipped);
      ByteRleDecoderImpl::next(&lastByte, 1, 0);
      remainingBits = 8 - (numValues % 8);
    }
  }

  void BooleanRleDecoderImpl::next(char* data, unsigned long numValues,
                                   char* notNull) {
    // next spot to fill in
    unsigned long position = 0;

    // use up any remaining bits
    if (notNull) {
      while(remainingBits > 0 && position < numValues) {
        if (notNull[position]) {
          remainingBits -= 1;
          data[position] = (static_cast<unsigned char>(lastByte) >>
                            remainingBits) & 0x1;
        } else {
          data[position] = 0;
        }
        position += 1;
      }
    } else {
      while(remainingBits > 0 && position < numValues) {
        remainingBits -= 1;
        data[position++] = (static_cast<unsigned char>(lastByte) >>
                            remainingBits) & 0x1;
      }
    }

    // count the number of nonNulls remaining
    unsigned long nonNulls = numValues - position;
    if (notNull) {
      for(unsigned long i=position; i < numValues; ++i) {
        if (!notNull[i]) {
          nonNulls -= 1;
        }
      }
    }

    // fill in the remaining values
    if (nonNulls == 0) {
      while (position < numValues) {
	data[position++] = 0;
      }
    } else if (position < numValues) {
      // read the new bytes into the array
      unsigned long bytesRead = (nonNulls + 7) / 8;
      ByteRleDecoderImpl::next(data + position, bytesRead, 0);
      lastByte = data[position + bytesRead - 1];
      remainingBits = bytesRead * 8 - nonNulls;
      // expand the array backwards so that we don't clobber the data
      unsigned long bitsLeft = bytesRead * 8 - remainingBits;
      if (notNull) {
        for(long i=static_cast<long>(numValues) - 1;
            i >= static_cast<long>(position); --i) {
          if (notNull[i]) {
            unsigned long shiftPosn = (-bitsLeft) % 8;
            data[i] = (data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
            bitsLeft -= 1;
          } else {
            data[i] = 0;
          }
        }
      } else {
        for(long i=static_cast<long>(numValues) - 1;
            i >= static_cast<long>(position); --i, --bitsLeft) {
          unsigned long shiftPosn = (-bitsLeft) % 8;
          data[i] = (data[position + (bitsLeft - 1) / 8] >> shiftPosn) & 0x1;
        }
      }
    }
  }

  std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input) {
    return std::unique_ptr<BooleanRleDecoderImpl>
      (new BooleanRleDecoderImpl(std::move(input)));
  }

}
