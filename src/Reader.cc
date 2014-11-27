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

#include "orc/Reader.hh"
#include "orc/OrcFile.hh"
#include "ColumnReader.hh"
#include "RLE.hh"

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace orc {

  Reader::~Reader() {
    // PASS
  }

  Type parseType(const proto::Footer& footer) {
  }

  static const int DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderImpl : public Reader {
  private:
    // inputs
    std::unique_ptr<InputStream> stream;
    ReaderOptions options;

    // postscript
    proto::Postscript postscript;
    long blockSize;
    CompressionKind compression;
    long postscriptLength;

    // footer
    proto::Footer footer;
    proto::Metadata metadata;
    std::unique_ptr<unsigned long[]> firstRowOfStripe;
    Type schema;

    // reading state
    long currentStripe;
    long currentRowInStripe;
    std::unique_ptr<ColumnReader> reader;

    void readPostscript(char * buffer, int length);
    void readFooter(char *buffer, int length);
    proto::StripeFooter getStripeFooter(long stripe);
    void startNextStripe();
    void ensureOrcFooter(char* buffer, int length);
    void checkOrcVersion();

  public:
    /**
     * Constructor that lets the user specify additional options.
     * @param stream the stream to read from
     * @param options options for reading
     * @throws IOException
     */
    ReaderImpl(std::unique_ptr<InputStream> stream, 
               const ReaderOptions& options);

    void close() override;

    int getCompression() const override;

    long getNumberOfRows() const override;

    int getRowStride() const override;

    std::string getStreamName() const override;
  };

  InputStream::~InputStream() {
    // PASS
  };

  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                         const ReaderOptions& opts
                         ): stream(input), options(opts) {
    // figure out the size of the file using the option or filesystem
    long size = std::min(options.getTailLocation(), 
                         stream->getLength());

    //read last bytes into buffer to get PostScript
    int readSize = (int) std::min((int)size, DIRECTORY_SIZE_GUESS);
    std::unique_ptr<char[]> buffer = new char[readSize];
    stream->read(buffer.get(), size - readSize, readSize);
    parsePostscript(buffer.get(), readSize);
    parseFooter(buffer.get(), readSize);
    currentStripe = 0;
    currentRowInStripe = 0;
    long rowTotal = 0;
    firstRowOfStripe.reset(new unsigned long[footer.stripes_size()]);
    for(int i=0; i < footer.stripes_size(); ++i) {
      firstRowOfStripe.get()[i] = rowTotal;
      rowTotal += footer.stripes(i).numberOfRows();
    }
    schema = parseType(footer);
  }
                         
  void ReaderImpl::close() {
    // TODO
  }

  CompressionKind ReaderImpl::getCompression() const { 
    return compression;
  }

  long ReaderImpl::getNumberOfRows() const { 
    return footer.numberofrows();
  }

  int ReaderImpl::getRowStride() const {
    return footer.rowindexstride();
  }

  std::string ReaderImpl::getStreamName() const {
    return stream->getName();
  }

  void ReaderImpl::readPostscript(char *buffer, int readSize) {

    //get length of PostScript
    postscriptLength = buffer[readSize - 1] & 0xff;

    ensureOrcFooter(buffer, readSize);

    //read the PostScript
    std::unique_ptr<ZeroCopyInputStream> pbStream = 
      std::unique_ptr(new SeekableArrayInputStream
                      (buffer + readSize - (1 + postscriptLength), 
                       postscriptLength));
    if (!postscript.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad postscript parse");
    }
    if (postscript.has_compressionblocksize()) {
      blockSize = postscript.compressionblocksize();
    } else {
      blockSize = 256 * 1024;
    }

    checkOrcVersion();

    //check compression codec
    switch (postscript.compression()) {
    case proto::NONE:
      compression = NONE;
      break;
    case proto::ZLIB:
      compression = ZLIB;
      break;
    case proto::SNAPPY:
      compression = SNAPPY;
      break;
    case proto::LZO:
      compression = LZO;
      break;
    default:
      throw std::invalid_argument("Unknown compression");
    }
  }

  void ReaderImpl::readFooter(char* buffer, int readSize,
                              unsigned long fileLength) {
    int footerSize = postscript.footerlength();
    int metadataSize = postscript.metadatalength();
    //check if extra bytes need to be read
    int tailSize = 1 + postscriptLength + footerSize + metadataSize;
    char *footerBuf = buffer;
    if (length < tailSize) {
      footerBuf = new char[tailSize];
      // more bytes need to be read, seek back to the right place and read 
      // extra bytes
      stream->read(footerBuf, fileLength - tailSize, tailSize - readSize);
      memcpy(footerBuf + tailSize - readSize, buffer, readSize);
    }
    int tailStart = readSize - tailSize;

    pbStream.reset(createCodec(compression,
                               std::unique_ptr<SeekableInputStream>
                                 (new SeekableArrayInputStream(buffer.get(),
                                                               tailStart, 
                                                               metadataSize)),
                               blockSize));
    if (!metadata.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad metadata parse");
    }

    pbStream.reset(createCodec(compression,
                               std::unique_ptr<SeekableInputStream>
                                 (new SeekableArrayInputStream(buffer.get(),
                                                               tailStart +
                                                                 metadataSize,
                                                               footerSize)),
                               blockSize));
    if (!footer.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad footer parse");
    }
  }

  proto::StripeFooter ReaderImpl::getStripeFooter(long stripeIx) {
    // TODO
  }

  void ReaderImpl::startNextStripe() {
    // TODO
  }

  void ReaderImpl::checkOrcVersion() {
    // TODO
  }

  std::unique_ptr<Reader> createReader(InputStream* stream, 
                                       const ReaderOptions& options) {
    return std::unique_ptr<Reader>(new ReaderImpl(stream, options));
  }
}
