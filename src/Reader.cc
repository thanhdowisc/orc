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
#include "TypeImpl.hh"

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace orc {

  struct ReaderOptionsPrivate {
    std::list<int> includedColumns;
    unsigned long dataStart;
    unsigned long dataLength;
    unsigned long tailLocation;
    ReaderOptionsPrivate() {
      includedColumns.push_back(1);
      dataStart = 0;
      dataLength = std::numeric_limits<unsigned long>::max();
      tailLocation = std::numeric_limits<unsigned long>::max();
    }
  };

  ReaderOptions::ReaderOptions(): 
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                  (new ReaderOptionsPrivate())) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs): 
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions&& rhs) {
    privateBits.swap(rhs.privateBits);
  }
  
  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }
  
  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::include(const std::list<int>& include) {
    privateBits->includedColumns.clear();
    for(int columnId: include) {
      privateBits->includedColumns.push_back(columnId);
    }
    return *this;
  }

  ReaderOptions& ReaderOptions::include(std::initializer_list<int> include) {
    privateBits->includedColumns.clear();
    for(int columnId: include) {
      privateBits->includedColumns.push_back(columnId);
    }
    return *this;
  }

  ReaderOptions& ReaderOptions::range(unsigned long offset, 
                                      unsigned long length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
  }

  ReaderOptions& ReaderOptions::setTailLocation(unsigned long offset) {
    privateBits->tailLocation = offset;
    return *this;
  }

  const std::list<int>& ReaderOptions::getInclude() const {
    return privateBits->includedColumns;
  }

  unsigned long ReaderOptions::getOffset() const {
    return privateBits->dataStart;
  }

  unsigned long ReaderOptions::getLength() const {
    return privateBits->dataLength;
  }

  unsigned long ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
  }

  Reader::~Reader() {
    // PASS
  }

  static const unsigned long DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderImpl : public Reader {
  private:
    // inputs
    std::unique_ptr<InputStream> stream;
    ReaderOptions options;
    std::unique_ptr<bool[]> includedColumns;

    // postscript
    proto::PostScript postscript;
    unsigned long blockSize;
    CompressionKind compression;
    unsigned long postscriptLength;

    // footer
    proto::Footer footer;
    proto::Metadata metadata;
    std::unique_ptr<unsigned long[]> firstRowOfStripe;
    std::unique_ptr<Type> schema;

    // reading state
    unsigned long currentStripe;
    unsigned long currentRowInStripe;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    // internal methods
    void readPostscript(char * buffer, unsigned long length);
    void readFooter(char *buffer, unsigned long length,
                    unsigned long fileLength);
    proto::StripeFooter getStripeFooter(const proto::StripeInformation& info);
    void startNextStripe();
    void ensureOrcFooter(char* buffer, unsigned long length);
    void checkOrcVersion();
    void selectTypeParent(int columnId);
    void selectTypeChildren(int columnId);

  public:
    /**
     * Constructor that lets the user specify additional options.
     * @param stream the stream to read from
     * @param options options for reading
     * @throws IOException
     */
    ReaderImpl(std::unique_ptr<InputStream> stream, 
               const ReaderOptions& options);

    CompressionKind getCompression() const override;

    unsigned long getNumberOfRows() const override;

    unsigned long getRowIndexStride() const override;

    const std::string& getStreamName() const override;
    
    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    unsigned long getCompressionSize() const override;

    const std::list<StripeInformation>& getStripes() const override;

    unsigned long getContentLength() const override;

    std::list<ColumnStatistics*> getStatistics() const override;

    const Type& getType() const override;

    bool next(ColumnVectorBatch& data) override;

    unsigned long getRowNumber() const override;

    void seekToRow(unsigned long rowNumber) override;
  };

  InputStream::~InputStream() {
    // PASS
  };

  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                         const ReaderOptions& opts
                         ): stream(std::move(input)), options(opts) {
    // figure out the size of the file using the option or filesystem
    unsigned long size = std::min(options.getTailLocation(), 
                                  static_cast<unsigned long>
                                     (stream->getLength()));

    //read last bytes into buffer to get PostScript
    unsigned long readSize = std::min(size, DIRECTORY_SIZE_GUESS);
    std::unique_ptr<char[]> buffer = 
      std::unique_ptr<char[]>(new char[readSize]);
    stream->read(buffer.get(), size - readSize, readSize);
    readPostscript(buffer.get(), readSize);
    readFooter(buffer.get(), readSize, size);
    currentStripe = 0;
    currentRowInStripe = 0;
    unsigned long rowTotal = 0;
    firstRowOfStripe.reset(new unsigned long[footer.stripes_size()]);
    for(int i=0; i < footer.stripes_size(); ++i) {
      firstRowOfStripe.get()[i] = rowTotal;
      rowTotal += footer.stripes(i).numberofrows();
    }
    for(int columnId: options.getInclude()) {
      selectTypeParent(columnId);
      selectTypeChildren(columnId);
    }
    schema = convertType(footer.types(0), footer);
  }
                         
  CompressionKind ReaderImpl::getCompression() const { 
    return compression;
  }

  unsigned long ReaderImpl::getCompressionSize() const {
    return postscript.compressionblocksize();
  }

  unsigned long ReaderImpl::getNumberOfRows() const { 
    return footer.numberofrows();
  }

  unsigned long ReaderImpl::getContentLength() const {
    return footer.contentlength();
  }

  unsigned long ReaderImpl::getRowIndexStride() const {
    return footer.rowindexstride();
  }

  const std::string& ReaderImpl::getStreamName() const {
    return stream->getName();
  }

  std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for(int i=0; i < footer.metadata_size(); ++i) {
      result.push_back(footer.metadata(i).name());
    }
    return result;
  }

  std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return footer.metadata(i).value();
      }
    }
    throw std::string("key not found");
  }

  bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return true;
      }
    }
    return false;
  }

  void ReaderImpl::selectTypeParent(int columnId) {
    for(int i=0; i < columnId; ++i) {
      for(int child: footer.types(i).subtypes()) {
        if (child == columnId) {
        }
      }
    }
  }

  void ReaderImpl::selectTypeChildren(int columnId) {
    if (!includedColumns->get()[columnId]) {
      includedColumns->get()[columnId] = true;
      for(int child: footer.types(columnId).subtypes()) {
        selectTypeChildren(child);
      }
    }
  }

  void ReaderImpl::ensureOrcFooter(char*, unsigned long) {
    // TODO fix me
  }

  void ReaderImpl::seekToRow(unsigned long) {
    throw std::string("not implemented yet");
  }

  void ReaderImpl::readPostscript(char *buffer, unsigned long readSize) {

    //get length of PostScript
    postscriptLength = buffer[readSize - 1] & 0xff;

    ensureOrcFooter(buffer, readSize);

    //read the PostScript
    std::unique_ptr<SeekableInputStream> pbStream = 
      std::unique_ptr<SeekableInputStream>(new SeekableArrayInputStream
                                           (buffer + readSize - 
                                               (1 + postscriptLength), 
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
    compression = static_cast<CompressionKind>(postscript.compression());
  }

  void ReaderImpl::readFooter(char* buffer, unsigned long readSize,
                              unsigned long fileLength) {
    unsigned long footerSize = postscript.footerlength();
    unsigned long metadataSize = postscript.metadatalength();
    //check if extra bytes need to be read
    unsigned long tailSize = 1 + postscriptLength + footerSize + metadataSize;
    char *footerBuf = buffer;
    if (readSize < tailSize) {
      footerBuf = new char[tailSize];
      // more bytes need to be read, seek back to the right place and read 
      // extra bytes
      stream->read(footerBuf, fileLength - tailSize, tailSize - readSize);
      memcpy(footerBuf + tailSize - readSize, buffer, readSize);
    }
    unsigned long tailStart = readSize - tailSize;
    std::unique_ptr<SeekableInputStream> pbStream = 
      createCodec(compression,
                  std::unique_ptr<SeekableInputStream>
                  (new SeekableArrayInputStream(buffer + tailStart, 
                                                metadataSize)),
                  blockSize);
    if (!metadata.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad metadata parse");
    }

    pbStream =createCodec(compression,
                          std::unique_ptr<SeekableInputStream>
                          (new SeekableArrayInputStream(buffer +
                                                          tailStart +
                                                          metadataSize,
                                                        footerSize)),
                          blockSize);
    if (!footer.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad footer parse");
    }
  }

  proto::StripeFooter ReaderImpl::getStripeFooter
                        (const proto::StripeInformation& info) {
    unsigned long footerStart = info.offset() + info.indexlength() +
      info.datalength();
    unsigned long footerLength = info.footerlength();
    std::unique_ptr<SeekableInputStream> pbStream = 
      createCodec(compression,
                  std::unique_ptr<SeekableInputStream>
                  (new SeekableFileInputStream(stream.get(), footerStart,
                                               footerLength, 
                                               static_cast<long>(blockSize+3)
                                               )),
                  blockSize);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
      throw std::string("bad parse");
    }
    return result;
  }

  void ReaderImpl::startNextStripe() {
    currentStripeInfo = footer.stripes(static_cast<int>(currentStripe));
    currentStripeFooter = getStripeFooter(currentStripeInformation);

  }

  void ReaderImpl::checkOrcVersion() {
    // TODO
  }

  bool ReaderImpl::next(ColumnVectorBatch& data) {
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream, 
                                       const ReaderOptions& options) {
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(stream), options));
  }
}
