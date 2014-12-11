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
#include "DataReader.hh"
#include "RLE.hh"
#include "orc_proto.pb.h"

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <fstream>
#include <limits>
#include <algorithm>

namespace orc {

    Reader::~Reader() {
        // PASS
    }

    class FileMetaInfo {
    public:
        proto::PostScript postscript ;
        proto::Footer footer ;
        proto::Metadata metadata ;
        // const CompressionCodec codec ;

        FileMetaInfo() {};

        FileMetaInfo(proto::PostScript &postscript, proto::Footer &footer, proto::Metadata &metadata):
            postscript(postscript), footer(footer), metadata(metadata) {};
    };


    class ReaderImpl : public Reader {
    private:
//        static const int DIRECTORY_SIZE_GUESS = 16 * 1024;
        int DIRECTORY_SIZE_GUESS;
        InputStream* stream;
//        const CompressionCodec codec;

        FileMetaInfo fileMetaInfo ;
        long totalRowIx;
        long totalRows;
        long stripeRowIx;
        long stripeRows;
        int stripeIx;
        int nColumns;
        std::vector<long> firstRowOfStripe ;
        std::vector<DataReader*> columnReaders ;
        std::vector<orc::proto::ColumnEncoding_Kind> columnEncodings ;

        /**
        * Build a version string out of an array.
        * @param version the version number as a list
        * @return the human readable form of the version string
        */
        static std::string versionString(std::vector<int> version) {
            std::string buffer;
            for(unsigned int i=0; i < version.size(); ++i) {
                if (i != 0) {
                    buffer.append(".");
                };
                buffer.append(std::to_string(version[i]));
            }
            return buffer;
        }

        void extractMetaInfoFromFooter(InputStream* stream, long maxFileLength) {

            // figure out the size of the file using the option or filesystem
            long size;
            if (maxFileLength == std::numeric_limits<long>::max())
                size =stream->getLength();
            else
                size = maxFileLength;

            //read last bytes into buffer to get PostScript
            int readSize = (int) std::min((int)size, DIRECTORY_SIZE_GUESS);
            ByteRange buffer;
            buffer.data = new char[readSize];
            buffer.length = readSize ;
            stream->read(buffer.data, size - readSize, buffer.length);

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.data[readSize - 1] & 0xff;

            // ensureOrcFooter(file, path, psLen, buffer);
            int psOffset = readSize - 1 - psLen;
            fileMetaInfo.postscript.ParseFromArray(buffer.data+psOffset, psLen);

            // checkOrcVersion(LOG, path, ps.getVersionList());

            int footerSize = (int) this->fileMetaInfo.postscript.footerlength();
            int metadataSize = (int) this->fileMetaInfo.postscript.metadatalength();

            //check compression codec
            switch (this->fileMetaInfo.postscript.compression()) {
              case proto::NONE:
                  break;
              case proto::ZLIB:
//                  break;
              case proto::SNAPPY:
//                  break;
              case proto::LZO:
//                  break;
              default:
                  throw std::invalid_argument("Unknown compression");
            };

            //check if extra bytes need to be read
            int extra = std::max(0, psLen + 1 + footerSize + metadataSize - readSize);
            if (extra > 0) {
                //more bytes need to be read, seek back to the right place and read extra bytes
                char* extraBuf = new char[readSize+extra];
                stream->read(extraBuf, size - readSize - extra, extra);
                //append with already read bytes
                memcpy(extraBuf+extra, buffer.data, buffer.length);
                delete[] buffer.data;
                buffer.data = extraBuf;
                buffer.length = extra + readSize;
            };

            this->fileMetaInfo.metadata.ParseFromArray(buffer.data, metadataSize);
            this->fileMetaInfo.footer.ParseFromArray(buffer.data+metadataSize, footerSize);

            delete[] buffer.data;
        };

        void loadStripe(long stripeIx) {
            orc::proto::StripeInformation stripeInfo = fileMetaInfo.footer.stripes(stripeIx);
            long stripeLength = stripeInfo.indexlength() + stripeInfo.datalength() + stripeInfo.footerlength();
            unsigned char* stripe = new unsigned char[stripeLength];
            stream->read(stripe, stripeInfo.offset(), stripeLength);

            // Extract stream info from the stripe footer
            orc::proto::StripeFooter stripeFooter ;
            stripeFooter.ParseFromArray(stripe + stripeInfo.indexlength() + stripeInfo.datalength(), stripeInfo.footerlength());

            // Cut the stripe into chunks and assign to data parsers
            // TODO: We only read DATA streams now; need to read ROW_INDEX, etc., too
            orc::proto::Stream streamInfo;
            long stripeStart = stripeInfo.offset();
            long streamStart = stripeStart;
            long streamLength;
            int columnIx;

            // First, reset all column readers with new encodings.
            // Each column reader should clear its streams and reset the encoding for the current stripe
            for (int columnIx=1; columnIx < stripeFooter.columns_size(); columnIx++)
                columnReaders[columnIx-1]->reset(stripeFooter.columns(columnIx));

            for (int streamIx=0; streamIx<stripeFooter.streams_size(); streamIx++) {
                streamInfo = stripeFooter.streams(streamIx);
                streamLength = streamInfo.length();
                columnIx = streamInfo.column();
                if (columnIx > 0 ) {
                    columnReaders[columnIx-1]->resetStream(stripe+(streamStart-stripeStart), streamLength,streamInfo.kind());
                }
                streamStart += streamLength;
            }

            delete stripe ;

            stripeRows = stripeInfo.numberofrows() ;
            stripeRowIx = 0;
            totalRowIx = firstRowOfStripe[stripeIx]+stripeRowIx;
        }


    public:
        /**
        * Constructor that let's the user specify additional options.
        * @param path pathname for file
        * @param options options for reading
        * @throws IOException
        */
        ReaderImpl(InputStream* stream, ReaderOptions& options) {
            DIRECTORY_SIZE_GUESS = 16 * 1024;

            this->stream = stream;
            extractMetaInfoFromFooter(stream, options.getMaxLength());

            totalRows = fileMetaInfo.footer.numberofrows();

           // Pre-calculate row offset for each stripe
           int firstRow = 0;
           for (int i=0; i<fileMetaInfo.footer.stripes_size(); i++) {
               firstRowOfStripe.push_back(firstRow);
               firstRow +=fileMetaInfo.footer.stripes(i).numberofrows() ;
           };

//           // Initialize column readers
           nColumns = fileMetaInfo.footer.types_size()-1;   // Skip the first column (index info)
           columnReaders.clear();
           columnReaders.reserve(nColumns);
           for (int i=0; i<nColumns; i++) {
               switch (fileMetaInfo.footer.types(i+1).kind()) {
               case orc::proto::Type_Kind_INT:
                   columnReaders.push_back(createIntegerReader());
                   break;
               case orc::proto::Type_Kind_STRING:
                   columnReaders.push_back(createStringReader());
                   break;
               default:
                   columnReaders.push_back(createDummyReader());
               }
           }

           stripeIx = 0;
           loadStripe(stripeIx);
        }

        bool hasNext() const override { return totalRowIx<totalRows; }

        std::vector<boost::any> next() override {
            // Check if the next stripe is required
            if ( hasNext() && stripeRowIx == stripeRows ) {
                stripeIx++;
                loadStripe(stripeIx);
            }

            std::vector<boost::any> row;
            for (int columnIx=0; columnIx < nColumns; columnIx++) {
                row.push_back(columnReaders[columnIx]->next());
            }

            stripeRowIx++;
            totalRowIx++;

            return row;
        }

        long getRowNumber() override { return totalRowIx; }

        float getProgress() override {   return (float)totalRowIx/totalRows ; }

        void close() override {
            for(unsigned int i=0; i<columnReaders.size();i++)
                delete columnReaders[i];
        }


       int getCompression() const { return (int)(fileMetaInfo.postscript.compression()) ; }

       long getNumberOfRows() const { return fileMetaInfo.footer.numberofrows() ; }

       int getRowStride() const { return fileMetaInfo.footer.rowindexstride() ; }

       std::string getStreamName() const { return stream->getName() ; }

       int getStreamSize() const { return stream->getLength() ; }

//        FileMetaInfo getFileMetaInfo() { return FileMetaInfo(compressionKind, bufferSize, metadataSize, footerByteBuffer, versionList); }
//
    };

    InputStream::~InputStream() {
        // PASS
    };

    class FileInputStream : public InputStream {
    private:
        std::string filename ;
        std::ifstream file;
        long long length;
        std::string name ;

    public:
        FileInputStream(std::string filename, std::string name="") {
            this->filename = filename ;
            file.open(filename.c_str(), std::ios::in | std::ios::binary);
            // TODO: Can we get the file size from the filesystem?
            file.seekg(0,file.end);
            length = file.tellg();
            this->name = (name.compare("")==0) ? filename : name ;
        }

        ~FileInputStream() { file.close(); }

        long getLength() const { return this->length; }

        void read(void* buffer, long offset, long length) {
            file.seekg(offset);
            file.read((char*)buffer, length);
        }

        const std::string& getName() const { return this->name; }
    };

    InputStream* readLocalFile(const std::string& path) {
        return new FileInputStream(path);
    }

    Reader* createReader(InputStream* stream) {
        orc::ReaderOptions opts;
        return new ReaderImpl(stream, opts);
    }
}
