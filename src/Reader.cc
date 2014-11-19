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
#include "orc_proto.pb.h"

#include <vector>
#include <string>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <limits>
#include <algorithm>

namespace orc {

    Reader::~Reader() {
        // PASS
    }

    class ReaderImpl : public Reader {
    private:
        class FileMetaInfo {
        public:
          proto::PostScript postscript ;
          proto::Footer footer ;
          proto::Metadata metadata ;
//          const CompressionCodec codec ;

          FileMetaInfo() {};

          FileMetaInfo(proto::PostScript &postscript, proto::Footer &footer, proto::Metadata &metadata):
              postscript(postscript), footer(footer), metadata(metadata) {};
        };

//        static const Log LOG = LogFactory.getLog(ReaderImpl.class);
        static const int DIRECTORY_SIZE_GUESS = 16 * 1024;
        InputStream* stream;
//        const CompressionCodec codec;

        FileMetaInfo fileMetaInfo ;

//        long deserializedSize = -1;
//
//        //serialized footer - Keeping this around for use by getFileMetaInfo()
//        // will help avoid cpu cycles spend in deserializing at cost of increased
//        // memory footprint.
//        const ByteRange footerByteBuffer;

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

//        class StripeInformationImpl: public StripeInformation {
//        private:
//            StripeInformation stripe;
//
//        public:
//            StripeInformationImpl(StripeInformation& stripe) { this->stripe = stripe; }
//
//            long offset() override { return stripe.offset(); }
//            long datalength() override { return stripe.datalength(); }
//            long footerlength() override { return stripe.footerlength(); }
//            long indexlength() override { return stripe.indexlength(); }
//            long length() override { return stripe.datalength() + indexlength() + footerlength(); }
//            long numberofrows() override { return stripe.numberofrows(); }
//            std::string toString() override {
//                return "offset: " + std::to_string(offset()) + " data: " + std::to_string(datalength()) +
//                        " rows: " + std::to_string(numberofrows()) + " tail: " + std::to_string(footerlength()) +
//                        " index: " + std::to_string(indexlength());
//            }
//        };

//        /**
//         * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl
//         *  from serialized fields.
//         * As the fields are final, the fields need to be initialized in the constructor and
//         *  can't be done in some helper function. So this helper class is used instead.
//         *
//         */
//        class MetaInfoObjExtractor {
//            const CompressionKind compressionKind;
//            // const CompressionCodec codec;
//            const int bufferSize;
//            const int metadataSize;
//            const Metadata metadata;
//            const Footer footer;
//            // const ObjectInspector inspector;
//
//            MetaInfoObjExtractor(std::string codecStr, int bufferSize, int metadataSize, ByteBuffer footerBuffer) {
//                this->compressionKind = codecStr;
//                this->bufferSize = bufferSize;
//                // this->codec = WriterImpl.createCodec(compressionKind);
//                this->metadataSize = metadataSize;
//
//                int position = footerBuffer.position();
//                int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;
//                footerBuffer.limit(position + metadataSize);
//
//                InputStream instream = InStream.create("metadata", new ByteBuffer[]{footerBuffer},
//                        new long[]{0L}, metadataSize, codec, bufferSize);
//                this->metadata = Metadata.parseFrom(instream);
//
//                footerBuffer.position(position + metadataSize);
//                footerBuffer.limit(position + metadataSize + footerBufferSize);
//                instream = InStream.create("footer", new ByteBuffer[]{footerBuffer},
//                        new long[]{0L}, footerBufferSize, codec, bufferSize);
//                this->footer = Footer.parseFrom(instream);
//
//                footerBuffer.position(position);
//                this->inspector = OrcStruct.createObjectInspector(0, footer.getTypesList());
//            }
//        };

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
            stream->read(buffer.data, size - readSize, buffer.length);

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.data[readSize - 1] & 0xff;
            // ensureOrcFooter(file, path, psLen, buffer);
            int psOffset = readSize - 1 - psLen;
            this->fileMetaInfo.postscript.ParseFromArray(buffer.data+psOffset, psLen);

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

//        long getRawDataSizeFromColIndices(std::vector<int> colIndices) {
//            long result = 0;
//            for (int colIdx : colIndices) {
//                result += getRawDataSizeOfColumn(colIdx);
//            }
//            return result;
//        }
//
//        long getRawDataSizeOfColumn(int colIdx) {
//            ColumnStatistics colStat = footer.statistics(colIdx);
//            long numVals = colStat.numberofvalues();
//            Type type = footer.types(colIdx);
//
//            switch (type.kind()) {
//            case BINARY:
//                // old orc format doesn't support binary statistics. checking for binary
//                // statistics is not required as protocol buffers takes care of it.
//                return colStat.binarystatistics().sum();
//            /*
//            case STRING:
//            case CHAR:
//            case VARCHAR:
//                // old orc format doesn't support sum for string statistics. checking for
//                // existence is not required as protocol buffers takes care of it.
//
//                // ORC strings are deserialized to java strings. so use java data model's
//                // string size
//                numVals = numVals == 0 ? 1 : numVals;
//                int avgStrLen = (int) (colStat.stringstatistics().sum() / numVals);
//                return numVals * JavaDataModel.get().lengthForStringOfLength(avgStrLen);
//            case TIMESTAMP:
//                return numVals * JavaDataModel.get().lengthOfTimestamp();
//            case DATE:
//                return numVals * JavaDataModel.get().lengthOfDate();
//            case DECIMAL:
//                return numVals * JavaDataModel.get().lengthOfDecimal();
//            case DOUBLE:
//            case LONG:
//                return numVals * JavaDataModel.get().primitive2();
//            case FLOAT:
//            case INT:
//            case SHORT:
//            case BOOLEAN:
//            case BYTE:
//                return numVals * JavaDataModel.get().primitive1();
//            */
//            default:
//                //LOG.debug("Unknown primitive category.");
//            break;
//            }
//            return 0;
//        }
//
//        std::vector<int> getColumnIndicesFromNames(std::vector<std::string> colNames) {
//            // top level struct
//            Type type = footer.types(0);
//            std::vector<int> colIndices ;
//            std::vector<std::string> fieldNames = type.fieldnames();
//            int fieldIdx = 0;
//            for (std::string colName : colNames) {
//                // check if fieldName contains colName
//                int fieldIx = 0;
//                while (fieldIx < fieldNames.size() && fieldNames[fieldIx].compare(colName)!=0)
//                    fieldIx++;
//                if (fieldIx == fieldNames.size()) // haven't found
//                    fieldIx = 0;
//
//                // a single field may span multiple columns. find start and end column
//                // index for the requested field
//                int idxStart = type.subtypes(fieldIdx);
//
//                int idxEnd;
//                // if the specified is the last field and then end index will be last column index
//                if (fieldIdx + 1 > fieldNames.size() - 1) {
//                    idxEnd = getLastIdx() + 1;
//                } else {
//                    idxEnd = type.subtypes(fieldIdx + 1);
//                };
//
//                // if start index and end index are same then the field is a primitive
//                // field else complex field (like map, list, struct, union)
//                if (idxStart == idxEnd) {
//                    // simple field
//                    colIndices.push_back(idxStart);
//                } else {
//                    // complex fields spans multiple columns
//                    for (int i = idxStart; i < idxEnd; i++) {
//                        colIndices.push_back(i);
//                    }
//                }
//            };
//            return colIndices;
//        }
//
//        int getLastIdx() {
//            std::list<int> indices;
//            for (Type type : footer.types()) {
//                for (int subtype: type.subtypes())
//                    indices.push_back(subtype);
//            };
//            return std::max_element(indices.begin(),indices.end());
//        };
//
//        /**
//         * MetaInfoObjExtractor - has logic to create the values for the fields in ReaderImpl
//         *  from serialized fields.
//         * As the fields are final, the fields need to be initialized in the constructor and
//         *  can't be done in some helper function. So this helper class is used instead.
//         *
//         */
//        class MetaInfoObjExtractor {
//            const CompressionKind compressionKind;
//            // const CompressionCodec codec;
//            const int bufferSize;
//            const int metadataSize;
//            const Metadata metadata;
//            const Footer footer;
//            // const ObjectInspector inspector;
//
//            MetaInfoObjExtractor(std::string codecStr, int bufferSize, int metadataSize, ByteRange footerBuffer) {
//                this->compressionKind = codecStr;
//                this->bufferSize = bufferSize;
//                // this->codec = WriterImpl.createCodec(compressionKind);
//                this->metadataSize = metadataSize;
//
//                int position = footerBuffer.position();
//                int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;
//                footerBuffer.limit(position + metadataSize);
//
//                InputStream instream = InStream.create("metadata", new ByteBuffer[]{footerBuffer},
//                        new long[]{0L}, metadataSize, codec, bufferSize);
//                this->metadata = Metadata.parseFrom(instream);
//
//                footerBuffer.position(position + metadataSize);
//                footerBuffer.limit(position + metadataSize + footerBufferSize);
//                instream = InStream.create("footer", new ByteBuffer[]{footerBuffer},
//                        new long[]{0L}, footerBufferSize, codec, bufferSize);
//                this->footer = Footer.parseFrom(instream);
//
//                footerBuffer.position(position);
//                this->inspector = OrcStruct.createObjectInspector(0, footer.getTypesList());
//            }
//        };

    public:
//        long getNumberOfRows() override { return footer.numberofrows(); }
//
//        std::vector<std::string> getMetadataKeys() override {
//            std::vector<std::string> result;
//            for(UserMetadataItem item: footer.metadata()) {
//                result.push_back(item.name());
//            }
//            return result;
//        }
//
//        std::string getMetadataValue(std::string key) override {
//            for(UserMetadataItem item: footer.metadata()) {
//                if (item.has_name() && item.name().compare(key)==0) {
//                    return item.value();
//                }
//            };
//            throw std::invalid_argument("Can't find user metadata " + key);
//        }
//
//        bool hasMetadataValue(std::string key) {
//            for(UserMetadataItem item: footer.metadata()) {
//                if (item.has_name() && item.name().compare(key)==0) {
//                    return true;
//                }
//            };
//            return false;
//        }
//
//        CompressionKind getCompression() override { return compressionKind; }
//
//        int getCompressionSize() override { return bufferSize; }
//
//        std::vector<StripeInformation> getStripes() {
//            std::vector<StripeInformation> result;
//            for(StripeInformation info: footer.stripes()) {
//                result.push_back(StripeInformationImpl(info));
//            }
//            return result;
//        }
//
//        // ObjectInspector getObjectInspector() override { return inspector; }
//
//        long getContentLength() override { return footer.contentlength(); }
//
//        std::vector<Type> getTypes() override { return footer.types(); }
//
//        int getRowIndexStride() override { return footer.rowindexstride(); }

//        std::vector<ColumnStatistics> getStatistics() override {
//            std::vector<ColumnStatistics> result(footer.types_size());
//            for(int i=0; i < result.size(); ++i) {
//                result[i] = ColumnStatisticsImpl.deserialize(footer.statistics(i));
//            }
//            return result;
//        }


//      /**
//       * Ensure this is an ORC file to prevent users from trying to read text
//       * files or RC files as ORC files.
//       * @param in the file being read
//       * @param path the filename for error messages
//       * @param psLen the postscript length
//       * @param buffer the tail of the file
//       */
//        //  TODO: implement this function
//        static void ensureOrcFooter(FSDataInputStream in,
//                                          Path path,
//                                          int psLen,
//                                          ByteBuffer buffer) {
//          int len = MAGIC.length();
//          if (psLen < len + 1) {
//              throw std::exception("Malformed ORC file " + path +  ". Invalid postscript length " + std::to_string(psLen));
//          }
//          int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1 - len;
//          byte[] array = buffer.array();
//          // now look for the magic string at the end of the postscript.
//          if (!Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
//              // If it isn't there, this may be the 0.11.0 version of ORC.
//              // Read the first 3 bytes of the file to check for the header
//              in.seek(0);
//              byte[] header = new byte[len];
//              in.readFully(header, 0, len);
//              // if it isn't there, this isn't an ORC file
//              if (!Text.decode(header, 0 , len).equals(OrcFile.MAGIC)) {
//                  throw new IOException("Malformed ORC file " + path + ". Invalid postscript.");
//              }
//          }
//      }


//        /**
//        * Check to see if this ORC file is from a future version and if so,
//        * warn the user that we may not be able to read all of the column encodings.
//        * @param log the logger to write any error message to
//        * @param path the filename for error messages
//        * @param version the version of hive that wrote the file.
//        */
//        //static void checkOrcVersion(Log log, Path path, List<Integer> version) {
//        static void checkOrcVersion(std::string path, std::vector<int> version) {
//            if (version.size() >= 1) {
//                int major = version[0];
//                int minor = 0;
//                if (version.size() >= 2) {
//                    minor = version[1];
//                };
//                if (major > CURRENT_VERSION.getMajor() ||
//                        (major == CURRENT_VERSION.getMajor() && minor > CURRENT_VERSION.getMinor())) {
//                    //log.warn("ORC file " + path + " was written by a future Hive version " + versionString(version) + ". This file may not be readable by this version of Hive.");
//                    std::cerr << "ORC file " << path << " was written by a future Hive version " << versionString(version) << ". This file may not be readable by this version of Hive." << std::endl;
//                };
//            }
//        }

        /**
        * Constructor that let's the user specify additional options.
        * @param path pathname for file
        * @param options options for reading
        * @throws IOException
        */
        ReaderImpl(InputStream* stream, ReaderOptions& options) {
//            FileMetaInfo footerMetaData;
//            if (options.getFileMetaInfo() != null) {
//                footerMetaData = options.getFileMetaInfo();
//            } else {
//                footerMetaData = extractMetaInfoFromFooter(path, options.getMaxLength());
//            };

            this->stream = stream;
            extractMetaInfoFromFooter(stream, options.getMaxLength());
        }

       int getCompression() const {
            return (int)(this->fileMetaInfo.postscript.compression()) ;
        }

//        FileMetaInfo getFileMetaInfo() { return FileMetaInfo(compressionKind, bufferSize, metadataSize, footerByteBuffer, versionList); }
//
//        RecordReader rows() override { return rowsOptions(Options()); }
//
//
//        RecordReader rowsOptions(Options options) override {
//            //LOG.info("Reading ORC rows from " + path + " with " + options);
//            std::vector<bool> include = options.getInclude();
//            return new RecordReaderImpl(this->getStripes(), fileSystem, path, options, footer.types(),
//                    codec, bufferSize, footer.rowindexstride(), conf);
//        }
//
//        RecordReader rows(boolean[] include) override { return rowsOptions(Options().include(include)); }
//
//        public RecordReader rows(long offset, long length, std::vector<bool> include) override {
//                  return rowsOptions(new Options().include(include).range(offset, length));
//        }
//
//
//        public RecordReader rows(long offset, long length, boolean[] include,
//                                       SearchArgument sarg, String[] columnNames) override {
//            return rowsOptions(new Options().include(include).range(offset, length).searchArgument(sarg, columnNames));
//        }

//        long getRawDataSize() override {
//            // if the deserializedSize is not computed, then compute it, else
//            // return the already computed size. since we are reading from the footer
//            // we don't have to compute deserialized size repeatedly
//            if (deserializedSize == -1) {
//                std::vector<ColumnStatistics> stats = footer.statistics();
//                std::vector<int> indices;
//                for (int i = 0; i < stats.size(); ++i) {
//                    indices.push_back(i);
//                }
//                deserializedSize = getRawDataSizeFromColIndices(indices);
//            }
//            return deserializedSize;
//        }
//
//        long getRawDataSizeOfColumns(std::vector<std::string> colNames) override {
//            std::vector<int> colIndices = getColumnIndicesFromNames(colNames);
//            return getRawDataSizeFromColIndices(colIndices);
//        }
//
//        Metadata getMetadata() override { return Metadata(metadata); }
//
//        std::vector<StripeStatistics> getOrcProtoStripeStatistics() { return metadata.stripestats(); }
//
//        std::vector<UserMetadataItem> getOrcProtoUserMetadata() { return footer.metadata(); }
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
