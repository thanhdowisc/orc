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

#include <DataReader.hh>

namespace orc {

    DataReader::~DataReader() {
        //PASS
    }

    class IntegerReader : public DataReader {
    private:
        orc::proto::ColumnEncoding encoding;
        //std::unique_ptr<orc::BoolRleDecoder> presentStream;
        std::unique_ptr<orc::RleDecoder> dataStream;

    public:
        ~IntegerReader() {}
        IntegerReader() {}

        void reset(orc::proto::ColumnEncoding encoding) override {
            this->encoding = encoding;
            orc::RleDecoder *pDataStream = dataStream.release();
            if (pDataStream != NULL)
                delete pDataStream;
        }

        void resetStream(unsigned char* stream, unsigned int streamLength, orc::proto::Stream_Kind kind) override {
            switch (kind) {
            case orc::proto::Stream_Kind_PRESENT:
                // TODO: Initialize the PRESENT stream
                break;
            case orc::proto::Stream_Kind_DATA:
                switch (encoding.kind()) {
                case orc::proto::ColumnEncoding_Kind_DIRECT :
                    dataStream = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            true,
                            orc::VERSION_1);
                    break;
                case orc::proto::ColumnEncoding_Kind_DIRECT_V2 :
                    dataStream = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            true,
                            orc::VERSION_2);
                    break;
                }
                break;
            }
        }

        boost::any next() override {
            ORC_INT output;
            dataStream->next(&output, 1, 0);
            return (boost::any)output ;
        }
    };

    class StringReader : public DataReader {
    private:
        orc::proto::ColumnEncoding encoding;
        //std::unique_ptr<orc::BoolRleDecoder> presentStream;
        std::unique_ptr<orc::RleDecoder> dataStream_Dictionary;
        std::string dataStream_Direct;
        long positionDataStream;
        std::string dictionaryDataStream;
        std::vector<long> lengthsDictionary;
        std::vector<long> offsetsDictionary;
        std::unique_ptr<orc::RleDecoder> lengthStream;

    public:
        ~StringReader() {}
        StringReader() {
            dataStream_Direct.clear();
            positionDataStream = 0;

            dictionaryDataStream.clear();
            lengthsDictionary.clear();
            offsetsDictionary.clear();
        }

        void reset(orc::proto::ColumnEncoding encoding) override {
            this->encoding = encoding;

            orc::RleDecoder *pDataStream_Dictionary = dataStream_Dictionary.release();
            if (pDataStream_Dictionary != NULL)
                delete pDataStream_Dictionary;

            dataStream_Direct.clear();
            positionDataStream = 0;

            dictionaryDataStream.clear();
            lengthsDictionary.clear();
            offsetsDictionary.clear();

            orc::RleDecoder *pLengthStream = lengthStream.release();
            if (pLengthStream != NULL)
                delete pLengthStream;
        }

        void resetStream(unsigned char* stream, unsigned int streamLength, orc::proto::Stream_Kind kind) override {
            switch (kind) {
            case orc::proto::Stream_Kind_PRESENT:
                // TODO: Initialize the PRESENT stream with a BoolRleDecoder
                break;
            case orc::proto::Stream_Kind_DATA:
                switch (encoding.kind()) {
                case orc::proto::ColumnEncoding_Kind_DIRECT :
                case orc::proto::ColumnEncoding_Kind_DIRECT_V2 :
                    dataStream_Direct.assign((const char*)stream, streamLength);
                    break;
                case orc::proto::ColumnEncoding_Kind_DICTIONARY :
                    dataStream_Dictionary = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            false,
                            orc::VERSION_1);
                    break;
                case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2 :
                    dataStream_Dictionary = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            false,
                            orc::VERSION_2);
                    break;
                };
                break;
            case orc::proto::Stream_Kind_DICTIONARY_DATA:
                dictionaryDataStream.assign((const char*)stream, streamLength);
                break;
            case orc::proto::Stream_Kind_LENGTH:
                switch (encoding.kind()) {
                case orc::proto::ColumnEncoding_Kind_DIRECT :
                case orc::proto::ColumnEncoding_Kind_DICTIONARY :
                    lengthStream = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            false,
                            orc::VERSION_1);
                    break;
                case orc::proto::ColumnEncoding_Kind_DIRECT_V2 :
                case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2 :
                    lengthStream = orc::createRleDecoder(
                            std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)),
                            false,
                            orc::VERSION_2);
                    break;
                };
            }
        }

        boost::any next() override {
            long length ;
            std::string value;

            switch (encoding.kind()) {
                case orc::proto::ColumnEncoding_Kind_DIRECT :
                case orc::proto::ColumnEncoding_Kind_DIRECT_V2 :
                    lengthStream->next(&length, 1, 0);
                    value = dataStream_Direct.substr(positionDataStream, length);
                    positionDataStream += length;
                    break;
                case orc::proto::ColumnEncoding_Kind_DICTIONARY :
                case orc::proto::ColumnEncoding_Kind_DICTIONARY_V2 :
                    if (offsetsDictionary.empty() || lengthsDictionary.empty()) {
                        offsetsDictionary.clear();
                        lengthsDictionary.clear();
                        long offset = 0;
                        for (int i=0; i< encoding.dictionarysize(); i++) {
                            lengthStream->next(&length, 1, 0);
                            lengthsDictionary.push_back(length);
                            offsetsDictionary.push_back(offset);
                            offset += length;
                        };
                    };
                    long itemIx ;
                    dataStream_Dictionary->next(&itemIx, 1, 0);
                    value = dictionaryDataStream.substr(offsetsDictionary[itemIx],lengthsDictionary[itemIx]);
                    break;
                };
            return (boost::any)value;
        }
    };

    class BoolReader : public DataReader {
     private:
         orc::proto::ColumnEncoding encoding;
         //std::unique_ptr<orc::BooleanRleDecoder> presentStream;
         std::unique_ptr<orc::RleDecoder> dataStream;

     public:
         ~BoolReader() {}
         BoolReader() {}

         void reset(orc::proto::ColumnEncoding encoding) override {
             this->encoding = encoding;
             orc::RleDecoder *pDataStream = dataStream.release();
             if (pDataStream != NULL)
                 delete pDataStream;
         }

         void resetStream(unsigned char* stream, unsigned int streamLength, orc::proto::Stream_Kind kind) override {
             switch (kind) {
             case orc::proto::Stream_Kind_PRESENT:
                 // TODO: Initialize the PRESENT stream
                 break;
             case orc::proto::Stream_Kind_DATA:
                 dataStream = orc::createBooleanRleDecoder(std::unique_ptr<orc::SeekableInputStream> (new SeekableArrayInputStream(stream, streamLength)));
                 break;
             };
         }

         boost::any next() override {
             char output;
             dataStream->next(&output, 1, 0);
             return (boost::any)((ORC_BOOL)output) ;
         }
     };




    class DummyReader : public DataReader {
    private:
        orc::proto::ColumnEncoding encoding;

    public:
         ~DummyReader() {}
         DummyReader() {}

         void reset(orc::proto::ColumnEncoding encoding) override {
             this->encoding = encoding;
         }

         void resetStream(unsigned char* stream, unsigned int streamLength, orc::proto::Stream_Kind kind) override { }

         boost::any next() override {
             long dummy = 123;
             return (boost::any)dummy;
         }
     };

    DataReader* createIntegerReader() { return new IntegerReader(); }
    DataReader* createStringReader() { return new StringReader(); }
    DataReader* createBoolReader() { return new BoolReader(); }
    DataReader* createDummyReader() { return new DummyReader(); }
}
