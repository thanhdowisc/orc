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
        std::unique_ptr<orc::RleDecoder> rle;

    public:
        ~IntegerReader() {}
        IntegerReader() {}

        void reset(SeekableArrayInputStream* stream, orc::proto::ColumnEncoding encoding) override {
            if (encoding.kind() == orc::proto::ColumnEncoding_Kind_DIRECT)
                rle = orc::createRleDecoder(std::move(std::unique_ptr<orc::SeekableInputStream> (stream)), true, orc::VERSION_1);
        }

        boost::any next() override {
            // ORC_INT output;
            long output;
            rle->next(&output, 1, 0);
            return (boost::any)output ;
        }
    };

    class StringReader : public DataReader {
    public:
        ~StringReader() {}
        StringReader() {}

        void reset(SeekableArrayInputStream* stream, orc::proto::ColumnEncoding encoding) override {}

        boost::any next() override {
            ORC_STRING output("=test=");
            return (boost::any)output;
        }
    };

    class DummyReader : public DataReader {
     public:
         ~DummyReader() {}
         DummyReader() {}

         void reset(SeekableArrayInputStream* stream, orc::proto::ColumnEncoding encoding) override {}

         boost::any next() override {
             long dummy = 54321;
             return (boost::any)dummy;
         }
     };


    DataReader* createIntegerReader() { return new IntegerReader(); }
    DataReader* createStringReader() { return new StringReader(); }
    DataReader* createDummyReader() { return new DummyReader(); }

}
