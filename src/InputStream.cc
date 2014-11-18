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

#include <iostream>
#include <string>

#include "InputStream.hh"
#include "orc/OrcFile.hh"

namespace orc {

    class FileInputStream : public InputStream {
    private:
        std::string filename ;
        std::ifstream file;
        long long length;
        std::string name ;

    public:
        virtual FileInputStream(std::string filename, std::string name="") {
            this->filename = filename ;
            file.open(filename.c_str(), std::ios::in | std::ios::binary);
            // TODO: Can we do this using filesystem info?
            file.seekg(0,file.end);
            length = file.tellg();
            this->name = (name.compare("")==0) ? filename : name ;
        }

        virtual ~FileInputStream() {
           file.close();
       }

       /**
        * Get the total length of the file in bytes.
        */
       virtual long getLength() const { return this->length; }

       /**
        * Read length bytes from the file starting at offset into
        * the buffer.
        * @param buffer the location to write the bytes to, which must be
        *        at least length bytes long
        * @param offset the position in the file to read from
        * @param length the number of bytes toread
        */
       virtual void read(void* buffer, long offset, long length) const {
           file.seekg(offset);
           file.read((char*)buffer, length);
       }

       /**
        * Get the name of the stream for error messages.
        */
       virtual const std::string& getName() const { return this->name; }

    };
}
