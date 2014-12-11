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

#ifndef ORC_FILE_HH
#define ORC_FILE_HH

#include <string>
#include "Reader.hh"

/** @file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace orc {

static const std::string MAGIC("ORC");

    class Version {
    private:
     std::string name;
     int major;
     int minor;

    public:
     Version(std::string name, int major, int minor) {
         this->name = name;
         this->major = major;
         this->minor = minor;
     }

     std::string getName()   { return this->name; }
     int getMajor()   { return this->major; }
     int getMinor()   { return this->minor; }
    };

    static const Version V_0_11("0.11", 0, 11);
    static const Version V_0_12("0.12", 0, 12);
    //static std::unordered_map<std::string, Version> Versions({{"0.11", V_0_11},{"0.12", V_0_12}});
    static const Version CURRENT_VERSION = V_0_12;

    /**
     * An abstract interface for providing ORC readers a stream of bytes.
     */
    class InputStream {
    public:
      virtual ~InputStream();

      /**
       * Get the total length of the file in bytes.
       */
      virtual long getLength() const = 0;

      /**
       * Read length bytes from the file starting at offset into
       * the buffer.
       * @param buffer the location to write the bytes to, which must be
       *        at least length bytes long
       * @param offset the position in the file to read from
       * @param length the number of bytes toread
       */
      virtual void read(void* buffer, long offset, long length) = 0;

      /**
       * Get the name of the stream for error messages.
       */
      virtual const std::string& getName() const = 0;
    };

    /**
     * Create a stream to a local file.
     * The resulting object should be deleted when the user is done with the
     * stream.
     * @param path the name of the file in the local file system
     */
    InputStream* readLocalFile(const std::string& path);

    /**
     * Create a reader to the for the ORC file.
     * @param stream the stream to read
     */
    Reader* createReader(InputStream* stream);


    /**
    * Options for creating a RecordReader.
    */
   class ReaderOptions {
   private:
 //      std::unique_ptr<ReaderOptionsPrivate> privateBits;
 //      FileMetaInfo fileMetaInfo;
       long maxLength = std::numeric_limits<long>::max();

   public:
     ReaderOptions() {};

     long getMaxLength() { return maxLength; };
   };

    // ORC data types
    #define ORC_INT long
    #define ORC_STRING std::string
    #define ORC_BOOL bool

}
#endif
