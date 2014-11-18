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

#ifndef ORC_INPUT_STREAM_HH
#define ORC_INPUT_STREAM_HH

namespace orc {

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
       virtual void read(void* buffer, long offset, long length) const = 0;

       /**
        * Get the name of the stream for error messages.
        */
       virtual const std::string& getName() const = 0;
     };

     class FileInputStream : public InputStream {} ;

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
}

#endif
