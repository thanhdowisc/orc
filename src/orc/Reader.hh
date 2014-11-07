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

#ifndef ORC_READER_HH
#define ORC_READER_HH

#include <list>
#include <memory>
#include <string>

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  class ReaderOptionsPrivate;
  
  /**
   * Options for creating a RecordReader.
   */
  class ReaderOptions {
  private:
    std::unique_ptr<ReaderOptionsPrivate> private;

  public:
    /**
     * Set the list of columns to read.
     * @param include a list of columns to read
     * @return this
     */
    ReaderOptions& include(const std::list<bool>& include);

    /**
     * Set the section of the file to process.
     * @param offset the starting byte offset
     * @param length the number of bytes to read
     * @return this
     */
    ReaderOptions& range(long offset, long length);

    /**
     * Set the location of the tail as defined by the logical length of the
     * file.
     */
    ReaderOptions& setTailLocation(long offset);

    const std::list<bool>& getInclude() const;

    long getOffset() const;

    long getLength() const;

    long getTailLocation() const;
  }

  /**
   * The interface for reading ORC file meta information. 
   * This is an an abstract class that will subclassed as necessary.
   *
   * One Reader can support multiple concurrent RecordReader.
   */
  class Reader {
  public:
    virtual ~OrcReader();

    /**
     * Get the number of rows in the file.
     * @return the number of rows
     */
    virtual long getNumberOfRows() const = 0;

    /**
     * Get the deserialized data size of the file
     * @return raw data size
     */
    virtual long getRawDataSize() const = 0;

    /**
     * Get the deserialized data size of the specified columns
     * @param colNames
     * @return raw data size of columns
     */
    virtual long getRawDataSizeOfColumns(const std::list<std::string>& 
                                           colNames) = 0;

    /**
     * Get the user metadata keys.
     * @return the set of metadata keys
     */
    virtual std::list<std::string> getMetadataKeys() const = 0;

    /**
     * Get a user metadata value.
     * @param key a key given by the user
     * @return the bytes associated with the given key
     */
    virtual ByteRange getMetadataValue(const std::string& key) const = 0;

    /**
     * Did the user set the given metadata value.
     * @param key the key to check
     * @return true if the metadata value was set
     */
    virutal boolean hasMetadataValue(const std::string& key) const = 0;

    /**
     * Get the compression kind.
     * @return the kind of compression in the file
     */
    virutal CompressionKind getCompression() const = 0;

    /**
     * Get the buffer size for the compression.
     * @return number of bytes to buffer for the compression codec.
     */
    virtual int getCompressionSize() const = 0;

    /**
     * Get the number of rows per a entry in the row index.
     * @return the number of rows per an entry in the row index or 0 if there
     * is no row index.
     */
    virtual int getRowIndexStride() const = 0;

    /**
     * Get the list of stripes.
     * @return the information about the stripes in order
     */
    const std::list<StripeInformation>& getStripes() const = 0;

    /**
     * Get the length of the file.
     * @return the number of bytes in the file
     */
    virtual long getContentLength() const = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual std::list<ColumnStatistics*> getStatistics() = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual const std::list<ColumnStatistics>& getStatistics() = 0;

    /**
     * Get the list of types contained in the file. The root type is the first
     * type in the list.
     * @return the list of flattened types
     */
    virtual const std::list<Type>& getTypes() const = 0;

    /**
     * Create a RecordReader that uses the options given.
     * @param options the options to read with
     * @return a new RecordReader
     * @throws IOException
     */
    virtual RecordReader rows(const ReaderOptions& options) const = 0;
  };
}

#endif
