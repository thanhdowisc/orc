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

#include "orc/Vector.hh"

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  class ReaderOptionsPrivate;
  
  class FileMetaInfo {
  public:
    const orc::proto::CompressionKind compression;
    const int bufferSize;
    const int metadataSize;
    const ByteRange footerBuffer;
    const std::vector<int> versionList;

    FileMetaInfo(CompressionKind compression, int bufferSize, int metadataSize,
            ByteRange footerBuffer, std::vector<int> versionList): compression(compression), bufferSize(bufferSize),
                    metadataSize(metadataSize), footerBuffer(footerBuffer), versionList(versionList) {};
//        this->footerBuffer.length = footerBuffer.length;
//        this->footerBuffer.data = new char[this->footerBuffer.length];
//        memcpy(this->footerBuffer, footerBuffer, footerBuffer.length);
//
//        for(std::vector<int>::iterator it=versionList.begin(); it!=versionList.end(); it++)
//            this->versionList.push_back(*it);
  };


  /**
   * Options for creating a RecordReader.
   */
  class ReaderOptions {
  private:
      std::unique_ptr<ReaderOptionsPrivate> privateBits;
      FileMetaInfo fileMetaInfo;
      long maxLength = std::numeric_limits<long>::max();

  public:
    ReaderOptions();

//    /**
//     * Set the list of columns to read.
//     * @param include a list of columns to read
//     * @return this
//     */
//    ReaderOptions& include(const std::list<bool>& include);
//
//    /**
//     * Set the section of the file to process.
//     * @param offset the starting byte offset
//     * @param length the number of bytes to read
//     * @return this
//     */
//    ReaderOptions& range(long offset, long length);
//
//    /**
//     * Set the location of the tail as defined by the logical length of the
//     * file.
//     */
//    ReaderOptions& setTailLocation(long offset);
//
//    const std::list<bool>& getInclude() const;
//
//    long getOffset() const;
//
//    long getLength() const;
//
//    long getTailLocation() const;
//
//    FileMetaInfo getFileMetaInfo() { return fileMetaInfo; };
//
//    //void setFileMetaInfo(FileMetaInfo& info) { fileMetaInfo = info; };
//
//    long getMaxLength() { return maxLength; };
//
//    void setMaxLength(long val) { maxLength = val; }
  };

  /**
   * The interface for reading ORC file meta information. 
   * This is an an abstract class that will subclassed as necessary.
   *
   * One Reader can support multiple concurrent RecordReader.
   */
  class Reader {
  public:
    virtual ~Reader();

//    /**
//     * Get the number of rows in the file.
//     * @return the number of rows
//     */
//    virtual long getNumberOfRows() const = 0;
//
//    /**
//     * Get the deserialized data size of the file
//     * @return raw data size
//     */
//    virtual long getRawDataSize() const = 0;
//
//    /**
//     * Get the deserialized data size of the specified columns
//     * @param colNames
//     * @return raw data size of columns
//     */
//    virtual long getRawDataSizeOfColumns(const std::list<std::string>& colNames) = 0;
//
//    /**
//     * Get the user metadata keys.
//     * @return the set of metadata keys
//     */
//    virtual std::list<std::string> getMetadataKeys() const = 0;
//
//    /**
//     * Get a user metadata value.
//     * @param key a key given by the user
//     * @return the bytes associated with the given key
//     */
//    virtual ByteRange getMetadataValue(const std::string& key) const = 0;
//
//    /**
//     * Did the user set the given metadata value.
//     * @param key the key to check
//     * @return true if the metadata value was set
//     */
//    virtual bool hasMetadataValue(const std::string& key) const = 0;
//
//    /**
//     * Get the compression kind.
//     * @return the kind of compression in the file
//     */
//    virtual CompressionKind getCompression() const = 0;
//
//    /**
//     * Get the buffer size for the compression.
//     * @return number of bytes to buffer for the compression codec.
//     */
//    virtual int getCompressionSize() const = 0;
//
//    /**
//     * Get the number of rows per a entry in the row index.
//     * @return the number of rows per an entry in the row index or 0 if there
//     * is no row index.
//     */
//    virtual int getRowIndexStride() const = 0;
//
//    /**
//     * Get the list of stripes.
//     * @return the information about the stripes in order
//     */
//    const std::list<StripeInformation>& getStripes() const = 0;
//
//    /**
//     * Get the length of the file.
//     * @return the number of bytes in the file
//     */
//    virtual long getContentLength() const = 0;

//    /**
//     * Get the statistics about the columns in the file.
//     * @return the information about the column
//     */
//    virtual std::list<ColumnStatistics*> getStatistics() = 0;
//
//    /**
//     * Get the statistics about the columns in the file.
//     * @return the information about the column
//     */
//    virtual const std::list<ColumnStatistics>& getStatistics() = 0;
//
//    /**
//     * Get the list of types contained in the file. The root type is the first
//     * type in the list.
//     * @return the list of flattened types
//     */
//    virtual const std::list<Type>& getTypes() const = 0;
//
//    /**
//     * Create a RecordReader that uses the options given.
//     * @param options the options to read with
//     * @return a new RecordReader
//     * @throws IOException
//     */
//    virtual RecordReader rows(const ReaderOptions& options) const = 0;
  };

  class ReaderImpl: public Reader {
      public:
          virtual ~ReaderImpl();

//          long getNumberOfRows() ;
//
//          std::vector<std::string> getMetadataKeys();
//
//          std::string getMetadataValue(std::string key);
//
//          bool hasMetadataValue(std::string key);
//
//          CompressionKind getCompression();
//
//          int getCompressionSize();
//
//          std::vector<StripeInformation> getStripes();
//
//          long getContentLength();
//
//          std::vector<Type> getTypes();
//
//          int getRowIndexStride();
//
//          //std::vector<ColumnStatistics> getStatistics() ;
//
//          /**
//          * Check to see if this ORC file is from a future version and if so,
//          * warn the user that we may not be able to read all of the column encodings.
//          * @param log the logger to write any error message to
//          * @param path the filename for error messages
//          * @param version the version of hive that wrote the file.
//          */
//          //static void checkOrcVersion(Log log, Path path, List<Integer> version) {
//          static void checkOrcVersion(std::string path, std::vector<int> version);
//
//          /**
//          * Constructor that let's the user specify additional options.
//          * @param path pathname for file
//          * @param options options for reading
//          * @throws IOException
//          */
//          ReaderImpl(std::string path, ReaderOptions options) ;
//
//          FileMetaInfo getFileMetaInfo() ;
//
//          long getRawDataSize();
//
//          long getRawDataSizeOfColumns(std::vector<std::string> colNames) ;
//
//          Metadata getMetadata() ;
//
//          std::vector<StripeStatistics> getOrcProtoStripeStatistics();
//
//          std::vector<UserMetadataItem> getOrcProtoUserMetadata();
  };


}

#endif
