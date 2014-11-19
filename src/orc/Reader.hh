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
#include <vector>
#include <memory>
#include <string>
#include <limits>

#include "Vector.hh"

namespace orc {

    enum CompressionKind {
        NONE = 0,
        ZLIB = 1,
        SNAPPY = 2,
        LZO = 3
    };

    // classes that hold data members so we can maintain binary compatibility
    class StripeInformationPrivate;
    class ColumnStatisticsPrivate;
    class ReaderOptionsPrivate;

    /**
     * Statistics that are available for all types of columns.
     */
    class ColumnStatistics {
    private:
        std::unique_ptr<ColumnStatisticsPrivate> privateBits;

    public:
        ColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
        virtual ~ColumnStatistics();

        /**
         * Get the number of values in this column. It will differ from the number
         * of rows because of NULL values and repeated values.
         * @return the number of values
         */
        long getNumberOfValues() const;
    };

    /**
    * Statistics for binary columns.
    */
    class BinaryColumnStatistics: public ColumnStatistics {
    public:
        BinaryColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
        virtual ~BinaryColumnStatistics();
        long getTotalLength() const;
    };

    /**
     * Statistics for boolean columns.
     */
   class BooleanColumnStatistics: public ColumnStatistics {
   public:
   BooleanColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~BooleanColumnStatistics();
   long getFalseCount() const;
   long getTrueCount() const;
   };
   /**
   * Statistics for date columns.
   */
   class DateColumnStatistics: public ColumnStatistics {
   public:
   DateColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~DateColumnStatistics();
   /**
   * Get the minimum value for the column.
   * @return minimum value
   */
   long getMinimum() const;
   /**
   * Get the maximum value for the column.
   * @return maximum value
   */
   long getMaximum() const;
   };
   /**
   * Statistics for decimal columns.
   */
   class DecimalColumnStatistics: public ColumnStatistics {
   public:
   DecimalColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~DecimalColumnStatistics();
   /**
   * Get the minimum value for the column.
   * @return minimum value
   */
   Decimal getMinimum() const;
   /**
   * Get the maximum value for the column.
   * @return maximum value
   */
   Decimal getMaximum() const;
   /**
   * Get the sum for the column.
   * @return sum of all the values
   */
   Decimal getSum() const;
   };
   /**
   * Statistics for float and double columns.
   */
   class DoubleColumnStatistics: public ColumnStatistics {
   public:
   DoubleColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~DoubleColumnStatistics();
   /**
   * Get the smallest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   * @return the minimum
   */
   double getMinimum() const;
   /**
   * Get the largest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   * @return the maximum
   */
   double getMaximum() const;
   /**
   * Get the sum of the values in the column.
   * @return the sum
   */
   double getSum() const;
   };
   /**
   * Statistics for all of the integer columns, such as byte, short, int, and
   * long.
   */
   class IntegerColumnStatistics: public ColumnStatistics {
   public:
   IntegerColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~IntegerColumnStatistics();
   /**
   * Get the smallest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   * @return the minimum
   */
   long getMinimum() const;
   /**
   * Get the largest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   * @return the maximum
   */
   long getMaximum() const;
   /**
   * Is the sum defined? If the sum overflowed the counter this will be
   * false.
   * @return is the sum available
   */
   bool isSumDefined() const;
   /**
   * Get the sum of the column. Only valid if isSumDefined returns true.
   * @return the sum of the column
   */
   long getSum() const;
   };
   /**
   * Statistics for string columns.
   */
   class StringColumnStatistics: public ColumnStatistics {
   public:
   StringColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~StringColumnStatistics();
   /**
   * Get the minimum value for the column.
   * @return minimum value
   */
   std::string getMinimum() const;
   /**
   * Get the maximum value for the column.
   * @return maximum value
   */
   std::string getMaximum() const;
   /**
   * Get the total length of all values.
   * @return total length of all the values
   */
   long getTotalLength() const;
   };
   /**
   * Statistics for stamp columns.
   */
   class TimestampColumnStatistics: public ColumnStatistics {
   public:
   TimestampColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data);
   virtual ~TimestampColumnStatistics();
   /**
   * Get the minimum value for the column.
   * @return minimum value
   */
   long getMinimum() const;
   /**
   * Get the maximum value for the column.
   * @return maximum value
   */
   long getMaximum() const;
   };
   class StripeInformation {
   private:
   std::unique_ptr<StripeInformationPrivate> privateBits;
   public:
   virtual ~StripeInformation();
   /**
   * Get the byte offset of the start of the stripe.
   * @return the bytes from the start of the file
   */
   virtual long getOffset() = 0;
   /**
   * Get the total length of the stripe in bytes.
   * @return the number of bytes in the stripe
   */
   virtual long getLength() = 0;
   /**
   * Get the length of the stripe's indexes.
   * @return the number of bytes in the index
   */
   virtual long getIndexLength() = 0;
   /**
   * Get the length of the stripe's data.
   * @return the number of bytes in the stripe
   */
   virtual long getDataLength() = 0;
   /**
   * Get the length of the stripe's tail section, which contains its index.
   * @return the number of bytes in the tail
   */
   virtual long getFooterLength() = 0;
   /**
   * Get the number of rows in the stripe.
   * @return a count of the number of rows
   */
   virtual long getNumberOfRows() = 0;
   };

   /**
   * Options for creating a RecordReader.
   */
  class ReaderOptions {
  private:
//      std::unique_ptr<ReaderOptionsPrivate> privateBits;
//      FileMetaInfo fileMetaInfo;
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
    long getMaxLength() { return maxLength; };
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
}

#endif
