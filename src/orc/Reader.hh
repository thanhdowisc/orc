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
#include <boost/any.hpp>

#include "Vector.hh"

namespace orc {

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
   * The interface for reading ORC file meta information.
   * This is an an abstract class that will subclassed as necessary.
   *
   * One Reader can support multiple concurrent RecordReader.
   */
  class Reader {
  public:
    virtual ~Reader();

    /**
     * Get the number of rows in the file.
     * @return the number of rows
     */
    virtual long getNumberOfRows() const = 0;

    /**
     * Get the compression kind.
     * @return the kind of compression in the file
     */
    virtual int getCompression() const = 0;

    virtual int getRowStride() const = 0;

    virtual std::string getStreamName() const = 0;

    virtual int getStreamSize() const = 0;

    /**
    * Does the reader have more rows available.
    * @return true if there are more rows
    * @throws java.io.IOException
    */
    virtual bool hasNext() const = 0;

    /**
    * Read the next row.
    * @param previous a row object that can be reused by the reader
    * @return the row that was read
    * @throws java.io.IOException
    */
    // virtual Object next(Object previous) = 0;
    virtual std::vector<boost::any> next() = 0;

//            /**
//            * Read the next row batch. The size of the batch to read cannot be controlled
//            * by the callers. Caller need to look at VectorizedRowBatch.size of the retunred
//            * object to know the batch size read.
//            * @param previousBatch a row batch object that can be reused by the reader
//            * @return the row batch that was read
//            * @throws java.io.IOException
//            */
//            virtual VectorizedRowBatch nextBatch(VectorizedRowBatch previousBatch) = 0;

    /**
    * Get the row number of the row that will be returned by the following
    * call to next().
    * @return the row number from 0 to the number of rows in the file
    * @throws java.io.IOException
    */
    virtual long getRowNumber() = 0;

    /**
    * Get the progress of the reader through the rows.
    * @return a fraction between 0.0 and 1.0 of rows read
    * @throws java.io.IOException
    */
    virtual float getProgress() = 0;

    /**
     * Get the statistics about the columns in the file.
     * @return the information about the column
     */
    virtual const std::list<std::unique_ptr<ColumnStatistics> >&
      getStatistics() = 0;

    /**
     * Get the type of the rows in the file. The top level is always a struct.
     * @return the root type
     */
    virtual const Type& getTypes() const = 0;

    /**
     * Create a RecordReader that uses the options given.
     * @param options the options to read with
     * @return a new RecordReader
     * @throws IOException
     */
    virtual std::unique_ptr<RecordReader> rows(const ReaderOptions& options
                                               ) const = 0;
  };
}

#endif
