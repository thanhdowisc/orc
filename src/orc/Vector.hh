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

#ifndef ORC_VECTOR_HH
#define ORC_VECTOR_HH

#include <array>
#include <list>
#include <memory>

namespace orc {

  class TypePrivate;

  enum TypeKind {
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    TIMESTAMP = 9,
    LIST = 10,
    MAP = 11,
    STRUCT = 12,
    UNION = 13,
    DECIMAL = 14,
    DATE = 15,
    VARCHAR = 16,
    CHAR = 17
  };

  class Type {
  private:
    std::unique_ptr<TypePrivate> privateBits;
  public:
    Type(TypeKind kind);
    Type(TypeKind kind, int maximumLength, int scale);
    Type(TypeKind kind, const std::list<Type>& subtypes);
    Type(TypeKind kind, const std::list<Type>& subtypes, 
	 const std::list<std::string>& fieldNames);

    TypeKind getKind();
    std::list<Type> getSubtypes();
    std::list<std::string> getFieldNames();
    int getMaximumLength();
    int getScale();
  };

  struct ColumnVectorBatch {
    ColumnVectorBatch(int capacity);
    virtual ~ColumnVectorBatch();

    // the number of slots available
    int capacity;
    // the number of current occupied slots
    int numElements;
    // an array of capacity length marking null values
    std::unique_ptr<bool[]> isNull;
    // whether there are any null values
    bool hasNulls;
  };

  struct LongVectorBatch: public ColumnVectorBatch {
    LongVectorBatch(int capacity);
    virtual ~LongVectorBatch();
    std::unique_ptr<long[]> data;
  };

  struct DoubleVectorBatch: public ColumnVectorBatch {
    DoubleVectorBatch(int capacity);
    virtual ~DoubleVectorBatch();
    std::unique_ptr<double[]> data;
  };

  struct ByteRange {
    char* data;
    int length;
  };

  struct ByteVectorBatch: public ColumnVectorBatch {
    ByteVectorBatch(int capacity);
    virtual ~ByteVectorBatch();
    std::unique_ptr<ByteRange[]> data;
  };

  struct StructVectorBatch: public ColumnVectorBatch {
    StructVectorBatch(int capacity);
    virtual ~StructVectorBatch();
    std::list<std::unique_ptr<ColumnVectorBatch> > fields;
  };

  struct Decimal {
    long lower;
    long upper;
  };
}

#endif
