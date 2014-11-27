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

#include "orc/Vector.hh"

namespace orc {

  class TypeImpl: public Type {
  private:
    TypeKind kind;
    std::unique_ptr<std::unique_ptr<Type>[]> subTypes;
    std::unique_ptr<std::string[]> fieldNames;
    int subtypeCount;
    int maxLength;
    int precision;
    int scale;
  public:
    TypeImpl(TypeKind _kind, 
             std::initializer_list<std::unique_ptr<Type> > types,
             std::initializer_list<std::string> fieldNames,
             int maxLength,
             int precision,
             int scale);
    virtual ~TypeImpl();

    TypeKind getKind() const override {
      return kind;
    }

    int getSubtypeCount() const override {
      return subtypeCount;
    }

    const std::unique_ptr<Type> *getSubtypes() const override {
      return subTypes.get();
    }

    const std::string *getFieldNames() const override {
      return fieldNames.get();
    }

    virtual int getMaximumLength() const override {
      return maxLength;
    }

    virtual int getPrecision() const override {
      return precision;
    }

    virtual int getScale() const override {
      return scale;
    }
  };

  Type::~Type() {
    // PASS
  }

  TypeImpl::TypeImpl(TypeKind _kind, 
                     std::initializer_list<std::unique_ptr<Type> > _types,
                     std::initializer_list<std::string> _fieldNames,
                     int _maxLength,
                     int _precision,
                     int _scale) {
    maxLength = _maxLength;
    precision = _precision;
    scale = _scale;
    subTypes.reset(new std::unique_ptr<Type>[_types.size()]);
    int i = 0;
    for(auto itr= _types.begin(); itr != _types.end(); ++itr) {
      subTypes.get()[i++].
        reset(const_cast<std::unique_ptr<Type>&>(*itr).release());
    }
    i = 0;
    fieldNames.reset(new std::string[_fieldNames.size()]);
    for(std::string field: _fieldNames) {
      fieldNames.get()[i++] = field;
    }
  }

  TypeImpl::~TypeImpl() {
    // PASS
  }

  std::unique_ptr<Type> createPrimitiveType(TypeKind kind) {
    return std::unique_ptr<Type>(new TypeImpl(kind, {}, {}, 0, 0, 0));
  }

  std::unique_ptr<Type> createCharType(bool isVarchar,
                                       int maxLength) {
    TypeKind kind = isVarchar ? VARCHAR : CHAR;
    return std::unique_ptr<Type>(new TypeImpl(kind, {}, {}, maxLength, 0, 0));
  }

  std::unique_ptr<Type> createDecimalType(int precision,
                                          int scale) {
    return std::unique_ptr<Type>(new TypeImpl(DECIMAL, {}, {}, 0, precision, 
                                              scale));
  }

  std::unique_ptr<Type> 
    createStructType(std::initializer_list<std::unique_ptr<Type> > types,
                     std::initializer_list<std::string> fieldNames) {
    return std::unique_ptr<Type>(new TypeImpl(STRUCT, types, fieldNames, 
                                              0, 0, 0));
  }

  std::unique_ptr<Type> createListType(std::unique_ptr<Type> element) {
    return std::unique_ptr<Type>(new TypeImpl(LIST, {std::move(element)}, 
                                              {}, 0, 0, 0));
  }

  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                      std::unique_ptr<Type> value) {
    return std::unique_ptr<Type>(new TypeImpl(MAP, {std::move(key), 
            std::move(value)}, {}, 0, 0, 0));
  }

  std::unique_ptr<Type> 
      createUnionType(std::initializer_list<std::unique_ptr<Type> > types) {
    return std::unique_ptr<Type>(new TypeImpl(UNION, types, {}, 0, 0, 0));
  }

  ColumnVectorBatch::ColumnVectorBatch(int cap) {
    capacity = cap;
    numElements = 0;
    isNull = 0;
    hasNulls = false;
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  LongVectorBatch::~LongVectorBatch() {
    // PASS
  }

  DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
  }

  ByteVectorBatch::~ByteVectorBatch() {
    // PASS
  }

  StructVectorBatch::~StructVectorBatch() {
    // PASS
  }

  LongVectorBatch::LongVectorBatch(int capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(std::unique_ptr<long[]>
                                           (new long[capacity])){
    // PASS
  }

  DoubleVectorBatch::DoubleVectorBatch(int capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(std::unique_ptr<double[]>
                                           (new double[capacity])){
    // PASS
  }

  ByteVectorBatch::ByteVectorBatch(int capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(std::unique_ptr<ByteRange[]>
                                           (new ByteRange[capacity])){
    // PASS
  }

  StructVectorBatch::StructVectorBatch(int capacity
                                       ): ColumnVectorBatch(capacity) {
    // PASS
  }
}
