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

#include "orc/OrcFile.hh"

#include <string>
#include <memory>
#include <iostream>
#include <string>

void printRow(const orc::ColumnVectorBatch* batch, unsigned long rowId) {
  if (typeid(*batch) == typeid(orc::LongVectorBatch)) {
    std::cout 
      << dynamic_cast<const orc::LongVectorBatch*>(batch)->data.get()[rowId];
  } else if (typeid(*batch) == typeid(orc::DoubleVectorBatch)) {
    std::cout 
      << dynamic_cast<const orc::DoubleVectorBatch*>(batch)->data.get()[rowId];
  } else if (typeid(*batch) == typeid(orc::StringVectorBatch)) {
    const orc::StringVectorBatch* strings = 
      dynamic_cast<const orc::StringVectorBatch*>(batch);
    std::cout.write(strings->data.get()[rowId],
                    strings->length.get()[rowId]);
  } else if (typeid(*batch) == typeid(orc::StructVectorBatch)) {
    const orc::StructVectorBatch* structs = 
      dynamic_cast<const orc::StructVectorBatch*>(batch);
    if (structs->numFields > 0) {
      printRow(structs->fields.get()[0].get(), rowId);
      for(unsigned long i=1; i < structs->numFields; ++i) {
        std::cout << "\t";
        printRow(structs->fields.get()[i].get(), rowId);
      }
      std::cout << "\n";
    }
  } else {
    throw std::logic_error("Unknown vector type");
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-dump <filename>\n";
  }
  orc::ReaderOptions opts;
  // opts.include({1});
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(std::string(argv[1])), opts);
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);

  while (reader->next(*batch)) {
    for(unsigned long i=0; i < batch->numElements; ++i) {
      printRow(batch.get(), i);
    }
  }
  return 0;
}



