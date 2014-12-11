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

class ColumnPrinter {
public:
  virtual void printRow(unsigned long rowId) = 0;
  virtual ~ColumnPrinter();
};

class LongColumnPrinter: public ColumnPrinter {
private:
  const long* data;
public:
  LongColumnPrinter(const orc::ColumnVectorBatch& batch);
  ~LongColumnPrinter() = default;
  void printRow(unsigned long rowId) override;
};

class DoubleColumnPrinter: public ColumnPrinter {
private:
  const double* data;
public:
  DoubleColumnPrinter(const orc::ColumnVectorBatch& batch);
  virtual ~DoubleColumnPrinter() = default;
  void printRow(unsigned long rowId) override;
};

class StringColumnPrinter: public ColumnPrinter {
private:
  char** start;
  long* length;
public:
  StringColumnPrinter(const orc::ColumnVectorBatch& batch);
  virtual ~StringColumnPrinter() = default;
  void printRow(unsigned long rowId) override;
};

class StructColumnPrinter: public ColumnPrinter {
private:
  ColumnPrinter** fields;
  unsigned long numFields;
public:
  StructColumnPrinter(const orc::ColumnVectorBatch& batch);
  virtual ~StructColumnPrinter() = default;
  void printRow(unsigned long rowId) override;
};

ColumnPrinter::~ColumnPrinter() {
}

LongColumnPrinter::LongColumnPrinter(const orc::ColumnVectorBatch& batch) {
  data = dynamic_cast<const orc::LongVectorBatch&>(batch).data.get();
}

void LongColumnPrinter::printRow(unsigned long rowId) {
  std::cout << data[rowId];
}

DoubleColumnPrinter::DoubleColumnPrinter(const orc::ColumnVectorBatch& batch) {
  data = dynamic_cast<const orc::DoubleVectorBatch&>(batch).data.get();
}

void DoubleColumnPrinter::printRow(unsigned long rowId) {
  std::cout << data[rowId];
}

StringColumnPrinter::StringColumnPrinter(const orc::ColumnVectorBatch& batch) {
  start = dynamic_cast<const orc::StringVectorBatch&>(batch).data.get();
  length = dynamic_cast<const orc::StringVectorBatch&>(batch).length.get();
}

void StringColumnPrinter::printRow(unsigned long rowId) {
  std::cout.write(start[rowId], length[rowId]);
}

StructColumnPrinter::StructColumnPrinter(const orc::ColumnVectorBatch& batch) {
  const orc::StructVectorBatch& structBatch = 
      dynamic_cast<const orc::StructVectorBatch&>(batch);
  numFields = structBatch.numFields;
  fields = new ColumnPrinter*[numFields];
  for(unsigned int i=0; i < numFields; ++i) {
    const orc::ColumnVectorBatch& subBatch = *(structBatch.fields.get()[i]);
    if (typeid(subBatch) == typeid(orc::LongVectorBatch)) {
      fields[i] = new LongColumnPrinter(subBatch);
    } else if (typeid(subBatch) == typeid(orc::DoubleVectorBatch)) {
      fields[i] = new DoubleColumnPrinter(subBatch);
    } else if (typeid(subBatch) == typeid(orc::StringVectorBatch)) {
      fields[i] = new StringColumnPrinter(subBatch);
    } else if (typeid(subBatch) == typeid(orc::StructVectorBatch)) {
      fields[i] = new StructColumnPrinter(subBatch);
    } else {
      throw std::logic_error("unknown batch type");
    }
  }
}

void StructColumnPrinter::printRow(unsigned long rowId) {
  if (numFields > 0) {
    fields[0]->printRow(rowId);
    for (unsigned long i=1; i < numFields; ++i) {
      std::cout << "\t";
      fields[i]->printRow(rowId);
    }
    std::cout << "\n";
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-dump <filename>\n";
  }
  std::cout << std::nounitbuf;
  orc::ReaderOptions opts;
  // opts.include({1});
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(std::string(argv[1])), opts);
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  StructColumnPrinter printer(*batch);

  while (reader->next(*batch)) {
    for(unsigned long i=0; i < batch->numElements; ++i) {
      printer.printRow(i);
    }
  }
  return 0;
}



