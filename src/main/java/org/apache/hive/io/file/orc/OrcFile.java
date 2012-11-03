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

package org.apache.hive.io.file.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;

public class OrcFile {

  public static final String MAGIC = "ORC";

  Reader getReader(Path path) {
    return null;
  }

  public static Writer getWriter(Path path,
                                 TypeInfo type,
                                 ObjectInspector inspector,
                                 CompressionKind compress,
                                 int bufferSize,
                                 Configuration conf) throws IOException {
    return new WriterImpl(path, type, inspector, compress, bufferSize, conf);
  }

  public static void main(String[] args) throws Exception {
    StructTypeInfo struct = new StructTypeInfo();
    ArrayList<String> fieldNames = new ArrayList<String>();
    fieldNames.add("field1");
    fieldNames.add("field2");
    struct.setAllStructFieldNames(fieldNames);
    PrimitiveTypeInfo field1 = new PrimitiveTypeInfo();
    field1.setTypeName("int");
    PrimitiveTypeInfo field2 = new PrimitiveTypeInfo();
    field2.setTypeName("int");
    ArrayList<TypeInfo> fieldTypes = new ArrayList<TypeInfo>();
    fieldTypes.add(field1);
    fieldTypes.add(field2);
    struct.setAllStructFieldTypeInfos(fieldTypes);
    getWriter(new Path("file:///tmp/owen.orc"), type, inspector,
      CompressionKind.NONE, 256 * 1024, new Configuration());
  }
}