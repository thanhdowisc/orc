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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class OrcStruct implements Writable {

  private final Object[] fields;

  OrcStruct(int children) {
    fields = new Object[children];
  }

  Object getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  void setFieldValue(int fieldIndex, Object value) {
    fields[fieldIndex] = value;
  }

   @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{");
    for(int i=0; i < fields.length; ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  static class Field implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;

    Field(String name, ObjectInspector inspector, int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  static class ORCStructInspector extends StructObjectInspector {
    private final List<StructField> fields;

    ORCStructInspector(StructTypeInfo info) {
      ArrayList<String> fieldNames = info.getAllStructFieldNames();
      ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
      fields = new ArrayList<StructField>(fieldNames.size());
      for(int i=0; i < fieldNames.size(); ++i) {
        fields.add(new Field(fieldNames.get(i),
          createObjectInspector(fieldTypes.get(i)), i));
      }
    }

    ORCStructInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      fields = new ArrayList<StructField>(fieldCount);
      for(int i=0; i < fieldCount; ++i) {
        int fieldType = type.getSubtypes(i);
        fields.add(new Field(type.getFieldNames(i),
          createObjectInspector(fieldType, types), i));
      }
    }

    @Override
    public List<StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for(StructField field: fields) {
        if (field.getFieldName().equals(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      return ((OrcStruct) object).fields[((Field) field).offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      OrcStruct struct = (OrcStruct) object;
      List<Object> result = new ArrayList<Object>(struct.fields.length);
      for(Object child: struct.fields) {
        result.add(child);
      }
      return result;
    }

    @Override
    public String getTypeName() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("struct{");
      for(int i=0; i < fields.size(); ++i) {
        StructField field = fields.get(i);
        if (i != 0) {
          buffer.append(", ");
        }
        buffer.append(field.getFieldName());
        buffer.append(": ");
        buffer.append(field.getFieldObjectInspector().getTypeName());
      }
      buffer.append("}");
      return buffer.toString();
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }
  }

  static ObjectInspector createObjectInspector(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
          case FLOAT:
            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
          case INT:
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          case STRING:
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          default:
            throw new IllegalArgumentException("Unknown primitive type " +
              ((PrimitiveTypeInfo) info).getPrimitiveCategory());
        }
      case STRUCT:
        return new ORCStructInspector((StructTypeInfo) info);
      default:
        throw new IllegalArgumentException("Unknown type " +
          info.getCategory());
    }
  }

  static ObjectInspector createObjectInspector(int columnId,
                                               List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case FLOAT:
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      case INT:
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      case STRING:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case STRUCT:
        return new ORCStructInspector(columnId, types);
      default:
        throw new UnsupportedOperationException("Unknown type " +
          type.getKind());
    }
  }
}
