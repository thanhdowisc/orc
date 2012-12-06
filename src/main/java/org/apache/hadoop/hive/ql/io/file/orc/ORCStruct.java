package org.apache.hadoop.hive.ql.io.file.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ORCStruct implements Writable {

  private final Object[] fields;
  private final StructObjectInspector inspector;

  ORCStruct(StructObjectInspector inspector) {
    fields = new Object[inspector.getAllStructFieldRefs().size()];
    this.inspector = inspector;
  }

  Object getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  void setFieldValue(int fieldIndex, Object value) {
    fields[fieldIndex] = value;
  }

  StructObjectInspector getObjectInspector() {
    return inspector;
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
      buffer.append(inspector.getAllStructFieldRefs().get(i).getFieldName());
      buffer.append(": ");
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  static class Field implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;
    private final int columnId;

    Field(String name, ObjectInspector inspector, int columnId,
                   int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
      this.columnId = columnId;
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

    public int getColumnId() {
      return columnId;
    }
  }

  static class ORCStructInspector extends StructObjectInspector {
    private final List<StructField> fields;

    ORCStructInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      fields = new ArrayList<StructField>(fieldCount);
      for(int i=0; i < fieldCount; ++i) {
        int fieldType = type.getSubtypes(i);
        fields.add(new Field(type.getFieldNames(i),
          createObjectInspector(fieldType, types), fieldType, i));
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
      return ((ORCStruct) object).fields[((Field) field).offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      ORCStruct struct = (ORCStruct) object;
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

  static ObjectInspector createObjectInspector(int columnId,
                                               List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
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
