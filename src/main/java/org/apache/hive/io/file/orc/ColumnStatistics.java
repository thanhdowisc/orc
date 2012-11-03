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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

class ColumnStatistics {
  private final int columnId;
  private final TypeInfo type;
  private final boolean keepMinMax;
  private Comparable<Object> minimum;
  private Comparable<Object> maximum;
  private long count;

  ColumnStatistics(int columnId, TypeInfo type) {
    this.columnId = columnId;
    this.type = type;
    if (type.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      PrimitiveObjectInspector.PrimitiveCategory kind= getPrimitiveCategory();
      keepMinMax =
        (kind != PrimitiveObjectInspector.PrimitiveCategory.BINARY);
    } else {
      keepMinMax = false;
    }
    reset();
  }

  long getCount() {
    return count;
  }

  void increment() {
    count += 1;
  }

  private PrimitiveObjectInspector.PrimitiveCategory getPrimitiveCategory() {
    return ((PrimitiveTypeInfo) type).getPrimitiveCategory();
  }

  @SuppressWarnings("unchecked")
  void update(Object obj) {
    if (keepMinMax) {
      if (minimum == null || minimum.compareTo(obj) > 0) {
        minimum = (Comparable) obj;
      }
      if (maximum == null || maximum.compareTo(obj) < 0) {
        maximum = (Comparable) obj;
      }
    }
  }

  private ByteString serialize(Comparable obj) throws IOException {
    switch (type.getCategory()) {
      case PRIMITIVE:
        switch (getPrimitiveCategory()) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            ByteString.Output out = ByteString.newOutput();
            SerializationUtils.writeVslong(out,
              ((Number) obj).longValue());
            return out.toByteString();
          case FLOAT:
            out = ByteString.newOutput();
            SerializationUtils.writeFloat(out, (Float) obj);
            return out.toByteString();
          case DOUBLE:
            out = ByteString.newOutput();
            SerializationUtils.writeDouble(out, (Double) obj);
            return out.toByteString();
          case STRING:
            return ByteString.copyFromUtf8((String) obj);
          default:
            throw new IllegalArgumentException("Can't serialize primitive "
              + getPrimitiveCategory());
        }
      default:
        throw new IllegalArgumentException("Can't serialize non-primitive " +
          type.getCategory());
    }
  }

  ByteString getSerializedMinimum() throws IOException {
    return minimum == null ? null : serialize(minimum);
  }

  ByteString getSerializedMaximum() throws IOException {
    return maximum == null ? null : serialize(maximum);
  }

  void merge(ColumnStatistics stats) {
    if (columnId != stats.columnId ||
      type != stats.type) {
      throw new IllegalArgumentException("Unmergeable column statistics");
    }
    count += stats.count;
    if (keepMinMax) {
      if (stats.minimum != null) {
        if (minimum == null || minimum.compareTo(stats.minimum) > 0) {
          minimum = stats.minimum;
        }
      }
      if (stats.maximum != null) {
        if (maximum == null || maximum.compareTo(stats.maximum) < 0) {
          maximum = stats.maximum;
        }
      }
    }
  }

  void reset() {
    minimum = null;
    maximum = null;
    count = 0;
  }
}
