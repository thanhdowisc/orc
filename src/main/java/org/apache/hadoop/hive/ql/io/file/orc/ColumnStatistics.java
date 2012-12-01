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
package org.apache.hadoop.hive.ql.io.file.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

class ColumnStatistics {
  private final int columnId;
  protected long count;

  private static class BucketStatistics extends ColumnStatistics {
    private final long[] counts;
    BucketStatistics(int columnId, int maxValues) {
      super(columnId);
      counts = new long[maxValues];
    }

    @Override
    void reset() {
      super.reset();
      for(int i=0; i < counts.length; ++i) {
        counts[i] = 0;
      }
    }

    @Override
    void updateBoolean(boolean value) {
      counts[value ? 1 : 0] += 1;
    }

    @Override
    void merge(ColumnStatistics other) {
      super.merge(other);
      BucketStatistics bkt = (BucketStatistics) other;
      if (counts.length != bkt.counts.length) {
        throw new IllegalArgumentException("Merging different sized buckets " +
          "is not supported " + counts.length + " vs " + bkt.counts.length);
      }
      for(int i=0; i < counts.length; ++i) {
        counts[i] += bkt.counts[i];
      }
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.BucketStatistics.Builder bucket =
        OrcProto.BucketStatistics.newBuilder();
      for(int i=0; i < counts.length; ++i) {
        bucket.addCount(counts[i]);
      }
      builder.setBucketStatistics(bucket);
      return builder;
    }
  }

  private static class IntegerStatistics extends ColumnStatistics {
    IntegerStatistics(int columnId) {
      super(columnId);
    }
    long minimum;
    long maximum;
    long sum;
    boolean overflow;

    @Override
    void reset() {
      super.reset();
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      sum = 0;
      overflow = false;
    }

    @Override
    void updateInteger(long value) {
      if (count == 0) {
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += value;
        if ((value >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    void merge(ColumnStatistics other) {
      IntegerStatistics otherInt = (IntegerStatistics) other;
      if (count == 0) {
        minimum = otherInt.minimum;
        maximum = otherInt.maximum;
      } else if (otherInt.minimum < minimum) {
        minimum = otherInt.minimum;
      } else if (otherInt.maximum > maximum) {
        maximum = otherInt.maximum;
      }
      super.merge(other);
      overflow |= otherInt.overflow;
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += otherInt.sum;
        if ((otherInt.sum >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.IntegerStatistics.Builder intb =
        OrcProto.IntegerStatistics.newBuilder();
      if (count != 0) {
        intb.setMinimum(minimum);
        intb.setMaximum(maximum);
        if (!overflow) {
          intb.setSum(sum);
        }
      }
      builder.setIntStatistics(intb);
      return builder;
    }
  }

  private static class DoubleStatistics extends ColumnStatistics {
    double minimum;
    double maximum;
    double sum;
    DoubleStatistics(int columnId) {
      super(columnId);
    }

    @Override
    void reset() {
      super.reset();
      minimum = Double.MAX_VALUE;
      maximum = Double.MIN_VALUE;
      sum = 0;
    }

    @Override
    void updateDouble(double value) {
      if (count == 0) {
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    @Override
    void merge(ColumnStatistics other) {
      DoubleStatistics dbl = (DoubleStatistics) other;
      if (count == 0) {
        minimum = dbl.minimum;
        maximum = dbl.maximum;
      } else if (dbl.minimum < minimum) {
        minimum = dbl.minimum;
      } else if (dbl.maximum > maximum) {
        maximum = dbl.maximum;
      }
      super.merge(other);
      sum += dbl.sum;
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.DoubleStatistics.Builder dbl =
        OrcProto.DoubleStatistics.newBuilder();
      if (count != 0) {
        dbl.setMinimum(minimum);
        dbl.setMaximum(maximum);
      }
      dbl.setSum(sum);
      builder.setDoubleStatistics(dbl);
      return builder;
    }
  }

  private static class StringStatistics extends ColumnStatistics {
    String minimum;
    String maximum;
    StringStatistics(int columnId) {
      super(columnId);
    }

    @Override
    void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    void updateString(String value) {
      if (count == 0) {
        minimum = value;
        maximum = value;
      } else if (minimum.compareTo(value) > 0) {
        minimum = value;
      } else if (maximum.compareTo(value) < 0) {
        maximum = value;
      }
    }

    @Override
    void merge(ColumnStatistics other) {
      StringStatistics str = (StringStatistics) other;
      if (count == 0) {
        minimum = str.minimum;
        maximum = str.maximum;
      } else if (minimum.compareTo(str.minimum) > 0) {
        minimum = str.minimum;
      } else if (maximum.compareTo(str.maximum) < 0) {
        maximum = str.maximum;
      }
      super.merge(other);
    }

    @Override
    OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.StringStatistics.Builder str =
        OrcProto.StringStatistics.newBuilder();
      if (count != 0) {
        str.setMinimum(minimum);
        str.setMaximum(maximum);
      }
      result.setStringStatistics(str);
      return result;
    }
  }

  ColumnStatistics(int columnId) {
    this.columnId = columnId;
    reset();
  }

  void increment() {
    count += 1;
  }

  void updateBoolean(boolean value) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  void updateInteger(long value) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  void updateString(String value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  void merge(ColumnStatistics stats) {
    if (columnId != stats.columnId) {
      throw new IllegalArgumentException("Unmergeable column statistics");
    }
    count += stats.count;
  }

  void reset() {
    count = 0;
  }

  OrcProto.ColumnStatistics.Builder serialize() {
    OrcProto.ColumnStatistics.Builder builder =
      OrcProto.ColumnStatistics.newBuilder();
    builder.setColumn(columnId);
    builder.setNumberOfValues(count);
    return builder;
  }

  public static ColumnStatistics create(int columnId,
                                        ObjectInspector inspector) {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case BOOLEAN:
            return new BucketStatistics(columnId, 2);
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return new IntegerStatistics(columnId);
          case FLOAT:
          case DOUBLE:
            return new DoubleStatistics(columnId);
          case STRING:
            return new StringStatistics(columnId);
          default:
            return new ColumnStatistics(columnId);
        }
      default:
        return new ColumnStatistics(columnId);
    }
  }
}
