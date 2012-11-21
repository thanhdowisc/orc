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

import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ReaderImpl implements Reader {

  private final FileInformation fileInformation;
  private static final int DIRECTORY_SIZE_GUESS = 64*1024;

  private static class StripeInformationImpl
      implements Reader.StripeInformation {
    private final OrcProto.StripeInformation stripe;

    StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getLength();
    }

    @Override
    public long getTailLength() {
      return stripe.getTailLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

  }

  private static class StructObject {
    final Object[] fields;

    StructObject(int fields) {
      this.fields = new Object[fields];
    }
  }

  private static class StructFieldImpl implements StructField {
    private final String fieldName;
    private final ObjectInspector inspector;
    private final int offset;

    StructFieldImpl(String fieldName, ObjectInspector inspector, int offset) {
      this.fieldName = fieldName;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return fieldName;
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

  private static class StructObjectInspectorImpl extends StructObjectInspector {
    private final List<StructFieldImpl> fields;

    StructObjectInspectorImpl(List<StructFieldImpl> fields) {
      this.fields = fields;
    }

    @Override
    public List<StructFieldImpl> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for(StructField f: fields) {
        if (s.equals(f.getFieldName())) {
          return f;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object o, StructField structField) {
      return ((StructObject) o).fields[((StructFieldImpl) structField).offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
      return Arrays.asList(((StructObject) o).fields);
    }

    @Override
    public String getTypeName() {
      StringBuilder buf = new StringBuilder();
      buf.append("struct{");
      boolean first = true;
      for(StructFieldImpl field: fields) {
        if (first) {
          first = false;
        } else {
          buf.append(", ");
        }
        buf.append(field.getFieldName());
        buf.append(": ");
        buf.append(field.getFieldObjectInspector().getTypeName());
      }
      buf.append("}");
      return buf.toString();
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }
  }

  private static ObjectInspector buildInspector(OrcProto.Footer footer,
                                                int columnId) {
    OrcProto.Type type = footer.getTypes(columnId);
    ObjectInspector result;
    switch (type.getKind()) {
      case INT:
        result = PrimitiveObjectInspectorFactory.
          getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.
            PrimitiveCategory.INT);
        break;
      case STRING:
        result = PrimitiveObjectInspectorFactory.
          getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.
            PrimitiveCategory.STRING);
        break;
      case STRUCT:
        int fieldCnt = type.getSubtypesCount();
        List<StructFieldImpl> fields = new ArrayList<StructFieldImpl>(fieldCnt);
        for(int i=0; i<fieldCnt; ++i) {
          StructFieldImpl field = new StructFieldImpl(type.getFieldNames(i),
            buildInspector(footer, type.getSubtypes(i)), i);
        }
        result = new StructObjectInspectorImpl(fields);
        break;
      default:
        throw new IllegalArgumentException("Don't know kind " + type.getKind());
    }
    return result;
  }

  private static class FileInformationImpl implements FileInformation {
    private final CompressionKind compression;
    private final int bufferSize;
    private final OrcProto.Footer footer;
    private final ObjectInspector inspector;

    FileInformationImpl(CompressionKind compression,
                        int bufferSize,
                        OrcProto.Footer footer) {
      this.compression = compression;
      this.bufferSize = bufferSize;
      this.footer = footer;
      this.inspector = buildInspector(footer, 0);
    }

    @Override
    public long getNumberOfRows() {
      return footer.getNumberOfRows();
    }

    @Override
    public Iterable<String> getMetadataKeys() {
      List<String> result = new ArrayList<String>();
      for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
        result.add(item.getName());
      }
      return result;
    }

    @Override
    public ByteBuffer getMetadataValue(String key) {
      for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
        if (item.hasName() && item.getName().equals(key)) {
          return item.getValue().asReadOnlyByteBuffer();
        }
      }
      throw new IllegalArgumentException("Can't find user metadata " + key);
    }

    @Override
    public CompressionKind getCompression() {
      return compression;
    }

    @Override
    public Iterable<StripeInformation> getStripes() {
      return new Iterable<StripeInformation>(){

        @Override
        public Iterator<StripeInformation> iterator() {
          return new Iterator<StripeInformation>(){
            Iterator<OrcProto.StripeInformation> inner =
              footer.getStripesList().iterator();

            @Override
            public boolean hasNext() {
              return inner.hasNext();
            }

            @Override
            public StripeInformation next() {
              return new StripeInformationImpl(inner.next());
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException("remove unsupported");
            }
          };
        }
      };
    }

    @Override
    public ObjectInspector getObjectInspector() {
      return inspector;
    }

    @Override
    public long getLength() {
      return footer.getBodyLength() + footer.getHeaderLength();
    }

    @Override
    public ColumnStatistics[] getStatistics() {
      ColumnStatistics[] result = new ColumnStatistics[footer.getTypesCount()];
      for(OrcProto.ColumnStatistics stats: footer.getStatisticsList()) {
        result[stats.getColumn()] = createColumnStatistics(stats);
      }
      return result;
    }

    public String toString() {
      return footer.toString();
    }
  }

  private static class ColumnStatisticsImpl implements ColumnStatistics {
    protected final OrcProto.ColumnStatistics stats;

    ColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      this.stats = stats;
    }

    @Override
    public long getNumberOfValues() {
      return stats.getNumberOfValues();
    }
  }

  private static class BooleanColumnStatisticsImpl extends ColumnStatisticsImpl
      implements BooleanColumnStatistics {

    BooleanColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
    }

    @Override
    public long getFalseCount() {
      return stats.getBucketStatistics().getCount(0);
    }

    @Override
    public long getTrueCount() {
      return stats.getBucketStatistics().getCount(1);
    }
  }

  private static class IntegerColumnStatisticsImpl extends ColumnStatisticsImpl
      implements IntegerColumnStatistics {

    IntegerColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
    }

    @Override
    public long getMinimum() {
      OrcProto.IntegerStatistics intStats = stats.getIntStatistics();
      return intStats.hasMinimum() ? intStats.getMinimum() : Long.MAX_VALUE;
    }

    @Override
    public long getMaximum() {
      OrcProto.IntegerStatistics intStats = stats.getIntStatistics();
      return intStats.hasMaximum() ? intStats.getMaximum() : Long.MIN_VALUE;
    }

    @Override
    public boolean isSumDefined() {
      return stats.getIntStatistics().hasSum();
    }

    @Override
    public long getSum() {
      return stats.getIntStatistics().hasSum() ?
        stats.getIntStatistics().getSum() : 0;
    }
  }

  private static class DoubleColumnStatisticsImpl extends ColumnStatisticsImpl
    implements DoubleColumnStatistics {

    DoubleColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
    }

    @Override
    public double getMinimum() {
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      return dbl.hasMinimum() ? dbl.getMinimum() : Double.MAX_VALUE;
    }

    @Override
    public double getMaximum() {
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      return dbl.hasMaximum() ? dbl.getMaximum() : Double.MIN_VALUE;
    }

    @Override
    public double getSum() {
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      return dbl.hasSum() ? dbl.getSum() : 0;
    }
  }

  private static class StringColumnStatisticsImpl extends ColumnStatisticsImpl
      implements StringColumnStatistics {

    StringColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
    }

    @Override
    public String getMinimum() {
      return stats.getStringStatistics().hasMinimum() ?
        stats.getStringStatistics().getMinimum() : null;
    }

    @Override
    public String getMaximum() {
      return stats.getStringStatistics().hasMaximum() ?
        stats.getStringStatistics().getMaximum() : null;
    }
  }

  private static ColumnStatisticsImpl
      createColumnStatistics(OrcProto.ColumnStatistics stats) {
    if (stats.hasBucketStatistics()) {
      return new BooleanColumnStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerColumnStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleColumnStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringColumnStatisticsImpl(stats);
    }
    return new ColumnStatisticsImpl(stats);
  }

  ReaderImpl(FileSystem fs, Path path) throws IOException {
    FSDataInputStream input = fs.open(path);
    long size = fs.getFileStatus(path).getLen();
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    input.seek(size - readSize);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    input.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
      buffer.remaining());
    int psLen = buffer.get(readSize - 1);
    int psOffset = readSize - 1 - psLen;
    CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
      buffer.arrayOffset() + psOffset, psLen);
    buffer.limit(psOffset);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    System.out.println("postscript:");
    System.out.println(ps);
    long footerSize = ps.getFooterLength();
    long bufferSize = ps.getCompressionBlockSize();
    CompressionKind kind;
    switch (ps.getCompression()) {
      case NONE:
        kind = CompressionKind.NONE;
        break;
      case ZLIB:
        kind = CompressionKind.ZLIB;
        break;
      case LZO:
        kind = CompressionKind.LZO;
        break;
      case SNAPPY:
        kind = CompressionKind.SNAPPY;
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    int extra = (int) Math.max(0, psLen + 1 + footerSize - readSize);
    if (extra > 0) {
      input.seek(size - readSize - extra);
      ByteBuffer extraBuf = ByteBuffer.allocate((int) extra + readSize);
      input.readFully(extraBuf.array(),
        extraBuf.arrayOffset() + extraBuf.position(), extra);
      extraBuf.position(extra);
      extraBuf.put(buffer);
      buffer = extraBuf;
    } else {
      buffer.position((int) (psOffset - footerSize));
    }
    CodedInputStream stm = CodedInputStream.newInstance(buffer.array(),
      buffer.arrayOffset() + buffer.position(), buffer.remaining());
    OrcProto.Footer footer = OrcProto.Footer.parseFrom(stm);
    System.out.println("footer:");
    System.out.println(footer);
    fileInformation = new FileInformationImpl(kind, (int) bufferSize, footer);
  }

  @Override
  public FileInformation getFileInformation() throws IOException {
    return fileInformation;
  }

  @Override
  public RecordReader rows() throws IOException {
    return rows(0, Long.MAX_VALUE);
  }

  @Override
  public RecordReader rows(long offset, long length) throws IOException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path("/tmp/owen.orc");
    ReaderImpl reader = new ReaderImpl(FileSystem.getLocal(conf), path);
  }
}
