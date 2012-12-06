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

import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.RecordReader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

class ReaderImpl implements Reader {

  private static final int DIRECTORY_SIZE_GUESS = 32*1024;

  private final FileInformationImpl fileInformation;
  private final FileSystem fileSystem;
  private final Path path;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final boolean[] included;

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
      this.inspector = ORCStruct.createObjectInspector(0,
        footer.getTypesList());
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

  /**
   * Recurse down into a type subtree turning on all of the sub-columns.
   * @param footer footer of the file
   * @param result the global view of columns that should be included
   * @param typeId the root of tree to enable
   */
  private static void includeColumnRecursive(OrcProto.Footer footer,
                                             boolean[] result,
                                             int typeId) {
    result[typeId] = true;
    OrcProto.Type type = footer.getTypes(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(footer, result, type.getSubtypes(i));
    }
  }

  /**
   * Take the configuration and figure out which columns we need to include.
   * @param footer the footer of the file
   * @param conf the configuration
   * @return true for each column that should be included
   */
  private static boolean[] findIncludedColumns(OrcProto.Footer footer,
                                               Configuration conf) {
    String includedStr =
      conf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR);
    if (includedStr == null) {
      return null;
    } else {
      int numColumns = footer.getTypesCount();
      boolean[] result = new boolean[numColumns+1];
      result[0] = true;
      // currently don't do row filtering
      result[numColumns] = false;
      OrcProto.Type root = footer.getTypes(0);
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      for(int i=0; i < result.length; ++i) {
        result[i] = false;
      }
      for(int i=0; i < root.getSubtypesCount(); ++i) {
        if (included.contains(i)) {
          includeColumnRecursive(footer, result, root.getSubtypes(i));
        }
      }
      // if we are filtering at least one column, return the boolean array
      for(int i=0; i < result.length; ++i) {
        if (!result[i]) {
          return result;
        }
      }
      return null;
    }
  }

  ReaderImpl(FileSystem fs, Path path,
             Configuration conf) throws IOException {
    this.fileSystem = fs;
    this.path = path;
    FSDataInputStream file = fs.open(path);
    long size = fs.getFileStatus(path).getLen();
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    file.seek(size - readSize);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    file.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
      buffer.remaining());
    int psLen = buffer.get(readSize - 1);
    int psOffset = readSize - 1 - psLen;
    CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
      buffer.arrayOffset() + psOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
    int footerSize = (int) ps.getFooterLength();
    bufferSize = (int) ps.getCompressionBlockSize();
    CompressionKind kind;
    switch (ps.getCompression()) {
      case NONE:
        kind = CompressionKind.NONE;
        codec = null;
        break;
      case ZLIB:
        kind = CompressionKind.ZLIB;
        codec = new ZlibCodec();
        break;
      case SNAPPY:
        kind = CompressionKind.SNAPPY;
        codec = new SnappyCodec();
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }
    int extra = Math.max(0, psLen + 1 + footerSize - readSize);
    if (extra > 0) {
      file.seek(size - readSize - extra);
      ByteBuffer extraBuf = ByteBuffer.allocate((int) extra + readSize);
      file.readFully(extraBuf.array(),
        extraBuf.arrayOffset() + extraBuf.position(), extra);
      extraBuf.position(extra);
      extraBuf.put(buffer);
      buffer = extraBuf;
      buffer.position(0);
      buffer.limit(footerSize);
    } else {
      buffer.position(psOffset - footerSize);
      buffer.limit(psOffset);
    }
    InputStream instream = InStream.create("footer", buffer, codec, bufferSize);
    OrcProto.Footer footer = OrcProto.Footer.parseFrom(instream);
    fileInformation = new FileInformationImpl(kind, (int) bufferSize, footer);
    file.close();
    this.included = findIncludedColumns(footer, conf);
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
    return new RecordReaderImpl(fileInformation, fileSystem,  path, offset,
      length, codec, bufferSize, included);
  }

  private static void readDemographics() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path("/tmp/demographics.orc");
    Reader file = new ReaderImpl(FileSystem.getLocal(conf), path, conf);
    Reader.RecordReader reader = file.rows();
    FileWriter writer = new FileWriter("/tmp/demographics.txt");
    ORCStruct row = null;
    while (reader.hasNext()) {
      row = (ORCStruct) reader.next(row);
      for(int i=0; i < 9; ++i) {
        writer.append(row.getFieldValue(i).toString());
        writer.append("|");
      }
      writer.append("\n");
    }
    writer.close();
  }

  private static void readSales() throws Exception {
    Configuration conf = new Configuration();
    long start = System.currentTimeMillis();
    Path path = new Path("/tmp/store_sales.orc");
    Reader file = new ReaderImpl(FileSystem.getLocal(conf), path, conf);
    Reader.RecordReader reader = file.rows();
    FileWriter writer = new FileWriter("/tmp/store_sales.txt");
    ORCStruct row = null;
    long r = 0;
    while (reader.hasNext()) {
      row = (ORCStruct) reader.next(row);
      for(int i=0; i < 23; ++i) {
        Object field = row.getFieldValue(i);
        if (field != null) {
          String val = field.toString();
          if (i > 10) {
            boolean negative = false;
            if (val.charAt(0) == '-') {
              negative = true;
              val = val.substring(1);
            }
            int len = val.length();
            if (len == 1 || len == 2) {
              val = "0.0".substring(0, 4-len) + val;
            } else {
              val = val.substring(0, len - 2) + "." +
                val.substring(len - 2, len);
            }
            if (negative) {
              val = "-" + val;
            }
          }
          writer.append(val);
        }
        writer.append("|");
      }
      writer.append("\n");
    }
    writer.close();
    long time = System.currentTimeMillis() - start;
    System.out.println("time = " + time);
  }

  public static void main(String[] args) throws Exception {
    readDemographics();
    readSales();
  }
}
