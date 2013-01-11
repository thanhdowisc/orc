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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class WriterImpl implements Writer {

  private static final int ROW_INDEX_FREQUENCY = 10000;
  private static final int BUFFER_SIZE = 256 * 1024;

  private final FileSystem fs;
  private final Path path;
  private final long stripeSize;
  private final CompressionKind compress;
  private final CompressionCodec codec;
  private final int bufferSize;
  // how many compressed bytes in the current stripe so far
  private long bytesInStripe = 0;
  // the streams that make up the current stripe
  private final Map<StreamName,BufferedStream> streams =
    new TreeMap<StreamName, BufferedStream>();

  private FSDataOutputStream rawWriter = null;
  // the compressed metadata information outStream
  private OutStream writer = null;
  // a protobuf outStream around writer
  private CodedOutputStream protobufWriter = null;
  private long headerLength;
  private int columnCount;
  private long rowCount = 0;
  private long rowsInStripe = 0;
  private int rowsInIndex = 0;
  private final List<OrcProto.StripeInformation> stripes =
    new ArrayList<OrcProto.StripeInformation>();
  private final Map<String, ByteString> userMetadata =
    new TreeMap<String, ByteString>();
  private final SectionWriter sectionWriter = new SectionWriterImpl();
  private final TreeWriter treeWriter;
  private final OrcProto.RowIndex.Builder rowIndex =
      OrcProto.RowIndex.newBuilder();

  WriterImpl(FileSystem fs,
             Path path,
             ObjectInspector inspector,
             long stripeSize,
             CompressionKind compress,
             int bufferSize) throws IOException {
    this.fs = fs;
    this.path = path;
    this.stripeSize = stripeSize;
    this.compress = compress;
    switch (compress) {
      case NONE:
        codec = null;
        break;
      case ZLIB:
        codec = new ZlibCodec();
        break;
      case SNAPPY:
        codec = new SnappyCodec();
        break;
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
          compress);
    }
    this.bufferSize = bufferSize;
    treeWriter = createTreeWriter(inspector, sectionWriter, false);
    // record the current position as the start of the stripe
    treeWriter.recordPosition();
  }

  private class BufferedStream implements OutStream.OutputReceiver {
    final OutStream outStream;
    final List<ByteBuffer> output = new ArrayList<ByteBuffer>();

    BufferedStream(String name, int bufferSize,
                   CompressionCodec codec) throws IOException {
      outStream = new OutStream(name, bufferSize, codec, this);
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.add(buffer);
      bytesInStripe += buffer.remaining();
    }

    public void flush() throws IOException {
      outStream.flush();
    }

    public void clear() throws IOException {
      outStream.clear();
      output.clear();
    }

    @Override
    public long getPosition() {
      long result = 0;
      for(ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }

    void spillTo(OutputStream out) throws IOException {
      for(ByteBuffer buffer: output) {
        out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
      }
    }
  }

  private class DirectStream implements OutStream.OutputReceiver {
    final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        buffer.remaining());
    }

    @Override
    public long getPosition() throws IOException {
      return output.getPos();
    }
  }

  interface SectionWriter {
    /**
     * Append a set of bytes onto a section
     * @param column the column id for the section
     * @param kind the kind of section
     * @return The output outStream that the section needs to be written to.
     * @throws IOException
     */
    PositionedOutputStream createSection(int column,
                                         OrcProto.StripeSection.Kind kind
                                         ) throws IOException;

    /**
     * Get the next column id.
     * @return a number from 0 to the number of columns - 1
     */
    int getNextColumnId();
  }

  private static class RowIndexPositionRecorder implements PositionRecorder {
    private final OrcProto.ColumnPosition.Builder builder =
      OrcProto.ColumnPosition.newBuilder();

    RowIndexPositionRecorder(int columnId) {
      builder.setColumn(columnId);
    }

    @Override
    public void addPosition(long position) {
      builder.addOffsets(position);
    }

    public OrcProto.ColumnPosition getPosition() {
      OrcProto.ColumnPosition result = builder.build();
      builder.clearOffsets();
      return result;
    }
  }

  private class SectionWriterImpl implements SectionWriter {

    @Override
    public PositionedOutputStream createSection(int column,
                                                OrcProto.StripeSection.Kind kind
                                                ) throws IOException {
      StreamName name = new StreamName(column, kind);
      BufferedStream result = streams.get(name);
      if (result == null) {
        result = new BufferedStream(name.toString(), bufferSize, codec);
        streams.put(name, result);
      }
      return result.outStream;
    }

    @Override
    public int getNextColumnId() {
      return columnCount++;
    }
  }

  private static abstract class TreeWriter {
    protected final int id;
    protected final ObjectInspector inspector;
    protected final SectionWriter writer;
    private final BitFieldWriter isPresent;
    protected final ColumnStatisticsImpl stripeStatistics;
    private final ColumnStatisticsImpl fileStatistics;
    protected TreeWriter[] childrenWriters;
    protected final RowIndexPositionRecorder rowIndexPosition;

    TreeWriter(int columnId, ObjectInspector inspector,
               SectionWriter writer, boolean nullable) throws IOException {
      this.id = columnId;
      this.inspector = inspector;
      this.writer = writer;
      if (nullable) {
        isPresent = new BitFieldWriter(writer.createSection(id,
          OrcProto.StripeSection.Kind.PRESENT), 1);
      } else {
        isPresent = null;
      }
      stripeStatistics = ColumnStatisticsImpl.create(id, inspector);
      fileStatistics = ColumnStatisticsImpl.create(id, inspector);
      childrenWriters = new TreeWriter[0];
      rowIndexPosition = new RowIndexPositionRecorder(id);
    }

    void write(Object obj) throws IOException {
      if (obj != null) {
        stripeStatistics.increment();
      }
      if (isPresent != null) {
        isPresent.append(obj == null ? 0: 1);
      }
    }

    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      if (isPresent != null) {
        isPresent.flush();
      }
    }

    TreeWriter[] getChildrenWriters() {
      return childrenWriters;
    }

    void recordPosition() throws IOException {
      if (isPresent != null) {
        isPresent.getPosition(rowIndexPosition);
      }
      for(TreeWriter child: childrenWriters) {
        child.recordPosition();
      }
    }

    void getPosition(OrcProto.RowIndexEntry.Builder entry) {
      entry.addPositions(rowIndexPosition.getPosition());
      entry.addStatistics(stripeStatistics.serialize());
      fileStatistics.merge(stripeStatistics);
      stripeStatistics.reset();
      for(TreeWriter child: childrenWriters) {
        child.getPosition(entry);
      }
    }

  }

  private static class BooleanTreeWriter extends TreeWriter {
    private final BitFieldWriter writer;

    BooleanTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      PositionedOutputStream out = writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA);
      this.writer = new BitFieldWriter(out, 1);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        boolean val = ((BooleanObjectInspector) inspector).get(obj);
        stripeStatistics.updateBoolean(val);
        writer.append(val ? 1 : 0);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      writer.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      writer.getPosition(rowIndexPosition);
    }
  }

  private static class ByteTreeWriter extends TreeWriter {
    private final RunLengthByteWriter writer;

    ByteTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.writer = new RunLengthByteWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA));
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        byte val = ((ByteObjectInspector) inspector).get(obj);
        stripeStatistics.updateInteger(val);
        writer.write(val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      writer.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      writer.getPosition(rowIndexPosition);
    }
  }

  private static class IntegerTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter writer;
    private final ShortObjectInspector shortInspector;
    private final IntObjectInspector intInspector;
    private final LongObjectInspector longInspector;

    IntegerTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      PositionedOutputStream out = writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA);
      this.writer = new RunLengthIntegerWriter(out, true);
      if (inspector instanceof IntObjectInspector) {
        intInspector = (IntObjectInspector) inspector;
        shortInspector = null;
        longInspector = null;
      } else {
        intInspector = null;
        if (inspector instanceof LongObjectInspector) {
          longInspector = (LongObjectInspector) inspector;
          shortInspector = null;
        } else {
          shortInspector = (ShortObjectInspector) inspector;
          longInspector = null;
        }
      }
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        long val;
        if (intInspector != null) {
          val = intInspector.get(obj);
        } else if (longInspector != null) {
          val = longInspector.get(obj);
        } else {
          val = shortInspector.get(obj);
        }
        stripeStatistics.updateInteger(val);
        writer.write(val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      writer.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      writer.getPosition(rowIndexPosition);
    }
  }

  private static class FloatTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;

    FloatTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.stream = writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        float val = ((FloatObjectInspector) inspector).get(obj);
        stripeStatistics.updateDouble(val);
        SerializationUtils.writeFloat(stream, val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      stream.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      stream.getPosition(rowIndexPosition);
    }
  }

  private static class DoubleTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;

    DoubleTreeWriter(int columnId,
                    ObjectInspector inspector,
                    SectionWriter writer,
                    boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.stream = writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        double val = ((DoubleObjectInspector) inspector).get(obj);
        stripeStatistics.updateDouble(val);
        SerializationUtils.writeDouble(stream, val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      stream.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      stream.getPosition(rowIndexPosition);
    }
  }

  private static class StringTreeWriter extends TreeWriter {
    private final PositionedOutputStream stringOutput;
    private final RunLengthIntegerWriter lengthOutput;
    private final RunLengthIntegerWriter rowOutput;
    private final RunLengthIntegerWriter countOutput;
    private final StringRedBlackTree dictionary = new StringRedBlackTree();
    private final DynamicIntArray rows = new DynamicIntArray();

    StringTreeWriter(int columnId,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      stringOutput = writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_DATA);
      lengthOutput = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.LENGTH), false);
      rowOutput = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.DATA), false);
      countOutput = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_COUNT), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        String val = ((StringObjectInspector) inspector)
          .getPrimitiveJavaObject(obj);
        rows.add(dictionary.add(val));
        stripeStatistics.updateString(val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      final int[] dumpOrder = new int[dictionary.size()];
      dictionary.visit(new StringRedBlackTree.Visitor() {
        int currentId = 0;
        @Override
        public void visit(StringRedBlackTree.VisitorContext context) throws IOException {
          context.writeBytes(stringOutput);
          lengthOutput.write(context.getLength());
          dumpOrder[context.getOriginalPosition()] = currentId++;
          countOutput.write(context.getCount());
        }
      });
      int length = rows.size();
      for(int i=0; i < length; ++i) {
        rowOutput.write(dumpOrder[rows.get(i)]);
      }
      stringOutput.flush();
      lengthOutput.flush();
      rowOutput.flush();
      countOutput.flush();
      dictionary.clear();
      rows.clear();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      // TODO????
    }
  }

  private static class BinaryTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;
    private final RunLengthIntegerWriter length;

    BinaryTreeWriter(int columnId,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.stream = writer.createSection(id,
          OrcProto.StripeSection.Kind.DATA);
      this.length = new RunLengthIntegerWriter(writer.createSection(id,
          OrcProto.StripeSection.Kind.LENGTH), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        BytesWritable val =
            ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(obj);
        stream.write(val.getBytes(), 0, val.getLength());
        length.write(val.getLength());
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      stream.flush();
      length.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      stream.getPosition(rowIndexPosition);
      length.getPosition(rowIndexPosition);
    }
  }

  static final int MILLIS_PER_SECOND = 1000;
  static final long BASE_TIMESTAMP =
      Timestamp.valueOf("2015-01-01 00:00:00").getTime() / MILLIS_PER_SECOND;

  private static class TimestampTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter seconds;
    private final RunLengthIntegerWriter nanos;

    TimestampTreeWriter(int columnId,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.seconds = new RunLengthIntegerWriter(writer.createSection(id,
          OrcProto.StripeSection.Kind.DATA), true);
      this.nanos = new RunLengthIntegerWriter(writer.createSection(id,
          OrcProto.StripeSection.Kind.NANO_DATA), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        Timestamp val =
            ((TimestampObjectInspector) inspector).
                getPrimitiveJavaObject(obj);
        seconds.write((val.getTime() / MILLIS_PER_SECOND) - BASE_TIMESTAMP);
        nanos.write(formatNanos(val.getNanos()));
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      seconds.flush();
      nanos.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      seconds.getPosition(rowIndexPosition);
      nanos.getPosition(rowIndexPosition);
    }

    private static long formatNanos(int nanos) {
      if (nanos == 0) {
        return 0;
      } else if (nanos % 100 != 0) {
        return ((long) nanos) << 3;
      } else {
        nanos /= 100;
        int trailingZeros = 1;
        while (nanos % 10 == 0 && trailingZeros < 7) {
          nanos /= 10;
          trailingZeros += 1;
        }
        return ((long) nanos) << 3 | trailingZeros;
      }
    }
  }

  private static class StructTreeWriter extends TreeWriter {
    private final List<? extends StructField> fields;
    StructTreeWriter(int columnId,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      StructObjectInspector structObjectInspector =
        (StructObjectInspector) inspector;
      fields = structObjectInspector.getAllStructFieldRefs();
      childrenWriters = new TreeWriter[fields.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(
          fields.get(i).getFieldObjectInspector(), writer, true);
      }
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        StructObjectInspector insp = (StructObjectInspector) inspector;
        for(int i = 0; i < fields.size(); ++i) {
          StructField field = fields.get(i);
          TreeWriter writer = childrenWriters[i];
          writer.write(insp.getStructFieldData(obj, field));
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder);
      }
    }
  }

  private static class ListTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter lengths;

    ListTreeWriter(int columnId,
                   ObjectInspector inspector,
                   SectionWriter writer,
                   boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      ListObjectInspector listObjectInspector = (ListObjectInspector) inspector;
      childrenWriters = new TreeWriter[1];
      childrenWriters[0] =
        createTreeWriter(listObjectInspector.getListElementObjectInspector(),
          writer, true);
      lengths =
        new RunLengthIntegerWriter(writer.createSection(columnId,
          OrcProto.StripeSection.Kind.LENGTH), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        ListObjectInspector insp = (ListObjectInspector) inspector;
        int len = insp.getListLength(obj);
        lengths.write(len);
        for(int i=0; i < len; ++i) {
          childrenWriters[0].write(insp.getListElement(obj, i));
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder);
      }
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      lengths.getPosition(rowIndexPosition);
    }
  }

  private static class MapTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter lengths;

    MapTreeWriter(int columnId,
                  ObjectInspector inspector,
                  SectionWriter writer,
                  boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      MapObjectInspector insp = (MapObjectInspector) inspector;
      childrenWriters = new TreeWriter[2];
      childrenWriters[0] =
        createTreeWriter(insp.getMapKeyObjectInspector(), writer, true);
      childrenWriters[1] =
        createTreeWriter(insp.getMapValueObjectInspector(), writer, true);
      lengths =
        new RunLengthIntegerWriter(writer.createSection(columnId,
          OrcProto.StripeSection.Kind.LENGTH), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        MapObjectInspector insp = (MapObjectInspector) inspector;
        int len = insp.getMapSize(obj);
        lengths.write(len);
        // this sucks, but it will have to do until we can get a better
        // accessor in the MapObjectInspector.
        Map<?,?> valueMap = insp.getMap(obj);
        for(Map.Entry<?,?> entry: valueMap.entrySet()) {
          childrenWriters[0].write(entry.getKey());
          childrenWriters[1].write(entry.getValue());
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder);
      }
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      lengths.getPosition(rowIndexPosition);
    }
  }

  private static class UnionTreeWriter extends TreeWriter {
    private final RunLengthByteWriter tags;

    UnionTreeWriter(int columnId,
                  ObjectInspector inspector,
                  SectionWriter writer,
                  boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      UnionObjectInspector insp = (UnionObjectInspector) inspector;
      List<ObjectInspector> choices = insp.getObjectInspectors();
      childrenWriters = new TreeWriter[choices.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(choices.get(i), writer, true);
      }
      tags =
        new RunLengthByteWriter(writer.createSection(columnId,
          OrcProto.StripeSection.Kind.DATA));
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        UnionObjectInspector insp = (UnionObjectInspector) inspector;
        byte tag = insp.getTag(obj);
        tags.write(tag);
        childrenWriters[tag].write(insp.getField(obj));
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      tags.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder);
      }
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      tags.getPosition(rowIndexPosition);
    }
  }

  private static TreeWriter createTreeWriter(ObjectInspector inspector,
                                             SectionWriter writer,
                                             boolean nullable
                                            ) throws IOException {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case BOOLEAN:
            return new BooleanTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case BYTE:
            return new ByteTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case SHORT:
          case INT:
          case LONG:
            return new IntegerTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case FLOAT:
            return new FloatTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case DOUBLE:
            return new DoubleTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case STRING:
            return new StringTreeWriter(writer.getNextColumnId(), inspector,
              writer, nullable);
          case BINARY:
            return new BinaryTreeWriter(writer.getNextColumnId(), inspector,
                writer, nullable);
          case TIMESTAMP:
            return new TimestampTreeWriter(writer.getNextColumnId(), inspector,
                writer, nullable);
          default:
            throw new IllegalArgumentException("Bad primitive category " +
              ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
        }
      case STRUCT:
        return new StructTreeWriter(writer.getNextColumnId(), inspector, writer,
          nullable);
      case MAP:
        return new MapTreeWriter(writer.getNextColumnId(), inspector, writer,
          nullable);
      case LIST:
        return new ListTreeWriter(writer.getNextColumnId(), inspector, writer,
          nullable);
      case UNION:
        return new UnionTreeWriter(writer.getNextColumnId(), inspector, writer,
          nullable);
      default:
        throw new IllegalArgumentException("Bad category: " +
          inspector.getCategory());
    }
  }

  private static void writeTypes(OrcProto.Footer.Builder builder,
                                 TreeWriter treeWriter) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    switch (treeWriter.inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) treeWriter.inspector).
                 getPrimitiveCategory()) {
          case BOOLEAN:
            type.setKind(OrcProto.Type.Kind.BOOLEAN);
            break;
          case BYTE:
            type.setKind(OrcProto.Type.Kind.BYTE);
            break;
          case SHORT:
            type.setKind(OrcProto.Type.Kind.SHORT);
            break;
          case INT:
            type.setKind(OrcProto.Type.Kind.INT);
            break;
          case LONG:
            type.setKind(OrcProto.Type.Kind.LONG);
            break;
          case FLOAT:
            type.setKind(OrcProto.Type.Kind.FLOAT);
            break;
          case DOUBLE:
            type.setKind(OrcProto.Type.Kind.DOUBLE);
            break;
          case STRING:
            type.setKind(OrcProto.Type.Kind.STRING);
            break;
          case BINARY:
            type.setKind(OrcProto.Type.Kind.BINARY);
            break;
          case TIMESTAMP:
            type.setKind(OrcProto.Type.Kind.TIMESTAMP);
            break;
          default:
            throw new IllegalArgumentException("Unknown primitive category: " +
              ((PrimitiveObjectInspector) treeWriter.inspector).
                getPrimitiveCategory());
        }
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.LIST);
        type.addSubtypes(treeWriter.childrenWriters[0].id);
        break;
      case MAP:
        type.setKind(OrcProto.Type.Kind.MAP);
        type.addSubtypes(treeWriter.childrenWriters[0].id);
        type.addSubtypes(treeWriter.childrenWriters[1].id);
        break;
      case STRUCT:
        type.setKind(OrcProto.Type.Kind.STRUCT);
        for(TreeWriter child: treeWriter.childrenWriters) {
          type.addSubtypes(child.id);
        }
        for(StructField field: ((StructTreeWriter) treeWriter).fields) {
          type.addFieldNames(field.getFieldName());
        }
        break;
      case UNION:
        type.setKind(OrcProto.Type.Kind.UNION);
        for(TreeWriter child: treeWriter.childrenWriters) {
          type.addSubtypes(child.id);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
          treeWriter.inspector.getCategory());
    }
    builder.addTypes(type);
    for(TreeWriter child: treeWriter.childrenWriters) {
      writeTypes(builder, child);
    }
  }

  private void ensureWriter() throws IOException {
    if (rawWriter == null) {
      rawWriter = fs.create(path, false, BUFFER_SIZE,
        fs.getDefaultReplication(),
          Math.min(stripeSize * 2L, Integer.MAX_VALUE));
      rawWriter.writeBytes(OrcFile.MAGIC);
      headerLength = rawWriter.getPos();
      writer = new OutStream("metadata", bufferSize, codec,
        new DirectStream(rawWriter));
      protobufWriter = CodedOutputStream.newInstance(writer);
    }
  }

  private void createRowIndexEntry() throws IOException {
    if (rowsInIndex > 0) {
    OrcProto.RowIndexEntry.Builder entry = OrcProto.RowIndexEntry.newBuilder();
    entry.setRowCount(rowsInIndex);
      rowsInStripe += rowsInIndex;
      rowCount += rowsInIndex;
      rowsInIndex = 0;
      // get the column positions and stats for this index entry
      treeWriter.getPosition(entry);
      rowIndex.addEntry(entry);
      // record the current positions for next time.
      treeWriter.recordPosition();
    }
  }

  private void writeRowIndex() throws IOException {
    PositionedOutputStream out =
      sectionWriter.createSection(columnCount,
        OrcProto.StripeSection.Kind.ROW_INDEX);
    rowIndex.build().writeTo(out);
    rowIndex.clear();
  }

  private void flushStripe() throws IOException {
    ensureWriter();
    if (rowsInStripe != 0) {
      writeRowIndex();
      OrcProto.StripeFooter.Builder builder = OrcProto.StripeFooter.newBuilder();
      treeWriter.writeStripe(builder);
      long start = rawWriter.getPos();
      long section = start;
      for(Map.Entry<StreamName,BufferedStream> pair: streams.entrySet()) {
        BufferedStream stream = pair.getValue();
        stream.flush();
        stream.spillTo(rawWriter);
        stream.clear();
        long end = rawWriter.getPos();
        StreamName name = pair.getKey();
        builder.addSections(OrcProto.StripeSection.newBuilder()
            .setColumn(name.getColumn())
            .setKind(name.getKind())
            .setLength(end-section));
        section = end;
      }
      builder.build().writeTo(protobufWriter);
      protobufWriter.flush();
      writer.flush();
      long end = rawWriter.getPos();
      OrcProto.StripeInformation dirEntry =
          OrcProto.StripeInformation.newBuilder()
              .setOffset(start)
              .setLength(end - start)
              .setNumberOfRows(rowsInStripe)
              .setTailLength(end - section).build();
      stripes.add(dirEntry);
      rowsInStripe = 0;
      rowsInIndex = 0;
      bytesInStripe = 0;
    }
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  private void writeFileStatistics(OrcProto.Footer.Builder builder,
                                   TreeWriter writer) throws IOException {
    builder.addStatistics(writer.fileStatistics.serialize());
    for(TreeWriter child: writer.getChildrenWriters()) {
      writeFileStatistics(builder, child);
    }
  }

  private int writeFooter(long bodyLength) throws IOException {
    ensureWriter();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setBodyLength(bodyLength);
    builder.setHeaderLength(headerLength);
    builder.setNumberOfRows(rowCount);
    // serialize the types
    writeTypes(builder, treeWriter);
    // add the stripe information
    for(OrcProto.StripeInformation stripe: stripes) {
      builder.addStripes(stripe);
    }
    // add the column statistics
    writeFileStatistics(builder, treeWriter);
    // add all of the user metadata
    for(Map.Entry<String, ByteString> entry: userMetadata.entrySet()) {
      builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
        .setName(entry.getKey()).setValue(entry.getValue()));
    }
    long startPosn = rawWriter.getPos();
    builder.build().writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    return (int) (rawWriter.getPos() - startPosn);
  }

  private int writePostScript(int footerLength) throws IOException {
    OrcProto.PostScript.Builder builder =
      OrcProto.PostScript.newBuilder()
        .setCompression(writeCompressionKind(compress))
        .setFooterLength(footerLength);
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    return (int) length;
  }

  @Override
  public void addUserMetadata(String name, ByteBuffer value) {
    userMetadata.put(name, ByteString.copyFrom(value));
  }

  @Override
  public void addRow(Object row) throws IOException {
    treeWriter.write(row);
    rowsInIndex += 1;
    boolean shouldFlushStripe = bytesInStripe >= stripeSize;
    if (rowsInIndex >= ROW_INDEX_FREQUENCY || shouldFlushStripe) {
      createRowIndexEntry();
    }
    if (shouldFlushStripe) {
      flushStripe();
    }
  }

  @Override
  public void close() throws IOException {
    createRowIndexEntry();
    flushStripe();
    int footerLength = writeFooter(rawWriter.getPos());
    rawWriter.writeByte(writePostScript(footerLength));
    rawWriter.close();
  }
}
