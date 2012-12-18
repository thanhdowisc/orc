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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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
  private OrcProto.RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();

  WriterImpl(FileSystem fs,
             Path path,
             ObjectInspector inspector,
             long stripeSize,
             CompressionKind compress,
             int bufferSize,
             Configuration conf) throws IOException {
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
  }

  static class StreamName implements Comparable<StreamName> {
    private final int column;
    private final OrcProto.StripeSection.Kind kind;

    public StreamName(int column, OrcProto.StripeSection.Kind kind) {
      this.column = column;
      this.kind = kind;
    }

    public boolean equals(Object obj) {
      if (obj == null || obj instanceof  StreamName) {
        StreamName other = (StreamName) obj;
        return other.column == column && other.kind == kind;
      } else {
        return false;
      }
    }

    @Override
    public int compareTo(StreamName streamName) {
      if (streamName == null) {
        return -1;
      } else if (column > streamName.column) {
        return 1;
      } else if (column < streamName.column) {
        return -1;
      } else {
        return kind.compareTo(streamName.kind);
      }
    }

    @Override
    public String toString() {
      return "Stream for column " + column + " kind " + kind;
    }

    @Override
    public int hashCode() {
      return column * 101 + kind.getNumber();
    }
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
  }

  abstract class TreeWriter {
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

  class StructTreeWriter extends TreeWriter {
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

  class IntegerTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter writer;

    IntegerTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      PositionedOutputStream out = writer.createSection(id,
        OrcProto.StripeSection.Kind.INT_ROW_DATA);
      this.writer = new RunLengthIntegerWriter(out, true);
    }

    @Override
    void write(Object obj) throws IOException {
      if (obj != null) {
        Integer val = ((IntObjectInspector) inspector).get(obj);
        stripeStatistics.updateInteger(val);
        writer.write(val);
      }
      super.write(obj);
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

  class FloatTreeWriter extends TreeWriter {
    private final Map<Float,Integer> idMap = new HashMap<Float,Integer>();
    private final RunLengthIntegerWriter rowData;
    private final PositionedOutputStream dictionary;

    FloatTreeWriter(int columnId,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(columnId, inspector, writer, nullable);
      this.dictionary = writer.createSection(id,
        OrcProto.StripeSection.Kind.FLOAT_ROW_DATA);
      this.rowData = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.INT_ROW_DATA), false);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        Float val = ((FloatObjectInspector) inspector).get(obj);
        stripeStatistics.updateDouble(val);
        Integer id = idMap.get(val);
        if (id == null) {
          id = idMap.size();
          idMap.put(val, id);
          SerializationUtils.writeFloat(dictionary, val);
        }
        rowData.write(id);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder) throws IOException {
      super.writeStripe(builder);
      dictionary.flush();
      rowData.flush();
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      rowData.getPosition(rowIndexPosition);
    }
  }

  class StringTreeWriter extends TreeWriter {
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
        OrcProto.StripeSection.Kind.DICTIONARY_LENGTH), false);
      rowOutput = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_ROWS), false);
      countOutput = new RunLengthIntegerWriter(writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_COUNT), false);
    }

    @Override
    void write(Object obj) throws IOException {
      if (obj != null) {
        String val = ((StringObjectInspector) inspector)
          .getPrimitiveJavaObject(obj);
        rows.add(dictionary.add(val));
        stripeStatistics.updateString(val);
      }
      super.write(obj);
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
    }

    @Override
    void recordPosition() throws IOException {
      super.recordPosition();
      // TODO????
    }
  }

  TreeWriter createTreeWriter(ObjectInspector inspector,
                              SectionWriter writer,
                              boolean nullable) throws IOException {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case INT:
            return new IntegerTreeWriter(columnCount++, inspector, writer,
              nullable);
          case FLOAT:
            return new FloatTreeWriter(columnCount++, inspector, writer,
              nullable);
          case STRING:
            return new StringTreeWriter(columnCount++, inspector, writer,
              nullable);
          default:
            throw new IllegalArgumentException("Bad primitive category " +
              ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
        }
      case STRUCT:
        return new StructTreeWriter(columnCount++, inspector, writer,
          nullable);
      default:
        throw new IllegalArgumentException("Bad category: " +
          inspector.getCategory());
    }
  }

  private void makeFlat(OrcProto.Footer.Builder builder,
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
            type.setKind(OrcProto.Type.Kind.TINYINT);
            break;
          case SHORT:
            type.setKind(OrcProto.Type.Kind.SMALLINT);
            break;
          case INT:
            type.setKind(OrcProto.Type.Kind.INT);
            break;
          case LONG:
            type.setKind(OrcProto.Type.Kind.BIGINT);
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
            type.setKind(OrcProto.Type.Kind.DATETIME);
            break;
          default:
            throw new IllegalArgumentException("Unknown primitive category: " +
              ((PrimitiveObjectInspector) treeWriter.inspector).
                getPrimitiveCategory());
        }
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.ARRAY);
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
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
          treeWriter.inspector.getCategory());
    }
    builder.addTypes(type);
    for(TreeWriter child: treeWriter.childrenWriters) {
      makeFlat(builder, child);
    }
  }

  private void writeTypes(OrcProto.Footer.Builder builder) {
    makeFlat(builder, treeWriter);
  }

  private FSDataOutputStream getWriter() throws IOException {
    if (rawWriter == null) {
      rawWriter = fs.create(path, false, BUFFER_SIZE,
        fs.getDefaultReplication(), Math.min(stripeSize * 2L, Integer.MAX_VALUE));
      rawWriter.writeBytes(OrcFile.MAGIC);
      headerLength = rawWriter.getPos();
      writer = new OutStream("metadata", bufferSize, codec,
        new DirectStream(rawWriter));
      protobufWriter = CodedOutputStream.newInstance(writer);
    }
    return rawWriter;
  }

  private void createRowIndexEntry() throws IOException {
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

  private void writeRowIndex() throws IOException {
    PositionedOutputStream out =
      sectionWriter.createSection(columnCount,
        OrcProto.StripeSection.Kind.ROW_INDEX);
    rowIndex.build().writeTo(out);
    rowIndex.clear();
  }

  private void flushStripe() throws IOException {
    getWriter();
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
        .setColumn(name.column)
        .setKind(name.kind)
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
    getWriter();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setBodyLength(bodyLength);
    builder.setHeaderLength(headerLength);
    builder.setNumberOfRows(rowCount);
    // serialize the types
    writeTypes(builder);
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
  public void addUserMetadata(String name, ByteBuffer value) throws IOException {
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
    FSDataOutputStream file = getWriter();
    int footerLength = writeFooter(file.getPos());
    file.writeByte(writePostScript(footerLength));
    file.close();
  }
}
