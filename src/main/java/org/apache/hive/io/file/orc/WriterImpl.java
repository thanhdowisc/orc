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
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

class WriterImpl implements Writer {

  private final Path path;
  private final Configuration conf;
  private final CompressionKind compress;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final TypeInfo type;
  private final TypeInfo[] allTypes;

  private FSDataOutputStream writer = null;
  private CodedOutputStream protobufWriter = null;
  private long stripeOffset;
  private long rowCount = 0;
  private final List<OrcProto.StripeInformation> stripes =
    new ArrayList<OrcProto.StripeInformation>();
  private final Map<String, ByteString> userMetadata =
    new TreeMap<String, ByteString>();
  private final Map<TypeInfo, Integer> typeIds =
    new IdentityHashMap<TypeInfo, Integer>();
  private final int nonPrimitiveTypes;
  private final TreeWriter treeWriter;

  WriterImpl(Path path,
             TypeInfo type,
             ObjectInspector inspector,
             CompressionKind compress,
             int bufferSize,
             Configuration conf) throws IOException {
    this.path = path;
    this.type = type;
    this.conf = conf;
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
    assignTypeIds(type, false);
    nonPrimitiveTypes = typeIds.size();
    assignTypeIds(type, true);
    // create an array with the complete set of types
    allTypes = new TypeInfo[typeIds.size()];
    for(Map.Entry<TypeInfo, Integer> x: typeIds.entrySet()) {
      allTypes[x.getValue()] = x.getKey();
    }
    treeWriter = createTreeWriter(type, inspector, new SectionWriterImpl(),
      false);
  }

  private void assignTypeIds(TypeInfo type, boolean assignLeaves) {
    switch (type.getCategory()) {
      case PRIMITIVE:
        if (assignLeaves) {
          typeIds.put(type, typeIds.size());
        }
        break;
      case LIST:
        if (!assignLeaves) {
          typeIds.put(type, typeIds.size());
        }
        ListTypeInfo listType = (ListTypeInfo) type;
        assignTypeIds(listType.getListElementTypeInfo(), assignLeaves);
        break;
      case STRUCT:
        if (!assignLeaves) {
          typeIds.put(type, typeIds.size());
        }
        StructTypeInfo structType = (StructTypeInfo) type;
        for(TypeInfo child: structType.getAllStructFieldTypeInfos()) {
          assignTypeIds(child, assignLeaves);
        }
        break;
      case MAP:
        if (!assignLeaves) {
          typeIds.put(type, typeIds.size());
        }
        MapTypeInfo mapType = (MapTypeInfo) type;
        assignTypeIds(mapType.getMapKeyTypeInfo(), assignLeaves);
        assignTypeIds(mapType.getMapValueTypeInfo(), assignLeaves);
        break;
      case UNION:
        if (!assignLeaves) {
          typeIds.put(type, typeIds.size());
        }
        UnionTypeInfo unionType = (UnionTypeInfo) type;
        for(TypeInfo child: unionType.getAllUnionObjectTypeInfos()) {
          assignTypeIds(child, assignLeaves);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type category: " +
          type.getCategory());
    }
  }

  interface SectionWriter {
    /**
     * Append a set of bytes onto a section
     * @param column the column id for the section
     * @param kind the kind of section
     * @return The output stream that the section needs to be written to.
     * @throws IOException
     */
    OutputStream createSection(int column,
                               OrcProto.StripeSection.Kind kind
                               ) throws IOException;
  }

  private static class StreamName implements Comparable<StreamName> {
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
        return -1;
      } else if (column < streamName.column) {
        return 1;
      } else {
        return kind.compareTo(streamName.kind);
      }
    }
  }

  private static class Stream extends OutputStream {
    private static final int HEADER_SIZE = 3;
    private final List<ByteBuffer> done = new ArrayList<ByteBuffer>();
    private ByteBuffer compressed = null;
    private ByteBuffer overflow = null;
    private ByteBuffer current;
    private final int bufferSize;
    private final CompressionCodec codec;

    private Stream(int bufferSize,
                   CompressionCodec codec) throws IOException {
      this.bufferSize = bufferSize;
      this.codec = codec;
      getNewInputBuffer();
    }

    /**
     * Write the length of the compressed bytes. Life is much easier if the
     * header is constant length, so just use 3 bytes. Considering most of the
     * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
     * be plenty. We also use the low bit for whether it is the original or
     * compressed bytes.
     * @param buffer the buffer to write the header to
     * @param position the position in the buffer to write at
     * @param val the size in the file
     * @param original is it uncompressed
     */
    private static void writeHeader(ByteBuffer buffer,
                                    int position,
                                    int val,
                                    boolean original) {
      buffer.put(position, (byte) ((val << 2) + (original ? 1 : 0)));
      buffer.put(position+1, (byte) (val >> 7));
      buffer.put(position+2, (byte) (val >> 15));
    }

    private void getNewInputBuffer() throws IOException {
      if (codec == null) {
        current = ByteBuffer.allocate(bufferSize);
      } else {
        current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
        writeHeader(current, 0, bufferSize, true);
      }
      current.mark();
    }

    private ByteBuffer getNewOutputBuffer() throws IOException {
      return ByteBuffer.allocate(bufferSize +
                                 (codec == null ? 0 : HEADER_SIZE));
    }

    private void flip() {
      current.limit(current.position());
      current.reset();
    }

    @Override
    public void write(int i) throws IOException {
      if (current.remaining() < 1) {
        flush();
      }
      current.put((byte) i);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      int remaining = Math.min(current.remaining(), length);
      current.put(bytes, offset, remaining);
      length -= remaining;
      while (length != 0) {
        flush();
        offset += remaining;
        remaining = Math.min(current.remaining(), length);
        current.put(bytes, offset, remaining);
        length -= remaining;
      }
    }

    @Override
    public void flush() throws java.io.IOException {
      flip();
      if (codec == null) {
        done.add(current);
        getNewInputBuffer();
      } else {
        if (compressed == null) {
          compressed = getNewOutputBuffer();
        } else if (overflow == null) {
          overflow = getNewOutputBuffer();
        }
        int sizePosn = compressed.position();
        compressed.position(compressed.position()+HEADER_SIZE);
        if (codec.compress(current, compressed, overflow)) {
          // move position back to after the header
          current.reset();
          // find the total bytes in the chunk
          int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;
          if (overflow != null) {
            totalBytes += overflow.position();
          }
          writeHeader(compressed, sizePosn, totalBytes, false);
          // if we have less than the next header left, flush it.
          if (compressed.remaining() < HEADER_SIZE) {
            compressed.flip();
            done.add(compressed);
            compressed = overflow;
            overflow = null;
          }
        } else {
          // we are using the original, but need to flush the current
          // compressed buffer first. So back up to where we started,
          // flip it and add it to done.
          compressed.position(sizePosn);
          compressed.flip();
          done.add(compressed);

          // if we have an overflow, clear it and make it the new compress
          // buffer
          if (overflow != null) {
            overflow.clear();
            compressed = overflow;
          }

          // now add the current buffer into the done list and get a new one.
          current.rewind();
          done.add(current);
          getNewInputBuffer();
        }
      }
    }
  }


  private class SectionWriterImpl implements SectionWriter {

    private final Map<StreamName,Stream> streams =
      new TreeMap<StreamName, Stream>();

    @Override
    public OutputStream createSection(int column,
                                      OrcProto.StripeSection.Kind kind
                                      ) throws IOException {
      StreamName name = new StreamName(column, kind);
      Stream result = streams.get(name);
      if (result == null) {
        result = new Stream(bufferSize, codec);
        streams.put(name, result);
      }
      return result;
    }
  }

  abstract class TreeWriter {
    protected final int id;
    protected final ObjectInspector inspector;
    protected final SectionWriter writer;
    private final BitFieldWriter isNull;
    protected final ColumnStatistics stripeStatistics;
    private final ColumnStatistics fileStatistics;
    protected TreeWriter[] childrenWriters;

    TreeWriter(TypeInfo type, ObjectInspector inspector,
               SectionWriter writer, boolean nullable) throws IOException {
      this.id = typeIds.get(type);
      this.inspector = inspector;
      this.writer = writer;
      if (nullable) {
        isNull = new BitFieldWriter(writer.createSection(id,
          OrcProto.StripeSection.Kind.PRESENT), 1);
      } else {
        isNull = null;
      }
      stripeStatistics = new ColumnStatistics(id, type);
      fileStatistics = new ColumnStatistics(id, type);
      childrenWriters = new TreeWriter[0];
    }

    void write(Object obj) throws IOException {
      if (obj != null) {
        stripeStatistics.increment();
      }
      if (isNull != null) {
        isNull.append(obj == null ? 1: 0);
      }
    }

    void writeStripe() throws IOException {
      if (isNull != null) {
        isNull.flush();
      }
      fileStatistics.merge(stripeStatistics);
      stripeStatistics.reset();
    }

    ColumnStatistics getStripeStatistics() {
      return stripeStatistics;
    }

    ColumnStatistics getFileStatistics() {
      return fileStatistics;
    }

    TreeWriter[] getChildrenWriters() {
      return childrenWriters;
    }
  }

  class StructTreeWriter extends TreeWriter {
    private final List<? extends StructField> fields;
    StructTreeWriter(TypeInfo type,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(type, inspector, writer, nullable);
      StructTypeInfo structTypeInfo = (StructTypeInfo) type;
      StructObjectInspector structObjectInspector =
        (StructObjectInspector) inspector;
      List<TypeInfo> childInfo = structTypeInfo.getAllStructFieldTypeInfos();
      fields = structObjectInspector.getAllStructFieldRefs();
      childrenWriters = new TreeWriter[childInfo.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(childInfo.get(i),
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
    void writeStripe() throws IOException {
      super.writeStripe();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe();
      }
    }
  }

  class IntegerTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter writer;

    IntegerTreeWriter(TypeInfo type,
                      ObjectInspector inspector,
                      SectionWriter writer,
                      boolean nullable) throws IOException {
      super(type, inspector, writer, nullable);
      OutputStream out = writer.createSection(id,
        OrcProto.StripeSection.Kind.INT_ROW_DATA);
      this.writer = new RunLengthIntegerWriter(out, true);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        Integer val = ((IntObjectInspector) inspector).get(obj);
        stripeStatistics.update(val);
        writer.write(val);
      }
    }

    @Override
    void writeStripe() throws IOException {
      super.writeStripe();
      writer.flush();
    }
  }

  class StringTreeWriter extends TreeWriter {
    private final DictionaryWriter writer;

    StringTreeWriter(TypeInfo type,
                     ObjectInspector inspector,
                     SectionWriter writer,
                     boolean nullable) throws IOException {
      super(type, inspector, writer, nullable);
      OutputStream outData = writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_DATA);
      OutputStream outLen = writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_LENGTH);
      OutputStream outRows = writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_ROWS);
      OutputStream outCounts = writer.createSection(id,
        OrcProto.StripeSection.Kind.DICTIONARY_COUNT);
      this.writer = new DictionaryWriter(outData, outLen, outRows, outCounts);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj);
      if (obj != null) {
        String val = ((StringObjectInspector) inspector)
          .getPrimitiveJavaObject(obj);
        // only update the min/max if it is a new word
        if (writer.write(val)) {
          stripeStatistics.update(val);
        }
      }
    }

    @Override
    void writeStripe() throws IOException {
      super.writeStripe();
      writer.flush();
    }
  }

  TreeWriter createTreeWriter(TypeInfo type,
                              ObjectInspector inspector,
                              SectionWriter writer,
                              boolean nullable) throws IOException {
    switch (type.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
          case INT:
            return new IntegerTreeWriter(type, inspector, writer,
              nullable);
          case STRING:
            return new StringTreeWriter(type, inspector, writer, nullable);
          default:
            throw new IllegalArgumentException("Bad primitive category " +
              ((PrimitiveTypeInfo) type).getPrimitiveCategory());
        }
      case STRUCT:
        return new StructTreeWriter(type, inspector, writer, nullable);
      default:
        throw new IllegalArgumentException("Bad category: " +
          type.getCategory());
    }
  }

  private OrcProto.Type makeFlat(Map<TypeInfo, Integer> ids, TypeInfo type) {
    OrcProto.Type.Builder builder = OrcProto.Type.newBuilder();
    switch (type.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
          case BOOLEAN:
            builder.setKind(OrcProto.Type.Kind.BOOLEAN);
            break;
          case BYTE:
            builder.setKind(OrcProto.Type.Kind.TINYINT);
            break;
          case SHORT:
            builder.setKind(OrcProto.Type.Kind.SMALLINT);
            break;
          case INT:
            builder.setKind(OrcProto.Type.Kind.INT);
            break;
          case LONG:
            builder.setKind(OrcProto.Type.Kind.BIGINT);
            break;
          case FLOAT:
            builder.setKind(OrcProto.Type.Kind.FLOAT);
            break;
          case DOUBLE:
            builder.setKind(OrcProto.Type.Kind.DOUBLE);
            break;
          case STRING:
            builder.setKind(OrcProto.Type.Kind.STRING);
            break;
          case BINARY:
            builder.setKind(OrcProto.Type.Kind.BINARY);
            break;
          case TIMESTAMP:
            builder.setKind(OrcProto.Type.Kind.DATETIME);
            break;
          default:
            throw new IllegalArgumentException("Unknown primitive category: " +
              ((PrimitiveTypeInfo) type).getPrimitiveCategory());
        }
        break;
      case LIST:
        builder.setKind(OrcProto.Type.Kind.ARRAY);
        builder.addSubtypes(ids.get(((ListTypeInfo) type).
          getListElementTypeInfo()));
        break;
      case MAP:
        builder.setKind(OrcProto.Type.Kind.MAP);
        MapTypeInfo mapType = (MapTypeInfo) type;
        builder.addSubtypes(ids.get(mapType.getMapKeyTypeInfo()));
        builder.addSubtypes(ids.get(mapType.getMapValueTypeInfo()));
        break;
      case STRUCT:
        builder.setKind(OrcProto.Type.Kind.STRUCT);
        StructTypeInfo structType = (StructTypeInfo) type;
        for(TypeInfo child: structType.getAllStructFieldTypeInfos()) {
          builder.addSubtypes(ids.get(child));
        }
        for(String child: structType.getAllStructFieldNames()) {
          builder.addFieldNames(child);
        }
        break;
      case UNION:
        builder.setKind(OrcProto.Type.Kind.UNION);
        UnionTypeInfo unionType = (UnionTypeInfo) type;
        for(TypeInfo child: unionType.getAllUnionObjectTypeInfos()) {
          builder.addSubtypes(ids.get(child));
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
          type.getCategory());
    }
    return builder.build();
  }

  private OrcProto.Type[] flattenTypes(TypeInfo info) {
    OrcProto.Type[] result = new OrcProto.Type[typeIds.size()];
    for(Map.Entry<TypeInfo, Integer> type: typeIds.entrySet()) {
      result[typeIds.get(type)] = makeFlat(typeIds, type.getKey());
    }
    return result;
  }

  private FSDataOutputStream getWriter() throws IOException {
    if (writer == null) {
      FileSystem fs =  path.getFileSystem(conf);
      writer = fs.create(path);
      writer.writeBytes(OrcFile.MAGIC);
      stripeOffset = writer.getPos();
    }
    return writer;
  }

  private CodedOutputStream getProtobufWriter() throws IOException {
    if (protobufWriter == null) {
      protobufWriter = CodedOutputStream.newInstance(getWriter());
    }
    return protobufWriter;
  }

  private void flushStripe() throws IOException {

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

  private long writeMessage(Message msg) throws IOException {
    FSDataOutputStream out = getWriter();
    CodedOutputStream proto = getProtobufWriter();
    long start = out.getPos();
    msg.writeTo(proto);
    proto.flush();
    return out.getPos() - start;
  }

  private void writeFileStatistics(OrcProto.Footer.Builder builder,
                                   TreeWriter writer) throws IOException {
    OrcProto.ColumnStatistics.Builder statsBuilder =
      OrcProto.ColumnStatistics.newBuilder();
    ColumnStatistics stats = writer.getFileStatistics();
    statsBuilder.setColumn(writer.id);
    statsBuilder.setNumberOfValues(stats.getCount());
    ByteString bs = stats.getSerializedMinimum();
    if (bs != null) {
      statsBuilder.setMinimum(bs);
    }
    bs = stats.getSerializedMaximum();
    if (bs != null) {
      statsBuilder.setMaximum(bs);
    }
    builder.addStatistics(statsBuilder);
    for(TreeWriter child: writer.getChildrenWriters()) {
      writeFileStatistics(builder, child);
    }
  }

  private int writeFooter(long bodyLength) throws IOException {
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setBodyLength(bodyLength);
    builder.setStripeOffset(stripeOffset);
    builder.setNumberOfRows(rowCount);
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
    return (int) writeMessage(builder.build());
  }

  private int writePostScript(int footerLength) throws IOException {
    OrcProto.PostScript.Builder builder =
      OrcProto.PostScript.newBuilder()
        .setCompression(writeCompressionKind(compress))
        .setFooterLength(footerLength);
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    long length = writeMessage(builder.build());
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
    rowCount += 1;
  }

  @Override
  public void close() throws IOException {
    flushStripe();
    int footerLength = writeFooter(writer.getPos());
    FSDataOutputStream file = getWriter();
    file.writeByte(writePostScript(footerLength));
    file.close();
  }
}
