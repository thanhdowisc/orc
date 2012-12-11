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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RecordReaderImpl implements RecordReader {
  private final FSDataInputStream file;
  private final long firstRow;
  private final List<StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final boolean[] included;
  private int currentStripe = -1;
  private long currentRow = 0;
  private final Map<WriterImpl.StreamName, InStream> streams =
    new HashMap<WriterImpl.StreamName, InStream>();
  private final TreeReader reader;

  RecordReaderImpl(Iterable<StripeInformation> stripes,
                   FileSystem fileSystem,
                   Path path,
                   long offset, long length,
                   List<OrcProto.Type> types,
                   CompressionCodec codec,
                   int bufferSize,
                   boolean[] included
                  ) throws IOException {
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.bufferSize = bufferSize;
    this.included = included;
    long rows = 0;
    long skippedRows = 0;
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < offset+length) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }
    firstRow = skippedRows;
    totalRowCount = rows;
    reader = createTreeReader(0, types, included);
  }

  private abstract static class TreeReader {
    protected final int columnId;
    private boolean done = true;
    private BitFieldReader present = null;
    protected boolean valuePresent = false;

    TreeReader(int columnId) {
      this.columnId = columnId;
    }

    void startStripe(Map<WriterImpl.StreamName,InStream> streams
                        ) throws IOException {
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.PRESENT);
      InStream in = streams.get(name);
      if (in == null) {
        present = null;
        valuePresent = true;
        done = false;
      } else {
        present = new BitFieldReader(in, 1);
        done = !present.hasNext();
        if (!done) {
          valuePresent = present.next() == 1;
        }
      }
    }

    abstract void seekToRow(long row) throws IOException;

    boolean hasNext() throws IOException {
      return !done;
    }

    Object next(Object previous) throws IOException {
      if (present != null) {
        done = !present.hasNext();
        if (!done) {
          valuePresent = present.next() == 1;
        }
      }
      return previous;
    }
  }

  private static class IntTreeReader extends TreeReader{
    private RunLengthIntegerReader reader = null;

    IntTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.INT_ROW_DATA);
      reader = new RunLengthIntegerReader(streams.get(name), true);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      IntWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new IntWritable();
        } else {
          result = (IntWritable) previous;
        }
        result.set(reader.next());
      }
      return super.next(result);
    }
  }

  private static class StringTreeReader extends TreeReader {
    private byte[][] dictionaryBuffer;
    private DynamicIntArray dictionaryOffsets;
    private DynamicIntArray dictionaryLengths;
    private RunLengthIntegerReader reader;

    StringTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);

      // read the dictionary blob
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      int avail = in.available();
      List<byte[]> work = new ArrayList<byte[]>();
      while (avail > 0) {
        byte[] part = new byte[avail];
        int offset = 0;
        while (offset < avail) {
          offset += in.read(part, offset, avail - offset);
        }
        work.add(part);
        avail = in.available();
      }
      dictionaryBuffer = work.toArray(new byte[work.size()][]);
      in.close();

      // read the lengths
      name = new WriterImpl.StreamName(columnId,
        OrcProto.StripeSection.Kind.DICTIONARY_LENGTH);
      in = streams.get(name);
      RunLengthIntegerReader lenReader = new RunLengthIntegerReader(in, false);
      int offset = 0;
      dictionaryOffsets = new DynamicIntArray();
      dictionaryLengths = new DynamicIntArray();
      while (lenReader.hasNext()) {
        dictionaryOffsets.add(offset);
        int len = lenReader.next();
        dictionaryLengths.add(len);
        offset += len;
      }
      in.close();

      // set up the row reader
      name = new WriterImpl.StreamName(columnId,
        OrcProto.StripeSection.Kind.DICTIONARY_ROWS);
      reader = new RunLengthIntegerReader(streams.get(name), false);
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    Object next(Object previous) throws IOException {
      Text result = null;
      if (valuePresent) {
        int entry = reader.next();
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int offset = dictionaryOffsets.get(entry);
        int length = dictionaryLengths.get(entry);
        int chunkIndex = 0;
        while (offset > dictionaryBuffer[chunkIndex].length) {
          offset -= dictionaryBuffer[chunkIndex].length;
          chunkIndex += 1;
        }
        // does the entry straddle the compression chunks?
        if (dictionaryBuffer[chunkIndex].length - offset < length) {
          int firstLength = dictionaryBuffer[chunkIndex].length - offset;
          result.set(dictionaryBuffer[chunkIndex], offset, firstLength);
          result.append(dictionaryBuffer[chunkIndex+1], 0, length - firstLength);
        } else {
          result.set(dictionaryBuffer[chunkIndex], offset, length);
        }
      }
      return super.next(result);
    }
  }

  private static class StructTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private final String[] fieldNames;

    StructTreeReader(int columnId,
                     List<OrcProto.Type> types,
                     boolean[] included) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getFieldNamesCount();
      this.fields = new TreeReader[fieldCount];
      this.fieldNames = new String[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included);
        }
        this.fieldNames[i] = type.getFieldNames(i);
      }
    }

    /**
     * Check to make sure that the kids all agree about whether there is another
     * row.
     * @return true if there is another row
     * @throws IOException
     */
    private boolean kidsHaveNext() throws IOException {
      if (fields.length == 0) {
        return true;
      }
      int field = 0;
      while (field < fields.length && fields[field] == null) {
        field += 1;
      }
      if (field == fields.length) {
        return false;
      }
      boolean result = fields[field].hasNext();
      for(int i=field+1; i < fields.length; ++i) {
        if (fields[i] != null && fields[i].hasNext() != result) {
          throw new IOException("Inconsistent struct length field " + field +
            " = " + result + " differs from field " + i);
        }
      }
      return result;
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || kidsHaveNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      OrcStruct result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcStruct(fields.length);
        } else {
          result = (OrcStruct) previous;
        }
        for(int i=0; i < fields.length; ++i) {
          if (fields[i] != null) {
            result.setFieldValue(i, fields[i].next(result.getFieldValue(i)));
          }
        }
      }
      return super.next(result);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] != null) {
          fields[i].startStripe(streams);
        }
      }
    }
  }

  private static TreeReader createTreeReader(int columnId,
                                             List<OrcProto.Type> types,
                                             boolean[] included
                                            ) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case INT:
        return new IntTreeReader(columnId);
      case STRING:
        return new StringTreeReader(columnId);
      case STRUCT:
        return new StructTreeReader(columnId, types, included);
      default:
        throw new IllegalArgumentException("Unsupported type " +
          type.getKind());
    }
  }

  private void readStripeFooter(StripeInformation stripe) throws IOException {
    OrcProto.StripeFooter footer;
    long offset = stripe.getOffset();
    int length = (int) stripe.getLength();
    int tailLength = (int) stripe.getTailLength();
    streams.clear();

    // read the footer
    ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
    file.seek(offset + length - tailLength);
    file.readFully(tailBuf.array(), tailBuf.arrayOffset(), tailLength);
    footer = OrcProto.StripeFooter.parseFrom(InStream.create("footer", tailBuf,
      codec, bufferSize));

    // if we aren't projecting columns, just read the whole stripe
    if (included == null) {
      byte[] buffer = new byte[length - tailLength];
      file.seek(offset);
      file.readFully(buffer, 0, buffer.length);
      int sectionOffset = 0;
      for(OrcProto.StripeSection section: footer.getSectionsList()) {
        int sectionLength = (int) section.getLength();
        ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset,
          sectionLength);
        WriterImpl.StreamName name =
          new WriterImpl.StreamName(section.getColumn(), section.getKind());
        streams.put(name,
          InStream.create(name.toString(), sectionBuffer, codec, bufferSize));
        sectionOffset += sectionLength;
      }
    } else {
      List<OrcProto.StripeSection> sections = footer.getSectionsList();
      // the index of the current section
      int currentSection = 0;
      // byte position of the current section relative to the stripe start
      long sectionOffset = 0;
      while (currentSection < sections.size()) {
        int bytes = 0;

        // find the first section that shouldn't be read
        int excluded=currentSection;
        while (excluded < sections.size() &&
               included[sections.get(excluded).getColumn()]) {
          excluded += 1;
          bytes += sections.get(excluded).getLength();
        }

        // actually read the bytes as a big chunk
        if (bytes != 0) {
          byte[] buffer = new byte[bytes];
          file.seek(offset + sectionOffset);
          file.readFully(buffer, 0, bytes);
          sectionOffset += bytes;

          // create the streams for the sections we just read
          bytes = 0;
          while (currentSection < excluded) {
            OrcProto.StripeSection section = sections.get(currentSection);
            WriterImpl.StreamName name =
              new WriterImpl.StreamName(section.getColumn(), section.getKind());
            streams.put(name,
              InStream.create(name.toString(),
                ByteBuffer.wrap(buffer, bytes,
                  (int) section.getLength()), codec, bufferSize));
            currentSection += 1;
            bytes += section.getLength();
          }
        }

        // skip forward until we get back to a section that we need
        while (currentSection < sections.size() &&
               !included[sections.get(currentSection).getColumn()]) {
          sectionOffset += sections.get(currentSection).getLength();
          currentSection += 1;
        }
      }
    }
    reader.startStripe(streams);
  }

  @Override
  public boolean hasNext() throws IOException {
    while (!reader.hasNext()) {
      if (currentStripe + 1 < stripes.size()) {
        currentStripe += 1;
        readStripeFooter(stripes.get(currentStripe));
      } else {
        return false;
      }
    }
    return currentRow < totalRowCount;
  }

  @Override
  public Object next(Object previous) throws IOException {
    currentRow += 1;
    return reader.next(previous);
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long getRowNumber() {
    return currentRow + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected
   * section of the file
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) currentRow)/totalRowCount;
  }
}
