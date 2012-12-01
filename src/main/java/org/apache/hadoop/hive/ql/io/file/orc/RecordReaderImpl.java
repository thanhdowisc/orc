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
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.hive.ql.io.file.orc.Reader.FileInformation;
import org.apache.hadoop.hive.ql.io.file.orc.Reader.StripeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RecordReaderImpl implements RecordReader {
  private final ReaderImpl.FileInformationImpl fileInfo;
  private final FSDataInputStream file;
  private final List<Reader.StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private final long totalRowCount;
  private final long totalByteCount;
  private final long offset;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final boolean[] included;
  private int currentStripe = -1;
  private long currentRow = 0;
  private long remainingRows = 0;
  private final Map<WriterImpl.StreamName, InStream> streams =
    new HashMap<WriterImpl.StreamName, InStream>();

  RecordReaderImpl(ReaderImpl.FileInformationImpl fileInfo,
                   FileSystem fileSystem,
                   Path path,
                   long offset, long length,
                   CompressionCodec codec,
                   int bufferSize,
                   boolean[] included
                  ) throws IOException {
    this.fileInfo = fileInfo;
    this.file = fileSystem.open(path);
    this.offset = offset;
    this.codec = codec;
    this.bufferSize = bufferSize;
    this.included = included;
    long rows = 0;
    long bytes = 0;
    for(StripeInformation stripe: fileInfo.getStripes()) {
      long stripeStart = stripe.getOffset();
      if (offset <= stripeStart && stripeStart < offset+length) {
        stripes.add(stripe);
        rows += stripe.getNumberOfRows();
        bytes += stripe.getLength();
      }
    }
    totalRowCount = rows;
    totalByteCount = bytes;
  }

  private static class TreeReader {

  }

  private static class IntTreeReader extends TreeReader{

  }

  private static class StringTreeReader extends TreeReader {

  }

  private static class StructTreeReader extends TreeReader {

  }

  private void readCurrentStripeFooter() throws IOException {
    StripeInformation info = stripes.get(currentStripe);
    OrcProto.StripeFooter footer;
    long offset = info.getOffset();
    int length = (int) info.getLength();
    int tailLength = (int) info.getTailLength();
    streams.clear();

    // if we aren't projecting columns, just read the whole stripe
    if (included == null) {
      byte[] buffer = new byte[length];
      file.seek(offset);
      file.readFully(buffer, 0, buffer.length);
      InStream tail = InStream.create(ByteBuffer.wrap(buffer,
        buffer.length-tailLength, tailLength), codec, bufferSize);
      footer = OrcProto.StripeFooter.parseFrom(tail);
      int sectionOffset = 0;
      for(OrcProto.StripeSection section: footer.getSectionsList()) {
        int sectionLength = (int) section.getLength();
        ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset,
          sectionLength);
        sectionOffset += sectionLength;
        streams.put(new WriterImpl.StreamName(section.getColumn(),
                                              section.getKind()),
          InStream.create(sectionBuffer, codec, bufferSize));
      }
    } else {
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.seek(offset + length - tailLength);
      footer = OrcProto.StripeFooter.parseFrom(InStream.create(tailBuf,
        codec, bufferSize));
      List<OrcProto.StripeSection> sections = footer.getSectionsList();
      int sectionOffset = 0;
      int nextSection = 0;
      while (nextSection < sections.size()) {
        int bytes = 0;

        // find the first section that shouldn't be read
        int excluded=nextSection;
        while (!included[sections.get(excluded).getColumn()] &&
               excluded < sections.size()) {
          excluded += 1;
          bytes += sections.get(excluded).getLength();
        }

        // actually read the bytes as a big chunk
        byte[] buffer = new byte[bytes];
        file.seek(offset + sectionOffset);
        file.readFully(buffer, 0, bytes);
        sectionOffset += bytes;

        // create the streams for the sections we just read
        bytes = 0;
        while (nextSection < excluded) {
          OrcProto.StripeSection section = sections.get(nextSection);
          streams.put(new WriterImpl.StreamName(section.getColumn(),
                                                section.getKind()),
                      InStream.create(ByteBuffer.wrap(buffer, bytes,
                        (int) section.getLength()), codec, bufferSize));
          nextSection += 1;
          bytes += section.getLength();
        }

        // skip forward until we get back to a section that we need
        while (nextSection < sections.size() &&
               !included[sections.get(nextSection).getColumn()]) {
          sectionOffset += sections.get(nextSection).getLength();
          nextSection += 1;
        }
      }
    }
  }

  @Override
  public boolean next(Object o, Object o1) throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Object createKey() {
    return null;
  }

  @Override
  public Object createValue() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get the estimated byte position based on the number of rows consumed
   * @return the estimated byte offset of the current position
   */
  @Override
  public long getPos() {
    return offset + currentRow * totalByteCount / totalRowCount;
  }

  @Override
  public void close() throws IOException {
    file.close();
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
