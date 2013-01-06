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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class OrcInputFormat  extends FileInputFormat<NullWritable, OrcStruct>
  implements InputFormatChecker {

  private static class OrcRecordReader
      implements RecordReader<NullWritable, OrcStruct> {
    private final org.apache.hadoop.hive.ql.io.orc.RecordReader reader;
    private final long offset;
    private final long length;
    private final OrcStruct row;
    private boolean firstRow = true;

    OrcRecordReader(Reader file, long offset, long length) throws IOException {
      this.reader = file.rows(offset, length);
      this.offset = offset;
      this.length = length;
      if (reader.hasNext()) {
        row = (OrcStruct) reader.next(null);
      } else {
        row = null;
      }
    }

    @Override
    public boolean next(NullWritable key, OrcStruct value) throws IOException {
      if (firstRow) {
        firstRow = false;
        assert value == row: "User didn't pass our value back " + value;
        return row != null;
      } else if (reader.hasNext()) {
        Object obj = reader.next(value);
        assert obj == value : "Reader returned different object " + obj;
        return true;
      }
      return false;
    }

    @Override
    public NullWritable createKey() {
      return NullWritable.get();
    }

    @Override
    public OrcStruct createValue() {
      return row;
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (reader.getProgress() * length);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return reader.getProgress();
    }
  }

  public OrcInputFormat() {
    // just set a really small lower bound
    setMinSplitSize(16 * 1024);
  }

  @Override
  public RecordReader<NullWritable, OrcStruct>
      getRecordReader(InputSplit inputSplit, JobConf conf,
                      Reporter reporter) throws IOException {
    FileSplit fileSplit = (FileSplit) inputSplit;
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    reporter.setStatus(fileSplit.toString());
    return new OrcRecordReader(OrcFile.createReader(fs, path, conf),
                               fileSplit.getStart(), fileSplit.getLength());
  }

  @Override
  public boolean validateInput(FileSystem fs, HiveConf conf,
                               ArrayList<FileStatus> files
                              ) throws IOException {
    if (files.size() <= 0) {
      return false;
    }
    for (FileStatus file : files) {
      try {
        OrcFile.createReader(fs, file.getPath(), conf);
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }
}
