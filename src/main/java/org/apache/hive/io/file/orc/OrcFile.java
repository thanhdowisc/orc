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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class OrcFile {

  public static final String MAGIC = "ORC";

  Reader getReader(Path path) {
    return null;
  }

  public static Writer getWriter(FileSystem fs,
                                 Path path,
                                 TypeInfo type,
                                 ObjectInspector inspector,
                                 CompressionKind compress,
                                 int bufferSize,
                                 Configuration conf) throws IOException {
    return new WriterImpl(fs, path, type, inspector, compress, bufferSize,
      conf);
  }

  public static class MyRecord implements Writable {
    public int field1;
    public String field2;
    public String field3;
    public String field4;
    public int field5;
    public String field6;
    public int field7;
    public int field8;
    public int field9;

    @Override
    public void write(DataOutput out) throws IOException {
      out.write(field1);
      Text.writeString(out, field2);
      Text.writeString(out, field3);
      Text.writeString(out, field4);
      out.write(field5);
      Text.writeString(out, field6);
      out.write(field7);
      out.write(field8);
      out.write(field9);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      field1 = in.readInt();
      field2 = Text.readString(in);
      field3 = Text.readString(in);
      field4 = Text.readString(in);
      field5 = in.readInt();
      field6 = Text.readString(in);
      field7 = in.readInt();
      field8 = in.readInt();
      field9 = in.readInt();
    }
  }

  static Iterable<MyRecord> fileReader(final String name) throws IOException {
    return new Iterable<MyRecord>(){
      public Iterator<MyRecord> iterator() {
        try {
          final BufferedReader reader = new BufferedReader(new FileReader(new File(name)));
          final MyRecord item = new MyRecord();
          return new Iterator<MyRecord>(){
            String next;
            {
              try {
                do {
                  next = reader.readLine();
                } while (next != null && next.length() == 0);
              } catch (IOException ioe) {
                throw new IllegalArgumentException("bad read", ioe);
              }
            }
            public MyRecord next() {
              String[] parts = next.split("\\|");
              item.field1 = Integer.parseInt(parts[0]);
              item.field2 = parts[1];
              item.field3 = parts[2];
              item.field4 = parts[3];
              item.field5 = Integer.parseInt(parts[4]);
              item.field6 = parts[5];
              item.field7 = Integer.parseInt(parts[6]);
              item.field8 = Integer.parseInt(parts[7]);
              item.field9 = Integer.parseInt(parts[8]);
              try {
                do {
                  next = reader.readLine();
                } while (next != null && next.length() == 0);
              } catch (IOException ioe) {
                throw new IllegalArgumentException("bad read", ioe);
              }
              return item;
            }
            public boolean hasNext() {
              return next != null;
            }
            public void remove() {
              throw new UnsupportedOperationException("no remove");
            }
          };
        } catch (FileNotFoundException fnf) {
          throw new IllegalArgumentException("bad file " + name, fnf);
        }
      }
    };
  }

  public static void main(String[] args) throws Exception {
    ObjectInspector inspect =
      ObjectInspectorFactory.getReflectionObjectInspector(MyRecord.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    TypeInfo type = TypeInfoUtils.getTypeInfoFromObjectInspector(inspect);
    long start;
    long end;
    Configuration conf = new Configuration();
    FileSystem local = FileSystem.getLocal(conf);
    org.apache.hadoop.io.compress.CompressionCodec codec = new DefaultCodec();
    /*
    // seq file writer
    start = System.currentTimeMillis();
    SequenceFile.Writer seq = SequenceFile.createWriter(local,
      conf, new Path("file:///tmp/owen.seq"), NullWritable.class,
      MyRecord.class, SequenceFile.CompressionType.BLOCK,
      codec, null, new SequenceFile.Metadata());
    for(MyRecord record: fileReader("customer_demographics.dat")) {
      seq.append(NullWritable.get(), record);
    }
    seq.close();
    end = System.currentTimeMillis();
    System.out.println("seq took " + (end - start));

    // rcfile writer
    start = System.currentTimeMillis();
    conf.set("hive.io.rcfile.column.number.conf", "9");
    SerDe serde = new LazyBinaryColumnarSerDe();
    Properties prop = new Properties();
    prop.setProperty("columns", "field1,field2,field3,field4,field5,field6,field7,field8,field9");
    prop.setProperty("columns.types", "int,string,string,string,int,string,int,int,int");
    serde.initialize(conf, prop);
    RCFile.Writer rcf = new RCFile.Writer(local, conf, new Path("file:///tmp/owen.rcf"), null, codec);
    for(MyRecord record: fileReader("customer_demographics.dat")) {
      rcf.append(serde.serialize(record, inspect));
    }
    rcf.close();
    end = System.currentTimeMillis();
    System.out.println("rcf took " + (end - start));
    */
    // orc file writer
    start = System.currentTimeMillis();
    Writer writer = getWriter(local, new Path("file:///tmp/owen.orc"), type,
      inspect, CompressionKind.ZLIB, 256 * 1024, conf);
    for(MyRecord record: fileReader("customer_demographics.dat")) {
      writer.addRow(record);
    }
    writer.close();
    end = System.currentTimeMillis();
    System.out.println("orc took " + (end - start));
  }
}