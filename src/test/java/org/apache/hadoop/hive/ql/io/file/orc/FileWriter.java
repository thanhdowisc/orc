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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

/**
 * A class that reads a TPCDS text data files and generates the equivalent
 * ORC and RC files.
 */
public class FileWriter {
  public static class Demographics implements Writable {
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

  static Iterable<Demographics> readDemographics(final String name) throws IOException {
    return new Iterable<Demographics>(){
      public Iterator<Demographics> iterator() {
        try {
          final BufferedReader reader = new BufferedReader(new FileReader(new File(name)));
          final Demographics item = new Demographics();
          return new Iterator<Demographics>(){
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
            public Demographics next() {
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
  public static class StoreSales implements Writable {
    public Integer field1;
    public Integer field2;
    public Integer field3;
    public Integer field4;
    public Integer field5;
    public Integer field6;
    public Integer field7;
    public Integer field8;
    public Integer field9;
    public Integer field10;
    public Integer field11;
    public Integer field12;
    public Integer field13;
    public Integer field14;
    public Integer field15;
    public Integer field16;
    public Integer field17;
    public Integer field18;
    public Integer field19;
    public Integer field20;
    public Integer field21;
    public Integer field22;
    public Integer field23;

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(field1);
      out.writeInt(field2);
      out.writeInt(field3);
      out.writeInt(field4);
      out.writeInt(field5);
      out.writeInt(field6);
      out.writeInt(field7);
      out.writeInt(field8);
      out.writeInt(field9);
      out.writeInt(field10);
      out.writeInt(field11);
      out.writeInt(field12);
      out.writeInt(field13);
      out.writeInt(field14);
      out.writeInt(field15);
      out.writeInt(field16);
      out.writeInt(field17);
      out.writeInt(field18);
      out.writeInt(field19);
      out.writeInt(field20);
      out.writeInt(field21);
      out.writeInt(field22);
      out.writeInt(field23);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      field1 = in.readInt();
      field2 = in.readInt();
      field3 = in.readInt();
      field4 = in.readInt();
      field5 = in.readInt();
      field6 = in.readInt();
      field7 = in.readInt();
      field8 = in.readInt();
      field9 = in.readInt();
      field10 = in.readInt();
      field11 = in.readInt();
      field12 = in.readInt();
      field13 = in.readInt();
      field14 = in.readInt();
      field15 = in.readInt();
      field16 = in.readInt();
      field17 = in.readInt();
      field18 = in.readInt();
      field19 = in.readInt();
      field20 = in.readInt();
      field21 = in.readInt();
      field22 = in.readInt();
      field23 = in.readInt();
    }
  }

  static Iterable<StoreSales> readStoreSales(final String name) throws IOException {
    return new Iterable<StoreSales>(){
      public Iterator<StoreSales> iterator() {
        try {
          final BufferedReader reader = new BufferedReader(new FileReader(new File(name)));
          final StoreSales item = new StoreSales();
          return new Iterator<StoreSales>(){
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
            private Integer parseInt(String val) {
              return "".equals(val) ? null :
                Integer.parseInt(val);
            }
            private Integer parseDecimal(String val) {
              return "".equals(val) ? null :
                Math.round(Float.parseFloat(val) * 100);
            }
            public StoreSales next() {
              String[] parts = next.split("\\|",24);
              item.field1 = parseInt(parts[0]);
              item.field2 = parseInt(parts[1]);
              item.field3 = parseInt(parts[2]);
              item.field4 = parseInt(parts[3]);
              item.field5 = parseInt(parts[4]);
              item.field6 = parseInt(parts[5]);
              item.field7 = parseInt(parts[6]);
              item.field8 = parseInt(parts[7]);
              item.field9 = parseInt(parts[8]);
              item.field10 = parseInt(parts[9]);
              item.field11 = parseInt(parts[10]);
              item.field12 = parseDecimal(parts[11]);
              item.field13 = parseDecimal(parts[12]);
              item.field14 = parseDecimal(parts[13]);
              item.field15 = parseDecimal(parts[14]);
              item.field16 = parseDecimal(parts[15]);
              item.field17 = parseDecimal(parts[16]);
              item.field18 = parseDecimal(parts[17]);
              item.field19 = parseDecimal(parts[18]);
              item.field20 = parseDecimal(parts[19]);
              item.field21 = parseDecimal(parts[20]);
              item.field22 = parseDecimal(parts[21]);
              item.field23 = parseDecimal(parts[22]);
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
    ObjectInspector demographicsInspector =
      ObjectInspectorFactory.getReflectionObjectInspector(Demographics.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    ObjectInspector salesInspector =
      ObjectInspectorFactory.getReflectionObjectInspector(StoreSales.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    long start;
    long end;
    Configuration conf = new Configuration();
    FileSystem local = FileSystem.getLocal(conf);
    org.apache.hadoop.io.compress.CompressionCodec codec =new DefaultCodec();
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
    */

    // rcfile writer
    start = System.currentTimeMillis();
    conf.set("hive.io.rcfile.column.number.conf", "9");
    SerDe serde = new LazyBinaryColumnarSerDe();
    Properties prop = new Properties();
    prop.setProperty("columns", "field1,field2,field3,field4,field5,field6,field7,field8,field9");
    prop.setProperty("columns.types", "int,string,string,string,int,string,int,int,int");
    serde.initialize(conf, prop);
    RCFile.Writer rcf = new RCFile.Writer(local, conf, new Path("file:///tmp/demographics.rcf"), null, codec);
    for(Demographics record: readDemographics("customer_demographics.dat")) {
      rcf.append(serde.serialize(record, demographicsInspector));
    }
    rcf.close();
    end = System.currentTimeMillis();
    System.out.println("rcf took " + (end - start));

    start = System.currentTimeMillis();
    conf.set("hive.io.rcfile.column.number.conf", "23");
    serde = new LazyBinaryColumnarSerDe();
    prop = new Properties();
    prop.setProperty("columns", "field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12,field13,field14,field15,field16,field17,field18,field19,field20,field21,field22,field23");
    prop.setProperty("columns.types", "int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int");
    serde.initialize(conf, prop);
    rcf = new RCFile.Writer(local, conf, new Path("file:///tmp/store_sales.rcf"), null, codec);
    for(StoreSales record: readStoreSales("store_sales.dat")) {
      rcf.append(serde.serialize(record, salesInspector));
    }
    rcf.close();
    end = System.currentTimeMillis();
    System.out.println("rcf took " + (end - start));

    // orc file writer
    start = System.currentTimeMillis();
    Writer writer = OrcFile.getWriter(local, new Path("file:///tmp/demographics.orc"),
      demographicsInspector, CompressionKind.ZLIB, 256 * 1024, conf);
    for(Demographics record: readDemographics("customer_demographics.dat")) {
      writer.addRow(record);
    }
    writer.close();
    end = System.currentTimeMillis();
    System.out.println("orc took " + (end - start));

    start = System.currentTimeMillis();
    writer = OrcFile.getWriter(local,
      new Path("file:///tmp/store_sales.orc"),
      salesInspector, CompressionKind.ZLIB, 256 * 1024, conf);
    long r = 0;
    for(StoreSales record: readStoreSales("store_sales.dat")) {
      writer.addRow(record);
    }
    writer.close();
    end = System.currentTimeMillis();
    System.out.println("orc took " + (end - start));
  }
}
