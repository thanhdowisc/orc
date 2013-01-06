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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Tests for the top level reader/writer of ORC files.
 */
public class TestOrcFile {

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, String string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    MiddleStruct(InnerStruct... items) {
      list.clear();
      for(InnerStruct item: items) {
        list.add(item);
      }
    }
  }

  public static class BigRow {
    Boolean boolean1;
    Byte byte1;
    Short short1;
    Integer int1;
    Long long1;
    Float float1;
    Double double1;
    BytesWritable bytes1;
    Text string1;
    MiddleStruct middle;
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();

    BigRow(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
           Double d1,
           BytesWritable b3, String s2, MiddleStruct m1,
           List<InnerStruct> l2, Map<Text, InnerStruct> m2) {
      this.boolean1 = b1;
      this.byte1 = b2;
      this.short1 = s1;
      this.int1 = i1;
      this.long1 = l1;
      this.float1 = f1;
      this.double1 = d1;
      this.bytes1 = b3;
      if (s2 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s2);
      }
      this.middle = m1;
      this.list = l2;
      this.map = m2;
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<Text, InnerStruct> map(InnerStruct... items)  {
    Map<Text, InnerStruct> result = new HashMap<Text, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(new Text(i.string1), i);
    }
    return result;
  }

  private static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    for(InnerStruct s: items) {
      result.add(s);
    }
    return result;
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  private static ByteBuffer byteBuf(int... items) {
     ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target/test/tmp"));

  @Test
  public void test1() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    ObjectInspector inspector =
        ObjectInspectorFactory.getReflectionObjectInspector(BigRow.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Path p = new Path(workDir, "file.orc");
    fs.delete(p, false);
    Writer writer = OrcFile.createWriter(fs, p, inspector,
        100000, CompressionKind.ZLIB, 10000);
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536, -4L,
        (float) 1.0, -5.0, bytes(0,1,2,3,4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map()));
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536, 42L,
        (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map(inner(5,"chani"), inner(1,"mauddib"))));
    writer.close();
    Reader reader = OrcFile.createReader(fs, p, conf);
    ObjectInspector readerInspector = reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct{boolean1: boolean, byte1: tinyint, short1: smallint, "
        + "int1: int, long1: bigint, float1: float, double1: double, bytes1: "
        + "binary, string1: string, middle: struct{list: list<struct{int1: int,"
        + " string1: string}>}, list: list<struct{int1: int, string1: string}>,"
        + " map: map<string, struct{int1: int, string1: string}>}",
        readerInspector.getTypeName());
  }

  @Test
  public void emptyFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(workDir, "file.orc");
    fs.delete(p, false);
    ObjectInspector inspector =
        ObjectInspectorFactory.getReflectionObjectInspector(BigRow.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Writer writer = OrcFile.createWriter(fs, p, inspector,
        1000, CompressionKind.NONE, 100);
    writer.close();
    Reader reader = OrcFile.createReader(fs, p, conf);
    assertEquals(false, reader.rows().hasNext());
    assertEquals(CompressionKind.NONE, reader.getCompression());
    assertEquals(0, reader.getNumberOfRows());
    assertEquals(0, reader.getCompressionSize());
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(3, reader.getLength());
    assertEquals(false, reader.getStripes().iterator().hasNext());
  }

  @Test
  public void metaData() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(workDir, "file.orc");
    fs.delete(p, false);
    ObjectInspector inspector =
        ObjectInspectorFactory.getReflectionObjectInspector(BigRow.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Writer writer = OrcFile.createWriter(fs, p, inspector,
        1000, CompressionKind.NONE, 100);
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128));
    writer.addUserMetadata("clobber", byteBuf(1,2,3));
    writer.addUserMetadata("clobber", byteBuf(4,3,2,1));
    writer.addRow(new BigRow(true, (byte) 127, (short) 1024, 42,
        42L*1024*1024*1024, (float) 3.1415, -2.713, null,
        null, null, null, null));
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();
    Reader reader = OrcFile.createReader(fs, p, conf);
    assertEquals(byteBuf(5,7,11,13,17,19), reader.getMetadataValue("clobber"));
    assertEquals(byteBuf(1,2,3,4,5,6,7,-1,-2,127,-128),
        reader.getMetadataValue("my.meta"));
    try {
      reader.getMetadataValue("unknown");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      // PASS
    }
    int i = 0;
    for(String key: reader.getMetadataKeys()) {
      if ("my.meta".equals(key) ||
          "clobber".equals(key)) {
        i += 1;
      } else {
        throw new IllegalArgumentException("unknown key " + key);
      }
    }
    assertEquals(2, i);
  }

  /**
   * We test union and timestamp separately since we need to make the
   * object inspector manually. (The Hive reflection-based doesn't handle
   * them properly.)
   */
  @Test
  public void testUnionAndTimestamp() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(workDir, "file.orc");
    fs.delete(p, false);
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT).
        addFieldNames("time").addFieldNames("union").
        addSubtypes(1).addSubtypes(2).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.TIMESTAMP).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.UNION).
        addSubtypes(3).addSubtypes(4).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).
        build());
    ObjectInspector inspector = OrcStruct.createObjectInspector(0, types);
    Writer writer = OrcFile.createWriter(fs, p, inspector,
        1000, CompressionKind.NONE, 100);
    OrcStruct row = new OrcStruct(2);
    OrcUnion union = new OrcUnion();
    row.setFieldValue(1, union);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-12 15:00:00"));
    union.set((byte) 0, new IntWritable(42));
    writer.addRow(row);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-20 12:00:00.123456789"));
    union.set((byte)1, new Text("hello"));
    writer.addRow(row);
    row.setFieldValue(0, null);
    row.setFieldValue(1, null);
    writer.addRow(row);
    row.setFieldValue(1, union);
    union.set((byte) 0, null);
    writer.addRow(row);
    union.set((byte) 1, null);
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(200000));
    row.setFieldValue(0, Timestamp.valueOf("1900-01-01 00:00:00"));
    writer.addRow(row);
    for(int i=1900; i < 2200; ++i) {
      row.setFieldValue(0, Timestamp.valueOf(i + "-05-05 12:34:56." + i));
      union.set((byte) (i & 1), new IntWritable(i*i));
      writer.addRow(row);
    }
  }
}
