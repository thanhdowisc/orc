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
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFileDump {

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target/test/tmp"));
  Path resourceDir = new Path(System.getProperty("test.resources.dir",
      "src/test/resources"));

  static class MyRecord {
    float f;
    double d;
    MyRecord(float f, double d) {
      this.f = f;
      this.d = d;
    }
  }

  private static final String outputFilename = "/orc-file-dump.out";

  private static void checkOutput(String expected,
                                  String actual) throws Exception {
    BufferedReader eStream =
        new BufferedReader(new FileReader(expected));
    BufferedReader aStream =
        new BufferedReader(new FileReader(actual));
    String line = eStream.readLine();
    while (line != null) {
      assertEquals(line, aStream.readLine());
      line = eStream.readLine();
    }
    assertNull(eStream.readLine());
    assertNull(aStream.readLine());
  }

  @Test
  public void testDump() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(workDir, "file.orc");
    fs.delete(p, false);
    ObjectInspector inspector =
        ObjectInspectorFactory.getReflectionObjectInspector(MyRecord.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Writer writer = OrcFile.createWriter(fs, p, inspector,
        100000, CompressionKind.ZLIB, 10000);
    Random r1 = new Random(1);
    for(int i=0; i < 21000; ++i) {
      writer.addRow(new MyRecord(r1.nextFloat(), r1.nextDouble()));
    }
    writer.close();
    PrintStream origOut = System.out;
    FileOutputStream myOut = new FileOutputStream(workDir +
        "/orc-file-dump.out");

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{p.toString()});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(resourceDir + outputFilename, workDir + outputFilename);
  }
}
