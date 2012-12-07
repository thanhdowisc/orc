package org.apache.hadoop.hive.ql.io.file.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileReader {
  private static void readDemographics() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path("/tmp/demographics.orc");
    Reader file = OrcFile.getReader(FileSystem.getLocal(conf), path, conf);
    Reader.RecordReader reader = file.rows();
    java.io.FileWriter writer = new java.io.FileWriter("/tmp/demographics.txt");
    ORCStruct row = null;
    while (reader.hasNext()) {
      row = (ORCStruct) reader.next(row);
      for(int i=0; i < 9; ++i) {
        writer.append(row.getFieldValue(i).toString());
        writer.append("|");
      }
      writer.append("\n");
    }
    writer.close();
  }

  private static void readSales() throws Exception {
    Configuration conf = new Configuration();
    long start = System.currentTimeMillis();
    Path path = new Path("/tmp/store_sales.orc");
    Reader file = OrcFile.getReader(FileSystem.getLocal(conf), path, conf);
    Reader.RecordReader reader = file.rows();
    java.io.FileWriter writer = new java.io.FileWriter("/tmp/store_sales.txt");
    ORCStruct row = null;
    long r = 0;
    while (reader.hasNext()) {
      row = (ORCStruct) reader.next(row);
      for(int i=0; i < 23; ++i) {
        Object field = row.getFieldValue(i);
        if (field != null) {
          String val = field.toString();
          if (i > 10) {
            boolean negative = false;
            if (val.charAt(0) == '-') {
              negative = true;
              val = val.substring(1);
            }
            int len = val.length();
            if (len == 1 || len == 2) {
              val = "0.0".substring(0, 4-len) + val;
            } else {
              val = val.substring(0, len - 2) + "." +
                val.substring(len - 2, len);
            }
            if (negative) {
              val = "-" + val;
            }
          }
          writer.append(val);
        }
        writer.append("|");
      }
      writer.append("\n");
    }
    writer.close();
    long time = System.currentTimeMillis() - start;
    System.out.println("time = " + time);
  }

  public static void main(String[] args) throws Exception {
    readDemographics();
    readSales();
  }

}
