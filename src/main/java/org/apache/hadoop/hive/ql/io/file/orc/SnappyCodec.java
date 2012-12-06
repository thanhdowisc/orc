package org.apache.hadoop.hive.ql.io.file.orc;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

class SnappyCodec implements CompressionCodec {
  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow) throws IOException {
    int inBytes = in.remaining();
    if (out.remaining() >= inBytes) {
      int outStart = out.position();
      Snappy.compress(in, out);
      return (out.position() - outStart < inBytes);
    } else {
      Snappy.compress(in, overflow);
      if (overflow.position() >= inBytes) {
        return false;
      }
      int bytesToCopy = out.remaining();
      overflow.flip();
      out.put(overflow.array(), overflow.arrayOffset(), bytesToCopy);
      overflow.position(bytesToCopy);
      overflow.compact();
      return true;
    }
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    Snappy.uncompress(in, out);
  }
}
