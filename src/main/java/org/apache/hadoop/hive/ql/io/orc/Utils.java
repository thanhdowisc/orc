package org.apache.hadoop.hive.ql.io.orc;

import java.util.Random;

public class Utils {

	/**
	 * Count the number of bits required to encode the given value
	 * 
	 * @param value
	 * @return bits required to store value
	 */
	public static int findNumBits(long value) {
		int count = 0;
		while (value > 0) {
			count++;
			value = value >>> 1;
		}
		return count;
	}

	/**
	 * zigzag encode the given value
	 * 
	 * @param val
	 * @return zigzag encoded value
	 */
	public static long zigzagEncode(long val) {
		return (val << 1) ^ (val >> 63);
	}

	/**
	 * zigzag decode the given value
	 * 
	 * @param val
	 * @return zizag decoded value
	 */
	public static long zigzagDecode(long val) {
		return (val >>> 1) ^ -(val & 1);
	}

	/**
	 * Next random long value
	 * 
	 * @param rng
	 * @param n
	 * @return random long value
	 */
	public static long nextLong(Random rng, long n) {
		long bits, val;
		do {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}

	// this is used only for testing
	public static long[] deltaEncode(long[] inp) {
		long[] output = new long[inp.length];
		for (int i = 0; i < inp.length; i++) {
			output[i] = zigzagEncode(inp[i]);
		}
		return output;
	}
	
	public static long readSignedLong(OutputBuffer inp)
			throws IllegalAccessException {
		long val = readUnsignedLong(inp);
		return (val >>> 1) ^ -(val & 1);
	}

	// read varint encoded value
	public static long readUnsignedLong(OutputBuffer inp)
			throws IllegalAccessException {
		long result = 0;
	    long b;
	    int offset = 0;
	    do {
	      b = inp.read();
	      result |= (0x7f & b) << offset;
	      offset += 7;
	    } while (b >= 0x80);
	    
		return result;
	}

	public static void writeSignedLong(OutputBuffer output, long currentVal) {
		// if it is signed long then use zigzag encoding, refer protobuf
		// encoding
		writeUnsignedLong(output, (currentVal << 1) ^ (currentVal >> 63));
	}

	// variable length encoding
	public static void writeUnsignedLong(OutputBuffer output, long currentVal) {
		while (true) {
			// if val is less than 128 then we can store it directly
			if ((currentVal & ~0x7f) == 0) {
				output.writeByte((byte) currentVal);
				return;
			} else {
				output.writeByte((byte) ((currentVal & 0x7f) | 0x80));
				// unsigned right shift
				currentVal = currentVal >>> 7;
			}
		}
	}

}
