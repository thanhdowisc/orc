package org.apache.hadoop.hive.ql.io.orc;

public class BitPackReader {
	static int current = 0;
	static int bitsLeft = 8;
	private static int numRead = 0;
	private static byte[] packed;

	private static void readByte() {
		current = 0xff & packed[numRead++];
		bitsLeft = 8;
	}

	public static long[] unpack(byte[] inp, int numBits, int n) {
		if (numBits == 0) {
			throw new RuntimeException("Number of fixed bits cannot be 0.");
		}
		numRead = 0;
		packed = inp;
		long[] unpacked = new long[n];
		long mask = (1L << numBits)-1;
		if(numBits == Long.SIZE) {
			mask = (long) (Math.pow(2, numBits) - 1);
		}
		readByte();
		for (int i = 0; i < n; i++) {
			long result = 0;
			int bitsLeftToRead = numBits;
			while (bitsLeftToRead > bitsLeft) {
				result <<= bitsLeft;
				result |= current & ((1 << bitsLeft) - 1);
				bitsLeftToRead -= bitsLeft;
				readByte();
			}
			if (bitsLeftToRead > 0) {
				result <<= bitsLeftToRead;
				bitsLeft -= bitsLeftToRead;
				result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
			}
			unpacked[i] = result & mask;
		}
		return unpacked;
	}
}
