package org.apache.hadoop.hive.ql.io.orc;

public class BitPackWriter {
	static int bytesInLong = Long.SIZE / 8;
	static byte[] packed;
	static int numPacked = 0;
	static byte current = 0;
	static int bitsLeft = 8;

	public static void flush() {
		if (bitsLeft != 8) {
			writeByte();
		}
	}

	private static void writeByte() {
		packed[numPacked++] = current;
		current = 0;
		bitsLeft = 8;
	}

	public static byte[] pack(long[] inp, int n, int numBits) {
		
		if (numBits == 0) {
			throw new RuntimeException("Number of fixed bits cannot be 0.");
		}
		numPacked = 0;
		int totalBytes = getTotalBytesRequired(n, numBits);
		packed = new byte[totalBytes];

		for (int i = 0; i < n; i++) {
			long value = inp[i];
			int bitsToWrite = numBits;
			while (bitsToWrite > bitsLeft) {
				// add the bits to the bottom of the current word
				current |= value >>> (bitsToWrite - bitsLeft);
				// subtract out the bits we just added
				bitsToWrite -= bitsLeft;
				// zero out the bits above bitsToWrite
				value &= (1L << bitsToWrite) - 1;
				writeByte();
			}
			bitsLeft -= bitsToWrite;
			current |= value << bitsLeft;
			if (bitsLeft == 0) {
				writeByte();
			}
		}

		flush();
		return packed;
	}

	public static int getTotalBytesRequired(int n, int numBits) {
		if (numBits == 0) {
			throw new RuntimeException("Number of fixed bits cannot be 0.");
		}
		return (int) Math.ceil((double) (n * numBits) / 8.0);
	}

}
