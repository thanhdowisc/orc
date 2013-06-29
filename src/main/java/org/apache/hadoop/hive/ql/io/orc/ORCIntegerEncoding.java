package org.apache.hadoop.hive.ql.io.orc;

/**
 * The structure of the new ORC integer encoding is as follows
 * 
 * 1 or 2 byte(s) header
 * Header is fragmented (DIRECT, DELTA or EXTENDED_FIXED_DELTA) as follows 
 * 2 bits - Encoding type (refer EncodingType enum)
 * 5 bits - Number of fixed bits (refer FixedBitSizes enum)
 * 9 bits - Run length (max 512 runs)
 * 
 * if encoding is FIXED_DELTA then 1 byte header is used which is fragmented as
 * 2 bits - Endoding type 
 * 6 bits - Runlength (max 64 runs)
 * 
 * Followed by header is the base field which is encoded as varint
 * base will be min value if DIRECT encoding is used
 * base will be the first element if DELTA encoding is used
 * base will be the first element if FIXED_DELTA/EXTENDED_FIXED_DELTA is used
 * 
 * Next field is a delta field which is encoded as varint
 * delta field is used only for FIXED_DELTA/EXTENDED_FIXED_DELTA to store delta
 * 
 * Last field is the blob which contains only positive values (zigzag deltas)
 * blob field is used only for DIRECT/DELTA encoding which stores the deltas
 * values computed from the base
 * 
 * @author pjayachandran
 * 
 */
public class ORCIntegerEncoding {

	enum EncodingType {
		DIRECT, DELTA, FIXED_DELTA, EXTENDED_FIXED_DELTA
	}

	public enum FixedBitSizes {
		ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE, THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN, TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX, TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;

		/**
		 * For a given fixed bit this function returns the corresponding ordinal
		 * 
		 * @param n
		 * @return ordinal value
		 */
		public static int fixedBitsToOrdinal(int n) {
			if (n == 0) {
				return ONE.ordinal();
			}

			if (n >= 1 && n <= 24) {
				return n - 1;
			} else if (n > 24 && n <= 26) {
				return TWENTYSIX.ordinal();
			} else if (n > 26 && n <= 28) {
				return TWENTYEIGHT.ordinal();
			} else if (n > 28 && n <= 30) {
				return THIRTY.ordinal();
			} else if (n > 30 && n <= 32) {
				return THIRTYTWO.ordinal();
			} else if (n > 32 && n <= 40) {
				return FORTY.ordinal();
			} else if (n > 40 && n <= 48) {
				return FORTYEIGHT.ordinal();
			} else if (n > 48 && n <= 56) {
				return FIFTYSIX.ordinal();
			} else {
				return SIXTYFOUR.ordinal();
			}
		}

		/**
		 * For a given ordinal this function returns the corresponding fixed bits
		 * 
		 * @param n
		 * @return fixed bit value
		 */
		public static int ordinalToFixedBits(int n) {

			if (n >= ONE.ordinal() && n <= TWENTYFOUR.ordinal()) {
				return n + 1;
			} else if (n == TWENTYSIX.ordinal()) {
				return 26;
			} else if (n == TWENTYEIGHT.ordinal()) {
				return 28;
			} else if (n == THIRTY.ordinal()) {
				return 30;
			} else if (n == THIRTYTWO.ordinal()) {
				return 32;
			} else if (n == FORTY.ordinal()) {
				return 40;
			} else if (n == FORTYEIGHT.ordinal()) {
				return 48;
			} else if (n == FIFTYSIX.ordinal()) {
				return 56;
			} else {
				return 64;
			}
		}

		/**
		 * For a given fixed bit this function will return the closest available
		 * fixed bit
		 * 
		 * @param n
		 * @return closest valid fixed bit
		 */
		public static int getClosestFixedBits(int n) {
			if (n == 0) {
				return 1;
			}

			if (n >= 1 && n <= 24) {
				return n;
			} else if (n > 24 && n <= 26) {
				return 26;
			} else if (n > 26 && n <= 28) {
				return 28;
			} else if (n > 28 && n <= 30) {
				return 30;
			} else if (n > 30 && n <= 32) {
				return 32;
			} else if (n > 32 && n <= 40) {
				return 40;
			} else if (n > 40 && n <= 48) {
				return 48;
			} else if (n > 48 && n <= 56) {
				return 56;
			} else {
				return 64;
			}
		}
	}

	// for encoding
	private static final int SCOPE = 512;
	private static final int MINIMUM_FIXED_RUNLENGTH = 3;
	private static long delta = 0;
	private static int fixedRunLength = 0;
	private static int variableRunLength = 0;
	private static long[] literals = new long[SCOPE];
	private static OutputBuffer output;
	private static EncodingType encoding = EncodingType.DIRECT;
	private static int numLiterals = 0;
	private static long min = 0;
	private static long max = 0;
	private static long base = 0;
	private static int numFixedBits = 0;
	public static long[] basedDeltas = null;
	public static long[] adjDeltas = null;
	private static long MIN_SCOPE = 16;
	private static int MAX_DELTA = 3;

	// for decoding
	private static long[] result;
	private static int resultIdx;

	private static void bitpackDeltas(long[] deltas) {
		// pack the encoding ordinal value to top 2 bits
		int enc = encoding.ordinal() << 6;
		// fixed bits ordinal values should be 5 bits long
		int fixedBitsOrdinal = FixedBitSizes.fixedBitsToOrdinal(numFixedBits);
		int fb = fixedBitsOrdinal << 1;
		// last 1 bit should store the 9th bit of variable run length
		int tailBits = (variableRunLength & 0x100) >>> 8;
		// pack the first byte
		int firstByte = enc | fb | tailBits;
		
		// second byte will store the remaining 8 bits of run length
		int secondByte = variableRunLength & 0xff;
		
		// write header
		output.writeByte((byte) firstByte);
		output.writeByte((byte) secondByte);
		
		// write base
		Utils.writeSignedLong(output, base);
		
		// bit packing the delta values and write each bytes
		int fixedBits = FixedBitSizes.getClosestFixedBits(numFixedBits);
		byte[] packed = BitPackWriter.pack(deltas, numLiterals, fixedBits);
		for (int i = 0; i < packed.length; i++) {
			output.writeByte(packed[i]);
		}
		
		// update the number of values written
		output.updateNumUValsWritten(variableRunLength + 1);
	}

	private static void writeValues() {
		if (encoding.equals(EncodingType.DIRECT)) {
			// System.out.println("DIRECT: " + variableRunLength);
			// for direct encoding pass the based deltas array
			bitpackDeltas(basedDeltas);
		} else if (encoding.equals(EncodingType.DELTA)) {
			// System.out.println("DELTA: " + variableRunLength);
			// for delta encoding pass the adjacent deltas array
			bitpackDeltas(adjDeltas);
		} else if (encoding.equals(EncodingType.FIXED_DELTA)) {
			// if the fixed run length is greater than 63 then use extended 
			// fixed run length encoding, else use fixed delta encoding which
			// uses 1 byte header
			if (fixedRunLength > 63) {
				// System.out.println("EXTENDED FIXED LENGTH: " + fixedRunLength
				// + " DELTA: " + delta);
				encoding = EncodingType.EXTENDED_FIXED_DELTA;
				writeValues();
			} else {
				// System.out.println("FIXED LENGTH: " + fixedRunLength +
				// " DELTA: " + delta);
				// in case of shorter runs, the top 2 bits of header will have
				// the encoding type and the remain 6 bits will store the short
				// run lengths
				int firstByte = encoding.ordinal() << 6;
				firstByte = firstByte | (fixedRunLength & 0x3f);
				
				// write header
				output.writeByte((byte) firstByte);
				
				// write the base value
				Utils.writeSignedLong(output, literals[0]);
				
				// write the fixed delta value
				Utils.writeSignedLong(output, delta);
				
				// update the number of values written
				output.updateNumUValsWritten(fixedRunLength + 1);
			}
		} else {
			// this is the extended version of fixed delta encoding
			// first byte will have 2 bits for encoding type, 5 bits which are
			// not used, 1 bit for storing the 9th bit of run length
			// FIXME: What to do with the wasted 5 bits? use it for run length?
			int firstByte = encoding.ordinal() << 6;
			firstByte = firstByte | ((fixedRunLength & 0x100) >>> 8);
			
			// second byte stores the remaining 8 bits of run length
			int secondByte = fixedRunLength & 0xff;
			
			// write header
			output.writeByte((byte) firstByte);
			output.writeByte((byte) secondByte);
			
			// write the base value
			Utils.writeSignedLong(output, literals[0]);
			
			// write the fixed delta value 
			Utils.writeSignedLong(output, delta);
			
			// update the number of values written
			output.updateNumUValsWritten(fixedRunLength + 1);
		}
		
		// clear all the variables
		clear();
	}

	/**
	 * clears all the static variables
	 */
	private static void clear() {
		numLiterals = 0;
		min = 0;
		max = 0;
		encoding = null;
		delta = 0;
		base = 0;
		numFixedBits = 0;
		basedDeltas = null;
		adjDeltas = null;
		fixedRunLength = 0;
		variableRunLength = 0;
	}

	public static void write(long val) {
		if (numLiterals == 0) {
			// start of run, initialize min, max
			literals[numLiterals++] = val;
			min = val;
			max = val;
		} else {
			// update the min, max values
			if (val < min) {
				min = val;
			}

			if (val > max) {
				max = val;
			}

			if (numLiterals == 1) {
				// first run can be a fixed or variable delta run
				delta = val - literals[0];
				literals[numLiterals++] = val;
				fixedRunLength = 1;
				variableRunLength = 1;
			} else {
				if (((val - literals[numLiterals - 1]) == delta)) {
					// fixed delta run
					
					// check if any variable runs exists that we need to flush out
					if (variableRunLength > 1) {
						// variable runs exists and the last two values are part
						// of fixed run length. so we should not consider last two
						// elements for variable run. however, we need to save
						// last two values since flush the variable run will 
						// reset numLiterals
						numLiterals -= 2;
						variableRunLength -= 2;
						long lastVal1 = literals[numLiterals];
						long lastVal2 = literals[numLiterals + 1];
						
						// if only 3 elements exists and if last 2 are part of
						// fixed run then consider the 1st element alone as
						// a small fixed delta of zero runlength 
						if (numLiterals == 1) {
							fixedRunLength = 0;
							encoding = EncodingType.FIXED_DELTA;
						} else {
							determineDirectOrDelta();
						}
						
						// flush the variable runs and copy the last two elements
						// that were part of fixed runs to the start of the literals
						writeValues();
						delta = lastVal2 - lastVal1;
						
						// copy last two vals that we saved to first and increment
						// fixed run counter
						literals[numLiterals++] = lastVal1;
						literals[numLiterals++] = lastVal2;
						fixedRunLength += 1;
					}
					
					// store the current value and increment the fixed run counter
					literals[numLiterals++] = val;
					fixedRunLength += 1;
					variableRunLength = 0;

					// if fixed run counter reaches the max scope, flush it out
					if (fixedRunLength == SCOPE - 1) {
						encoding = EncodingType.FIXED_DELTA;
						writeValues();
					}
				} else {
					// variable delta run
					
					// if there are any fixed runs and if that fixed runs are 
					// greater than minimum threshold, then first flush it out
					if (fixedRunLength > MINIMUM_FIXED_RUNLENGTH - 1) {
						encoding = EncodingType.FIXED_DELTA;
						writeValues();
					}

					// if the fixed runs doesn't meet the minimum threshold, then
					// consider it as a part of variable run
					if (fixedRunLength != 0) {
						variableRunLength = fixedRunLength;
						fixedRunLength = 0;
					}

					// if previous fixed run was flushed out, the counters will
					// be reset. so start a new run. 
					if (numLiterals == 0) {
						literals[numLiterals++] = val;
						min = val;
						max = val;
					} else {
						// save the current value and keep incrementing the 
						// variable run counter
						delta = val - literals[numLiterals - 1];
						literals[numLiterals++] = val;
						variableRunLength += 1;
						
						// this condition improves the compression ratio for
						// dataset with many outliers. If the variable run
						// exceeds the minimum scope, it will count the number
						// of fixed bits required for elements encountered so far.
						// The number of fixed required so far is compared with
						// the fixed bits required for subsequent value. if the
						// difference between them exceeds MAX_DELTA threshold 
						// then the run is cut, the current contents are flushed
						// out and new run is started. 
						if (variableRunLength > MIN_SCOPE - 1) {
							long range = max - min;
							int currentDelta = Utils.findNumBits(range);
							int nextDelta = Utils.findNumBits(val);
							int deltaDiff = nextDelta - currentDelta;
							if (Math.abs(deltaDiff) > MAX_DELTA) {
								// do not include the current element since it
								// is exceeding delta limit
								variableRunLength -= 1;
								numLiterals -= 1;
								determineDirectOrDelta();
								writeValues();
							}
							
							// new run started, persist the current element
							if (numLiterals == 0) {
								literals[numLiterals++] = val;
								min = val;
								max = val;
							}
						} 
						fixedRunLength = 0;
						
						// if the variable runs is equal to max scope then flush
						if (variableRunLength == SCOPE - 1) {
							determineDirectOrDelta();
							writeValues();
						}
					}

				}
			}
		}
	}

	/**
	 * This function determines which algorithm (delta or direct) will require 
	 * lesser number of bits to encode all the values in the buffer. 
	 * 
	 */
	private static void determineDirectOrDelta() {
		// stores the difference from base of all values in input array
		basedDeltas = new long[numLiterals];
		
		// stores the delta difference between adjacent values in input array
		adjDeltas = new long[numLiterals];
		
		// first element doesn't have adjacent element, hence 0
		adjDeltas[0] = 0;
		
		// stores the min of basedDeltas array
		long basedMin = 0;
		// stores the max of basedDeltas array
		long basedMax = 0;
		// stores the range of values in basedDeltas array, this value will be
		// used to find the number of fixed bits required to store all values 
		long basedRange = 0;
		
		// stores the min of adjDeltas array
		long adjMin = 0;
		// stores the max of basedDeltas array
		long adjMax = 0;
		// stores the range of values in adjDeltas array, this value will be
		// used to find the number of fixed bits required to store all values
		long adjRange = 0;
		
		// the delta payload will all be +ve numbers and so all delta values are 
		// zigzag encoded
		for (int i = 0; i < numLiterals; i++) {
			basedDeltas[i] = Utils.zigzagEncode(literals[i] - min);
			if (basedDeltas[i] < basedMin) {
				basedMin = basedDeltas[i];
			}
			if (basedDeltas[i] > basedMax) {
				basedMax = basedDeltas[i];
			}

			// we do not need to consider the first element, since its already
			// initialized to 0
			if (i >= 1) {
				adjDeltas[i] = Utils
						.zigzagEncode(literals[i] - literals[i - 1]);
				if (adjDeltas[i] < adjMin) {
					adjMin = adjDeltas[i];
				}
				if (adjDeltas[i] > adjMax) {
					adjMax = adjDeltas[i];
				}
			}
		}
		basedRange = basedMax - basedMin;
		adjRange = adjMax - adjMin;
		
		// find the number of fixed bits required for each encoding and select
		// the encoding with lesser number of fixed bits
		int bitsReqBased = Utils.findNumBits(basedRange);
		int bitsReqAdj = Utils.findNumBits(adjRange);
		
		// base value for DIRECT encoding will be the minimum value in the array
		// base value for DELTA encoding will be the first element in the array
		if (bitsReqBased <= bitsReqAdj) {
			encoding = EncodingType.DIRECT;
			base = min;
			numFixedBits = bitsReqBased;
		} else {
			encoding = EncodingType.DELTA;
			base = literals[0];
			numFixedBits = bitsReqAdj;
		}
	}

	public static OutputBuffer encode(long[] input) {
		clear();
		output = new OutputBuffer(input.length * Long.SIZE / 8);
		output.setNumUncompressedValues(input.length);
		// iterate over the input array and write them to output buffer
		for (int i = 0; i < input.length; i++) {
			write(input[i]);
		}

		// if only one element is left in buffer then use direct encoding
		// FIXME: Merge it with the next batch?
		if (numLiterals == 1) {
			encoding = EncodingType.FIXED_DELTA;
			fixedRunLength = 0;
			writeValues();
		}

		// if variable runs are not 0 then determine the delta encoding to use
		// and flush out the buffer
		if (variableRunLength != 0) {
			determineDirectOrDelta();
			writeValues();
		}

		// if fixed runs are not 0 then flush out the buffer
		if (fixedRunLength != 0) {
			encoding = EncodingType.FIXED_DELTA;
			writeValues();
		}

		// this is just for debuggin purpose. it checks if the total encoded values
		// is equal to total input values or not. 
		if (output.getTotalEncodedValues() != output.getNumUValsWritten()) {
			throw new RuntimeException("Not all values are written to buffer. "
					+ output.getNumUValsWritten() + " out of "
					+ output.getTotalEncodedValues());
		}
		return output;
	}

	public static long[] decode(OutputBuffer buffer)
			throws IllegalAccessException {
		output = buffer;
		result = new long[buffer.getTotalEncodedValues()];
		resultIdx = 0;
		// read the first 2 bits and determine the encoding type
		while (resultIdx < output.getTotalEncodedValues()) {
			int enc = (output.peek() >>> 6) & 0x03;
			if (EncodingType.DIRECT.ordinal() == enc) {
				readDirectValues();
			} else if (EncodingType.DELTA.ordinal() == enc) {
				readDeltaValues();
			} else if (EncodingType.FIXED_DELTA.ordinal() == enc) {
				readFixedDeltaValues();
			} else {
				readExtendedFixedDeltaValues();
			}
		}
		return result;
	}

	private static void readDeltaValues() throws IllegalAccessException {
		// read and extract values from header
		int firstByte = output.read();
		int fixedWidth = (firstByte >>> 1) & 0x1f;
		fixedWidth = FixedBitSizes.ordinalToFixedBits(fixedWidth);
		int runLength = (firstByte & 1) << 8;
		runLength |= output.read();
		// run lengths are always one off
		runLength = runLength + 1;
		
		// read the base value
		long base = Utils.readSignedLong(output);
		
		// number of bytes of blob
		int numBytesToRead = BitPackWriter.getTotalBytesRequired(runLength,
				fixedWidth);
		
		// create buffer space to fill up the blob and unpack the bit packed values
		byte[] buffer = new byte[numBytesToRead];
		for (int i = 0; i < numBytesToRead; i++) {
			buffer[i] = output.readByte();
		}
		long[] unpacked = BitPackReader.unpack(buffer, fixedWidth, runLength);
		
		// write the unpacked values (also zigzag decoded) to result buffer
		result[resultIdx++] = base;
		for (int i = 1; i < unpacked.length; i++) {
			result[resultIdx++] = result[resultIdx - 2]
					+ Utils.zigzagDecode(unpacked[i]);
		}
	}

	private static void readDirectValues() throws IllegalAccessException {
		// read and extract values from header
		int firstByte = output.read();
		int fixedWidth = (firstByte >>> 1) & 0x1f;
		fixedWidth = FixedBitSizes.ordinalToFixedBits(fixedWidth);
		int runLength = (firstByte & 1) << 8;
		runLength |= output.read();
		// run lengths are always one off
		runLength = runLength + 1;
		
		// read the base value
		long base = Utils.readSignedLong(output);
		
		// number of bytes of blob
		int numBytesToRead = BitPackWriter.getTotalBytesRequired(runLength,
				fixedWidth);
		
		// create buffer space to fill up the blob and unpack the bit packed values
		byte[] buffer = new byte[numBytesToRead];
		for (int i = 0; i < numBytesToRead; i++) {
			buffer[i] = output.readByte();
		}
		
		// write the unpacked values (also zigzag decoded) to result buffer
		long[] unpacked = BitPackReader.unpack(buffer, fixedWidth, runLength);
		for (int i = 0; i < unpacked.length; i++) {
			result[resultIdx++] = base + Utils.zigzagDecode(unpacked[i]);
		}
	}

	private static void readFixedDeltaValues() throws IllegalAccessException {
		// read 1 byte header and extract the run length
		int runLen = output.read() & 0x3f;
		
		// read base value
		long base = Utils.readSignedLong(output);
		
		// read delta value
		long del = Utils.readSignedLong(output);
		
		// for the run length repeat base + (delta * i)
		for (int i = 0; i < runLen + 1; i++) {
			result[resultIdx++] = base + (del * i);
		}
	}

	private static void readExtendedFixedDeltaValues()
			throws IllegalAccessException {
		// read 2 byte header and extract the run length
		int runLen = (output.read() & 1) << 8;
		runLen = runLen | output.read();
		
		// read base value
		long base = Utils.readSignedLong(output);
		
		// read delta value
		long del = Utils.readSignedLong(output);
		
		// for the run length repeat base + (delta * i)
		for (int i = 0; i < runLen + 1; i++) {
			result[resultIdx++] = base + (del * i);
		}
	}
}