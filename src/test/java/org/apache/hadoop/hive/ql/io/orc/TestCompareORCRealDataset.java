package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertArrayEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class TestCompareORCRealDataset {

	private static final String resDir = "src/test/resources";
	private static HashMap<Integer, Stats> newOrcStats = new HashMap<Integer, Stats>();
	private static int testNum = 0;

	private class Stats {
		public String name;
		public double compressionRatio;
		public long encodingTime;
		public long decodingTime;

		public Stats(String n, double cr, long et, long dt) {
			this.name = n;
			this.compressionRatio = cr;
			this.encodingTime = et;
			this.decodingTime = dt;
		}

		@Override
		public String toString() {
			return name + "- Compression Ratio: " + compressionRatio
					+ ". Encoding Time: " + encodingTime
					+ "ms. Decoding Time: " + decodingTime + "ms.";
		}
	}

	private void runTest(String res, long[] input) throws Exception {
		float totalBytes = input.length * Long.SIZE / 8;
		float cr = 0;

		System.out.println("\n" + res);
		// ORC's RLE encoder assumes all values are signed for integer types
//		long sEnc = System.currentTimeMillis();
//		OutputBuffer b = ORCRunLengthCompression.encode(input, true);
//		long eEnc = System.currentTimeMillis();
//		long sDec = System.currentTimeMillis();
//		long[] o = ORCRunLengthCompression.decode(b, true);
//		long eDec = System.currentTimeMillis();
//		cr = totalBytes / (float) b.getPosition();
//		assertArrayEquals(input, o);
//		System.out.println("OLD-ORC => Compression ratio: " + cr
//				+ ". Encoding time: " + (eEnc - sEnc) + " ms. Decoding time: "
//				+ (eDec - sDec) + " ms.");

		long sEnc1 = System.currentTimeMillis();
		OutputBuffer buffer = ORCIntegerEncoding.encode(input);
		long eEnc1 = System.currentTimeMillis();
		long sDec1 = System.currentTimeMillis();
		long[] output = ORCIntegerEncoding.decode(buffer);
		long eDec1 = System.currentTimeMillis();
		cr = totalBytes / (float) buffer.getPosition();
		assertArrayEquals(input, output);
		System.out.println("NEW-ORC => Compression ratio: " + cr
				+ ". Encoding time: " + (eEnc1 - sEnc1)
				+ " ms. Decoding time: " + (eDec1 - sDec1) + " ms.");
		// FIXME: add CSV output of stats.
		newOrcStats.put(testNum++, new Stats(res, cr, (eEnc1 - sEnc1),
				(eDec1 - sDec1)));
	}

	public long[] fetchData(String path) throws IOException {
		int idx = 0;
		long[] input = null;
		if (input == null) {
			FileInputStream stream = new FileInputStream(new File(path));
			try {
				FileChannel fc = stream.getChannel();
				MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
						fc.size());
				/* Instead of using default, pass in a decoder. */
				String[] lines = Charset.defaultCharset().decode(bb).toString()
						.split("\n");
				input = new long[lines.length];
				for (String line : lines) {
					long val = 0;
					try {
						val = Long.parseLong(line);
					} catch (NumberFormatException e) {
						// for now lets ignore (assign 0)
					}
					input[idx++] = val;
				}
			} finally {
				stream.close();
			}
		}
		return input;
	}

	@Test
	public void testAllRealDatasets() throws Exception {
		String path = resDir + File.separator;
		List<String> resources = new ArrayList<String>();

		// tpc-ds datasets
		for (int i = 1; i < 20; i++) {
			resources.add("web_sales_col_" + i);
		}
		resources.add("customer_demographics_col_1");
		resources.add("customer_demographics_col_5");
		resources.add("customer_demographics_col_7");

		// real datasets
		resources.add("aol_qlog_epoch_1");
		resources.add("usaf_data_1");
		resources.add("twitter_api_id");
		resources.add("twitter_search_id");
		resources.add("nasdaq_stock_volume_a");
		resources.add("nasdaq_stock_volume_b");
		resources.add("nasdaq_stock_volume_c");

		// int datatype
		resources.add("github_payload_id");
		resources.add("github_payload_number");
		resources.add("github_payload_size");
		resources.add("github_pull_request_comments");
		resources.add("github_repository_forks");
		resources.add("github_repository_size");
		resources.add("github_comment_id");
		resources.add("github_comment_position");
		resources.add("github_repository_watchers");
		resources.add("github_repository_open_issues");
		resources.add("github_payload_issue");
		resources.add("github_pull_request_additions");

		// string data
		resources.add("github_actor_name_data");
		resources.add("github_actor_name_len");
		resources.add("github_member_login");
		resources.add("github_member_login_len");
		resources.add("github_page_name_data");
		resources.add("github_page_name_len");
		resources.add("github_payload_name_data");
		resources.add("github_payload_name_len");
		resources.add("github_pull_request_base_repo_data");
		resources.add("github_pull_request_base_repo_len");
		resources.add("github_repo_name_data");
		resources.add("github_repo_name_len");

		// http archive
		resources.add("httparchive_bytes_js");
		resources.add("httparchive_bytes_json");
		resources.add("httparchive_bytes_total");
		resources.add("httparchive_create_date");
		resources.add("httparchive_page_speed");
		resources.add("httparchive_pageid");
		resources.add("httparchive_render_start");
		resources.add("httparchive_request_js");
		resources.add("httparchive_request_total");
		resources.add("httparchive_wptrun");

		for (String res : resources) {
			long[] input = fetchData(path + res);
			runTest(res, input);
		}

		serializeMapToFile("/tmp/orc.stats");
	}

	private void serializeMapToFile(String path) throws IOException {
		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		} else {
			file.delete();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		DecimalFormat df = new DecimalFormat("#.##");
		for (int i = 0; i < newOrcStats.size(); i++) {
			Stats entry = newOrcStats.get(i);
			String line = entry.name;
			line += "," + df.format(entry.compressionRatio) + ","
					+ entry.encodingTime + "," + entry.decodingTime + "\n";
			bw.write(line);
		}
		bw.close();
	}
}
