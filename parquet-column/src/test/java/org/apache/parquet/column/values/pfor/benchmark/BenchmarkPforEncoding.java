/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.pfor.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import java.io.IOException;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.pfor.PforValuesReaderForInt;
import org.apache.parquet.column.values.pfor.PforValuesReaderForLong;
import org.apache.parquet.column.values.pfor.PforValuesWriter;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * PFOR encoding/decoding benchmarks for INT32 and INT64.
 *
 * <p>Data distributions are inspired by Snowflake's NumericComprBenchmark.cpp,
 * covering key archetypes that exercise PFOR's cost model differently:
 * <ul>
 *   <li>Constant: bit_width=0, best case</li>
 *   <li>Sequential: small range, ideal FOR</li>
 *   <li>SmallRange: clustered random, good FOR compression</li>
 *   <li>HighBaseSmallRange: high absolute values, small delta (timestamps)</li>
 *   <li>WithOutliers: tests exception handling path</li>
 *   <li>Random: worst case, full bit-width</li>
 *   <li>TPC-DS DateSk: realistic surrogate key distribution</li>
 *   <li>TPC-DS Quantity: bimodal small values</li>
 * </ul>
 *
 * <p>Excluded from normal test runs via surefire's benchmark exclusion pattern.
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-pfor-encoding")
public class BenchmarkPforEncoding {

  private static final int NUM_VALUES = 500_000;

  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  // ========== Pre-computed data ==========
  private static int[] intConstant;
  private static int[] intSequential;
  private static int[] intSmallRange;
  private static int[] intHighBaseSmallRange;
  private static int[] intWithOutliers;
  private static int[] intRandom;
  private static int[] intTpcdsSoldDateSk;
  private static int[] intTpcdsQuantity;

  private static long[] longConstant;
  private static long[] longSequential;
  private static long[] longSmallRange;
  private static long[] longHighBaseSmallRange;
  private static long[] longWithOutliers;
  private static long[] longRandom;
  private static long[] longTpcdsSoldDateSk;

  // Pre-encoded bytes for decode benchmarks
  private static byte[] intConstantBytes;
  private static byte[] intSequentialBytes;
  private static byte[] intSmallRangeBytes;
  private static byte[] intHighBaseSmallRangeBytes;
  private static byte[] intWithOutliersBytes;
  private static byte[] intRandomBytes;
  private static byte[] intTpcdsSoldDateSkBytes;
  private static byte[] intTpcdsQuantityBytes;

  private static byte[] longConstantBytes;
  private static byte[] longSequentialBytes;
  private static byte[] longSmallRangeBytes;
  private static byte[] longHighBaseSmallRangeBytes;
  private static byte[] longWithOutliersBytes;
  private static byte[] longRandomBytes;
  private static byte[] longTpcdsSoldDateSkBytes;

  @BeforeClass
  public static void prepare() throws IOException {
    // Generate INT32 distributions
    intConstant = genIntConstant(NUM_VALUES);
    intSequential = genIntSequential(NUM_VALUES);
    intSmallRange = genIntSmallRange(NUM_VALUES);
    intHighBaseSmallRange = genIntHighBaseSmallRange(NUM_VALUES);
    intWithOutliers = genIntWithOutliers(NUM_VALUES);
    intRandom = genIntRandom(NUM_VALUES);
    intTpcdsSoldDateSk = genIntTpcdsSoldDateSk(NUM_VALUES);
    intTpcdsQuantity = genIntTpcdsQuantity(NUM_VALUES);

    // Generate INT64 distributions
    longConstant = genLongConstant(NUM_VALUES);
    longSequential = genLongSequential(NUM_VALUES);
    longSmallRange = genLongSmallRange(NUM_VALUES);
    longHighBaseSmallRange = genLongHighBaseSmallRange(NUM_VALUES);
    longWithOutliers = genLongWithOutliers(NUM_VALUES);
    longRandom = genLongRandom(NUM_VALUES);
    longTpcdsSoldDateSk = genLongTpcdsSoldDateSk(NUM_VALUES);

    // Pre-encode for decode benchmarks
    intConstantBytes = encodeInts(intConstant);
    intSequentialBytes = encodeInts(intSequential);
    intSmallRangeBytes = encodeInts(intSmallRange);
    intHighBaseSmallRangeBytes = encodeInts(intHighBaseSmallRange);
    intWithOutliersBytes = encodeInts(intWithOutliers);
    intRandomBytes = encodeInts(intRandom);
    intTpcdsSoldDateSkBytes = encodeInts(intTpcdsSoldDateSk);
    intTpcdsQuantityBytes = encodeInts(intTpcdsQuantity);

    longConstantBytes = encodeLongs(longConstant);
    longSequentialBytes = encodeLongs(longSequential);
    longSmallRangeBytes = encodeLongs(longSmallRange);
    longHighBaseSmallRangeBytes = encodeLongs(longHighBaseSmallRange);
    longWithOutliersBytes = encodeLongs(longWithOutliers);
    longRandomBytes = encodeLongs(longRandom);
    longTpcdsSoldDateSkBytes = encodeLongs(longTpcdsSoldDateSk);

    // Print compression ratios
    System.out.println("=== PFOR Compression Ratios (compressed / uncompressed) ===");
    printIntRatio("Constant", intConstantBytes, intConstant.length);
    printIntRatio("Sequential", intSequentialBytes, intSequential.length);
    printIntRatio("SmallRange", intSmallRangeBytes, intSmallRange.length);
    printIntRatio("HighBaseSmallRange", intHighBaseSmallRangeBytes, intHighBaseSmallRange.length);
    printIntRatio("WithOutliers", intWithOutliersBytes, intWithOutliers.length);
    printIntRatio("Random", intRandomBytes, intRandom.length);
    printIntRatio("TpcdsSoldDateSk", intTpcdsSoldDateSkBytes, intTpcdsSoldDateSk.length);
    printIntRatio("TpcdsQuantity", intTpcdsQuantityBytes, intTpcdsQuantity.length);
    System.out.println("---");
    printLongRatio("Constant", longConstantBytes, longConstant.length);
    printLongRatio("Sequential", longSequentialBytes, longSequential.length);
    printLongRatio("SmallRange", longSmallRangeBytes, longSmallRange.length);
    printLongRatio("HighBaseSmallRange", longHighBaseSmallRangeBytes, longHighBaseSmallRange.length);
    printLongRatio("WithOutliers", longWithOutliersBytes, longWithOutliers.length);
    printLongRatio("Random", longRandomBytes, longRandom.length);
    printLongRatio("TpcdsSoldDateSk", longTpcdsSoldDateSkBytes, longTpcdsSoldDateSk.length);
  }

  // ========== INT32 Encode Benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntConstant() throws IOException {
    benchmarkIntEncode(intConstant);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntSequential() throws IOException {
    benchmarkIntEncode(intSequential);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntSmallRange() throws IOException {
    benchmarkIntEncode(intSmallRange);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntHighBaseSmallRange() throws IOException {
    benchmarkIntEncode(intHighBaseSmallRange);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntWithOutliers() throws IOException {
    benchmarkIntEncode(intWithOutliers);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntRandom() throws IOException {
    benchmarkIntEncode(intRandom);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntTpcdsSoldDateSk() throws IOException {
    benchmarkIntEncode(intTpcdsSoldDateSk);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeIntTpcdsQuantity() throws IOException {
    benchmarkIntEncode(intTpcdsQuantity);
  }

  // ========== INT32 Decode Benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntConstant() throws IOException {
    benchmarkIntDecode(intConstantBytes, intConstant.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntSequential() throws IOException {
    benchmarkIntDecode(intSequentialBytes, intSequential.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntSmallRange() throws IOException {
    benchmarkIntDecode(intSmallRangeBytes, intSmallRange.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntHighBaseSmallRange() throws IOException {
    benchmarkIntDecode(intHighBaseSmallRangeBytes, intHighBaseSmallRange.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntWithOutliers() throws IOException {
    benchmarkIntDecode(intWithOutliersBytes, intWithOutliers.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntRandom() throws IOException {
    benchmarkIntDecode(intRandomBytes, intRandom.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntTpcdsSoldDateSk() throws IOException {
    benchmarkIntDecode(intTpcdsSoldDateSkBytes, intTpcdsSoldDateSk.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeIntTpcdsQuantity() throws IOException {
    benchmarkIntDecode(intTpcdsQuantityBytes, intTpcdsQuantity.length);
  }

  // ========== INT64 Encode Benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongConstant() throws IOException {
    benchmarkLongEncode(longConstant);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongSequential() throws IOException {
    benchmarkLongEncode(longSequential);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongSmallRange() throws IOException {
    benchmarkLongEncode(longSmallRange);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongHighBaseSmallRange() throws IOException {
    benchmarkLongEncode(longHighBaseSmallRange);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongWithOutliers() throws IOException {
    benchmarkLongEncode(longWithOutliers);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongRandom() throws IOException {
    benchmarkLongEncode(longRandom);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void encodeLongTpcdsSoldDateSk() throws IOException {
    benchmarkLongEncode(longTpcdsSoldDateSk);
  }

  // ========== INT64 Decode Benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongConstant() throws IOException {
    benchmarkLongDecode(longConstantBytes, longConstant.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongSequential() throws IOException {
    benchmarkLongDecode(longSequentialBytes, longSequential.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongSmallRange() throws IOException {
    benchmarkLongDecode(longSmallRangeBytes, longSmallRange.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongHighBaseSmallRange() throws IOException {
    benchmarkLongDecode(longHighBaseSmallRangeBytes, longHighBaseSmallRange.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongWithOutliers() throws IOException {
    benchmarkLongDecode(longWithOutliersBytes, longWithOutliers.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongRandom() throws IOException {
    benchmarkLongDecode(longRandomBytes, longRandom.length);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  @Test
  public void decodeLongTpcdsSoldDateSk() throws IOException {
    benchmarkLongDecode(longTpcdsSoldDateSkBytes, longTpcdsSoldDateSk.length);
  }

  // ========== Benchmark Helpers ==========

  private void benchmarkIntEncode(int[] values) throws IOException {
    int capacity = Math.max(256, values.length * 8);
    PforValuesWriter.IntPforValuesWriter writer =
        new PforValuesWriter.IntPforValuesWriter(capacity, capacity, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }
    writer.getBytes();
    writer.close();
  }

  private void benchmarkIntDecode(byte[] encoded, int numValues) throws IOException {
    PforValuesReaderForInt reader = new PforValuesReaderForInt();
    reader.initFromPage(numValues,
        ByteBufferInputStream.wrap(java.nio.ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readInteger();
    }
  }

  private void benchmarkLongEncode(long[] values) throws IOException {
    int capacity = Math.max(512, values.length * 16);
    PforValuesWriter.LongPforValuesWriter writer =
        new PforValuesWriter.LongPforValuesWriter(capacity, capacity, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    writer.getBytes();
    writer.close();
  }

  private void benchmarkLongDecode(byte[] encoded, int numValues) throws IOException {
    PforValuesReaderForLong reader = new PforValuesReaderForLong();
    reader.initFromPage(numValues,
        ByteBufferInputStream.wrap(java.nio.ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readLong();
    }
  }

  // ========== Data Generators ==========

  private static int[] genIntConstant(int n) {
    int[] v = new int[n];
    java.util.Arrays.fill(v, 42);
    return v;
  }

  private static int[] genIntSequential(int n) {
    int[] v = new int[n];
    for (int i = 0; i < n; i++) v[i] = i;
    return v;
  }

  private static int[] genIntSmallRange(int n) {
    int[] v = new int[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 100000 + rng.nextInt(100001);
    return v;
  }

  private static int[] genIntHighBaseSmallRange(int n) {
    int[] v = new int[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 1704067200 + rng.nextInt(1001);
    return v;
  }

  private static int[] genIntWithOutliers(int n) {
    int[] v = new int[n];
    Random rng = new Random(42);
    for (int i = 0; i < n; i++) v[i] = 1000 + rng.nextInt(256);
    // ~1% outliers
    int numOutliers = Math.max(1, n / 100);
    for (int i = 0; i < numOutliers; i++) {
      v[rng.nextInt(n)] = Integer.MAX_VALUE / 2 + i;
    }
    return v;
  }

  private static int[] genIntRandom(int n) {
    int[] v = new int[n];
    Random rng = new Random(99);
    for (int i = 0; i < n; i++) v[i] = rng.nextInt();
    return v;
  }

  private static int[] genIntTpcdsSoldDateSk(int n) {
    int[] v = new int[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 2450815 + rng.nextInt(1821);
    return v;
  }

  private static int[] genIntTpcdsQuantity(int n) {
    int[] v = new int[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) {
      v[i] = (rng.nextInt(100) < 90) ? (1 + rng.nextInt(10)) : (11 + rng.nextInt(90));
    }
    return v;
  }

  private static long[] genLongConstant(int n) {
    long[] v = new long[n];
    java.util.Arrays.fill(v, 42L);
    return v;
  }

  private static long[] genLongSequential(int n) {
    long[] v = new long[n];
    for (int i = 0; i < n; i++) v[i] = i;
    return v;
  }

  private static long[] genLongSmallRange(int n) {
    long[] v = new long[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 100000L + rng.nextInt(100001);
    return v;
  }

  private static long[] genLongHighBaseSmallRange(int n) {
    long[] v = new long[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 1704067200L + rng.nextInt(1001);
    return v;
  }

  private static long[] genLongWithOutliers(int n) {
    long[] v = new long[n];
    Random rng = new Random(42);
    for (int i = 0; i < n; i++) v[i] = 1000L + rng.nextInt(256);
    int numOutliers = Math.max(1, n / 100);
    for (int i = 0; i < numOutliers; i++) {
      v[rng.nextInt(n)] = Long.MAX_VALUE / 2 + i;
    }
    return v;
  }

  private static long[] genLongRandom(int n) {
    long[] v = new long[n];
    Random rng = new Random(99);
    for (int i = 0; i < n; i++) v[i] = rng.nextLong();
    return v;
  }

  private static long[] genLongTpcdsSoldDateSk(int n) {
    long[] v = new long[n];
    Random rng = new Random(12345);
    for (int i = 0; i < n; i++) v[i] = 2450815L + rng.nextInt(1821);
    return v;
  }

  // ========== Encoding Helpers ==========

  private static byte[] encodeInts(int[] values) throws IOException {
    int capacity = Math.max(256, values.length * 8);
    PforValuesWriter.IntPforValuesWriter writer =
        new PforValuesWriter.IntPforValuesWriter(capacity, capacity, new DirectByteBufferAllocator());
    for (int v : values) {
      writer.writeInteger(v);
    }
    byte[] result = writer.getBytes().toByteArray();
    writer.close();
    return result;
  }

  private static byte[] encodeLongs(long[] values) throws IOException {
    int capacity = Math.max(512, values.length * 16);
    PforValuesWriter.LongPforValuesWriter writer =
        new PforValuesWriter.LongPforValuesWriter(capacity, capacity, new DirectByteBufferAllocator());
    for (long v : values) {
      writer.writeLong(v);
    }
    byte[] result = writer.getBytes().toByteArray();
    writer.close();
    return result;
  }

  private static void printIntRatio(String name, byte[] encoded, int numValues) {
    double ratio = 100.0 * encoded.length / (numValues * 4);
    System.out.printf("  INT32 %-25s: %6d bytes -> %6d bytes (%.1f%%)\n",
        name, numValues * 4, encoded.length, ratio);
  }

  private static void printLongRatio(String name, byte[] encoded, int numValues) {
    double ratio = 100.0 * encoded.length / (numValues * 8);
    System.out.printf("  INT64 %-25s: %6d bytes -> %6d bytes (%.1f%%)\n",
        name, numValues * 8, encoded.length, ratio);
  }
}
