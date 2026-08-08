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
package org.apache.parquet.column.values.alp.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Benchmark for ALP (Adaptive Lossless floating-Point) encoding.
 *
 * <p>Measures encode and decode throughput for float and double values across
 * multiple data patterns: decimal, integer, constant, and mixed with special
 * values. Also reports compressed size for compression ratio analysis.
 *
 * <p>Mirrors the C++ parquet-encoding-alp-benchmark for cross-language
 * performance comparison.
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-alp-encoding")
public class AlpEncodingBenchmark {

  private static final int NUM_VALUES = 50_000; // matching C++ benchmark element count

  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  // ========== Float data & compressed blobs ==========
  private static float[] floatDecimalData;
  private static float[] floatIntegerData;
  private static float[] floatConstantData;
  private static float[] floatMixedData;

  private static byte[] floatDecimalCompressed;
  private static byte[] floatIntegerCompressed;
  private static byte[] floatConstantCompressed;
  private static byte[] floatMixedCompressed;

  // ========== Double data & compressed blobs ==========
  private static double[] doubleDecimalData;
  private static double[] doubleIntegerData;
  private static double[] doubleConstantData;
  private static double[] doubleMixedData;

  private static byte[] doubleDecimalCompressed;
  private static byte[] doubleIntegerCompressed;
  private static byte[] doubleConstantCompressed;
  private static byte[] doubleMixedCompressed;

  @BeforeClass
  public static void prepare() throws IOException {
    Random rng = new Random(42);

    // --- Float datasets ---
    floatDecimalData = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatDecimalData[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }

    floatIntegerData = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatIntegerData[i] = (float) (rng.nextInt(100000));
    }

    floatConstantData = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatConstantData[i] = 3.14f;
    }

    floatMixedData = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatMixedData[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }
    // Inject ~2% special values
    for (int i = 0; i < NUM_VALUES; i += 50) {
      switch (i % 200) {
        case 0:
          floatMixedData[i] = Float.NaN;
          break;
        case 50:
          floatMixedData[i] = Float.POSITIVE_INFINITY;
          break;
        case 100:
          floatMixedData[i] = Float.NEGATIVE_INFINITY;
          break;
        case 150:
          floatMixedData[i] = -0.0f;
          break;
      }
    }

    // --- Double datasets ---
    doubleDecimalData = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleDecimalData[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }

    doubleIntegerData = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleIntegerData[i] = (double) (rng.nextInt(100000));
    }

    doubleConstantData = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleConstantData[i] = 3.14;
    }

    doubleMixedData = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleMixedData[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }
    for (int i = 0; i < NUM_VALUES; i += 50) {
      switch (i % 200) {
        case 0:
          doubleMixedData[i] = Double.NaN;
          break;
        case 50:
          doubleMixedData[i] = Double.POSITIVE_INFINITY;
          break;
        case 100:
          doubleMixedData[i] = Double.NEGATIVE_INFINITY;
          break;
        case 150:
          doubleMixedData[i] = -0.0;
          break;
      }
    }

    // --- Pre-compress all datasets ---
    floatDecimalCompressed = compressFloats(floatDecimalData);
    floatIntegerCompressed = compressFloats(floatIntegerData);
    floatConstantCompressed = compressFloats(floatConstantData);
    floatMixedCompressed = compressFloats(floatMixedData);

    doubleDecimalCompressed = compressDoubles(doubleDecimalData);
    doubleIntegerCompressed = compressDoubles(doubleIntegerData);
    doubleConstantCompressed = compressDoubles(doubleConstantData);
    doubleMixedCompressed = compressDoubles(doubleMixedData);

    // --- Print compression ratios ---
    System.out.println("=== ALP Compression Ratios ===");
    printRatio("Float decimal", floatDecimalCompressed.length, NUM_VALUES * 4);
    printRatio("Float integer", floatIntegerCompressed.length, NUM_VALUES * 4);
    printRatio("Float constant", floatConstantCompressed.length, NUM_VALUES * 4);
    printRatio("Float mixed", floatMixedCompressed.length, NUM_VALUES * 4);
    printRatio("Double decimal", doubleDecimalCompressed.length, NUM_VALUES * 8);
    printRatio("Double integer", doubleIntegerCompressed.length, NUM_VALUES * 8);
    printRatio("Double constant", doubleConstantCompressed.length, NUM_VALUES * 8);
    printRatio("Double mixed", doubleMixedCompressed.length, NUM_VALUES * 8);
  }

  private static void printRatio(String label, int compressedSize, int rawSize) {
    double ratio = 100.0 * compressedSize / rawSize;
    System.out.printf("  %-20s: %6d / %6d bytes = %5.1f%%%n", label, compressedSize, rawSize, ratio);
  }

  // ========== Float encode benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeFloatDecimal() throws IOException {
    compressFloats(floatDecimalData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeFloatInteger() throws IOException {
    compressFloats(floatIntegerData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeFloatConstant() throws IOException {
    compressFloats(floatConstantData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeFloatMixed() throws IOException {
    compressFloats(floatMixedData);
  }

  // ========== Float decode benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeFloatDecimal() throws IOException {
    decompressFloats(floatDecimalCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeFloatInteger() throws IOException {
    decompressFloats(floatIntegerCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeFloatConstant() throws IOException {
    decompressFloats(floatConstantCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeFloatMixed() throws IOException {
    decompressFloats(floatMixedCompressed, NUM_VALUES);
  }

  // ========== Double encode benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeDoubleDecimal() throws IOException {
    compressDoubles(doubleDecimalData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeDoubleInteger() throws IOException {
    compressDoubles(doubleIntegerData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeDoubleConstant() throws IOException {
    compressDoubles(doubleConstantData);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void encodeDoubleMixed() throws IOException {
    compressDoubles(doubleMixedData);
  }

  // ========== Double decode benchmarks ==========

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeDoubleDecimal() throws IOException {
    decompressDoubles(doubleDecimalCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeDoubleInteger() throws IOException {
    decompressDoubles(doubleIntegerCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeDoubleConstant() throws IOException {
    decompressDoubles(doubleConstantCompressed, NUM_VALUES);
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void decodeDoubleMixed() throws IOException {
    decompressDoubles(doubleMixedCompressed, NUM_VALUES);
  }

  // ========== Helpers ==========

  private static byte[] compressFloats(float[] values) throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static byte[] compressDoubles(double[] values) throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void decompressFloats(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readFloat();
    }
  }

  private static void decompressDoubles(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readDouble();
    }
  }
}
