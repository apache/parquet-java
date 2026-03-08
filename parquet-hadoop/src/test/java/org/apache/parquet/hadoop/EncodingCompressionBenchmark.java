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
package org.apache.parquet.hadoop;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFloat;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Codec-level benchmark comparing ALP, ZSTD (plain), and ByteStreamSplit+ZSTD.
 *
 * <p>Comparable to C++ encoding_alp_benchmark.cc. Reports encode and decode
 * throughput in MB/s plus compression ratio for each encoding strategy.
 */
public class EncodingCompressionBenchmark {

  private static final int NUM_VALUES = 1_000_000;
  private static final int WARMUP = 10;
  private static final int MEASURED = 30;

  // Datasets
  private static double[] doubleDecimal;
  private static double[] doubleInteger;
  private static double[] doubleMixed;
  private static float[] floatDecimal;
  private static float[] floatInteger;
  private static float[] floatMixed;

  @BeforeClass
  public static void setup() {
    Random rng = new Random(42);

    doubleDecimal = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleDecimal[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }

    doubleInteger = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleInteger[i] = (double) rng.nextInt(100000);
    }

    doubleMixed = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      doubleMixed[i] = Math.round(rng.nextDouble() * 10000) / 100.0;
    }
    for (int i = 0; i < NUM_VALUES; i += 50) {
      doubleMixed[i] = Double.NaN;
    }

    floatDecimal = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatDecimal[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }

    floatInteger = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatInteger[i] = (float) rng.nextInt(100000);
    }

    floatMixed = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      floatMixed[i] = Math.round(rng.nextFloat() * 10000) / 100.0f;
    }
    for (int i = 0; i < NUM_VALUES; i += 50) {
      floatMixed[i] = Float.NaN;
    }
  }

  @Test
  public void measureThroughput() throws IOException {
    System.out.println();
    System.out.println("=== Encoding/Compression Benchmark (1M values, " + MEASURED + " iters) ===");
    System.out.println();

    String hdr = String.format(
        "%-35s %10s %10s %10s %10s %8s", "Dataset / Encoding", "Enc MB/s", "Dec MB/s", "Raw KB", "Comp KB", "Ratio");
    String sep = "-".repeat(hdr.length());

    // --- Double datasets ---
    System.out.println("=== DOUBLE (8 bytes/value) ===");
    System.out.println(hdr);
    System.out.println(sep);

    benchAllDouble("double_decimal", doubleDecimal);
    System.out.println();
    benchAllDouble("double_integer", doubleInteger);
    System.out.println();
    benchAllDouble("double_mixed(2%exc)", doubleMixed);
    System.out.println();

    // --- Float datasets ---
    System.out.println("=== FLOAT (4 bytes/value) ===");
    System.out.println(hdr);
    System.out.println(sep);

    benchAllFloat("float_decimal", floatDecimal);
    System.out.println();
    benchAllFloat("float_integer", floatInteger);
    System.out.println();
    benchAllFloat("float_mixed(2%exc)", floatMixed);
    System.out.println();
  }

  // ---- Double benchmarks ----

  private void benchAllDouble(String datasetName, double[] data) throws IOException {
    long rawBytes = (long) data.length * Double.BYTES;

    // ALP
    byte[] alpComp = alpEncodeDoubles(data);
    benchEncodeDecode(
        datasetName + " ALP",
        rawBytes,
        alpComp.length,
        () -> alpEncodeDoubles(data),
        () -> alpDecodeDoubles(alpComp, data.length));

    // PLAIN + ZSTD
    byte[] plainBytes = plainEncodeDoubles(data);
    byte[] plainZstd = Zstd.compress(plainBytes);
    benchEncodeDecode(
        datasetName + " PLAIN+ZSTD",
        rawBytes,
        plainZstd.length,
        () -> Zstd.compress(plainEncodeDoubles(data)),
        () -> {
          byte[] dec = Zstd.decompress(plainZstd, plainBytes.length);
          consumeDoublesFromPlain(dec, data.length);
        });

    // BSS
    byte[] bssBytes = bssEncodeDoubles(data);
    benchEncodeDecode(
        datasetName + " BSS",
        rawBytes,
        bssBytes.length,
        () -> bssEncodeDoubles(data),
        () -> bssDecodeDoubles(bssBytes, data.length));

    // BSS + ZSTD
    byte[] bssZstd = Zstd.compress(bssBytes);
    benchEncodeDecode(
        datasetName + " BSS+ZSTD",
        rawBytes,
        bssZstd.length,
        () -> Zstd.compress(bssEncodeDoubles(data)),
        () -> {
          byte[] dec = Zstd.decompress(bssZstd, bssBytes.length);
          bssDecodeDoublesFromRaw(dec, data.length);
        });
  }

  // ---- Float benchmarks ----

  private void benchAllFloat(String datasetName, float[] data) throws IOException {
    long rawBytes = (long) data.length * Float.BYTES;

    // ALP
    byte[] alpComp = alpEncodeFloats(data);
    benchEncodeDecode(
        datasetName + " ALP",
        rawBytes,
        alpComp.length,
        () -> alpEncodeFloats(data),
        () -> alpDecodeFloats(alpComp, data.length));

    // PLAIN + ZSTD
    byte[] plainBytes = plainEncodeFloats(data);
    byte[] plainZstd = Zstd.compress(plainBytes);
    benchEncodeDecode(
        datasetName + " PLAIN+ZSTD",
        rawBytes,
        plainZstd.length,
        () -> Zstd.compress(plainEncodeFloats(data)),
        () -> {
          byte[] dec = Zstd.decompress(plainZstd, plainBytes.length);
          consumeFloatsFromPlain(dec, data.length);
        });

    // BSS
    byte[] bssBytes = bssEncodeFloats(data);
    benchEncodeDecode(
        datasetName + " BSS",
        rawBytes,
        bssBytes.length,
        () -> bssEncodeFloats(data),
        () -> bssDecodeFloats(bssBytes, data.length));

    // BSS + ZSTD
    byte[] bssZstd = Zstd.compress(bssBytes);
    benchEncodeDecode(
        datasetName + " BSS+ZSTD",
        rawBytes,
        bssZstd.length,
        () -> Zstd.compress(bssEncodeFloats(data)),
        () -> {
          byte[] dec = Zstd.decompress(bssZstd, bssBytes.length);
          bssDecodeFloatsFromRaw(dec, data.length);
        });
  }

  // ---- Benchmark harness ----

  @FunctionalInterface
  interface BenchRunnable {
    void run() throws Exception;
  }

  private void benchEncodeDecode(String name, long rawBytes, long compBytes, BenchRunnable enc, BenchRunnable dec)
      throws IOException {
    try {
      // Warmup
      for (int i = 0; i < WARMUP; i++) {
        enc.run();
      }
      long encNanos = 0;
      for (int i = 0; i < MEASURED; i++) {
        long t0 = System.nanoTime();
        enc.run();
        encNanos += System.nanoTime() - t0;
      }

      for (int i = 0; i < WARMUP; i++) {
        dec.run();
      }
      long decNanos = 0;
      for (int i = 0; i < MEASURED; i++) {
        long t0 = System.nanoTime();
        dec.run();
        decNanos += System.nanoTime() - t0;
      }

      double encMBps = (rawBytes * (double) MEASURED / (encNanos / 1e9)) / (1024.0 * 1024.0);
      double decMBps = (rawBytes * (double) MEASURED / (decNanos / 1e9)) / (1024.0 * 1024.0);
      double ratio = (double) compBytes / rawBytes * 100.0;

      System.out.printf(
          "%-35s %10.1f %10.1f %10d %10d %7.1f%%%n",
          name, encMBps, decMBps, rawBytes / 1024, compBytes / 1024, ratio);
    } catch (Exception e) {
      throw new IOException("Benchmark failed for " + name, e);
    }
  }

  // ---- ALP encode/decode ----

  private static byte[] alpEncodeDoubles(double[] values) throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void alpDecodeDoubles(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readDouble();
    }
  }

  private static byte[] alpEncodeFloats(float[] values) throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void alpDecodeFloats(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readFloat();
    }
  }

  // ---- PLAIN encode/decode ----

  private static byte[] plainEncodeDoubles(double[] values) {
    ByteBuffer buf = ByteBuffer.allocate(values.length * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (double v : values) {
      buf.putDouble(v);
    }
    return buf.array();
  }

  private static void consumeDoublesFromPlain(byte[] raw, int numValues) {
    ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numValues; i++) {
      buf.getDouble();
    }
  }

  private static byte[] plainEncodeFloats(float[] values) {
    ByteBuffer buf = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (float v : values) {
      buf.putFloat(v);
    }
    return buf.array();
  }

  private static void consumeFloatsFromPlain(byte[] raw, int numValues) {
    ByteBuffer buf = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numValues; i++) {
      buf.getFloat();
    }
  }

  // ---- ByteStreamSplit encode/decode ----

  private static byte[] bssEncodeDoubles(double[] values) throws IOException {
    ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
            64 * 1024, 8 * 1024 * 1024, HeapByteBufferAllocator.getInstance());
    for (double v : values) {
      writer.writeDouble(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void bssDecodeDoubles(byte[] encoded, int numValues) throws IOException {
    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readDouble();
    }
  }

  private static void bssDecodeDoublesFromRaw(byte[] encoded, int numValues) throws IOException {
    ByteStreamSplitValuesReaderForDouble reader = new ByteStreamSplitValuesReaderForDouble();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readDouble();
    }
  }

  private static byte[] bssEncodeFloats(float[] values) throws IOException {
    ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter writer =
        new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
            64 * 1024, 8 * 1024 * 1024, HeapByteBufferAllocator.getInstance());
    for (float v : values) {
      writer.writeFloat(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void bssDecodeFloats(byte[] encoded, int numValues) throws IOException {
    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readFloat();
    }
  }

  private static void bssDecodeFloatsFromRaw(byte[] encoded, int numValues) throws IOException {
    ByteStreamSplitValuesReaderForFloat reader = new ByteStreamSplitValuesReaderForFloat();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < numValues; i++) {
      reader.readFloat();
    }
  }
}
