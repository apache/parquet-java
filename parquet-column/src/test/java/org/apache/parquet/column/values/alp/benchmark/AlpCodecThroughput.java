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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Codec-level ALP throughput benchmark reporting MB/s.
 *
 * <p>Comparable to C++ encoding_alp_benchmark.cc. Measures encode and decode
 * throughput at the codec level (no Parquet pipeline overhead).
 */
public class AlpCodecThroughput {

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

  // Pre-compressed
  private static byte[] doubleDecimalComp;
  private static byte[] doubleIntegerComp;
  private static byte[] doubleMixedComp;
  private static byte[] floatDecimalComp;
  private static byte[] floatIntegerComp;
  private static byte[] floatMixedComp;

  @BeforeClass
  public static void setup() throws IOException {
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

    doubleDecimalComp = compressDoubles(doubleDecimal);
    doubleIntegerComp = compressDoubles(doubleInteger);
    doubleMixedComp = compressDoubles(doubleMixed);
    floatDecimalComp = compressFloats(floatDecimal);
    floatIntegerComp = compressFloats(floatInteger);
    floatMixedComp = compressFloats(floatMixed);
  }

  @Test
  public void measureThroughput() throws IOException {
    System.out.println();
    System.out.println("=== ALP Codec-Level Throughput (1M values) ===");
    System.out.printf("%-30s %10s %10s %10s %10s%n",
        "Dataset", "Enc MB/s", "Dec MB/s", "Raw KB", "Comp KB");
    System.out.println("------------------------------" +
        " ---------- ---------- ---------- ----------");

    benchDouble("double_decimal", doubleDecimal, doubleDecimalComp);
    benchDouble("double_integer", doubleInteger, doubleIntegerComp);
    benchDouble("double_mixed(2%exc)", doubleMixed, doubleMixedComp);
    benchFloat("float_decimal", floatDecimal, floatDecimalComp);
    benchFloat("float_integer", floatInteger, floatIntegerComp);
    benchFloat("float_mixed(2%exc)", floatMixed, floatMixedComp);

    System.out.println();
  }

  private void benchDouble(String name, double[] data, byte[] compressed) throws IOException {
    long rawBytes = (long) data.length * Double.BYTES;

    // Warmup encode
    for (int i = 0; i < WARMUP; i++) {
      compressDoubles(data);
    }
    // Measure encode
    long encNanos = 0;
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      compressDoubles(data);
      encNanos += System.nanoTime() - t0;
    }

    // Warmup decode
    for (int i = 0; i < WARMUP; i++) {
      decompressDoubles(compressed, data.length);
    }
    // Measure decode
    long decNanos = 0;
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      decompressDoubles(compressed, data.length);
      decNanos += System.nanoTime() - t0;
    }

    double encMBps = (rawBytes * MEASURED / (encNanos / 1e9)) / (1024.0 * 1024.0);
    double decMBps = (rawBytes * MEASURED / (decNanos / 1e9)) / (1024.0 * 1024.0);

    System.out.printf("%-30s %10.1f %10.1f %10d %10d%n",
        name, encMBps, decMBps, rawBytes / 1024, compressed.length / 1024);
  }

  private void benchFloat(String name, float[] data, byte[] compressed) throws IOException {
    long rawBytes = (long) data.length * Float.BYTES;

    for (int i = 0; i < WARMUP; i++) {
      compressFloats(data);
    }
    long encNanos = 0;
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      compressFloats(data);
      encNanos += System.nanoTime() - t0;
    }

    for (int i = 0; i < WARMUP; i++) {
      decompressFloats(compressed, data.length);
    }
    long decNanos = 0;
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      decompressFloats(compressed, data.length);
      decNanos += System.nanoTime() - t0;
    }

    double encMBps = (rawBytes * MEASURED / (encNanos / 1e9)) / (1024.0 * 1024.0);
    double decMBps = (rawBytes * MEASURED / (decNanos / 1e9)) / (1024.0 * 1024.0);

    System.out.printf("%-30s %10.1f %10.1f %10d %10d%n",
        name, encMBps, decMBps, rawBytes / 1024, compressed.length / 1024);
  }

  private static byte[] compressDoubles(double[] values) throws IOException {
    AlpValuesWriter.DoubleAlpValuesWriter writer = new AlpValuesWriter.DoubleAlpValuesWriter();
    for (double v : values) {
      writer.writeDouble(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static byte[] compressFloats(float[] values) throws IOException {
    AlpValuesWriter.FloatAlpValuesWriter writer = new AlpValuesWriter.FloatAlpValuesWriter();
    for (float v : values) {
      writer.writeFloat(v);
    }
    return writer.getBytes().toByteArray();
  }

  private static void decompressDoubles(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForDouble reader = new AlpValuesReaderForDouble();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readDouble();
    }
  }

  private static void decompressFloats(byte[] compressed, int numValues) throws IOException {
    AlpValuesReaderForFloat reader = new AlpValuesReaderForFloat();
    reader.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(compressed)));
    for (int i = 0; i < numValues; i++) {
      reader.readFloat();
    }
  }
}
