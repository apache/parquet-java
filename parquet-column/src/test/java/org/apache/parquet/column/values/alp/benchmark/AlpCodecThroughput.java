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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Codec-level ALP throughput benchmark using real Spotify dataset columns.
 *
 * <p>Comparable to C++ encoding_alp_benchmark.cc. Measures encode and decode
 * throughput at the codec level (no Parquet pipeline overhead). Uses the same
 * Spotify audio features dataset as the C++ benchmark for direct comparison.
 *
 * <p>The CSV source has 15K rows per column; values are tiled to 1M for stable
 * measurement.
 */
public class AlpCodecThroughput {

  private static final int TARGET_VALUES = 1_000_000;
  private static final int WARMUP = 10;
  private static final int MEASURED = 30;

  private static final String DOUBLE_CSV = "alp_spotify1_expect.csv";
  private static final String FLOAT_CSV = "alp_float_spotify1_expect.csv";

  // Spotify column names matching C++ benchmark
  private static final String[] COLUMNS = {
    "valence", "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "tempo"
  };

  private static double[][] doubleColumns;
  private static float[][] floatColumns;
  private static byte[][] doubleCompressed;
  private static byte[][] floatCompressed;

  @BeforeClass
  public static void setup() throws IOException {
    // Load double columns from Spotify CSV
    double[][] rawDoubles = loadDoubleCsv(DOUBLE_CSV);
    doubleColumns = new double[rawDoubles.length][];
    doubleCompressed = new byte[rawDoubles.length][];
    for (int c = 0; c < rawDoubles.length; c++) {
      doubleColumns[c] = tile(rawDoubles[c], TARGET_VALUES);
      doubleCompressed[c] = compressDoubles(doubleColumns[c]);
    }

    // Load float columns from Spotify CSV
    float[][] rawFloats = loadFloatCsv(FLOAT_CSV);
    floatColumns = new float[rawFloats.length][];
    floatCompressed = new byte[rawFloats.length][];
    for (int c = 0; c < rawFloats.length; c++) {
      floatColumns[c] = tile(rawFloats[c], TARGET_VALUES);
      floatCompressed[c] = compressFloats(floatColumns[c]);
    }
  }

  @Test
  public void measureThroughput() throws IOException {
    System.out.println();
    System.out.printf("=== ALP Codec-Level Throughput (%dK values, Spotify dataset) ===%n",
        TARGET_VALUES / 1000);
    System.out.println();

    // Double columns
    System.out.printf("%-30s %10s %10s %10s %10s%n",
        "Double Column", "Enc MB/s", "Dec MB/s", "Raw KB", "Comp KB");
    System.out.println("------------------------------"
        + " ---------- ---------- ---------- ----------");
    for (int c = 0; c < doubleColumns.length; c++) {
      benchDouble(COLUMNS[c], doubleColumns[c], doubleCompressed[c]);
    }

    System.out.println();

    // Float columns
    System.out.printf("%-30s %10s %10s %10s %10s%n",
        "Float Column", "Enc MB/s", "Dec MB/s", "Raw KB", "Comp KB");
    System.out.println("------------------------------"
        + " ---------- ---------- ---------- ----------");
    for (int c = 0; c < floatColumns.length; c++) {
      benchFloat(COLUMNS[c], floatColumns[c], floatCompressed[c]);
    }

    System.out.println();
  }

  // ========== CSV loading ==========

  private static double[][] loadDoubleCsv(String resource) throws IOException {
    try (InputStream is = AlpCodecThroughput.class.getClassLoader().getResourceAsStream(resource)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resource);
      }
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String header = br.readLine();
      int numCols = header.split(",").length;

      List<double[]> rows = new ArrayList<>();
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        double[] row = new double[numCols];
        for (int i = 0; i < numCols; i++) {
          row[i] = Double.parseDouble(parts[i]);
        }
        rows.add(row);
      }

      // Transpose: rows -> columns
      double[][] columns = new double[numCols][rows.size()];
      for (int r = 0; r < rows.size(); r++) {
        double[] row = rows.get(r);
        for (int c = 0; c < numCols; c++) {
          columns[c][r] = row[c];
        }
      }
      return columns;
    }
  }

  private static float[][] loadFloatCsv(String resource) throws IOException {
    try (InputStream is = AlpCodecThroughput.class.getClassLoader().getResourceAsStream(resource)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resource);
      }
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String header = br.readLine();
      int numCols = header.split(",").length;

      List<float[]> rows = new ArrayList<>();
      String line;
      while ((line = br.readLine()) != null) {
        String[] parts = line.split(",");
        float[] row = new float[numCols];
        for (int i = 0; i < numCols; i++) {
          row[i] = Float.parseFloat(parts[i]);
        }
        rows.add(row);
      }

      float[][] columns = new float[numCols][rows.size()];
      for (int r = 0; r < rows.size(); r++) {
        float[] row = rows.get(r);
        for (int c = 0; c < numCols; c++) {
          columns[c][r] = row[c];
        }
      }
      return columns;
    }
  }

  // ========== Tiling ==========

  private static double[] tile(double[] source, int targetSize) {
    double[] result = new double[targetSize];
    for (int i = 0; i < targetSize; i++) {
      result[i] = source[i % source.length];
    }
    return result;
  }

  private static float[] tile(float[] source, int targetSize) {
    float[] result = new float[targetSize];
    for (int i = 0; i < targetSize; i++) {
      result[i] = source[i % source.length];
    }
    return result;
  }

  // ========== Benchmark methods ==========

  private void benchDouble(String name, double[] data, byte[] compressed) throws IOException {
    long rawBytes = (long) data.length * Double.BYTES;

    for (int i = 0; i < WARMUP; i++) {
      compressDoubles(data);
    }
    long encNanos = 0;
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      compressDoubles(data);
      encNanos += System.nanoTime() - t0;
    }

    for (int i = 0; i < WARMUP; i++) {
      decompressDoubles(compressed, data.length);
    }
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

  // ========== Compress / Decompress ==========

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
