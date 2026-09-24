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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
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

  private static final int TILE_FACTOR = 6; // 15K * 6 = 90K values
  private static final int WARMUP = 10;
  private static final int MEASURED = 30;

  private static final String CSV_DIR = "parquet-hadoop/src/test/resources";
  private static final String DOUBLE_CSV = "alp_spotify1_expect.csv.gz";
  private static final String FLOAT_CSV = "alp_float_spotify1_expect.csv.gz";

  private static final String[] COLUMNS = {
    "valence", "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "tempo"
  };

  private static double[][] doubleColumns;
  private static float[][] floatColumns;

  @BeforeClass
  public static void setup() throws IOException {
    Path csvDir = findCsvDir();

    double[][] rawDoubles = loadDoubleCsv(csvDir.resolve(DOUBLE_CSV));
    doubleColumns = new double[rawDoubles.length][];
    for (int c = 0; c < rawDoubles.length; c++) {
      doubleColumns[c] = tile(rawDoubles[c], rawDoubles[c].length * TILE_FACTOR);
    }

    float[][] rawFloats = loadFloatCsv(csvDir.resolve(FLOAT_CSV));
    floatColumns = new float[rawFloats.length][];
    for (int c = 0; c < rawFloats.length; c++) {
      floatColumns[c] = tile(rawFloats[c], rawFloats[c].length * TILE_FACTOR);
    }
  }

  @Test
  public void measureThroughput() throws IOException {
    System.out.println();
    System.out.printf("=== Encoding/Compression Benchmark (%d values per column, Spotify dataset, %d iters) ===%n",
        doubleColumns[0].length, MEASURED);
    System.out.println();

    String hdr = String.format(
        "%-35s %10s %10s %10s %10s %8s", "Column / Encoding", "Enc MB/s", "Dec MB/s", "Raw KB", "Comp KB", "Ratio");
    String sep = "-".repeat(hdr.length());

    // --- Double columns ---
    System.out.println("=== DOUBLE (8 bytes/value) ===");
    System.out.println(hdr);
    System.out.println(sep);

    for (int c = 0; c < doubleColumns.length; c++) {
      benchAllDouble(COLUMNS[c], doubleColumns[c]);
      System.out.println();
    }

    // --- Float columns ---
    System.out.println("=== FLOAT (4 bytes/value) ===");
    System.out.println(hdr);
    System.out.println(sep);

    for (int c = 0; c < floatColumns.length; c++) {
      benchAllFloat(COLUMNS[c], floatColumns[c]);
      System.out.println();
    }
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

  // ---- CSV loading and tiling ----

  private static Path findCsvDir() throws IOException {
    Path dir = Paths.get("").toAbsolutePath();
    for (int i = 0; i < 3; i++) {
      Path candidate = dir.resolve(CSV_DIR);
      if (Files.isDirectory(candidate) && Files.exists(candidate.resolve(DOUBLE_CSV))) {
        return candidate;
      }
      dir = dir.getParent();
      if (dir == null) break;
    }
    throw new IOException("Cannot find CSV directory '" + CSV_DIR
        + "'. Run from the parquet-java project root.");
  }

  private static double[][] loadDoubleCsv(Path csvPath) throws IOException {
    try (InputStream is = new GZIPInputStream(new FileInputStream(csvPath.toFile()))) {
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

  private static float[][] loadFloatCsv(Path csvPath) throws IOException {
    try (InputStream is = new GZIPInputStream(new FileInputStream(csvPath.toFile()))) {
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

  private static double[] tile(double[] source, int targetSize) {
    double[] result = new double[targetSize];
    int len = source.length;
    for (int i = 0; i < targetSize; i++) {
      int copyIdx = i / len; // 0 for first copy, 1 for second, etc.
      result[i] = source[i % len] + copyIdx;
    }
    return result;
  }

  private static float[] tile(float[] source, int targetSize) {
    float[] result = new float[targetSize];
    int len = source.length;
    for (int i = 0; i < targetSize; i++) {
      int copyIdx = i / len;
      result[i] = source[i % len] + copyIdx;
    }
    return result;
  }
}
