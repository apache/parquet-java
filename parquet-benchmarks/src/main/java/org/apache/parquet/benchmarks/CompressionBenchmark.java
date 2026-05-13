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
package org.apache.parquet.benchmarks;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Isolated JMH benchmarks for raw Parquet compression and decompression throughput.
 *
 * <p>Measures the performance of {@link CompressionCodecFactory.BytesInputCompressor}
 * and {@link CompressionCodecFactory.BytesInputDecompressor} for each supported codec,
 * using the direct-memory {@link CodecFactory} path (same as actual Parquet file I/O).
 * Input data is generated to approximate realistic Parquet page content (a mix of
 * sequential, repeated, and random byte patterns).
 *
 * <p>This benchmark isolates the codec hot path from file I/O, encoding, and other
 * Parquet overhead, making it ideal for measuring compression-specific optimizations.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@State(Scope.Thread)
public class CompressionBenchmark {

  @Param({"SNAPPY", "ZSTD", "LZ4_RAW", "GZIP", "BROTLI"})
  public String codec;

  @Param({"65536", "131072", "262144", "1048576"})
  public int pageSize;

  private byte[] uncompressedData;
  private byte[] compressedData;
  private int decompressedSize;

  private CompressionCodecFactory.BytesInputCompressor compressor;
  private CompressionCodecFactory.BytesInputDecompressor decompressor;
  private CodecFactory factory;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    uncompressedData = generatePageData(pageSize, 42L);
    decompressedSize = uncompressedData.length;

    Configuration conf = new Configuration();
    factory = CodecFactory.createDirectCodecFactory(conf, DirectByteBufferAllocator.getInstance(), pageSize);
    CompressionCodecName codecName = CompressionCodecName.valueOf(codec);

    compressor = factory.getCompressor(codecName);
    decompressor = factory.getDecompressor(codecName);

    // Pre-compress for decompression benchmark; copy to a stable byte array
    // since the compressor may reuse its internal buffer.
    BytesInput compressed = compressor.compress(BytesInput.from(uncompressedData));
    compressedData = compressed.toByteArray();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    factory.release();
  }

  @Benchmark
  public BytesInput compress() throws IOException {
    return compressor.compress(BytesInput.from(uncompressedData));
  }

  @Benchmark
  public byte[] decompress() throws IOException {
    // Force materialization of the decompressed data. Without this, codecs using
    // the stream-based HeapBytesDecompressor (e.g. GZIP) would return a lazy
    // StreamBytesInput, deferring the actual work. toByteArray() is essentially
    // free for our optimized implementations (returns the existing byte[]).
    return decompressor
        .decompress(BytesInput.from(compressedData), decompressedSize)
        .toByteArray();
  }

  /**
   * Generates byte data that approximates realistic Parquet page content.
   * Mixes sequential runs, repeated values, low-range random, and full random
   * to produce a realistic compression ratio (~2-4x for fast codecs).
   */
  static byte[] generatePageData(int size, long seed) {
    Random random = new Random(seed);
    byte[] data = new byte[size];
    int i = 0;
    while (i < size) {
      int patternType = random.nextInt(4);
      int chunkSize = Math.min(random.nextInt(256) + 64, size - i);
      switch (patternType) {
        case 0: // Sequential bytes (highly compressible)
          for (int j = 0; j < chunkSize && i < size; j++) {
            data[i++] = (byte) (j & 0xFF);
          }
          break;
        case 1: // Repeated value (highly compressible)
          byte val = (byte) random.nextInt(256);
          for (int j = 0; j < chunkSize && i < size; j++) {
            data[i++] = val;
          }
          break;
        case 2: // Small range random (moderately compressible)
          for (int j = 0; j < chunkSize && i < size; j++) {
            data[i++] = (byte) random.nextInt(16);
          }
          break;
        case 3: // Full random (low compressibility)
          byte[] randomChunk = new byte[chunkSize];
          random.nextBytes(randomChunk);
          int toCopy = Math.min(chunkSize, size - i);
          System.arraycopy(randomChunk, 0, data, i, toCopy);
          i += toCopy;
          break;
      }
    }
    return data;
  }
}
