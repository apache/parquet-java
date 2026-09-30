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

import static org.apache.parquet.benchmarks.BenchmarkConstants.BLOCK_SIZE_DEFAULT;
import static org.apache.parquet.benchmarks.BenchmarkConstants.FIXED_LEN_BYTEARRAY_SIZE;
import static org.apache.parquet.benchmarks.BenchmarkConstants.ONE_MILLION;
import static org.apache.parquet.benchmarks.BenchmarkConstants.PAGE_SIZE_DEFAULT;
import static org.apache.parquet.benchmarks.BenchmarkFiles.configuration;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.openjdk.jmh.annotations.Scope.Thread;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * End-to-end write benchmark: writes 1M rows to a real Parquet file for each compression
 * codec, measuring the full write path (encoding + compression + filesystem I/O) as the primary
 * time metric, plus the resulting file size as the {@code compressedBytes} secondary metric.
 *
 * <p>Parameterized by {@code codec} so every codec is covered uniformly. This replaces the
 * previous layout that had one hand-written {@code @Benchmark} method per codec/block/page
 * combination (and therefore only covered UNCOMPRESSED, SNAPPY, GZIP and LZO). Block and page
 * size are fixed at their defaults; add {@code @Param} fields for them if that dimension is
 * needed. Run a subset with e.g. {@code -p codec=ZSTD,LZ4_RAW}.
 *
 * <p><b>Reading the size metric.</b> {@code compressedBytes} is a JMH {@link AuxCounters} of type
 * {@link AuxCounters.Type#EVENTS}, reported as the <em>sum</em> over measurement iterations;
 * divide by the {@code Cnt} column for the per-file size in bytes (it is deterministic per codec).
 */
@State(Thread)
@BenchmarkMode(Mode.SingleShotTime)
public class WriteBenchmarks {

  @Param({"UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"})
  public String codec;

  private final DataGenerator dataGenerator = new DataGenerator();

  /** Exposes the resulting file size to JMH as a secondary metric. */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Thread)
  public static class Sizes {
    public long compressedBytes;

    @Setup(Level.Iteration)
    public void reset() {
      compressedBytes = 0;
    }
  }

  @Setup(Level.Iteration)
  public void setup() {
    // clean existing test data at the beginning of each iteration
    dataGenerator.cleanup();
  }

  @Benchmark
  public void write1MRows(Sizes sizes) throws IOException {
    Path out = DataGenerator.benchmarkFile(CompressionCodecName.valueOf(codec));
    dataGenerator.generateData(
        out,
        configuration,
        PARQUET_2_0,
        BLOCK_SIZE_DEFAULT,
        PAGE_SIZE_DEFAULT,
        FIXED_LEN_BYTEARRAY_SIZE,
        CompressionCodecName.valueOf(codec),
        ONE_MILLION);
    sizes.compressedBytes =
        out.getFileSystem(configuration).getFileStatus(out).getLen();
  }
}
