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

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * End-to-end read benchmark: reads back 1M rows from a real Parquet file for each compression
 * codec, measuring the full read path (filesystem I/O + decompression + decoding).
 *
 * <p>Parameterized by {@code codec} so every codec is covered uniformly. This replaces the
 * previous layout that had one hand-written {@code @Benchmark} method per codec/block/page
 * combination (and therefore only covered UNCOMPRESSED, SNAPPY, GZIP and LZO). The file for the
 * selected codec is generated once per trial in {@link #generateFileForRead()} (skipped if it
 * already exists), reading the shared {@link DataGenerator#benchmarkFile corpus} that can be
 * pre-built for every codec via {@code DataGenerator generate} (see {@code run.sh generate}). Run
 * a subset with e.g. {@code -p codec=ZSTD,LZ4_RAW}.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
public class ReadBenchmarks {

  @Param({"UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"})
  public String codec;

  private Path file;

  @Setup(Level.Trial)
  public void generateFileForRead() throws IOException {
    CompressionCodecName codecName = CompressionCodecName.valueOf(codec);
    file = DataGenerator.benchmarkFile(codecName);
    new DataGenerator()
        .generateData(
            file,
            configuration,
            PARQUET_2_0,
            BLOCK_SIZE_DEFAULT,
            PAGE_SIZE_DEFAULT,
            FIXED_LEN_BYTEARRAY_SIZE,
            codecName,
            ONE_MILLION);
  }

  @Benchmark
  public void read1MRows(Blackhole blackhole) throws IOException {
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(configuration)
        .build()) {
      for (int i = 0; i < ONE_MILLION; i++) {
        Group group = reader.read();
        blackhole.consume(group.getBinary("binary_field", 0));
        blackhole.consume(group.getInteger("int32_field", 0));
        blackhole.consume(group.getLong("int64_field", 0));
        blackhole.consume(group.getBoolean("boolean_field", 0));
        blackhole.consume(group.getFloat("float_field", 0));
        blackhole.consume(group.getDouble("double_field", 0));
        blackhole.consume(group.getBinary("flba_field", 0));
        blackhole.consume(group.getInt96("int96_field", 0));
      }
    }
  }
}
