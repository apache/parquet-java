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
import java.util.concurrent.TimeUnit;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * CPU-only read benchmarks measuring decoding and decompression throughput through the
 * example {@link Group} API, isolated from filesystem I/O. A Parquet file is written
 * to an in-memory byte array during setup, then read back from an {@link InMemoryInputFile}
 * during the benchmark so that no disk access contaminates the results.
 *
 * <p>Parameterized across compression codec and writer version. For end-to-end benchmarks
 * that include filesystem I/O, see {@link FileReadBenchmark}.
 *
 * <p>{@link Mode#SingleShotTime} is used because each invocation does enough work
 * (a full read of {@value TestDataFactory#DEFAULT_ROW_COUNT} rows) that JIT
 * amortization across invocations is unnecessary. Ten measurement iterations
 * provide stable statistics for SS mode.
 */
@BenchmarkMode(Mode.SingleShotTime)
@Fork(1)
@Warmup(iterations = 5, batchSize = 1)
@Measurement(iterations = 10, batchSize = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CpuReadBenchmark {

  @Param({"UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP", "LZ4_RAW", "BROTLI", "LZO"})
  public String codec;

  @Param({"PARQUET_1_0", "PARQUET_2_0"})
  public String writerVersion;

  private byte[] fileBytes;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Group[] rows = TestDataFactory.generateRows(
        TestDataFactory.newGroupFactory(), TestDataFactory.DEFAULT_ROW_COUNT, TestDataFactory.DEFAULT_SEED);
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .withCompressionCodec(CompressionCodecName.valueOf(codec))
        .withWriterVersion(WriterVersion.valueOf(writerVersion))
        .withDictionaryEncoding(true)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
    fileBytes = outputFile.toByteArray();
  }

  @Benchmark
  public void readFile(Blackhole bh) throws IOException {
    InMemoryInputFile inputFile = new InMemoryInputFile(fileBytes);
    try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(inputFile) {
      @Override
      protected ReadSupport<Group> getReadSupport() {
        return new GroupReadSupport();
      }
    }.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        bh.consume(group);
      }
    }
  }
}
