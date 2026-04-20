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
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
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

/**
 * File-level write benchmarks measuring end-to-end Parquet write throughput through the
 * example {@link Group} API. Row contents are pre-generated during setup so compression
 * and writer settings dominate the timed section, while writes still flow through the
 * full Parquet writer path.
 *
 * <p>Writes are sent to a {@link BlackHoleOutputFile} to isolate CPU and encoding cost
 * from filesystem I/O. Parameterized across compression codec, writer version, and
 * dictionary encoding.
 *
 * <p>{@link Mode#SingleShotTime} is used because each invocation does enough work
 * (a full write of {@value TestDataFactory#DEFAULT_ROW_COUNT} rows) that JIT
 * amortization across invocations is unnecessary.
 */
@BenchmarkMode(Mode.SingleShotTime)
@Fork(1)
@Warmup(iterations = 3, batchSize = 1)
@Measurement(iterations = 5, batchSize = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FileWriteBenchmark {

  @Param({"UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP"})
  public String codec;

  @Param({"PARQUET_1_0", "PARQUET_2_0"})
  public String writerVersion;

  @Param({"true", "false"})
  public String dictionary;

  private Group[] rows;

  @Setup(Level.Trial)
  public void setup() {
    rows = TestDataFactory.generateRows(
        TestDataFactory.newGroupFactory(), TestDataFactory.DEFAULT_ROW_COUNT, TestDataFactory.DEFAULT_SEED);
  }

  @Benchmark
  public void writeFile() throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .withCompressionCodec(CompressionCodecName.valueOf(codec))
        .withWriterVersion(WriterVersion.valueOf(writerVersion))
        .withDictionaryEncoding(Boolean.parseBoolean(dictionary))
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
  }
}
