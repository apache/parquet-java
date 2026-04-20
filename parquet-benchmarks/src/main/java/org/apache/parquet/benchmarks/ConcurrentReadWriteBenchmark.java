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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Multi-threaded benchmarks measuring independent read and write throughput under
 * concurrency. Uses {@code @Threads(4)} by default (overridable via JMH {@code -t} flag).
 * This benchmark does not assert correctness; it measures the cost of each thread
 * writing a full file to a stateless sink or reading a shared pre-generated file.
 *
 * <ul>
 *   <li>{@link #concurrentWrite()} - each thread independently writes to a shared
 *       {@link BlackHoleOutputFile} (stateless sink)</li>
 *   <li>{@link #concurrentRead(Blackhole)} - each thread independently reads the same
 *       pre-generated Parquet file</li>
 * </ul>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, batchSize = 1)
@Measurement(iterations = 5, batchSize = 1)
@Threads(4)
@State(Scope.Benchmark)
public class ConcurrentReadWriteBenchmark {

  private File tempFile;
  private Group[] readRows;

  @State(Scope.Thread)
  public static class ThreadData {
    private Group[] rows;

    @Setup(Level.Trial)
    public void setup() {
      rows = TestDataFactory.generateRows(
          TestDataFactory.newGroupFactory(), TestDataFactory.DEFAULT_ROW_COUNT, 42L);
    }
  }

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // Generate a shared file for concurrent reads
    tempFile = File.createTempFile("parquet-concurrent-bench-", ".parquet");
    tempFile.deleteOnExit();
    tempFile.delete();

    readRows = TestDataFactory.generateRows(
        TestDataFactory.newGroupFactory(), TestDataFactory.DEFAULT_ROW_COUNT, 42L);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(tempFile.toPath()))
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .build()) {
      for (Group row : readRows) {
        writer.write(row);
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (tempFile != null && tempFile.exists()) {
      tempFile.delete();
    }
  }

  /**
   * Each thread writes a full file independently to the shared stateless
   * {@link BlackHoleOutputFile} sink.
   */
  @Benchmark
  public void concurrentWrite(ThreadData threadData) throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(TestDataFactory.FILE_BENCHMARK_SCHEMA)
        .build()) {
      for (Group row : threadData.rows) {
        writer.write(row);
      }
    }
  }

  /**
   * Each thread reads the full pre-generated file independently.
   */
  @Benchmark
  public void concurrentRead(Blackhole bh) throws IOException {
    InputFile inputFile = new LocalInputFile(tempFile.toPath());
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
