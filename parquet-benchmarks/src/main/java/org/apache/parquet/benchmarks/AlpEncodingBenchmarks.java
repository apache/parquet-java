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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for ALP (Adaptive Lossless floating-Point) encoding.
 *
 * <p>Compares ALP vs PLAIN encoding for float and double columns across write and read paths.
 * Both encodings use no compression codec (UNCOMPRESSED), so the comparison isolates the
 * effect of the encoding itself — not any downstream codec like ZSTD or SNAPPY.
 * Uses realistic floating-point data with limited decimal precision — the type of data ALP
 * is designed to compress (e.g. sensor readings, prices, timestamps as doubles).
 *
 * <p>Run with:
 * <pre>
 *   mvn package -pl parquet-benchmarks -am -DskipTests
 *   java -jar parquet-benchmarks/target/parquet-benchmarks.jar AlpEncodingBenchmarks
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AlpEncodingBenchmarks {

  private static final int N_ROWS = 1_000_000;

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
      "message alp_bench { required double double_col; required float float_col; }");

  // Pre-generated data arrays — allocated once, reused across iterations.
  private static final double[] DOUBLES = new double[N_ROWS];
  private static final float[] FLOATS = new float[N_ROWS];

  static {
    // Simulate sensor/metric data: values with 2-3 decimal digits of precision.
    // ALP excels at this pattern; plain encoding stores all 8 bytes per double regardless.
    for (int i = 0; i < N_ROWS; i++) {
      DOUBLES[i] = Math.round((i * 0.01 + 100.0) * 100.0) / 100.0;
      FLOATS[i] = (float) (Math.round((i * 0.01f + 10.0f) * 100.0f) / 100.0f);
    }
  }

  // Files written once per trial (for read benchmarks).
  private Path alpReadFile;
  private Path plainReadFile;

  // Files written fresh each iteration (for write benchmarks).
  private Path writeTarget;

  @Setup(Level.Trial)
  public void generateReadFiles() throws IOException {
    alpReadFile = Files.createTempFile("alp_bench_alp_", ".parquet");
    Files.delete(alpReadFile); // LocalOutputFile must not pre-exist
    writeParquetFile(alpReadFile, true);

    plainReadFile = Files.createTempFile("alp_bench_plain_", ".parquet");
    Files.delete(plainReadFile);
    writeParquetFile(plainReadFile, false);
  }

  @TearDown(Level.Trial)
  public void deleteReadFiles() throws IOException {
    if (alpReadFile != null) Files.deleteIfExists(alpReadFile);
    if (plainReadFile != null) Files.deleteIfExists(plainReadFile);
  }

  @Setup(Level.Iteration)
  public void prepareWriteTarget() throws IOException {
    writeTarget = Files.createTempFile("alp_bench_write_", ".parquet");
    Files.delete(writeTarget); // LocalOutputFile must not pre-exist
  }

  @TearDown(Level.Iteration)
  public void deleteWriteTarget() throws IOException {
    if (writeTarget != null) Files.deleteIfExists(writeTarget);
  }

  // ---------------------------------------------------------------------------
  // Write benchmarks
  // ---------------------------------------------------------------------------

  @Benchmark
  public void writeDoubleAndFloatALP() throws IOException {
    writeParquetFile(writeTarget, true);
  }

  @Benchmark
  public void writeDoubleAndFloatPlain() throws IOException {
    writeParquetFile(writeTarget, false);
  }

  // ---------------------------------------------------------------------------
  // Read benchmarks
  // ---------------------------------------------------------------------------

  @Benchmark
  public void readDoubleAndFloatALP(Blackhole bh) throws IOException {
    readParquetFile(alpReadFile, bh);
  }

  @Benchmark
  public void readDoubleAndFloatPlain(Blackhole bh) throws IOException {
    readParquetFile(plainReadFile, bh);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void writeParquetFile(Path path, boolean alp) throws IOException {
    try (org.apache.parquet.hadoop.ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(path))
            .withType(SCHEMA)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withAlpEncoding(alp)
            .withDictionaryEncoding(false)
            .build()) {
      for (int i = 0; i < N_ROWS; i++) {
        SimpleGroup row = new SimpleGroup(SCHEMA);
        row.add("double_col", DOUBLES[i]);
        row.add("float_col", FLOATS[i]);
        writer.write(row);
      }
    }
  }

  private static void readParquetFile(Path path, Blackhole bh) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(path))) {
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(SCHEMA);
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        RecordReader<Group> records =
            columnIO.getRecordReader(pages, new GroupRecordConverter(SCHEMA));
        long rowCount = pages.getRowCount();
        for (long i = 0; i < rowCount; i++) {
          Group row = records.read();
          bh.consume(row.getDouble("double_col", 0));
          bh.consume(row.getFloat("float_col", 0));
        }
      }
    }
  }
}
