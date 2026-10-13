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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** The same rows written with and without content defined chunking, to {@link BlackHoleOutputFile}. */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ContentDefinedChunkingWriteBenchmarks {

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m {"
      + " required int64 l; optional double d; optional binary s (STRING);"
      + " optional group list (LIST) { repeated group list { optional int64 element; } } }");

  @Param({"false", "true"})
  public boolean chunking;

  private final List<Group> rows = new ArrayList<>();

  @Setup
  public void setup() {
    Random random = new Random(TestDataFactory.DEFAULT_SEED);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    for (int i = 0; i < 100_000; i++) {
      Group row = factory.newGroup().append("l", random.nextLong());
      if (random.nextInt(10) > 0) {
        row.append("d", random.nextDouble()).append("s", Long.toString(random.nextLong(), 36));
      }
      Group list = row.addGroup("list");
      for (int n = random.nextInt(9); n > 0; n--) {
        list.addGroup("list").append("element", random.nextLong());
      }
      rows.add(row);
    }
  }

  /** Dictionary encoding is off and the page row count limit lifted, so that the hashing dominates. */
  @Benchmark
  public void write() throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(chunking ? CdcOptions.DEFAULT : null)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
  }
}
