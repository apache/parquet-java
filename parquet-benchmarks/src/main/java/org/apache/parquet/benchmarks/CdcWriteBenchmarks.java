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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
 * What content defined chunking costs to write with: the same rows, the same page row count limit,
 * chunking the only difference.
 *
 * <p>Both variants raise the page row count limit. At its default of 20 000 rows the limit cuts a
 * page long before a default sized chunk is reached, so the comparison would otherwise be measuring
 * page count rather than hashing. The rows are built once and written to {@link BlackHoleOutputFile},
 * so neither data generation nor filesystem I/O dilutes the difference.
 *
 * <p>This does not measure what the feature costs while it is off, which is the number that matters
 * to everyone not using it. That one is the existing {@link WriteBenchmarks} figures compared
 * across revisions, since a disabled chunker is the absence of work rather than a variant of it.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CdcWriteBenchmarks {

  private static final int ROW_COUNT = 100_000;

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message m {"
      + "  required int64 long_field;"
      + "  required binary binary_field;"
      + "  required group group {"
      + "    repeated int32 int_field;"
      + "  }"
      + "}");

  @Param({"false", "true"})
  public boolean chunking;

  private List<Group> rows;

  @Setup(Level.Trial)
  public void setup() {
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    Binary[] binaries = TestDataFactory.generateBinaryData(ROW_COUNT, 128, 0, TestDataFactory.DEFAULT_SEED);
    Random random = new Random(TestDataFactory.DEFAULT_SEED);
    rows = new ArrayList<>(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      Group group = factory.newGroup();
      group.append("long_field", (long) i);
      group.append("binary_field", binaries[i]);
      Group subGroup = group.addGroup("group");
      for (int j = 0; j < 8; j++) {
        subGroup.append("int_field", random.nextInt());
      }
      rows.add(group);
    }
  }

  @Benchmark
  public void write() throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withContentDefinedChunking(chunking)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
  }
}
