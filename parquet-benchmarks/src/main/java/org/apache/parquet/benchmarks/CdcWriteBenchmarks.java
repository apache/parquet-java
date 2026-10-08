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
 * The write cost of content defined chunking: the same rows written with and without it, for rows of
 * several shapes, since the cost is the hashing of every value's and level's bytes.
 *
 * <p>Both variants lift the page row count limit and turn dictionary encoding off: chunking changes
 * where pages end and when a dictionary falls back, either of which would dwarf the hashing. Rows are
 * built once and written to {@link BlackHoleOutputFile}, so neither data generation nor I/O is
 * measured. The cost while disabled is {@link WriteBenchmarks} compared across revisions.
 */
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CdcWriteBenchmarks {

  private static final int ROW_COUNT = 100_000;

  @Param({"false", "true"})
  public boolean chunking;

  /**
   * mixed: a long, a 128-byte binary and a list of eight ints; numbers: an int, a long and a nullable
   * double; strings: short strings, one of them nullable; lists: nullable lists of zero to eight
   * nullable longs.
   */
  @Param({"mixed", "numbers", "strings", "lists"})
  public String data;

  private MessageType schema;
  private List<Group> rows;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(TestDataFactory.DEFAULT_SEED);
    switch (data) {
      case "mixed":
        schema = MessageTypeParser.parseMessageType(
            "message m { required int64 l; required binary b; required group g { repeated int32 i; } }");
        break;
      case "numbers":
        schema = MessageTypeParser.parseMessageType(
            "message m { required int32 i; required int64 l; optional double d; }");
        break;
      case "strings":
        schema = MessageTypeParser.parseMessageType(
            "message m { required binary s (STRING); optional binary t (STRING); }");
        break;
      case "lists":
        schema = MessageTypeParser.parseMessageType(
            "message m { optional group l (LIST) { repeated group list { optional int64 element; } } }");
        break;
      default:
        throw new IllegalArgumentException("unknown data " + data);
    }
    Binary[] binaries = TestDataFactory.generateBinaryData(ROW_COUNT, 128, 0, TestDataFactory.DEFAULT_SEED);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    rows = new ArrayList<>(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      Group row = factory.newGroup();
      switch (data) {
        case "mixed":
          row.append("l", (long) i).append("b", binaries[i]);
          Group g = row.addGroup("g");
          for (int j = 0; j < 8; j++) {
            g.append("i", random.nextInt());
          }
          break;
        case "numbers":
          row.append("i", random.nextInt()).append("l", random.nextLong());
          if (random.nextInt(10) > 0) {
            row.append("d", random.nextDouble());
          }
          break;
        case "strings":
          row.append("s", "s" + random.nextInt(1_000_000));
          if (random.nextInt(10) > 0) {
            row.append("t", Long.toString(random.nextLong(), 36));
          }
          break;
        default:
          if (random.nextInt(10) > 0) {
            Group list = row.addGroup("l");
            for (int n = random.nextInt(9); n > 0; n--) {
              Group element = list.addGroup("list");
              if (random.nextInt(10) > 0) {
                element.append("element", random.nextLong());
              }
            }
          }
      }
      rows.add(row);
    }
  }

  @Benchmark
  public void write() throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BlackHoleOutputFile.INSTANCE)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withDictionaryEncoding(false)
        .withContentDefinedChunkingEnabled(chunking)
        .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
  }
}
