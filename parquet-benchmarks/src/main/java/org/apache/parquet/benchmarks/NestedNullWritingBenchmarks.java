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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.openjdk.jmh.annotations.Mode.SingleShotTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

import java.io.IOException;
import java.util.Random;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark to measure writing nested null values. (See PARQUET-343 for details.)
 * <p>
 * To execute this benchmark a jar file shall be created of this module. Then the jar file can be executed using the JMH
 * framework.<br>
 * The following one-liner (shall be executed in the parquet-benchmarks submodule) generates result statistics in the
 * file {@code jmh-result.json}. This json might be visualized by using the tool at
 * <a href="https://jmh.morethan.io">https://jmh.morethan.io</a>.
 *
 * <pre>
 * mvn clean package &amp;&amp; java -jar target/parquet-benchmarks.jar org.apache.parquet.benchmarks.NestedNullWritingBenchmarks -rf json
 * </pre>
 */
@BenchmarkMode(SingleShotTime)
@Fork(1)
@Warmup(iterations = 10, batchSize = 1)
@Measurement(iterations = 50, batchSize = 1)
@OutputTimeUnit(MILLISECONDS)
@State(Benchmark)
public class NestedNullWritingBenchmarks {
  private static final MessageType SCHEMA = Types.buildMessage()
      .optionalList()
      .optionalElement(INT32)
      .named("int_list")
      .optionalList()
      .optionalListElement()
      .optionalElement(BINARY)
      .named("dummy_list")
      .optionalMap()
      .key(BINARY)
      .value(BINARY, OPTIONAL)
      .named("dummy_map")
      .optionalGroup()
      .optional(BINARY).named("dummy_group_value1")
      .optional(BINARY).named("dummy_group_value2")
      .optional(BINARY).named("dummy_group_value3")
      .named("dummy_group")
      .named("msg");
  private static final int RECORD_COUNT = 10_000_000;
  private static final double NULL_RATIO = 0.99;
  private static final OutputFile BLACK_HOLE = new OutputFile() {
    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return -1L;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new PositionOutputStream() {
        private long pos;

        @Override
        public long getPos() throws IOException {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          ++pos;
        }
      };
    }

    @Override
    public String getPath() {
      throw new UnsupportedOperationException();
    }
  };

  private static class ValueGenerator {
    private static final GroupFactory FACTORY = new SimpleGroupFactory(SCHEMA);
    private static final Group NULL = FACTORY.newGroup();
    private final Random random = new Random(42);

    public Group nextValue() {
      if (random.nextDouble() > NULL_RATIO) {
        Group group = FACTORY.newGroup();
        group.addGroup("int_list").addGroup("list").append("element", random.nextInt());
        return group;
      } else {
        return NULL;
      }
    }
  }

  @Benchmark
  public void benchmarkWriting() throws IOException {
    ValueGenerator generator = new ValueGenerator();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(BLACK_HOLE)
        .withWriteMode(Mode.OVERWRITE)
        .withType(SCHEMA)
        .build()) {
      for (int i = 0; i < RECORD_COUNT; ++i) {
        writer.write(generator.nextValue());
      }
    }
  }
}
