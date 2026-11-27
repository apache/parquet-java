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
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Encoding-level micro-benchmarks for the DELTA_BINARY_PACKED encoding across
 * the Parquet types that support it: {@code INT32} and {@code INT64}.
 * Decoding benchmarks live in {@link DeltaBinaryPackedDecodingBenchmark}.
 *
 * <p>The {@code dataPattern} parameter exercises delta encoding across
 * different value distributions: sequential (small constant deltas), random
 * (large varying deltas), low-cardinality (many zero deltas from repeated
 * values), and high-cardinality (all unique, shuffled).
 *
 * <p>Each invocation encodes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class DeltaBinaryPackedEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;

  @Param({"SEQUENTIAL", "RANDOM", "LOW_CARDINALITY", "HIGH_CARDINALITY"})
  public String dataPattern;

  private int[] intData;
  private long[] longData;

  @Setup(Level.Trial)
  public void setup() {
    long seed = TestDataFactory.DEFAULT_SEED;
    int distinct = TestDataFactory.LOW_CARDINALITY_DISTINCT;

    switch (dataPattern) {
      case "SEQUENTIAL":
        intData = TestDataFactory.generateSequentialInts(VALUE_COUNT);
        longData = TestDataFactory.generateSequentialLongs(VALUE_COUNT);
        break;
      case "RANDOM":
        intData = TestDataFactory.generateRandomInts(VALUE_COUNT, seed);
        longData = TestDataFactory.generateRandomLongs(VALUE_COUNT, seed);
        break;
      case "LOW_CARDINALITY":
        intData = TestDataFactory.generateLowCardinalityInts(VALUE_COUNT, distinct, seed);
        longData = TestDataFactory.generateLowCardinalityLongs(VALUE_COUNT, distinct, seed);
        break;
      case "HIGH_CARDINALITY":
        intData = TestDataFactory.generateHighCardinalityInts(VALUE_COUNT, seed);
        longData = TestDataFactory.generateHighCardinalityLongs(VALUE_COUNT, seed);
        break;
      default:
        throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
    }
  }

  // ---- Writer factories ----

  private static DeltaBinaryPackingValuesWriterForInteger newIntWriter() {
    return new DeltaBinaryPackingValuesWriterForInteger(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DeltaBinaryPackingValuesWriterForLong newLongWriter() {
    return new DeltaBinaryPackingValuesWriterForLong(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ---- INT32 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeInt() throws IOException {
    ValuesWriter w = newIntWriter();
    for (int v : intData) {
      w.writeInteger(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- INT64 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeLong() throws IOException {
    ValuesWriter w = newLongWriter();
    for (long v : longData) {
      w.writeLong(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
