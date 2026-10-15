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
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.io.api.Binary;
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
 * Encoding micro-benchmarks for the DELTA_BYTE_ARRAY encoding (also known as
 * incremental or front-compression encoding) across the Parquet types that
 * support it: {@code BINARY} and {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>Decoding benchmarks live in {@link DeltaByteArrayDecodingBenchmark}.
 *
 * <p>BINARY and FIXED_LEN_BYTE_ARRAY benchmarks use separate inner
 * {@link State} classes so their independent parameters ({@code stringLength}
 * vs {@code fixedLength}) do not form a JMH cross-product.
 *
 * <p>Each invocation encodes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DeltaByteArrayEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  // ---- Inner state classes ----

  /**
   * BINARY data parameterized by string length and data pattern. Sorted data
   * exercises prefix sharing (the encoding's primary design intent); random
   * data is the worst case with no shared prefixes.
   */
  @State(Scope.Thread)
  public static class BinaryState {

    @Param({"10", "100", "1000"})
    public int stringLength;

    @Param({"RANDOM", "SORTED"})
    public String dataPattern;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      switch (dataPattern) {
        case "RANDOM":
          data = TestDataFactory.generateBinaryData(
              VALUE_COUNT, stringLength, 0, TestDataFactory.DEFAULT_SEED);
          break;
        case "SORTED":
          data = TestDataFactory.generateSortedBinaryData(
              VALUE_COUNT, stringLength, TestDataFactory.DEFAULT_SEED);
          break;
        default:
          throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
      }
    }
  }

  /**
   * FIXED_LEN_BYTE_ARRAY data parameterized by fixed length and data pattern.
   * Fixed lengths map to common logical types: 2 = FLOAT16, 12 = INT96, 16 = UUID.
   */
  @State(Scope.Thread)
  public static class FlbaState {

    @Param({"2", "12", "16"})
    public int fixedLength;

    @Param({"RANDOM", "SORTED"})
    public String dataPattern;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      switch (dataPattern) {
        case "RANDOM":
          data = TestDataFactory.generateFixedLenByteArrays(
              VALUE_COUNT, fixedLength, 0, TestDataFactory.DEFAULT_SEED);
          break;
        case "SORTED":
          data = TestDataFactory.generateSortedFixedLenByteArrays(
              VALUE_COUNT, fixedLength, TestDataFactory.DEFAULT_SEED);
          break;
        default:
          throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
      }
    }
  }

  // ---- Writer factory ----

  private static DeltaByteArrayWriter newWriter() {
    return new DeltaByteArrayWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ---- BINARY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBinary(BinaryState state) throws IOException {
    ValuesWriter w = newWriter();
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- FIXED_LEN_BYTE_ARRAY ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFlba(FlbaState state) throws IOException {
    ValuesWriter w = newWriter();
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
