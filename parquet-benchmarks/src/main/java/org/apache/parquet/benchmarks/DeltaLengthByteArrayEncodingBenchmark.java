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
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
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
 * Encoding micro-benchmarks for the DELTA_LENGTH_BYTE_ARRAY encoding, which
 * stores variable-length binary values by delta-encoding their lengths (via
 * DELTA_BINARY_PACKED) followed by the concatenated raw bytes.
 *
 * <p>Decoding benchmarks live in {@link DeltaLengthByteArrayDecodingBenchmark}.
 *
 * <p>The {@code stringLength} parameter exercises the encoding across different
 * value sizes. The {@code dataPattern} parameter controls whether all values
 * have identical length (UNIFORM_LENGTH — length deltas are all zero) or
 * varying lengths (VARIABLE_LENGTH — non-trivial deltas that exercise the
 * DELTA_BINARY_PACKED sub-encoding of lengths).
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
public class DeltaLengthByteArrayEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"10", "100", "1000"})
  public int stringLength;

  @Param({"UNIFORM_LENGTH", "VARIABLE_LENGTH"})
  public String dataPattern;

  private Binary[] data;

  @Setup(Level.Trial)
  public void setup() {
    switch (dataPattern) {
      case "UNIFORM_LENGTH":
        data = TestDataFactory.generateBinaryData(VALUE_COUNT, stringLength, 0, TestDataFactory.DEFAULT_SEED);
        break;
      case "VARIABLE_LENGTH":
        data = TestDataFactory.generateVariableLengthBinaryData(
            VALUE_COUNT, stringLength, TestDataFactory.DEFAULT_SEED);
        break;
      default:
        throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
    }
  }

  private static DeltaLengthByteArrayValuesWriter newWriter() {
    return new DeltaLengthByteArrayValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encode() throws IOException {
    ValuesWriter w = newWriter();
    for (Binary v : data) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
