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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding micro-benchmarks for the DELTA_LENGTH_BYTE_ARRAY encoding.
 * Encoding benchmarks live in {@link DeltaLengthByteArrayEncodingBenchmark}.
 *
 * <p>The {@code stringLength} parameter exercises the decoding across different
 * value sizes. The {@code dataPattern} parameter controls whether all values
 * have identical length (UNIFORM_LENGTH) or varying lengths (VARIABLE_LENGTH).
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class DeltaLengthByteArrayDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"10", "100", "1000"})
  public int stringLength;

  @Param({"UNIFORM_LENGTH", "VARIABLE_LENGTH"})
  public String dataPattern;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Binary[] data;
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

    ValuesWriter w = new DeltaLengthByteArrayValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (Binary v : data) {
      w.writeBytes(v);
    }
    encoded = w.getBytes().toByteArray();
    w.close();
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decode(Blackhole bh) throws IOException {
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }
}
