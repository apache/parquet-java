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
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
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
 * Decoding micro-benchmark for synthetic dictionary-id pages encoded with
 * {@link RunLengthBitPackingHybridEncoder}. This isolates the dictionary-id
 * decode path and is intentionally separate from {@link IntEncodingBenchmark},
 * which measures full INT32 value decode paths.
 *
 * <p>Per-invocation overhead (decoder construction and {@link ByteBufferInputStream}
 * wrapping) is amortized over {@value #VALUE_COUNT} reads via
 * {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class RleDictionaryIndexDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;
  private static final int BIT_WIDTH = 10;
  private static final int MAX_ID = 1 << BIT_WIDTH;

  static {
    if (TestDataFactory.LOW_CARDINALITY_DISTINCT > MAX_ID) {
      throw new IllegalStateException("LOW_CARDINALITY_DISTINCT (" + TestDataFactory.LOW_CARDINALITY_DISTINCT
          + ") must fit within BIT_WIDTH=" + BIT_WIDTH + " (MAX_ID=" + MAX_ID + ")");
    }
  }

  @Param({"SEQUENTIAL", "RANDOM", "LOW_CARDINALITY"})
  public String indexPattern;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    int[] ids = generateDictionaryIds();
    try (RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
        BIT_WIDTH, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator())) {
      for (int id : ids) {
        encoder.writeInt(id);
      }
      encoded = encoder.toBytes().toByteArray();
    }
  }

  private int[] generateDictionaryIds() {
    switch (indexPattern) {
      case "SEQUENTIAL":
        int[] sequential = new int[VALUE_COUNT];
        for (int i = 0; i < VALUE_COUNT; i++) {
          sequential[i] = i % MAX_ID;
        }
        return sequential;
      case "RANDOM":
        return TestDataFactory.generateLowCardinalityInts(VALUE_COUNT, MAX_ID, TestDataFactory.DEFAULT_SEED);
      case "LOW_CARDINALITY":
        return TestDataFactory.generateLowCardinalityInts(
            VALUE_COUNT, TestDataFactory.LOW_CARDINALITY_DISTINCT, TestDataFactory.DEFAULT_SEED);
      default:
        throw new IllegalArgumentException("Unknown index pattern: " + indexPattern);
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDictionaryIds(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(BIT_WIDTH, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(decoder.readInt());
    }
  }
}
