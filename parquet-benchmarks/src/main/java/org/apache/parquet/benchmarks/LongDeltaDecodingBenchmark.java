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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding micro-benchmark for INT64 values encoded with DELTA_BINARY_PACKED.
 *
 * <p>Exercises the {@link DeltaBinaryPackingValuesReader} long path, which invokes the
 * long bit-packing unpacker ({@code BytePackerForLong}) once per miniblock of 32 values.
 * Covers the per-value decode entry point ({@code readLong}).
 *
 * <p>Data patterns cover the bit-width ranges that drive which packer is selected:
 * <ul>
 *   <li>{@code SEQUENTIAL_DENSE}: deltas == 1 → 1 bit width
 *   <li>{@code SEQUENTIAL_STRIDED}: deltas of ~1000 → ~10-bit width
 *   <li>{@code RANDOM_SMALL}: deltas in [-256, 256] → ~9-bit width
 *   <li>{@code RANDOM_WIDE}: deltas spanning 48+ bits → high-bit widths (the worst case
 *       for the generated unpacker bodies)
 *   <li>{@code TIMESTAMP_MILLIS}: monotonically increasing with jitter, ~17-bit width
 * </ul>
 *
 * <p>This complements {@link DeltaBinaryPackedDecodingBenchmark#decodeInt} which only covers INT32.
 * The long path is separately relevant because the generated {@code BytePackerForLong}
 * code is twice the size of the INT32 packer and has been observed to stress the JIT
 * inlining heuristics differently.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class LongDeltaDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;

  @Param({"SEQUENTIAL_DENSE", "SEQUENTIAL_STRIDED", "RANDOM_SMALL", "RANDOM_WIDE", "TIMESTAMP_MILLIS"})
  public String dataPattern;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    long[] data = generateData();
    try (DeltaBinaryPackingValuesWriterForLong writer =
        new DeltaBinaryPackingValuesWriterForLong(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator())) {
      for (long v : data) {
        writer.writeLong(v);
      }
      encoded = writer.getBytes().toByteArray();
    }
  }

  private long[] generateData() {
    long[] data = new long[VALUE_COUNT];
    Random r = new Random(42);
    switch (dataPattern) {
      case "SEQUENTIAL_DENSE":
        for (int i = 0; i < VALUE_COUNT; i++) {
          data[i] = 1_000_000L + i;
        }
        return data;
      case "SEQUENTIAL_STRIDED":
        long v = 1_000_000L;
        for (int i = 0; i < VALUE_COUNT; i++) {
          v += 500 + r.nextInt(1000);
          data[i] = v;
        }
        return data;
      case "RANDOM_SMALL":
        long base = 0;
        for (int i = 0; i < VALUE_COUNT; i++) {
          base += r.nextInt(513) - 256;
          data[i] = base;
        }
        return data;
      case "RANDOM_WIDE":
        for (int i = 0; i < VALUE_COUNT; i++) {
          // Deltas spanning ~48 bits forces the delta-packer to pick a high bit-width.
          data[i] = r.nextLong() >> 16;
        }
        return data;
      case "TIMESTAMP_MILLIS":
        long t = 1_700_000_000_000L; // Nov 2023 epoch-millis baseline
        for (int i = 0; i < VALUE_COUNT; i++) {
          t += 50 + r.nextInt(100_000); // 50ms-100s jitter
          data[i] = t;
        }
        return data;
      default:
        throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDeltaLong(Blackhole bh) throws IOException {
    DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readLong());
    }
  }
}
