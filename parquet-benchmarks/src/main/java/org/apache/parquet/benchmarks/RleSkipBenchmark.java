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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
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

/**
 * Measures {@link RunLengthBitPackingHybridDecoder#skipInts(int)} vs. the equivalent
 * discard-via-readInt loop. Both paths decode each run the same way; the win comes from
 * dropping the per-value mode-switch, array-index arithmetic and method-call overhead of
 * {@code readInt()} in favour of a single {@code currentCount -= consume} per run.
 *
 * <p>Parameters:
 * <ul>
 *   <li>{@link #bitWidth} -- key width, chosen at 3/8/16 to bracket typical dictionary-index
 *       widths.</li>
 *   <li>{@link #pattern} -- {@code rle} produces long runs of a single value (best case for both
 *       paths); {@code packed} produces mostly distinct values so the encoder emits bit-packed
 *       groups (worst case for the old skip); {@code mixed} interleaves the two.</li>
 * </ul>
 *
 * Each invocation re-wraps a pre-encoded byte buffer and calls {@code skipInts(VALUE_COUNT)} in
 * the {@code skip} benchmark, versus a {@code VALUE_COUNT}-long {@code readInt()} discard loop
 * in the {@code readSkip} benchmark.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class RleSkipBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB = 64 * 1024;
  private static final int PAGE = 4 * 1024 * 1024;

  @Param({"3", "8", "16"})
  public int bitWidth;

  @Param({"rle", "packed", "mixed"})
  public String pattern;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    int mask = bitWidth == 32 ? -1 : ((1 << bitWidth) - 1);
    RunLengthBitPackingHybridEncoder enc =
        new RunLengthBitPackingHybridEncoder(bitWidth, INIT_SLAB, PAGE, new DirectByteBufferAllocator());
    Random r = new Random(42);
    switch (pattern) {
      case "rle":
        // Long stretches of the same value -> the encoder emits RLE runs.
        for (int i = 0; i < VALUE_COUNT; i++) {
          enc.writeInt(((i / 500) & 0x1F) & mask);
        }
        break;
      case "packed":
        // Random values -> the encoder emits bit-packed groups.
        for (int i = 0; i < VALUE_COUNT; i++) {
          enc.writeInt(r.nextInt() & mask);
        }
        break;
      case "mixed":
        // Alternating 250-value RLE blocks and 250-value random blocks.
        for (int block = 0; block * 250 < VALUE_COUNT; block++) {
          int val = r.nextInt() & mask;
          boolean rle = (block & 1) == 0;
          int limit = Math.min(250, VALUE_COUNT - block * 250);
          for (int j = 0; j < limit; j++) {
            enc.writeInt(rle ? val : (r.nextInt() & mask));
          }
        }
        break;
      default:
        throw new IllegalArgumentException("unknown pattern: " + pattern);
    }
    encoded = enc.toBytes().toByteArray();
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void skip() throws Exception {
    ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded));
    RunLengthBitPackingHybridDecoder dec = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    dec.skipInts(VALUE_COUNT);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int readSkip() throws Exception {
    ByteBufferInputStream in = ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded));
    RunLengthBitPackingHybridDecoder dec = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    int sink = 0;
    for (int i = 0; i < VALUE_COUNT; i++) {
      sink ^= dec.readInt();
    }
    return sink;
  }
}
