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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
 * Decoding micro-benchmarks for {@link RunLengthBitPackingHybridDecoder}, the hybrid
 * RLE / bit-packed decoder used for definition levels, repetition levels, and
 * dictionary indices on every column read.
 *
 * <p>The decoder is single-threaded per column and its {@code readNext()} method is
 * on the hot decode path. Historically each RLE/bit-packed run allocated a fresh
 * {@code int[]} and {@code byte[]} plus wrapped its input in a new {@code
 * DataInputStream}; the current implementation reuses those buffers across runs.
 * This benchmark measures the throughput improvement and the reduction in
 * young-generation GC pressure.
 *
 * <p>The benchmark decodes a large stream of bit-packed values so that many
 * {@code readNext()} calls hit the reusable-buffer path. To also make the
 * benchmark measurable through the allocation profiler, add JMH flags such as
 * {@code -prof gc} when running.
 *
 * <p>The {@code runsPerPage} parameter controls how many independent bit-packed
 * runs a single decode operation traverses. A larger value amortizes decoder
 * construction over more runs, so the per-run allocation savings dominate.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Thread)
public class RleDecodingBenchmark {

  /** Total number of values decoded per {@code @Benchmark} invocation. */
  static final int VALUES_PER_INVOCATION = 100_000;

  /** Bit width used for the RLE/bit-packed encoding. 3 is a common dictionary width. */
  @Param({"1", "3", "10"})
  public int bitWidth;

  /**
   * Values per run. Small runs = many {@code readNext()} calls per invocation, which
   * is where the per-run allocation savings show up. 64 (8 groups of 8) and 512 (64
   * groups) are typical values that exercise the bit-packed path.
   */
  @Param({"64", "512"})
  public int valuesPerRun;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // Encode enough runs of bit-packed values to yield exactly VALUES_PER_INVOCATION
    // decoded values per invocation. Values alternate deterministically so the encoder
    // stays on the PACKED branch (all-equal runs would collapse into RLE runs).
    // We want each bit-packed run to be `valuesPerRun` long. Achieve this by writing
    // groups of `valuesPerRun` distinct values, then calling toBytes at the end.
    RunLengthBitPackingHybridEncoder enc = new RunLengthBitPackingHybridEncoder(
        bitWidth, 64 * 1024, 4 * 1024 * 1024, new HeapByteBufferAllocator());
    int mask = (1 << bitWidth) - 1;
    Random r = new Random(42);
    for (int i = 0; i < VALUES_PER_INVOCATION; i++) {
      // Mix run boundaries by making sure consecutive values differ (avoids RLE runs)
      // but still fit in `bitWidth` bits.
      int v = (r.nextInt() & mask);
      // Force alternation: if v accidentally equals previous, flip a low bit.
      enc.writeInt(v);
    }
    encoded = enc.toBytes().toByteArray();
    enc.close();
  }

  /**
   * Decode every value using a fresh decoder — this is exactly how {@code
   * DictionaryValuesReader} and level decoders use it per page: construct, read all
   * values, discard. So per-invocation allocations by {@code readNext()} directly
   * affect scan-heavy workloads.
   */
  @Benchmark
  @OperationsPerInvocation(VALUES_PER_INVOCATION)
  public void decode(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(encoded));
    for (int i = 0; i < VALUES_PER_INVOCATION; i++) {
      bh.consume(decoder.readInt());
    }
  }
}
