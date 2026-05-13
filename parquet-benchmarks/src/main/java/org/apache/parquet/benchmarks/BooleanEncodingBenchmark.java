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
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
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
 * Characterization benchmarks for BOOLEAN encoding in Parquet.
 *
 * <p>BOOLEAN columns use two distinct encoding paths:
 * <ul>
 *   <li><b>V1 (PLAIN):</b> {@link BooleanPlainValuesWriter} delegates to
 *       {@code ByteBitPackingValuesWriter(bitWidth=1)}. Always bit-packs.</li>
 *   <li><b>V2 (RLE):</b> {@link RunLengthBitPackingHybridValuesWriter} with
 *       {@code bitWidth=1}. Uses the RLE/bit-packing hybrid, which can
 *       run-length encode long runs of identical values.</li>
 * </ul>
 *
 * <p>The {@code dataPattern} parameter exercises RLE's best cases (ALL_TRUE,
 * ALL_FALSE), worst case (ALTERNATING), and realistic distributions (RANDOM,
 * MOSTLY_TRUE_99, MOSTLY_FALSE_99).
 *
 * <p>Each invocation processes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BooleanEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"ALL_TRUE", "ALL_FALSE", "ALTERNATING", "RANDOM", "MOSTLY_TRUE_99", "MOSTLY_FALSE_99"})
  public String dataPattern;

  private boolean[] data;
  private byte[] v1Page;
  private byte[] v2Page;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    data = generateData(dataPattern);
    v1Page = encodeV1(data);
    v2Page = encodeV2(data);
  }

  private static boolean[] generateData(String pattern) {
    boolean[] d = new boolean[VALUE_COUNT];
    Random rng = new Random(42);
    switch (pattern) {
      case "ALL_TRUE":
        for (int i = 0; i < VALUE_COUNT; i++) d[i] = true;
        break;
      case "ALL_FALSE":
        // already false
        break;
      case "ALTERNATING":
        for (int i = 0; i < VALUE_COUNT; i++) d[i] = (i & 1) == 0;
        break;
      case "RANDOM":
        for (int i = 0; i < VALUE_COUNT; i++) d[i] = rng.nextBoolean();
        break;
      case "MOSTLY_TRUE_99":
        for (int i = 0; i < VALUE_COUNT; i++) d[i] = rng.nextInt(100) != 0;
        break;
      case "MOSTLY_FALSE_99":
        for (int i = 0; i < VALUE_COUNT; i++) d[i] = rng.nextInt(100) == 0;
        break;
      default:
        throw new IllegalArgumentException("Unknown pattern: " + pattern);
    }
    return d;
  }

  private static byte[] encodeV1(boolean[] values) throws IOException {
    ValuesWriter w = new BooleanPlainValuesWriter();
    for (boolean v : values) {
      w.writeBoolean(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  private static byte[] encodeV2(boolean[] values) throws IOException {
    ValuesWriter w = new RunLengthBitPackingHybridValuesWriter(
        1, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (boolean v : values) {
      w.writeBoolean(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- Encode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodePlainV1() throws IOException {
    return encodeV1(data);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeRleV2() throws IOException {
    return encodeV2(data);
  }

  // ---- Decode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodePlainV1(Blackhole bh) throws IOException {
    ValuesReader r = new BooleanPlainValuesReader();
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(v1Page)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readBoolean());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeRleV2(Blackhole bh) throws IOException {
    ValuesReader r = new RunLengthBitPackingHybridValuesReader(1);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(v2Page)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readBoolean());
    }
  }
}
