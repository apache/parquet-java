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
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
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
 * Encoding and decoding micro-benchmarks for synthetic dictionary-id pages using
 * {@link RunLengthBitPackingHybridEncoder} and {@link RunLengthBitPackingHybridDecoder}.
 * This isolates the RLE/bit-packing hybrid codec paths and is intentionally
 * separate from full INT32/INT64 value encode/decode path benchmarks.
 *
 * <p>The encode benchmark measures the RLE encoder's {@code pack32Values} fast path
 * and bit-packing throughput. The decode benchmark measures the corresponding
 * {@code unpack32Values} fast path and RLE run expansion.
 *
 * <p>The {@code bitWidth} parameter exercises different packing densities (1-bit to
 * 16-bit), and the {@code indexPattern} parameter exercises pure RLE (CONSTANT),
 * mixed (LOW_CARDINALITY), and pure bit-packing (SEQUENTIAL, RANDOM) paths.
 *
 * <p>Per-invocation overhead (encoder/decoder construction and {@link ByteBufferInputStream}
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

  @Param({"1", "4", "8", "10", "16"})
  public int bitWidth;

  @Param({"SEQUENTIAL", "RANDOM", "LOW_CARDINALITY", "CONSTANT"})
  public String indexPattern;

  /** Raw RLE-encoded bytes (no length prefix). */
  private byte[] encoded;

  private int[] ids;

  /** RLE-encoded bytes with 4-byte LE length prefix (ValuesReader format). */
  private byte[] encodedWithLengthPrefix;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    int maxId = 1 << bitWidth;
    ids = generateDictionaryIds(maxId);
    try (RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
        bitWidth, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator())) {
      for (int id : ids) {
        encoder.writeInt(id);
      }
      encoded = encoder.toBytes().toByteArray();
    }

    // Prepend 4-byte LE length for ValuesReader.initFromPage() format
    encodedWithLengthPrefix = new byte[4 + encoded.length];
    ByteBuffer.wrap(encodedWithLengthPrefix).order(ByteOrder.LITTLE_ENDIAN).putInt(encoded.length);
    System.arraycopy(encoded, 0, encodedWithLengthPrefix, 4, encoded.length);
  }

  private int[] generateDictionaryIds(int maxId) {
    switch (indexPattern) {
      case "SEQUENTIAL":
        int[] sequential = new int[VALUE_COUNT];
        for (int i = 0; i < VALUE_COUNT; i++) {
          sequential[i] = i % maxId;
        }
        return sequential;
      case "RANDOM":
        return TestDataFactory.generateLowCardinalityInts(VALUE_COUNT, maxId, TestDataFactory.DEFAULT_SEED);
      case "LOW_CARDINALITY":
        int distinct = Math.min(TestDataFactory.LOW_CARDINALITY_DISTINCT, maxId);
        return TestDataFactory.generateLowCardinalityInts(VALUE_COUNT, distinct, TestDataFactory.DEFAULT_SEED);
      case "CONSTANT":
        int[] constant = new int[VALUE_COUNT];
        java.util.Arrays.fill(constant, 0);
        return constant;
      default:
        throw new IllegalArgumentException("Unknown index pattern: " + indexPattern);
    }
  }

  // ---- Scalar encode via encoder ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDictionaryIds() throws IOException {
    try (RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
        bitWidth, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator())) {
      for (int id : ids) {
        encoder.writeInt(id);
      }
      return encoder.toBytes().toByteArray();
    }
  }

  // ---- Scalar decode via decoder ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDictionaryIds(Blackhole bh) {
    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, ByteBuffer.wrap(encoded));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(decoder.readInt());
    }
  }

  // ---- Scalar decode via ValuesReader wrapper ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeValuesReader(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(bitWidth);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(encodedWithLengthPrefix)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }
}
