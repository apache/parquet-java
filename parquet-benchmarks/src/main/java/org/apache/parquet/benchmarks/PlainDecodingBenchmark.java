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
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding micro-benchmarks for the PLAIN encoding across the four numeric primitive
 * types: {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE}.
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values. Per-value methods measure
 * scalar read throughput; batch methods measure bulk array-fill throughput using
 * {@link PlainValuesReader}'s bulk {@code ByteBuffer} view reads.
 *
 * <p>INT32 per-value and batch decode are also available in {@link IntEncodingBenchmark}
 * alongside other INT32 encodings. This benchmark focuses on the PLAIN encoding path
 * for all four types to validate the bulk view buffer optimization uniformly.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class PlainDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;

  private byte[] intPage;
  private byte[] longPage;
  private byte[] floatPage;
  private byte[] doublePage;

  // Pre-allocated destination arrays to avoid per-invocation allocation noise
  private int[] intDest;
  private long[] longDest;
  private float[] floatDest;
  private double[] doubleDest;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random r = new Random(42);

    // Pre-allocate destination arrays
    intDest = new int[VALUE_COUNT];
    longDest = new long[VALUE_COUNT];
    floatDest = new float[VALUE_COUNT];
    doubleDest = new double[VALUE_COUNT];

    // Encode INT32
    PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (int i = 0; i < VALUE_COUNT; i++) {
      w.writeInteger(r.nextInt());
    }
    intPage = w.getBytes().toByteArray();
    w.close();

    // Encode INT64
    w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (int i = 0; i < VALUE_COUNT; i++) {
      w.writeLong(r.nextLong());
    }
    longPage = w.getBytes().toByteArray();
    w.close();

    // Encode FLOAT
    w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (int i = 0; i < VALUE_COUNT; i++) {
      w.writeFloat(r.nextFloat());
    }
    floatPage = w.getBytes().toByteArray();
    w.close();

    // Encode DOUBLE
    w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (int i = 0; i < VALUE_COUNT; i++) {
      w.writeDouble(r.nextDouble());
    }
    doublePage = w.getBytes().toByteArray();
    w.close();
  }

  // ---- INT32 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeInt(Blackhole bh) throws IOException {
    PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(intPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public int[] decodeIntBatch() throws IOException {
    PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(intPage)));
    reader.readIntegers(intDest, 0, VALUE_COUNT);
    return intDest;
  }

  // ---- INT64 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeLong(Blackhole bh) throws IOException {
    PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readLong());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public long[] decodeLongBatch() throws IOException {
    PlainValuesReader.LongPlainValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(longPage)));
    reader.readLongs(longDest, 0, VALUE_COUNT);
    return longDest;
  }

  // ---- FLOAT ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloat(Blackhole bh) throws IOException {
    PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readFloat());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public float[] decodeFloatBatch() throws IOException {
    PlainValuesReader.FloatPlainValuesReader reader = new PlainValuesReader.FloatPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(floatPage)));
    reader.readFloats(floatDest, 0, VALUE_COUNT);
    return floatDest;
  }

  // ---- DOUBLE ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDouble(Blackhole bh) throws IOException {
    PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doublePage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public double[] decodeDoubleBatch() throws IOException {
    PlainValuesReader.DoublePlainValuesReader reader = new PlainValuesReader.DoublePlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(doublePage)));
    reader.readDoubles(doubleDest, 0, VALUE_COUNT);
    return doubleDest;
  }
}
