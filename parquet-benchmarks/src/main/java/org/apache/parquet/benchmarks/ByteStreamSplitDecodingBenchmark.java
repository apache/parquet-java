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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReader;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFloat;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForLong;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
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
 * Decoding-level micro-benchmarks for the BYTE_STREAM_SPLIT encoding across the four
 * primitive widths supported by Parquet ({@code FLOAT}, {@code DOUBLE}, {@code INT32},
 * {@code INT64}).
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values; throughput is reported
 * per-value via {@link OperationsPerInvocation}. The cost includes both
 * {@code initFromPage} (which eagerly transposes the entire page) and the per-value
 * read calls. Page transposition is the part this benchmark is primarily designed
 * to exercise.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class ByteStreamSplitDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  private byte[] floatPage;
  private byte[] doublePage;
  private byte[] intPage;
  private byte[] longPage;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    int[] intData = new int[VALUE_COUNT];
    long[] longData = new long[VALUE_COUNT];
    float[] floatData = new float[VALUE_COUNT];
    double[] doubleData = new double[VALUE_COUNT];
    for (int i = 0; i < VALUE_COUNT; i++) {
      intData[i] = random.nextInt();
      longData[i] = random.nextLong();
      floatData[i] = random.nextFloat();
      doubleData[i] = random.nextDouble();
    }

    {
      ValuesWriter w = new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
          INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (float v : floatData) {
        w.writeFloat(v);
      }
      floatPage = w.getBytes().toByteArray();
      w.close();
    }
    {
      ValuesWriter w = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
          INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (double v : doubleData) {
        w.writeDouble(v);
      }
      doublePage = w.getBytes().toByteArray();
      w.close();
    }
    {
      ValuesWriter w = new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
          INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int v : intData) {
        w.writeInteger(v);
      }
      intPage = w.getBytes().toByteArray();
      w.close();
    }
    {
      ValuesWriter w = new ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter(
          INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (long v : longData) {
        w.writeLong(v);
      }
      longPage = w.getBytes().toByteArray();
      w.close();
    }
  }

  private static void init(ByteStreamSplitValuesReader r, byte[] page) throws IOException {
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(java.nio.ByteBuffer.wrap(page)));
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloat(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForFloat r = new ByteStreamSplitValuesReaderForFloat();
    init(r, floatPage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readFloat());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDouble(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForDouble r = new ByteStreamSplitValuesReaderForDouble();
    init(r, doublePage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeInt(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForInteger r = new ByteStreamSplitValuesReaderForInteger();
    init(r, intPage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeLong(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForLong r = new ByteStreamSplitValuesReaderForLong();
    init(r, longPage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readLong());
    }
  }
}
