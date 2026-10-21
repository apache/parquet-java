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
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.alp.AlpValuesReaderForDouble;
import org.apache.parquet.column.values.alp.AlpValuesReaderForFloat;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding-level micro-benchmarks for ALP, with PLAIN on the same data as a baseline. The data sets
 * are those of {@link AlpEncodingBenchmark}.
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values; throughput is reported per-value via
 * {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class AlpDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"decimal", "random"})
  public String data;

  private byte[] alpDoublePage;
  private byte[] plainDoublePage;
  private byte[] alpFloatPage;
  private byte[] plainFloatPage;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    double[] doubles = AlpEncodingBenchmark.doubles(data, VALUE_COUNT);
    float[] floats = AlpEncodingBenchmark.floats(data, VALUE_COUNT);
    alpDoublePage = page(
        new AlpValuesWriter.DoubleAlpValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()),
        doubles);
    plainDoublePage =
        page(new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()), doubles);
    alpFloatPage = page(
        new AlpValuesWriter.FloatAlpValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()),
        floats);
    plainFloatPage = page(new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()), floats);
  }

  private static byte[] page(ValuesWriter w, double[] values) throws IOException {
    for (double v : values) {
      w.writeDouble(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  private static byte[] page(ValuesWriter w, float[] values) throws IOException {
    for (float v : values) {
      w.writeFloat(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  private static void init(ValuesReader r, byte[] page) throws IOException {
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)));
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDouble(Blackhole bh) throws IOException {
    AlpValuesReaderForDouble r = new AlpValuesReaderForDouble();
    init(r, alpDoublePage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDoublePlain(Blackhole bh) throws IOException {
    PlainValuesReader.DoublePlainValuesReader r = new PlainValuesReader.DoublePlainValuesReader();
    init(r, plainDoublePage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readDouble());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloat(Blackhole bh) throws IOException {
    AlpValuesReaderForFloat r = new AlpValuesReaderForFloat();
    init(r, alpFloatPage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readFloat());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFloatPlain(Blackhole bh) throws IOException {
    PlainValuesReader.FloatPlainValuesReader r = new PlainValuesReader.FloatPlainValuesReader();
    init(r, plainFloatPage);
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readFloat());
    }
  }
}
