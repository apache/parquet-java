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
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.alp.AlpValuesWriter;
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

/**
 * Encoding-level micro-benchmarks for ALP, with PLAIN on the same data as a baseline.
 *
 * <p>{@code decimal} data is what ALP is designed for; {@code random} data is almost all
 * exceptions, its worst case.
 *
 * <p>Each invocation encodes {@value #VALUE_COUNT} values; throughput is reported per-value via
 * {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class AlpEncodingBenchmark {

  // Enough values that each run includes both the full parameter search, used until enough
  // vectors are sampled, and the preset search that follows, as a column chunk would.
  static final int VALUE_COUNT = 1_000_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 16 * 1024 * 1024;

  @Param({"decimal", "random"})
  public String data;

  private double[] doubleData;
  private float[] floatData;

  @Setup(Level.Trial)
  public void setup() {
    doubleData = doubles(data, VALUE_COUNT);
    floatData = floats(data, VALUE_COUNT);
  }

  static double[] doubles(String data, int count) {
    Random random = new Random(42);
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      values[i] = "decimal".equals(data) ? Math.round(random.nextDouble() * 1e6) / 100.0 : random.nextDouble();
    }
    return values;
  }

  static float[] floats(String data, int count) {
    Random random = new Random(42);
    float[] values = new float[count];
    for (int i = 0; i < count; i++) {
      values[i] = "decimal".equals(data) ? Math.round(random.nextFloat() * 1e6f) / 100.0f : random.nextFloat();
    }
    return values;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDouble() throws IOException {
    return encode(
        new AlpValuesWriter.DoubleAlpValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()));
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDoublePlain() throws IOException {
    return encode(new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()));
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFloat() throws IOException {
    return encodeFloats(
        new AlpValuesWriter.FloatAlpValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()));
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFloatPlain() throws IOException {
    return encodeFloats(new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator()));
  }

  private byte[] encode(ValuesWriter w) throws IOException {
    for (double v : doubleData) {
      w.writeDouble(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  private byte[] encodeFloats(ValuesWriter w) throws IOException {
    for (float v : floatData) {
      w.writeFloat(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
