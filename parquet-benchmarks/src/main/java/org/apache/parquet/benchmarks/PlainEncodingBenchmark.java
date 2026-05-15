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
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
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
 * Encoding micro-benchmarks for the PLAIN encoding across all Parquet primitive types:
 * {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
 * {@code BINARY}, and {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>Measures per-value scalar write throughput using direct ByteBuffer I/O
 * through {@code CapacityByteArrayOutputStream}.
 *
 * <p>Fixed-width types (BOOLEAN through DOUBLE) are data-independent for PLAIN
 * encoding -- the cost per value is constant regardless of the value content or
 * distribution pattern -- so no {@code @Param} is needed.
 *
 * <p>Variable/fixed-length byte types use inner {@link State} classes with
 * {@code @Param} for the dimension that genuinely affects PLAIN throughput:
 * <ul>
 *   <li><b>BINARY:</b> parameterized by {@link BinaryState#stringLength} (10, 100,
 *       1000) because PLAIN writes a 4-byte length prefix + N content bytes.</li>
 *   <li><b>FIXED_LEN_BYTE_ARRAY:</b> parameterized by {@link FlbaState#fixedLength}
 *       (2, 12, 16) because PLAIN writes exactly N bytes per value, covering
 *       FLOAT16, INT96/legacy-timestamp, and UUID sizes.</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class PlainEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  // ---- Fixed-width data (no @Param needed -- PLAIN cost is constant per value) ----

  private boolean[] boolData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;

  @Setup(Level.Trial)
  public void setup() {
    Random r = new Random(42);
    boolData = new boolean[VALUE_COUNT];
    intData = new int[VALUE_COUNT];
    longData = new long[VALUE_COUNT];
    floatData = new float[VALUE_COUNT];
    doubleData = new double[VALUE_COUNT];
    for (int i = 0; i < VALUE_COUNT; i++) {
      boolData[i] = r.nextBoolean();
      intData[i] = r.nextInt();
      longData[i] = r.nextLong();
      floatData[i] = r.nextFloat();
      doubleData[i] = r.nextDouble();
    }
  }

  // ---- BINARY state: parameterized by string length ----

  /**
   * Separate state for BINARY benchmarks so the {@code stringLength} parameter only
   * creates trials for binary-related benchmark methods, not for fixed-width types.
   */
  @State(Scope.Thread)
  public static class BinaryState {
    /** Short (10), medium (100), and long (1000) strings. */
    @Param({"10", "100", "1000"})
    public int stringLength;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      data = TestDataFactory.generateBinaryData(VALUE_COUNT, stringLength, 0, TestDataFactory.DEFAULT_SEED);
    }
  }

  // ---- FLBA state: parameterized by fixed byte length ----

  /**
   * Separate state for FIXED_LEN_BYTE_ARRAY benchmarks so the {@code fixedLength}
   * parameter only creates trials for FLBA-related benchmark methods.
   * Values: 2 = FLOAT16, 12 = INT96, 16 = UUID.
   */
  @State(Scope.Thread)
  public static class FlbaState {
    /** FLOAT16 (2), INT96 (12), UUID (16). */
    @Param({"2", "12", "16"})
    public int fixedLength;

    Binary[] data;

    @Setup(Level.Trial)
    public void setup() {
      data = TestDataFactory.generateFixedLenByteArrays(
          VALUE_COUNT, fixedLength, 0, TestDataFactory.DEFAULT_SEED);
    }
  }

  // ---- Writer factories ----

  private static PlainValuesWriter newWriter() {
    return new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static BooleanPlainValuesWriter newBoolWriter() {
    return new BooleanPlainValuesWriter();
  }

  private static FixedLenByteArrayPlainValuesWriter newFlbaWriter(int fixedLength) {
    return new FixedLenByteArrayPlainValuesWriter(
        fixedLength, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ---- BOOLEAN ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBoolean() throws IOException {
    ValuesWriter w = newBoolWriter();
    for (boolean v : boolData) {
      w.writeBoolean(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- INT32 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeInt() throws IOException {
    PlainValuesWriter w = newWriter();
    for (int v : intData) {
      w.writeInteger(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- INT64 ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeLong() throws IOException {
    PlainValuesWriter w = newWriter();
    for (long v : longData) {
      w.writeLong(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- FLOAT ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFloat() throws IOException {
    PlainValuesWriter w = newWriter();
    for (float v : floatData) {
      w.writeFloat(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- DOUBLE ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDouble() throws IOException {
    PlainValuesWriter w = newWriter();
    for (double v : doubleData) {
      w.writeDouble(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- BINARY (parameterized by string length) ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBinary(BinaryState state) throws IOException {
    PlainValuesWriter w = newWriter();
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }

  // ---- FIXED_LEN_BYTE_ARRAY (parameterized by fixed length) ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeFixedLenByteArray(FlbaState state) throws IOException {
    FixedLenByteArrayPlainValuesWriter w = newFlbaWriter(state.fixedLength);
    for (Binary v : state.data) {
      w.writeBytes(v);
    }
    byte[] bytes = w.getBytes().toByteArray();
    w.close();
    return bytes;
  }
}
