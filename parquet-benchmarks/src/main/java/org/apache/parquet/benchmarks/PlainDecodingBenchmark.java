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
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesReader;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Decoding micro-benchmarks for the PLAIN encoding across all Parquet primitive types:
 * {@code BOOLEAN}, {@code INT32}, {@code INT64}, {@code FLOAT}, {@code DOUBLE},
 * {@code BINARY}, and {@code FIXED_LEN_BYTE_ARRAY}.
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values. Per-value methods measure
 * scalar read throughput using direct ByteBuffer access.
 *
 * <p>Fixed-width types (BOOLEAN through DOUBLE) are data-independent for PLAIN
 * decoding -- the cost per value is constant regardless of the value content or
 * distribution pattern -- so no {@code @Param} is needed.
 *
 * <p>Variable/fixed-length byte types use inner {@link State} classes with
 * {@code @Param} for the dimension that genuinely affects PLAIN throughput:
 * <ul>
 *   <li><b>BINARY:</b> parameterized by {@link BinaryState#stringLength} (10, 100,
 *       1000) because PLAIN reads a 4-byte length prefix then slices N content
 *       bytes.</li>
 *   <li><b>FIXED_LEN_BYTE_ARRAY:</b> parameterized by {@link FlbaState#fixedLength}
 *       (2, 12, 16) because PLAIN slices exactly N bytes per value, covering
 *       FLOAT16, INT96/legacy-timestamp, and UUID sizes.</li>
 * </ul>
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
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  // ---- Pre-encoded pages for fixed-width types ----

  private byte[] boolPage;
  private byte[] intPage;
  private byte[] longPage;
  private byte[] floatPage;
  private byte[] doublePage;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random r = new Random(42);

    // Encode BOOLEAN
    {
      BooleanPlainValuesWriter w = new BooleanPlainValuesWriter();
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeBoolean(r.nextBoolean());
      }
      boolPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode INT32
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeInteger(r.nextInt());
      }
      intPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode INT64
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeLong(r.nextLong());
      }
      longPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode FLOAT
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeFloat(r.nextFloat());
      }
      floatPage = w.getBytes().toByteArray();
      w.close();
    }

    // Encode DOUBLE
    {
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (int i = 0; i < VALUE_COUNT; i++) {
        w.writeDouble(r.nextDouble());
      }
      doublePage = w.getBytes().toByteArray();
      w.close();
    }
  }

  // ---- BINARY state: parameterized by string length ----

  /**
   * Separate state for BINARY decode benchmarks. Pre-encodes a page of binary values
   * so the {@code stringLength} parameter only creates trials for binary-related
   * benchmark methods.
   */
  @State(Scope.Thread)
  public static class BinaryState {
    /** Short (10), medium (100), and long (1000) strings. */
    @Param({"10", "100", "1000"})
    public int stringLength;

    byte[] page;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Binary[] data =
          TestDataFactory.generateBinaryData(VALUE_COUNT, stringLength, 0, TestDataFactory.DEFAULT_SEED);
      PlainValuesWriter w = new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (Binary v : data) {
        w.writeBytes(v);
      }
      page = w.getBytes().toByteArray();
      w.close();
    }
  }

  // ---- FLBA state: parameterized by fixed byte length ----

  /**
   * Separate state for FIXED_LEN_BYTE_ARRAY decode benchmarks. Pre-encodes a page of
   * FLBA values so the {@code fixedLength} parameter only creates trials for
   * FLBA-related benchmark methods. Values: 2 = FLOAT16, 12 = INT96, 16 = UUID.
   */
  @State(Scope.Thread)
  public static class FlbaState {
    /** FLOAT16 (2), INT96 (12), UUID (16). */
    @Param({"2", "12", "16"})
    public int fixedLength;

    byte[] page;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      Binary[] data = TestDataFactory.generateFixedLenByteArrays(
          VALUE_COUNT, fixedLength, 0, TestDataFactory.DEFAULT_SEED);
      FixedLenByteArrayPlainValuesWriter w = new FixedLenByteArrayPlainValuesWriter(
          fixedLength, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
      for (Binary v : data) {
        w.writeBytes(v);
      }
      page = w.getBytes().toByteArray();
      w.close();
    }
  }

  // ---- BOOLEAN ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBoolean(Blackhole bh) throws IOException {
    ValuesReader reader = new BooleanPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(boolPage)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBoolean());
    }
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

  // ---- BINARY (parameterized by string length) ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBinary(BinaryState state, Blackhole bh) throws IOException {
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(state.page)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  // ---- FIXED_LEN_BYTE_ARRAY (parameterized by fixed length) ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeFixedLenByteArray(FlbaState state, Blackhole bh) throws IOException {
    FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(state.fixedLength);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(state.page)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }
}
