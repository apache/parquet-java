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
 * Micro-benchmarks for {@link Binary#equals}, {@link Binary#compareTo}, and
 * {@link Binary#hashCode()}. These sit on statistics min/max maintenance,
 * dictionary hash-map probing, predicate evaluation, and bloom-filter build,
 * so improvements are broadly amortized across reader and writer paths.
 *
 * <p>Each invocation performs {@value #PAIRS} comparisons. The
 * {@code length} parameter covers three regimes:
 * <ul>
 *   <li>{@code 8} — short strings (dictionary keys, enum codes). Intrinsics
 *       give only a modest win here.</li>
 *   <li>{@code 64} — medium strings (typical categorical column). Vector
 *       mismatch amortizes well.</li>
 *   <li>{@code 512} — long strings (URLs, JSON keys). The intrinsic path
 *       is significantly faster than the scalar loop.</li>
 * </ul>
 *
 * <p>{@code equalsMatch} compares equal binaries (the worst case: the
 * mismatch scan walks the full length). {@code equalsMismatch} places the
 * differing byte at length/2 (average case). {@code compareTo} uses the same
 * mid-mismatch data.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BinaryComparisonBenchmark {

  /** Number of paired comparisons per @Benchmark invocation. */
  static final int PAIRS = 10_000;

  @Param({"8", "64", "512"})
  public int length;

  // Heap-array backed
  private Binary[] leftArr;
  private Binary[] rightEqualArr;
  private Binary[] rightMidMismatchArr;

  // ByteBuffer-backed (heap)
  private Binary[] leftBuf;
  private Binary[] rightEqualBuf;
  private Binary[] rightMidMismatchBuf;

  @Setup(Level.Trial)
  public void setup() {
    Random r = new Random(TestDataFactory.DEFAULT_SEED);
    leftArr = new Binary[PAIRS];
    rightEqualArr = new Binary[PAIRS];
    rightMidMismatchArr = new Binary[PAIRS];
    leftBuf = new Binary[PAIRS];
    rightEqualBuf = new Binary[PAIRS];
    rightMidMismatchBuf = new Binary[PAIRS];

    for (int i = 0; i < PAIRS; i++) {
      byte[] a = new byte[length];
      r.nextBytes(a);
      byte[] aCopy = a.clone();
      byte[] aMidMismatch = a.clone();
      // Flip a bit at position length/2 to force a mid-length mismatch.
      aMidMismatch[length / 2] ^= (byte) 0xFF;

      leftArr[i] = Binary.fromConstantByteArray(a);
      rightEqualArr[i] = Binary.fromConstantByteArray(aCopy);
      rightMidMismatchArr[i] = Binary.fromConstantByteArray(aMidMismatch);

      leftBuf[i] = Binary.fromConstantByteBuffer(ByteBuffer.wrap(a.clone()));
      rightEqualBuf[i] = Binary.fromConstantByteBuffer(ByteBuffer.wrap(aCopy.clone()));
      rightMidMismatchBuf[i] = Binary.fromConstantByteBuffer(ByteBuffer.wrap(aMidMismatch.clone()));
    }
  }

  // ---- byte[] vs byte[] ----

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void equalsMatch_bytesBytes(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].equals(rightEqualArr[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void equalsMismatch_bytesBytes(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].equals(rightMidMismatchArr[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void compareTo_bytesBytes(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].compareTo(rightMidMismatchArr[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void hashCode_bytes(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].hashCode());
    }
  }

  // ---- ByteBuffer vs ByteBuffer (heap-backed) ----

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void equalsMatch_bufBuf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftBuf[i].equals(rightEqualBuf[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void equalsMismatch_bufBuf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftBuf[i].equals(rightMidMismatchBuf[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void compareTo_bufBuf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftBuf[i].compareTo(rightMidMismatchBuf[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void hashCode_buf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftBuf[i].hashCode());
    }
  }

  // ---- Mixed: byte[] vs ByteBuffer (predicate pushdown pattern) ----

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void equalsMismatch_bytesBuf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].equals(rightMidMismatchBuf[i]));
    }
  }

  @Benchmark
  @OperationsPerInvocation(PAIRS)
  public void compareTo_bytesBuf(Blackhole bh) {
    for (int i = 0; i < PAIRS; i++) {
      bh.consume(leftArr[i].compareTo(rightMidMismatchBuf[i]));
    }
  }
}
