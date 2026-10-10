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
 * Decoding-level micro-benchmarks for the RLE/bit-packing hybrid encoding used
 * for {@code BOOLEAN} values in Parquet data pages V2.
 * Encoding benchmarks live in {@link RleEncodingBenchmark}.
 *
 * <p>The {@code dataPattern} parameter exercises RLE's best cases (ALL_TRUE,
 * ALL_FALSE), worst case (ALTERNATING), and realistic distributions (RANDOM,
 * MOSTLY_TRUE_99, MOSTLY_FALSE_99).
 *
 * <p>Each invocation decodes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class RleDecodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  @Param({"ALL_TRUE", "ALL_FALSE", "ALTERNATING", "RANDOM", "MOSTLY_TRUE_99", "MOSTLY_FALSE_99"})
  public String dataPattern;

  /** RLE-encoded bytes with 4-byte LE length prefix (ValuesReader format). */
  private byte[] encodedWithLengthPrefix;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    boolean[] data = RleEncodingBenchmark.generateData(dataPattern);

    // Encode using the scalar ValuesWriter path
    RunLengthBitPackingHybridValuesWriter w =
        new RunLengthBitPackingHybridValuesWriter(1, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (boolean v : data) {
      w.writeBoolean(v);
    }
    encodedWithLengthPrefix = w.getBytes().toByteArray();
    w.close();
  }

  // ---- Scalar decode via ValuesReader ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBoolean(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridValuesReader r = new RunLengthBitPackingHybridValuesReader(1);
    r.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(encodedWithLengthPrefix)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(r.readBoolean());
    }
  }
}
