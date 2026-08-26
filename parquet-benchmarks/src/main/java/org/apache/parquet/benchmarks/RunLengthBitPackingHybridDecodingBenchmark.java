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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class RunLengthBitPackingHybridDecodingBenchmark {

  private static final int VALUE_COUNT = 100_000;

  @Param({"1", "8", "16"})
  public int bitWidth;

  private byte[] encoded;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(bitWidth, 64 * 1024, 4 * 1024 * 1024,
            new DirectByteBufferAllocator());
    int mask = (1 << bitWidth) - 1;
    for (int i = 0; i < VALUE_COUNT; i += 32) {
      for (int j = 0; j < 16 && i + j < VALUE_COUNT; j++) {
        encoder.writeInt((i + j) & mask);
      }
      int repeated = (i / 32) & mask;
      for (int j = 16; j < 32 && i + j < VALUE_COUNT; j++) {
        encoder.writeInt(repeated);
      }
    }
    encoded = encoder.toBytes().toByteArray();
    encoder.close();
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decode(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridDecoder decoder =
        new RunLengthBitPackingHybridDecoder(bitWidth, new ByteArrayInputStream(encoded));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(decoder.readInt());
    }
  }
}
