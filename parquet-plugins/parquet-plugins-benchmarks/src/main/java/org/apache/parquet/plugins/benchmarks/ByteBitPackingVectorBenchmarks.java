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
package org.apache.parquet.plugins.benchmarks;

import java.util.concurrent.TimeUnit;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * This class uses the java17 vector API, add VM options --add-modules=jdk.incubator.vector
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 1, batchSize = 100000)
@Measurement(iterations = 1, batchSize = 100000)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(jvmArgsPrepend = {"--add-modules=jdk.incubator.vector"})
public class ByteBitPackingVectorBenchmarks {
  /**
   * The range of bitWidth is 1 ~ 32, change it directly if test other bitWidth.
   */
  @Param({
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
  })
  private int bitWidth;

  private int outputValues = 2048;

  private byte[] input;
  private int[] output;
  private int[] outputVector;
  private int totalBytesCount;
  private int outCountPerVector;
  private int totalByteCountVector;
  private int inputByteCountPerVector;

  private BytePacker bytePacker;
  private BytePacker bytePackerVector;

  @Setup(Level.Trial)
  public void getInputBytes() {
    input = new byte[outputValues * bitWidth / 8];
    output = new int[outputValues];
    outputVector = new int[outputValues];
    totalBytesCount = input.length;

    bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    bytePackerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);

    outCountPerVector = bytePackerVector.getUnpackCount();
    inputByteCountPerVector = outCountPerVector / 8 * bitWidth;
    totalByteCountVector = totalBytesCount - inputByteCountPerVector;

    for (int i = 0; i < input.length; i++) {
      input[i] = (byte) i;
    }
  }

  @Benchmark
  public void testUnpack() {
    for (int i = 0, j = 0; i < input.length; i += bitWidth, j += 8) {
      bytePacker.unpack8Values(input, i, output, j);
    }
  }

  @Benchmark
  public void testUnpackVector() {
    int byteIndex = 0;
    int valueIndex = 0;
    for (;
        byteIndex < totalByteCountVector;
        byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
      bytePackerVector.unpackValuesUsingVector(input, byteIndex, outputVector, valueIndex);
    }

    // tail bytes processed
    for (; byteIndex < totalBytesCount; byteIndex += bitWidth, valueIndex += 8) {
      bytePacker.unpack8Values(input, byteIndex, outputVector, valueIndex);
    }
  }
}
