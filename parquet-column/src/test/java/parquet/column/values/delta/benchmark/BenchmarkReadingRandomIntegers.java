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
package parquet.column.values.delta.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import java.io.IOException;
import java.util.Random;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-encoding-reading-random")
public class BenchmarkReadingRandomIntegers {
  public static int blockSize = 128;
  public static int miniBlockNum = 4;
  public static byte[] deltaBytes;
  public static byte[] rleBytes;
  public static int[] data;
  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  @BeforeClass
  public static void prepare() throws IOException {
    Random random = new Random();
    data = new int[100000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(100) - 200;
    }

    ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100, 20000);
    ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100, 20000);

    for (int i = 0; i < data.length; i++) {
      delta.writeInteger(data[i]);
      rle.writeInteger(data[i]);
    }
    deltaBytes = delta.getBytes().toByteArray();
    rleBytes = rle.getBytes().toByteArray();
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void readingDelta() throws IOException {
    for (int j = 0; j < 10; j++) {

      DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
      readData(reader, deltaBytes);
    }
  }

  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
  @Test
  public void readingRLE() throws IOException {
    for (int j = 0; j < 10; j++) {

      ValuesReader reader = new RunLengthBitPackingHybridValuesReader(32);
      readData(reader, rleBytes);
    }
  }

  private void readData(ValuesReader reader, byte[] deltaBytes) throws IOException {
    reader.initFromPage(data.length, deltaBytes, 0);
    for (int i = 0; i < data.length; i++) {
      reader.readInteger();
    }
  }

}

