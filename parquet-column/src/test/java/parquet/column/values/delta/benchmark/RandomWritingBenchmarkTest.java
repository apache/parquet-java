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
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import java.util.Random;

@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-encoding-writing-random")
public class RandomWritingBenchmarkTest extends BenchMarkTest{
  public static int blockSize=128;
  public static int miniBlockNum=4;
  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  @BeforeClass
  public static void prepare() {
    Random random=new Random();
    data = new int[10000 * blockSize];
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt(100) - 200;
    }
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 2)
  @Test
  public void writeDeltaPackingTest(){
    DeltaBinaryPackingValuesWriter writer = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100, 20000);
    runWriteTest(writer);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 2)
  @Test
  public void writeRLETest(){
    ValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(32, 100, 20000);
    runWriteTest(writer);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 2)
  @Test
  public void writeDeltaPackingTest2(){
    DeltaBinaryPackingValuesWriter writer = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100, 20000);
    runWriteTest(writer);
  }
}
