/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.delta.benchmark;

import org.junit.Test;
import parquet.bytes.DirectByteBufferAllocator;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import java.util.Random;

public class BenchmarkIntegerOutputSize {
  public static int blockSize=128;
  public static int miniBlockNum=4;
  public static int dataSize=10000 * blockSize;

  private interface IntFunc {
    public int getIntValue();
  }

  @Test
  public void testBigNumbers() {
    final Random r=new Random();
    testRandomIntegers(new IntFunc() {
      @Override
      public int getIntValue() {
        return r.nextInt();
      }
    },32);
  }

  @Test
  public void testRangedNumbersWithSmallVariations() {
    final Random r=new Random();
    testRandomIntegers(new IntFunc() {
      @Override
      public int getIntValue() {
        return 1000+r.nextInt(20);
      }
    },10);
  }

  @Test
  public void testSmallNumbersWithSmallVariations() {
    final Random r=new Random();
    testRandomIntegers(new IntFunc() {
      @Override
      public int getIntValue() {
        return 40+r.nextInt(20);
      }
    },6);
  }

  @Test
  public void testSmallNumberVariation() {
      final Random r=new Random();
      testRandomIntegers(new IntFunc() {
        @Override
        public int getIntValue() {
          return r.nextInt(20)-10;
        }
      },4);
  }

  public void testRandomIntegers(IntFunc func,int bitWidth) {
    DeltaBinaryPackingValuesWriter delta=new DeltaBinaryPackingValuesWriter(blockSize,miniBlockNum,100, new DirectByteBufferAllocator());
    RunLengthBitPackingHybridValuesWriter rle= new RunLengthBitPackingHybridValuesWriter(bitWidth,100, new DirectByteBufferAllocator());
    for (int i = 0; i < dataSize; i++) {
      int v = func.getIntValue();
      delta.writeInteger(v);
      rle.writeInteger(v);
    }
    System.out.println("delta size: "+delta.getBytes().size());
    System.out.println("estimated size"+estimatedSize());
    System.out.println("rle size: "+rle.getBytes().size());
  }

  private double estimatedSize(){
    int miniBlockSize = blockSize / miniBlockNum;
    double miniBlockFlushed = Math.ceil(((double) dataSize - 1) / miniBlockSize);
    double blockFlushed = Math.ceil(((double) dataSize - 1) / blockSize);
    double estimatedSize = 4 * 5 //blockHeader
            + 4 * miniBlockFlushed * miniBlockSize //data(aligned to miniBlock)
            + blockFlushed * miniBlockNum //bitWidth of mini blocks
            + (5.0 * blockFlushed);//min delta for each block
    return estimatedSize;
  }
}
