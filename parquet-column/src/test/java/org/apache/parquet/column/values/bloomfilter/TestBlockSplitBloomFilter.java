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
package org.apache.parquet.column.values.bloomfilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.parquet.column.values.RandomStr;
import org.apache.parquet.io.api.Binary;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlockSplitBloomFilter {
  @Test
  public void testConstructor () {
    BloomFilter bloomFilter1 = new BlockSplitBloomFilter(0);
    assertEquals(bloomFilter1.getBitsetSize(), BlockSplitBloomFilter.DEFAULT_MINIMUM_BYTES);
    BloomFilter bloomFilter2 = new BlockSplitBloomFilter(BlockSplitBloomFilter.DEFAULT_MAXIMUM_BYTES + 1);
    assertEquals(bloomFilter2.getBitsetSize(), BlockSplitBloomFilter.DEFAULT_MAXIMUM_BYTES);
    BloomFilter bloomFilter3 = new BlockSplitBloomFilter(1000);
    assertEquals(bloomFilter3.getBitsetSize(), 1024);
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  /*
   * This test is used to test basic operations including inserting, finding and
   * serializing and de-serializing.
   */
  @Test
  public void testBasic () throws IOException {
    final String[] testStrings = {"hello", "parquet", "bloom", "filter"};
    BloomFilter bloomFilter = new BlockSplitBloomFilter(1024);

    for(int i = 0; i < testStrings.length; i++) {
      bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(testStrings[i])));
    }

    File testFile = temp.newFile();
    FileOutputStream fileOutputStream = new FileOutputStream(testFile);
    bloomFilter.writeTo(fileOutputStream);
    fileOutputStream.close();
    FileInputStream fileInputStream = new FileInputStream(testFile);

    byte[] value = new byte[4];
    fileInputStream.read(value);
    int length = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    assertEquals(length, 1024);

    fileInputStream.read(value);
    int hash = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    assertEquals(hash, BloomFilter.HashStrategy.XXH64.ordinal());

    fileInputStream.read(value);
    int algorithm = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    assertEquals(algorithm, BloomFilter.Algorithm.BLOCK.ordinal());

    byte[] bitset = new byte[length];
    fileInputStream.read(bitset);
    bloomFilter = new BlockSplitBloomFilter(bitset);
    for(int i = 0; i < testStrings.length; i++) {
      assertTrue(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(testStrings[i]))));
    }
  }

  @Test
  public void testFPP() throws IOException {
    final int totalCount = 100000;
    final double FPP = 0.01;
    final long SEED = 104729;

    BloomFilter bloomFilter = new BlockSplitBloomFilter(BlockSplitBloomFilter.optimalNumOfBits(totalCount, FPP));
    List<String> strings = new ArrayList<>();
    RandomStr randomStr = new RandomStr(new Random(SEED));
    for(int i = 0; i < totalCount; i++) {
      String str = randomStr.get(10);
      strings.add(str);
      bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(str)));
    }

    // The exist counts the number of times FindHash returns true.
    int exist = 0;
    for (int i = 0; i < totalCount; i++) {
      String str = randomStr.get(8);
      if (bloomFilter.findHash(bloomFilter.hash(Binary.fromString(str)))) {
        exist ++;
      }
    }

    // The exist should be probably less than 1000 according FPP 0.01.
    assertTrue(exist < totalCount * FPP);
  }
}
