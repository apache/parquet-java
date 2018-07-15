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

package org.apache.parquet.column.values.bloom;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.parquet.column.values.RandomStr;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestBloomFilter {

  @Test
  public void testConstructor () throws IOException {
    BloomFilter bloomFilter1 = new BloomFilter(0);
    assertEquals(bloomFilter1.getBitsetSize(), BloomFilter.MINIMUM_BLOOM_FILTER_BYTES);

    BloomFilter bloomFilter2 = new BloomFilter(256 * 1024 * 1024);
    assertEquals(bloomFilter2.getBitsetSize(), BloomFilter.MAXIMUM_BLOOM_FILTER_BYTES);

    BloomFilter bloomFilter3 = new BloomFilter(1000);
    assertEquals(bloomFilter3.getBitsetSize(), 1024);
  }

  /*
   * This test is used to test basic operations including inserting, finding and
   * serializing and de-serializing.
   */
  @Test
  public void testBasic () throws IOException {
    BloomFilter bloomFilter = new BloomFilter(512);

    for(int i = 0; i < 10; i++) {
      bloomFilter.insert(bloomFilter.hash(i));
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream((int) bloomFilter.getBitsetSize() +
      BloomFilter.HEADER_SIZE);
    bloomFilter.writeTo(baos);

    ByteBuffer bloomBuffer = ByteBuffer.wrap(baos.toByteArray());

    int length = Integer.reverseBytes(bloomBuffer.getInt());
    assertEquals(length, 512);

    int hash = Integer.reverseBytes(bloomBuffer.getInt());
    assertEquals(hash, BloomFilter.HashStrategy.MURMUR3_X64_128.ordinal());

    int algorithm = Integer.reverseBytes(bloomBuffer.getInt());
    assertEquals(algorithm, BloomFilter.Algorithm.BLOCK.ordinal());

    byte[] bitset = new byte[length];
    bloomBuffer.get(bitset);

    bloomFilter = new BloomFilter(bitset);

    for(int i = 0; i < 10; i++) {
      assertTrue(bloomFilter.find(bloomFilter.hash(i)));
    }
  }

  @Test
  public void testFPP() throws IOException {
    final int totalCount = 100000;
    final double FPP = 0.01;
    final long SEED = 104729;

    BloomFilter bloomFilter = new BloomFilter(BloomFilter.optimalNumOfBits(totalCount, FPP));
    List<String> strings = new ArrayList<>();
    RandomStr randomStr = new RandomStr(new Random(SEED));
    for(int i = 0; i < totalCount; i++) {
      String str = randomStr.get(10);
      strings.add(str);
      bloomFilter.insert(bloomFilter.hash(Binary.fromString(str)));
    }

    // The exist is a counter which is increased by one when find return true.
    int exist = 0;
    for (int i = 0; i < totalCount; i++) {
      String str = randomStr.get(8);
      if (bloomFilter.find(bloomFilter.hash(Binary.fromString(str)))) {
        exist ++;
      }
    }

    // The exist should be probably less than 1000 according FPP 0.01.
    assertTrue(exist < totalCount * FPP);
  }
}
