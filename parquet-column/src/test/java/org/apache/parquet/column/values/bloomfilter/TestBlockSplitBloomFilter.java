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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
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
    assertEquals(bloomFilter1.getBitsetSize(), BlockSplitBloomFilter.LOWER_BOUND_BYTES);
    BloomFilter bloomFilter2 = new BlockSplitBloomFilter(BlockSplitBloomFilter.UPPER_BOUND_BYTES + 1);
    assertEquals(bloomFilter2.getBitsetSize(), BlockSplitBloomFilter.UPPER_BOUND_BYTES);
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
  public void testBloomFilterBasicReadWrite() throws IOException {
    final String[] testStrings = {"hello", "parquet", "bloom", "filter"};
    BloomFilter bloomFilter = new BlockSplitBloomFilter(1024);

    for (String string : testStrings) {
      bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(string)));
    }

    File testFile = temp.newFile();
    FileOutputStream fileOutputStream = new FileOutputStream(testFile);
    bloomFilter.writeTo(fileOutputStream);
    fileOutputStream.close();
    FileInputStream fileInputStream = new FileInputStream(testFile);

    byte[] bitset = new byte[1024];
    int bytes = fileInputStream.read(bitset);
    assertEquals(bytes, 1024);
    bloomFilter = new BlockSplitBloomFilter(bitset);
    for (String testString : testStrings) {
      assertTrue(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(testString))));
    }
  }

  @Test
  public void testBloomFilterFPPAccuracy() {
    final int totalCount = 100000;
    final double FPP = 0.01;

    BloomFilter bloomFilter = new BlockSplitBloomFilter(BlockSplitBloomFilter.optimalNumOfBits(totalCount, FPP) / 8);

    Set<String> distinctStrings = new HashSet<>();
    while (distinctStrings.size() < totalCount) {
      String str = RandomStringUtils.randomAlphabetic(12);
      if (distinctStrings.add(str)) {
        bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(str)));
      }
    }

    distinctStrings.clear();
    // The exist counts the number of times FindHash returns true.
    int exist = 0;
    while(distinctStrings.size() < totalCount) {
      String str = RandomStringUtils.randomAlphabetic(10);
      if (distinctStrings.add(str) && bloomFilter.findHash(bloomFilter.hash(Binary.fromString(str)))) {
        exist ++;
      }
    }

    // The exist should be probably less than 1000 according FPP 0.01. Add 10% here for error space.
    assertTrue(exist < totalCount * (FPP * 1.1));
  }

  @Test
  public void testBloomFilterNDVs(){
    // a row group of 128M with one column of long type.
    int ndv = 128 * 1024 * 1024 / 8;
    double fpp = 0.01;

    // the optimal value formula
    double numBits = -8 * ndv / Math.log(1 - Math.pow(0.01, 1.0 / 8));
    int bytes = (int)numBits / 8;
    assertTrue(bytes < BlockSplitBloomFilter.UPPER_BOUND_BYTES);

    // a row group of 128MB with one column of UUID type
    ndv = 128 * 1024 * 1024 / java.util.UUID.randomUUID().toString().length();
    numBits = -8 * ndv / Math.log(1 - Math.pow(fpp, 1.0 / 8));
    bytes = (int)numBits / 8;
    assertTrue(bytes < 5 * 1024 * 1024);
  }

}
