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
package org.apache.parquet.column.statistics.bloomfilter;

import org.apache.parquet.Preconditions;

/**
 * This class builds bloom filter options which contains the num of bits and number of hash functions by using the expected entries and the value of false positive probability.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Bloom_filter">https://en.wikipedia.org/wiki/Bloom_filter</a>
 */
public class BloomFilterOptBuilder {
  private double falsePositiveProbability = BloomFilter.DEFAULT_FALSE_POSITIVE_PROBABILITY;
  private long expectedEntries;
  private boolean enable = false;

  /**
   * @param enable whether to enable the bloom filter statistics
   * @return
   */
  public BloomFilterOptBuilder enable(boolean enable) {
    this.enable = enable;
    return this;
  }

  /**
   * @param expectedEntries the number of inserted elements
   * @return
   */
  public BloomFilterOptBuilder expectedEntries(long expectedEntries) {
    this.expectedEntries = expectedEntries;
    return this;
  }

  /**
   * @param falsePositiveProbability false positive probability
   * @return
   */
  public BloomFilterOptBuilder falsePositiveProbability(double falsePositiveProbability) {
    this.falsePositiveProbability = falsePositiveProbability;
    return this;
  }

  static int optimalNumOfHashFunctions(long n, long m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static int optimalNumOfBits(long n, double p) {
    return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  /**
   * It will build the options required by bloom filter with value of false positive probability and the number of inserted elements.
   *
   * @return
   */
  public BloomFilterOpts build() {
    if (enable) {
      Preconditions.checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
      Preconditions.checkArgument(falsePositiveProbability > 0.0 && falsePositiveProbability < 1.0,
          "False positive probability should be > 0.0 & < 1.0");
      int nb = optimalNumOfBits(expectedEntries, falsePositiveProbability);
      // make 'm' multiple of 64
      int numBits = nb + (Long.SIZE - (nb % Long.SIZE));
      int numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
      return new BloomFilterOpts(numBits, numHashFunctions);
    } else {
      return new BloomFilterOpts();
    }
  }
}
