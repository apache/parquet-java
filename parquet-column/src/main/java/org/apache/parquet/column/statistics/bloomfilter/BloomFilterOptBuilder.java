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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class builds bloom filter options which contains the num of bits and number of hash functions by using the expected entries and the value of false positive probability.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Bloom_filter">https://en.wikipedia.org/wiki/Bloom_filter</a>
 */
public class BloomFilterOptBuilder {
  private String falsePositiveProbabilities = "";
  private String expectedEntries = "";
  private String colNames = "";

  /**
   * @param colNames column names to enable bloom filter
   * @return
   */
  public BloomFilterOptBuilder enableCols(String colNames) {
    this.colNames = colNames;
    return this;
  }

  /**
   * @param expectedEntries the number of inserted elements
   * @return
   */
  public BloomFilterOptBuilder expectedEntries(String expectedEntries) {
    this.expectedEntries = expectedEntries;
    return this;
  }

  /**
   * @param falsePositiveProbabilities false positive probability
   * @return
   */
  public BloomFilterOptBuilder falsePositiveProbabilities(String falsePositiveProbabilities) {
    this.falsePositiveProbabilities = falsePositiveProbabilities;
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
  public BloomFilterOpts build(MessageType messageType) {
    if (colNames.isEmpty()) {
      return null;
    }
    String cols[] = colNames.split(",");
    double fpps[] = new double[cols.length];
    if (falsePositiveProbabilities.isEmpty()) {
      Arrays.fill(fpps, BloomFilter.DEFAULT_FALSE_POSITIVE_PROBABILITY);
    } else {
      String[] fs = falsePositiveProbabilities.split(",");
      Preconditions.checkArgument(fs.length == cols.length,
          "the number of column name should be the same as fpp values");
      for (int i = 0; i < fpps.length; i++) {
        fpps[i] = Double.valueOf(fs[i]);
      }
    }

    // expected entries
    String [] entries = expectedEntries.split(",");
    Preconditions.checkArgument(entries.length == cols.length,
        "Col number should be the same as cols");

    Map<ColumnDescriptor, BloomFilterOpts.BloomFilterEntry> columnDescriptorMap =
        new HashMap<ColumnDescriptor, BloomFilterOpts.BloomFilterEntry>();
    for(int i = 0; i < cols.length;i++){
      ColumnDescriptor columnDescriptor =
          messageType.getColumnDescription(new String[] { cols[i] });
      long expectedNum = Long.valueOf(entries[i]);
      Preconditions.checkArgument(expectedNum > 0, "expectedEntries should be > 0");
      Preconditions
          .checkArgument(fpps[i] > 0.0 && fpps[i] < 1.0,
              "False positive probability should be > 0.0 & < 1.0");
      int nb = optimalNumOfBits(expectedNum, fpps[i]);
      // make 'm' multiple of 64
      int numBits = nb + (Long.SIZE - (nb % Long.SIZE));
      int numHashFunctions = optimalNumOfHashFunctions(expectedNum, numBits);
      columnDescriptorMap
          .put(columnDescriptor, new BloomFilterOpts.BloomFilterEntry(numBits, numHashFunctions));
    }
    return new BloomFilterOpts(columnDescriptorMap);
  }
}
