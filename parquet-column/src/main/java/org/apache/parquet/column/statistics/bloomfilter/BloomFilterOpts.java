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

import org.apache.parquet.column.ColumnDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the otions required for constructing a bloom filter including the number of bits and the number of hash functions.
 */
public class BloomFilterOpts {
  public static class BloomFilterEntry {
    private int numBits;
    private int numHashFunctions;

    /**
     * @param numBits          number of bits
     * @param numHashFunctions num of hash functions
     */
    public BloomFilterEntry(
        int numBits,
        int numHashFunctions) {
      this.numBits = numBits;
      this.numHashFunctions = numHashFunctions;
    }

    public int getNumBits() {
      return numBits;
    }

    public int getNumHashFunctions() {
      return numHashFunctions;
    }
  }

  Map<ColumnDescriptor, BloomFilterEntry> filterEntryList =
      new HashMap<ColumnDescriptor, BloomFilterEntry>();

  public BloomFilterOpts(Map<ColumnDescriptor, BloomFilterEntry> filterEntryList) {
    this.filterEntryList = filterEntryList;
  }

  public Map<ColumnDescriptor, BloomFilterEntry> getFilterEntryList() {
    return filterEntryList;
  }
}