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
package org.apache.parquet.column.statistics.bloomFilter;

public class BloomFilterOptBuilder {
  private long expectedEntries = -1;
  private double fpp = BloomFilter.DEFAULT_FPP;
  private boolean enable = false;

  public BloomFilterOptBuilder enable(boolean enable){
    this.enable = enable;
    return this;
  }

  public BloomFilterOptBuilder expectedEntries(long expectedEntries) {
    this.expectedEntries = expectedEntries;
    return this;
  }

  public BloomFilterOptBuilder fpp(double fpp) {
    this.fpp = fpp;
    return this;
  }

  private void checkArgument(boolean expression, String errorMessage) {
    if (!expression) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  static int optimalNumOfHashFunctions(long n, long m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static int optimalNumOfBits(long n, double p) {
    return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  public BloomFilterOpts build() {
    if (enable) {
      checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
      checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");
      int nb = optimalNumOfBits(expectedEntries, fpp);
      // make 'm' multiple of 64
      int numBits = nb + (Long.SIZE - (nb % Long.SIZE));
      int numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
      return new BloomFilterOpts(numBits, numHashFunctions);
    } else {
      return new BloomFilterOpts();
    }
  }
}
